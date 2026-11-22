#!/usr/bin/env python3
"""离线单元自测（不启动服务，标准库 + asyncio）：
  1) 上游同一份答复的 T2/T3 使用同一钟差快照（延迟期间调钟不影响该答复）
  2) 后发轮次先收到有效答复后，更早轮次的未超时答复不得校时/刷新窗口
  3) 新轮次尚无有效答复时，旧轮次的未超时答复仍可校时
  4) 超时答复仍然丢弃
运行：python3 -I tests/test_unit.py
"""
import asyncio
import os
import sys
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import ntp                                   # noqa: E402
from relay.clock import RelayClock                       # noqa: E402
from relay.upstream_client import Stats, UpstreamClient  # noqa: E402
from upstream import source as usource                   # noqa: E402


class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, data, addr):
        self.sent.append(data)


class TestUpstreamOffsetSnapshot(unittest.IsolatedAsyncioTestCase):
    async def test_delayed_reply_uses_one_offset_snapshot(self):
        """延迟 50ms 回包期间把钟差从 0 调到 -10s，答复的 T2/T3 仍同为 0 钟差。"""
        cfg = usource.Settings(offset_seconds=0.0, delay_ms=50)
        src = usource.Source(cfg)
        transport = FakeTransport()
        src.connection_made(transport)

        fake_now = [1000.0]
        request = ntp.build_request(1000.0)
        with mock.patch.object(usource.time, "time", side_effect=lambda: fake_now[0]):
            src.datagram_received(request, ("127.0.0.1", 1))
            # 回包还在延迟队列里，值班员调钟
            cfg.offset_seconds = -10.0
            fake_now[0] = 1000.05
            await asyncio.sleep(0.08)

        self.assertEqual(len(transport.sent), 1)
        msg = ntp.parse(transport.sent[0])
        self.assertAlmostEqual(msg["recv"], 1000.0, places=6)
        self.assertAlmostEqual(msg["xmit"], 1000.05, places=6)
        # 若混用新旧钟差，T3 会是 990.05，T3-T2 变成 -9.95s
        self.assertAlmostEqual(msg["xmit"] - msg["recv"], 0.05, places=6)

    async def test_new_request_after_change_uses_new_offset(self):
        cfg = usource.Settings(offset_seconds=-10.0, delay_ms=0)
        src = usource.Source(cfg)
        transport = FakeTransport()
        src.connection_made(transport)

        with mock.patch.object(usource.time, "time", return_value=1000.0):
            src.datagram_received(ntp.build_request(1000.0), ("127.0.0.1", 1))

        msg = ntp.parse(transport.sent[0])
        self.assertAlmostEqual(msg["recv"], 990.0, places=6)
        self.assertAlmostEqual(msg["xmit"], 990.0, places=6)


class TestRoundOrdering(unittest.IsolatedAsyncioTestCase):
    def make_client(self, timeout=1.5):
        clock = RelayClock()
        stats = Stats()
        client = UpstreamClient(clock, stats, ("127.0.0.1", 1), timeout)
        client.connection_made(FakeTransport())
        return clock, stats, client

    def reply_for(self, request, recv, xmit):
        return ntp.build_response(ntp.LI_OK, 1, request[40:48], recv, xmit)

    async def test_newer_accepted_then_older_discarded(self):
        clock, stats, client = self.make_client(timeout=5.0)
        with mock.patch.object(clock, "update", wraps=clock.update) as spy:
            client.send_request()
            r1 = client.transport.sent[-1]
            client.send_request()
            r2 = client.transport.sent[-1]
            # 后发轮次先到并被采用
            client.datagram_received(self.reply_for(r2, 100.0, 100.0),
                                     ("127.0.0.1", 1))
            self.assertEqual(client.accepted_round, 2)
            self.assertEqual(stats.valid, 1)
            self.assertTrue(clock.synchronized())
            last_valid = clock.last_valid_mono

            # 先发轮次的答复随后到达（远未超时），不得校时、不得刷新窗口
            client.datagram_received(self.reply_for(r1, 100.1, 100.1),
                                     ("127.0.0.1", 1))
            self.assertEqual(stats.stale_round, 1)
            self.assertEqual(stats.valid, 1)
            self.assertEqual(stats.late, 0)
            self.assertEqual(clock.last_valid_mono, last_valid)
            self.assertEqual(spy.call_count, 1)

    async def test_older_still_valid_while_newer_outstanding(self):
        clock, stats, client = self.make_client(timeout=5.0)
        with mock.patch.object(clock, "update", wraps=clock.update) as spy:
            client.send_request()
            r1 = client.transport.sent[-1]
            client.send_request()  # 新轮已发出，但还没收到有效答复
            # 旧轮先到：仍可校时
            client.datagram_received(self.reply_for(r1, 100.0, 100.0),
                                     ("127.0.0.1", 1))
            self.assertEqual(client.accepted_round, 1)
            self.assertEqual(stats.valid, 1)
            self.assertEqual(stats.stale_round, 0)
            self.assertEqual(spy.call_count, 1)

    async def test_timeout_still_discarded(self):
        clock, stats, client = self.make_client(timeout=0.02)
        with mock.patch.object(clock, "update", wraps=clock.update) as spy:
            client.send_request()
            r1 = client.transport.sent[-1]
            await asyncio.sleep(0.05)
            client.datagram_received(self.reply_for(r1, 100.0, 100.0),
                                     ("127.0.0.1", 1))
            self.assertEqual(stats.late, 1)
            self.assertEqual(stats.valid, 0)
            self.assertEqual(spy.call_count, 0)
            self.assertFalse(clock.synchronized())


if __name__ == "__main__":
    unittest.main(verbosity=2)
