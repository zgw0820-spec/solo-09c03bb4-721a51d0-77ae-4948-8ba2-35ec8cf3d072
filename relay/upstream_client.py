"""中继的上游 NTP 客户端：异步收发，并按“轮次”丢弃乱序晚到的旧答复。

每发一次请求记为一个轮次（单调递增的 round_id）。后发轮次的有效答复一旦被
采用，更早轮次的答复即作废——即便尚未超时，也不能再校时或刷新 5 秒同步
窗口。新轮次尚无有效答复时，旧轮次仍可凭未超时的有效答复校时。
"""
import asyncio
import logging
import time

from common import ntp
from relay.clock import RelayClock

log = logging.getLogger("relay.upstream")


class Stats:
    def __init__(self):
        self.sent = 0         # 发出的上游请求
        self.valid = 0        # 采用并用于校时的有效答复
        self.late = 0         # 超时答复（丢弃，不刷新同步窗口）
        self.stale_round = 0  # 更早轮次的晚到答复（此后不得再校时）
        self.unknown = 0      # 无法匹配任何请求的答复


class UpstreamClient(asyncio.DatagramProtocol):
    """NTP 客户端：周期性向上游发请求；等待答复期间事件循环照常服务下游。"""

    def __init__(self, clock: RelayClock, stats: Stats, upstream_addr,
                 reply_timeout: float = 1.5):
        self.clock = clock
        self.stats = stats
        self.upstream_addr = upstream_addr
        self.reply_timeout = reply_timeout
        self.transport = None
        self.round_id = 0           # 已发出的最后一轮编号
        self.accepted_round = None  # 最近一次用于校时的轮次编号
        # origin 时间戳 -> (轮次, 发送单调时刻, 超时截止, 发送 unix 时刻 T1)
        self.outstanding = {}

    def connection_made(self, transport):
        self.transport = transport

    def send_request(self):
        if self.transport is None:
            return
        self.round_id += 1
        now_mono = time.monotonic()
        t1 = time.time()
        pkt = ntp.build_request(t1)
        self.outstanding[pkt[40:48]] = (
            self.round_id, now_mono, now_mono + self.reply_timeout, t1)
        # 清理早已超时的记录，避免泄漏
        stale = [k for k, v in self.outstanding.items()
                 if v[2] < now_mono - 60]
        for k in stale:
            del self.outstanding[k]
        self.stats.sent += 1
        self.transport.sendto(pkt, self.upstream_addr)

    def datagram_received(self, data, addr):
        now_mono = time.monotonic()
        t4 = time.time()
        if len(data) < 48:
            return
        msg = ntp.parse(data)
        rec = self.outstanding.pop(msg["origin_raw"], None)
        if rec is None:
            self.stats.unknown += 1
            return
        round_id, _send_mono, deadline, t1 = rec
        if now_mono > deadline:
            # 超时答复：丢弃。不得更新时钟，也不得延长 5 秒同步窗口
            self.stats.late += 1
            log.info("丢弃超时答复（轮次 %d，晚到 %.0f ms）",
                     round_id, (now_mono - deadline) * 1000)
            return
        if self.accepted_round is not None and round_id < self.accepted_round:
            # 后发轮次已先收到并采用有效答复：本答复来自更早轮次，作废。
            # 即便尚未超时，也不得校时或刷新 5 秒同步窗口。
            self.stats.stale_round += 1
            log.info("丢弃旧轮次答复（轮次 %d，已采用轮次 %d）",
                     round_id, self.accepted_round)
            return
        # 四点时间戳估算偏差：theta = ((T2-T1)+(T3-T4))/2
        # T2/T3 由上游在同一份答复内按同一钟差生成，T1/T4 为本地时刻。
        theta = ((msg["recv"] - t1) + (msg["xmit"] - t4)) / 2.0
        upstream_now = t4 + theta
        before = self.clock.now(now_mono)
        self.clock.update(upstream_now, now_mono)
        self.accepted_round = round_id
        self.stats.valid += 1
        gap = self.clock.last_gap
        if before is None:
            log.info("首次对时完成，时刻=%.3f", upstream_now)
        elif gap is not None and abs(gap) > 0.05:
            log.info("校准偏差 %.1f ms，走时速度调整为 %.0f%%",
                     gap * 1000, self.clock.status()["rate"] * 100)

    def error_received(self, exc):  # ICMP 不可达等，交由超时机制处理
        log.warning("上游通道异常: %s", exc)
