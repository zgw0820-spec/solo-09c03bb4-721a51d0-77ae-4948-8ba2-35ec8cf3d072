"""本地 NTP 时间源的服务端实现（UDP 授时 + 钟差/延迟/断开模拟）。

钟差在“收到请求”时取快照：延迟回包期间值班员调钟，这份答复的接收与发送
时间戳仍使用同一个钟差；调钟后新收到的请求才按新钟差处理。
"""
import asyncio
import time

from common import ntp


class Settings:
    def __init__(self, offset_seconds: float = 0.0, delay_ms: float = 0.0,
                 disconnected: bool = False):
        self.offset_seconds = offset_seconds
        self.delay_ms = delay_ms
        self.disconnected = disconnected


settings = Settings()


class Source(asyncio.DatagramProtocol):
    """上游 NTP 服务：收到查询后按配置延迟回包；断开时直接丢包。"""

    def __init__(self, settings_obj: Settings | None = None):
        self.settings = settings_obj if settings_obj is not None else settings
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if len(data) < 48 or (data[0] & 0x7) != ntp.MODE_CLIENT:
            return
        if self.settings.disconnected:
            return  # 模拟断网：丢包
        origin = data[40:48]
        # 同一份答复的 T2/T3 共用此刻的钟差快照
        offset = self.settings.offset_seconds
        recv = time.time() + offset
        delay = self.settings.delay_ms / 1000.0
        if delay > 0:
            asyncio.get_running_loop().call_later(
                delay, self._reply, origin, recv, addr, offset)
        else:
            self._reply(origin, recv, addr, offset)

    def _reply(self, origin, recv, addr, offset):
        if self.settings.disconnected:
            return  # 延迟期间被断开：丢包
        xmit = time.time() + offset
        pkt = ntp.build_response(ntp.LI_OK, 1, origin, recv, xmit,
                                 ref_id=b"GPS0")
        self.transport.sendto(pkt, addr)
