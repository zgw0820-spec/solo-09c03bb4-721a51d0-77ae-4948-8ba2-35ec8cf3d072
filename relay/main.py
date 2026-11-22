"""中继主体：定时经 UDP 向上游取时（异步、不阻塞下游），同时用 NTPv4
给下游授时；并内嵌 FastAPI 值班网页。全部跑在同一个 asyncio 事件循环里。"""
import asyncio
import logging
import os
import socket
import time

import uvicorn

from common import ntp
from relay.clock import RelayClock
from relay.upstream_client import Stats, UpstreamClient
from relay.web import create_app

log = logging.getLogger("relay")

UPSTREAM_HOST = os.getenv("UPSTREAM_HOST", "127.0.0.1")
UPSTREAM_PORT = int(os.getenv("UPSTREAM_PORT", "10124"))
POLL_INTERVAL = float(os.getenv("RELAY_POLL_INTERVAL", "1.0"))   # 取时周期
REPLY_TIMEOUT = float(os.getenv("RELAY_REPLY_TIMEOUT", "1.5"))   # 答复超时
LISTEN_PORT = int(os.getenv("RELAY_UDP_PORT", "10123"))          # 对下游授时端口
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))


class DownstreamServer(asyncio.DatagramProtocol):
    """NTP 服务端：随时回应下游查询；未同步时回包置 LI=3。"""

    def __init__(self, clock: RelayClock):
        self.clock = clock
        self.queries = 0
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if len(data) < 48 or (data[0] & 0x7) != ntp.MODE_CLIENT:
            return
        self.queries += 1
        now_mono = time.monotonic()
        t = self.clock.now(now_mono)
        synced = self.clock.synchronized(now_mono)
        li = ntp.LI_OK if synced else ntp.LI_ALARM
        stratum = 2 if synced else 16
        serve = t if t is not None else 0.0  # 从未对时：时刻置 0，仅标未同步
        pkt = ntp.build_response(li, stratum, data[40:48], serve, serve)
        self.transport.sendto(pkt, addr)


async def poll_loop(client: UpstreamClient):
    while True:
        client.send_request()
        await asyncio.sleep(POLL_INTERVAL)


async def monitor_loop(clock: RelayClock):
    last = None
    while True:
        synced = clock.synchronized()
        if last is not None and synced != last:
            log.warning("同步状态变化 -> %s", "同步" if synced else "未同步")
        last = synced
        await asyncio.sleep(0.5)


async def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    loop = asyncio.get_running_loop()
    clock = RelayClock()
    stats = Stats()

    # 启动时解析一次上游地址（compose 网络内主机名稳定）
    infos = await loop.getaddrinfo(UPSTREAM_HOST, UPSTREAM_PORT,
                                   type=socket.SOCK_DGRAM)
    upstream_addr = infos[0][4]
    log.info("上游 %s:%s -> %s", UPSTREAM_HOST, UPSTREAM_PORT, upstream_addr)

    client = UpstreamClient(clock, stats, upstream_addr, REPLY_TIMEOUT)
    await loop.create_datagram_endpoint(lambda: client,
                                        local_addr=("0.0.0.0", 0))

    server = DownstreamServer(clock)
    await loop.create_datagram_endpoint(lambda: server,
                                        local_addr=("0.0.0.0", LISTEN_PORT))
    log.info("下游授时 UDP 端口 %d；取时周期 %.1fs；答复超时 %.1fs",
             LISTEN_PORT, POLL_INTERVAL, REPLY_TIMEOUT)

    asyncio.create_task(poll_loop(client))
    asyncio.create_task(monitor_loop(clock))

    app = create_app(clock, stats, server, client)
    config = uvicorn.Config(app, host="0.0.0.0", port=WEB_PORT,
                            log_level="info")
    await uvicorn.Server(config).serve()


if __name__ == "__main__":
    asyncio.run(main())
