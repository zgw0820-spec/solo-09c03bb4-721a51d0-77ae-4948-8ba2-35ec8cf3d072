"""本地 NTP 时间源（上游）：按真实系统时间授时，支持网页下发的
钟差、回包延迟和断开（丢包）模拟。UDP 授时 + FastAPI 控制面同进程。"""
import asyncio
import logging
import os
import time

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from upstream.source import Source, settings

log = logging.getLogger("upstream")

UDP_PORT = int(os.getenv("UPSTREAM_UDP_PORT", "10124"))
API_PORT = int(os.getenv("UPSTREAM_API_PORT", "8001"))


class ConfigIn(BaseModel):
    offset_seconds: float | None = None
    delay_ms: float | None = None
    disconnected: bool | None = None


app = FastAPI(title="上游 NTP 时间源控制")


@app.get("/api/config")
async def get_config():
    now = time.time()
    return {
        "offset_seconds": settings.offset_seconds,
        "delay_ms": settings.delay_ms,
        "disconnected": settings.disconnected,
        "system_time": now,
        "effective_time": now + settings.offset_seconds,
    }


@app.post("/api/config")
async def set_config(cfg: ConfigIn):
    if cfg.offset_seconds is not None:
        settings.offset_seconds = cfg.offset_seconds
    if cfg.delay_ms is not None:
        settings.delay_ms = cfg.delay_ms
    if cfg.disconnected is not None:
        settings.disconnected = cfg.disconnected
    log.info("配置变更: offset=%.3fs delay=%.0fms disconnected=%s",
             settings.offset_seconds, settings.delay_ms, settings.disconnected)
    return await get_config()


async def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings.offset_seconds = float(os.getenv("UPSTREAM_OFFSET", "0"))
    settings.delay_ms = float(os.getenv("UPSTREAM_DELAY_MS", "0"))
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(lambda: Source(settings),
                                        local_addr=("0.0.0.0", UDP_PORT))
    log.info("上游 NTP 时间源 UDP 端口 %d，控制面端口 %d", UDP_PORT, API_PORT)
    config = uvicorn.Config(app, host="0.0.0.0", port=API_PORT,
                            log_level="info")
    await uvicorn.Server(config).serve()


if __name__ == "__main__":
    asyncio.run(main())
