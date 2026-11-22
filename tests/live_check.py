#!/usr/bin/env python3
"""实机链路自测（需 docker compose 已启动；仅标准库）：
  1) 同一份答复的 T2/T3 共用同一钟差：延迟回包期间调钟，该答复不得混用新旧钟差
  2) 乱序：后发请求先收到有效答复后，更早请求的未超时答复不得再校时/刷新窗口
  3) 新轮仍在等待时，旧轮的未超时答复仍可校时
  4) 取时周期保持、下游查询照常应答
运行：python3 tests/live_check.py
"""
import json
import os
import socket
import struct
import sys
import time
import urllib.request

BASE = os.environ.get("BASE", "http://127.0.0.1:8000")
UPSTREAM_UDP = (os.environ.get("UPSTREAM_HOST", "127.0.0.1"),
                int(os.environ.get("UPSTREAM_PORT", "10124")))
RESULTS = []

NTP_EPOCH_OFFSET = 2208988800
PACKET = struct.Struct("!BBBbII4s8s8s8s8s")


def api(path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}  {detail}")


def status():
    return api("/api/status")


def query():
    return api("/api/query", {})


def set_upstream(**kw):
    return api("/api/upstream", kw)


def wait_synced(want=True, timeout=30.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if status()["synchronized"] == want:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def unix_to_ntp(ts):
    ntp = ts + NTP_EPOCH_OFFSET
    sec = int(ntp)
    frac = int((ntp - sec) * (1 << 32)) & 0xFFFFFFFF
    return struct.pack("!II", sec, frac)


def ntp_to_unix(raw):
    sec, frac = struct.unpack("!II", raw)
    if sec == 0 and frac == 0:
        return 0.0
    return (sec - NTP_EPOCH_OFFSET) + frac / (1 << 32)


def raw_query(addr, timeout=4.0):
    """直接向上游发一次 NTP 请求，返回 T2/T3 与本地发送/接收时刻。"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        t1 = time.time()
        pkt = PACKET.pack((0 << 6) | (4 << 3) | 3, 0, 0, 0, 0, 0,
                          b"\x00" * 4, b"\x00" * 8, b"\x00" * 8,
                          b"\x00" * 8, unix_to_ntp(t1))
        sock.sendto(pkt, addr)
        data, _ = sock.recvfrom(512)
        t4 = time.time()
        (_, _, _, _, _, _, _, _, _, recv_raw, xmit_raw) = PACKET.unpack(data[:48])
        return {"t1": t1, "t4": t4,
                "t2": ntp_to_unix(recv_raw), "t3": ntp_to_unix(xmit_raw)}
    finally:
        sock.close()


def main():
    print("== 0. 复位并等待首次对时 ==")
    set_upstream(offset_seconds=0, delay_ms=0, disconnected=False)
    check("首次对时", wait_synced(True))

    print("== 1. 延迟回包期间调钟：同一份答复不混用新旧钟差 ==")
    set_upstream(offset_seconds=0, delay_ms=800)
    time.sleep(0.3)  # 让新配置生效，且让中继的在途请求也走 800ms
    # 这是“旧钟差(0)”下收到、将在 800ms 后回包的请求
    result = {}

    def do_raw():
        result.update(raw_query(UPSTREAM_UDP))

    import threading
    th = threading.Thread(target=do_raw)
    th.start()
    time.sleep(0.25)  # 请求已被上游收到（T2 已用旧钟差 0 取快照）
    set_upstream(offset_seconds=-20)  # 回包前调钟
    th.join()
    # 立即复位，避免中继长时间缓调
    set_upstream(offset_seconds=0)
    r = result
    skew = r["t2"] - r["t1"]
    span = r["t3"] - r["t2"]
    check("T2 使用调钟前钟差（未混入 -20s）",
          abs(skew) < 2.0, f"T2-T1={skew:+.3f}s")
    check("T3-T2 约等于回包延迟（同一钟差）",
          0.5 < span < 1.5, f"T3-T2={span:.3f}s")
    # 若 T3 混用了新钟差，T3-T2 会变成约 -19.2s；若 T2 混用新钟差，T2-T1 约 -20s
    check("整份答复时间戳自洽", abs(skew) < 2.0 and 0.5 < span < 1.5,
          f"T2-T1={skew:+.3f}s, T3-T2={span:.3f}s")
    set_upstream(delay_ms=0)
    wait_synced(True)
    time.sleep(1.2)  # 等缓调收敛

    print("== 2. 回包乱序：后发先到，旧轮未超时回包不得再校时/刷新窗口 ==")
    s0 = status()
    set_upstream(offset_seconds=0, delay_ms=1200)  # < 中继 1.5s 超时
    time.sleep(1.3)  # 让若干旧轮在途
    set_upstream(delay_ms=0)  # 缩短延迟：新请求立刻回包，旧请求仍在途中
    t_switch = time.time()
    # 高分辨率采样：单次 status 内同时取 stale_round / valid / last_valid_age
    samples = []
    end = t_switch + 3.2
    while time.time() < end:
        s = status()
        samples.append((time.time(), s["stats"]["stale_round"],
                        s["stats"]["valid"], s["last_valid_age"],
                        s["synchronized"]))
        time.sleep(0.02)
    s1 = status()
    stale0 = s0["stats"]["stale_round"]
    sign_idx = [i for i in range(1, len(samples))
                if samples[i][1] > samples[i - 1][1]]
    check("旧轮晚到答复被识别并丢弃",
          s1["stats"]["stale_round"] > stale0,
          f"stale_round {stale0} -> {s1['stats']['stale_round']}")
    # 只采信“无新轮有效答复同时到达”的旧轮到达样本，避免采样竞态误判：
    # 这些样本里 last_valid_age 必须继续增长，不能回落到 0（窗口未被刷新）。
    unambiguous = [i for i in sign_idx
                   if samples[i][2] == samples[i - 1][2]
                   and samples[i][3] is not None
                   and samples[i - 1][3] is not None]
    refreshed = [i for i in unambiguous
                 if samples[i][3] < samples[i - 1][3] - 0.05]
    check("旧轮答复不刷新 5 秒同步窗口",
          len(unambiguous) > 0 and not refreshed,
          f"旧轮到达样本 {len(unambiguous)} 个，年龄均未回落"
          if not refreshed else
          f"发现 {len(refreshed)} 次年龄回落: "
          + ", ".join(f"{samples[i][3]:.2f}s" for i in refreshed))
    # 有效次数只随新轮增长（约 1/s），旧轮到达不得增加 valid
    dur = samples[-1][0] - t_switch
    dv = s1["stats"]["valid"] - s0["stats"]["valid"]
    check("有效校时次数只随新轮增长",
          dv <= int(dur) + 2,
          f"{dur:.1f}s 内 valid +{dv}，stale +{s1['stats']['stale_round'] - stale0}")
    check("乱序期间保持同步", all(s[4] for s in samples),
          f"同步样本 {sum(1 for s in samples if s[4])}/{len(samples)}")

    print("== 3. 新轮等待中，旧轮未超时答复仍可校时 ==")
    set_upstream(delay_ms=1100)
    v0 = status()["stats"]["valid"]
    time.sleep(3.0)
    s = status()
    check("固定延迟下持续收到有效答复",
          s["stats"]["valid"] > v0 and s["synchronized"],
          f"valid {v0} -> {s['stats']['valid']}, age={s['last_valid_age']:.2f}s")
    check("此场景不产生旧轮丢弃", s["stats"]["stale_round"] == s1["stats"]["stale_round"],
          f"stale_round={s['stats']['stale_round']}")
    set_upstream(delay_ms=0)
    wait_synced(True)

    print("== 4. 取时周期与下游应答 ==")
    before = status()["stats"]["sent"]
    time.sleep(2.0)
    after = status()["stats"]["sent"]
    check("取时周期约 1s", 1 <= after - before <= 3,
          f"2s 内发出 {after - before} 次")
    q = query()
    check("下游查询照常应答且已同步",
          q["ok"] and q["synchronized"] and q["li"] == 0 and q["stratum"] == 2,
          f"li={q['li']} stratum={q['stratum']} rtt={q['rtt_ms']}ms")
    check("授时时刻精确到毫秒",
          isinstance(q["server_time_str"], str) and "." in q["server_time_str"],
          q.get("server_time_str", ""))

    print("== 5. 复位 ==")
    set_upstream(offset_seconds=0, delay_ms=0, disconnected=False)
    check("复位后重新同步", wait_synced(True))

    failed = sum(1 for _, ok in RESULTS if not ok)
    print(f"\n==== 实机自测完成: {len(RESULTS) - failed}/{len(RESULTS)} 通过 ====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
