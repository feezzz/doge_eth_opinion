"""Binance 合约 K 线取数 — 多周期并行拉取
用法: python doge_klines.py --align --symbol ETHUSDT
可选环境变量: HTTPS_PROXY (如 http://127.0.0.1:7892)
"""
import json, socket, sys, time, urllib.request, os

# fapi.binance.com 解析出 IPv6 时走代理会握手失败，强制只解析 IPv4
_orig_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda *a, **kw: [r for r in _orig_getaddrinfo(*a, **kw) if r[0] == socket.AF_INET]

sys.stdout.reconfigure(encoding="utf-8")

# 代理（可选）
proxy_url = os.environ.get("HTTPS_PROXY", "")
if proxy_url:
    proxy = urllib.request.ProxyHandler({"https": proxy_url, "http": proxy_url.replace("https://", "http://")})
    opener = urllib.request.build_opener(proxy)
else:
    opener = urllib.request.build_opener()

def get(url):
    return json.loads(opener.open(url, timeout=15).read())

PERIODS = [("5m", 8), ("15m", 6), ("30m", 4), ("1h", 4), ("4h", 4)]

def fetch_all(now, symbol="ETHUSDT"):
    """拉取所有周期K线，返回 (输出行列表, scope列表, 5m最后收线label)。"""
    lines = []
    scopes = []
    last_5m_closed = None
    for interval, limit in PERIODS:
        sec = int(interval[:-1]) * 60 if interval.endswith("m") else int(interval[:-1]) * 3600
        klines = get("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d" % (symbol, interval, limit))
        lines.append("[%s]" % interval)
        latest_closed = None
        for k in klines:
            o = k[0] / 1000
            closed = (o + sec) <= now
            label = time.strftime("%H:%M", time.localtime(o))
            lines.append("  %s [%s] O=%-8s H=%-8s L=%-8s C=%-8s V=%s" % (
                label,
                "收" if closed else "形",
                k[1], k[2], k[3], k[4], round(float(k[5]))))
            if closed:
                latest_closed = label
        if latest_closed:
            scopes.append("%s=%s" % (interval, latest_closed))
        if interval == "5m":
            last_5m_closed = latest_closed
    return lines, scopes, last_5m_closed

def main():
    align = "--align" in sys.argv
    symbol = "ETHUSDT"
    for i, arg in enumerate(sys.argv):
        if arg == "--symbol" and i + 1 < len(sys.argv):
            symbol = sys.argv[i + 1].upper()
    period_str = ""
    expected_5m = ""

    if align:
        grid = round(time.time() / 300) * 300  # nearest 5-min boundary
        wait = grid + 10 - time.time()
        if wait > 0:
            time.sleep(wait)
        period_str = time.strftime("%H:%M", time.localtime(grid))
        expected_5m = time.strftime("%H:%M", time.localtime(grid - 300))

    # 拉取数据，如果5m收线没更新就重试
    max_retries = 6
    for attempt in range(max_retries):
        now = get("https://fapi.binance.com/fapi/v1/time")["serverTime"] / 1000
        lines, scopes, last_5m = fetch_all(now, symbol)

        if not expected_5m or last_5m == expected_5m:
            break

        if attempt < max_retries - 1:
            time.sleep(2)
            continue

    # 输出
    print("品种: %s" % symbol)
    print("实际时间(北京): %s" % time.strftime("%H:%M:%S", time.localtime(now)))
    if period_str:
        print("周期: %s" % period_str)
    for line in lines:
        print(line)
    print("SCOPE: " + symbol + " " + " ".join(scopes), file=sys.stderr)

main()
