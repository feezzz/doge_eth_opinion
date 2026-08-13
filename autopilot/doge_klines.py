"""Binance 合约 K 线取数 — V7 大周期主导版
用法: python doge_klines.py --align --symbol ETHUSDT
周期层级: 1D/4H 定主线，2H/1H 定方向，30m/15m 定结构，5m 只做入场时机。
"""
import json, socket, sys, time, urllib.request, os

_orig_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda *a, **kw: [r for r in _orig_getaddrinfo(*a, **kw) if r[0] == socket.AF_INET]
sys.stdout.reconfigure(encoding="utf-8")

proxy_url = os.environ.get("HTTPS_PROXY", "")
if proxy_url:
    proxy = urllib.request.ProxyHandler({"https": proxy_url, "http": proxy_url.replace("https://", "http://")})
    opener = urllib.request.build_opener(proxy)
else:
    opener = urllib.request.build_opener()

def get(url):
    return json.loads(opener.open(url, timeout=15).read())

# 越大的周期给越长的观察窗口，避免被最近几根短K牵着走。
PERIODS = [
    ("1d", 12),
    ("4h", 18),
    ("2h", 18),
    ("1h", 24),
    ("30m", 18),
    ("15m", 18),
    ("5m", 12),
]

def interval_seconds(interval):
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    if interval.endswith("h"):
        return int(interval[:-1]) * 3600
    if interval.endswith("d"):
        return int(interval[:-1]) * 86400
    raise ValueError(interval)

def label_for(interval, open_ts):
    if interval.endswith("d"):
        return time.strftime("%m-%d", time.localtime(open_ts))
    if interval in {"4h", "2h"}:
        return time.strftime("%m-%d %H:%M", time.localtime(open_ts))
    return time.strftime("%H:%M", time.localtime(open_ts))

def fetch_all(now, symbol="ETHUSDT"):
    lines, scopes = [], []
    last_5m_closed = None
    for interval, limit in PERIODS:
        sec = interval_seconds(interval)
        klines = get("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d" % (symbol, interval, limit))
        lines.append("[%s]" % interval)
        latest_closed = None
        for k in klines:
            o = k[0] / 1000
            closed = (o + sec) <= now
            label = label_for(interval, o)
            lines.append("  %s [%s] O=%-8s H=%-8s L=%-8s C=%-8s V=%s" % (
                label, "收" if closed else "形", k[1], k[2], k[3], k[4], round(float(k[5]))
            ))
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
        grid = round(time.time() / 300) * 300
        wait = grid + 10 - time.time()
        if wait > 0:
            time.sleep(wait)
        period_str = time.strftime("%H:%M", time.localtime(grid))
        expected_5m = time.strftime("%H:%M", time.localtime(grid - 300))

    max_retries = 6
    for attempt in range(max_retries):
        now = get("https://fapi.binance.com/fapi/v1/time")["serverTime"] / 1000
        lines, scopes, last_5m = fetch_all(now, symbol)
        if not expected_5m or last_5m == expected_5m:
            break
        if attempt < max_retries - 1:
            time.sleep(2)

    print("品种: %s" % symbol)
    print("实际时间(北京): %s" % time.strftime("%H:%M:%S", time.localtime(now)))
    if period_str:
        print("周期: %s" % period_str)
    for line in lines:
        print(line)
    print("SCOPE: " + symbol + " " + " ".join(scopes), file=sys.stderr)

if __name__ == "__main__":
    main()
