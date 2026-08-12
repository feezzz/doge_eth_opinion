"""解析 analysis/*.md → 生成 dashboard/data.js"""
import json, os, re, glob
from datetime import datetime

ANALYSIS_DIR = r"D:\code\doge\doge_eth_opinion\analysis"
POSITIONS_FILE = r"C:\Users\rl109\.claude\doge_positions.json"
OUTPUT = os.path.join(os.path.dirname(__file__), "data.js")

def parse_log_file(filepath):
    """解析每日滚动日志文件，返回条目列表。"""
    entries = []
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    pending = {"group": "", "symbol": "ETHUSDT", "summary": "", "body_lines": [],
               "ts": "", "period": "", "data_range": {}}
    body_started = False

    def flush():
        """将 pending 转为 entry 并加入列表。"""
        nonlocal body_started
        body = "".join(pending["body_lines"]).strip()
        # 跳过空 body 或占位符
        if not body or body.startswith("自动分析: 周期"):
            pending["body_lines"] = []
            pending["summary"] = ""
            body_started = False
            return
        ts = pending["ts"] or pending["group"]
        period = pending["period"] or pending["group"]
        entries.append({
            "ts": ts,
            "group": pending["group"],
            "period": period,
            "symbol": pending["symbol"],
            "summary": pending["summary"],
            "body": body,
            "data_range": dict(pending["data_range"]),
            "placeholder": "自动分析" in body[:20],
        })
        pending["body_lines"] = []
        pending["summary"] = ""
        pending["data_range"] = {}
        body_started = False

    is_old_format = True  # 08-07~09: no ## headers
    for i, raw in enumerate(lines):
        line = raw.rstrip("\n").rstrip("\r")

        # 跳过文件标题
        if line.startswith("# ") and ("行情分析" in line or "ETH" in line[:30]):
            continue
        if line.startswith("> ") or line.startswith("- ") and "ETH" not in line:
            continue

        # ## HH:MM 或 ## HH:MM — SYMBOL
        if line.startswith("## "):
            is_old_format = False
            if pending["body_lines"]:
                flush()
            m = re.match(r"## (\d{2}:\d{2})(?: — ([A-Z0-9]+))?", line)
            if m:
                pending["group"] = m.group(1)
                if m.group(2):
                    pending["symbol"] = m.group(2)
            continue

        # **摘要:** ...
        if line.startswith("**摘要:**"):
            pending["summary"] = line.replace("**摘要:**", "").strip()
            continue

        # ### 实际 ... 或 ### SYMBOL 实际 ...
        if line.startswith("### "):
            if pending["body_lines"] and pending["ts"]:
                flush()

            header = line.replace("### ", "").replace("（北京）", "")
            # 新格式: ETHUSDT 实际 09:01:04 | 周期 09:00
            m_new = re.match(r"([A-Z0-9]+) 实际 (\d{2}:\d{2}:\d{2}) \| 周期 (\d{2}:\d{2})", header)
            if m_new:
                pending["symbol"] = m_new.group(1)
                pending["ts"] = m_new.group(2)
                pending["period"] = m_new.group(3)
                body_started = True
                continue
            # 旧格式: 实际 00:06:01 | 周期 00:05
            m_old = re.match(r"实际 (\d{2}:\d{2}:\d{2}) \| 周期 (\d{2}:\d{2})", header)
            if m_old:
                pending["ts"] = m_old.group(1)
                pending["period"] = m_old.group(2)
                body_started = True
                continue
            # 更旧格式: 实际 17:52:05
            m_vold = re.match(r"实际 (\d{2}:\d{2}:\d{2})", header)
            if m_vold:
                pending["ts"] = m_vold.group(1)
                body_started = True
                continue
            continue

        # 数据范围:
        if line.startswith("数据范围:") or line.startswith("数据范围："):
            scope = line.replace("数据范围:", "").replace("数据范围：", "").strip()
            # 去掉开头的 SYMBOL
            scope = re.sub(r"^[A-Z0-9]+USDT\s+", "", scope)
            parts = scope.split()
            for p in parts:
                if "=" in p:
                    k, v = p.split("=", 1)
                    v = v.replace("[收]", "").replace("[形]", "")
                    pending["data_range"][k] = v
            continue

        # 正文
        if body_started and line.strip():
            pending["body_lines"].append(raw)  # 保留原始换行
        elif body_started and not line.strip() and pending["body_lines"]:
            pending["body_lines"].append("\n")

    # EOF flush
    if pending["body_lines"]:
        flush()

    # 去重
    seen = set()
    unique = []
    for e in entries:
        key = (e["ts"], e["symbol"], e["body"][:50])
        if key not in seen:
            seen.add(key)
            unique.append(e)
    return unique


def parse_summary_file(filepath):
    """解析每日总结文档（08-03~05），返回 HTML。"""
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()
    # 简单 markdown → HTML
    html = text
    html = re.sub(r"^# (.+)$", r"<h2>\1</h2>", html, flags=re.MULTILINE)
    html = re.sub(r"^## (.+)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)
    html = re.sub(r"^### (.+)$", r"<h4>\1</h4>", html, flags=re.MULTILINE)
    html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)
    html = re.sub(r"^- (.+)$", r"<li>\1</li>", html, flags=re.MULTILINE)
    html = re.sub(r"(\|.+?\|)", r"<pre>\1</pre>", html)
    html = "<div>" + html.replace("\n\n", "</div><div>") + "</div>"
    return html


def load_positions():
    try:
        with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def main():
    days = {}
    all_symbols = set()

    files = sorted(glob.glob(os.path.join(ANALYSIS_DIR, "2026-*.md")))
    for fp in files:
        date = os.path.splitext(os.path.basename(fp))[0]
        with open(fp, "r", encoding="utf-8") as f:
            head = "".join(f.readline() for _ in range(15))

        if "当日概况" in head or "走势回顾" in head:
            # summary doc
            days[date] = {"type": "summary", "html": parse_summary_file(fp), "symbols": ["ETHUSDT"]}
            all_symbols.add("ETHUSDT")
        else:
            # log file
            entries = parse_log_file(fp)
            symbols = sorted(set(e["symbol"] for e in entries))
            all_symbols.update(symbols)
            days[date] = {"type": "log", "symbols": symbols, "entries": entries}

    positions = load_positions()

    data = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "meta": {
            "total_entries": sum(len(d.get("entries", [])) for d in days.values()),
            "symbols": sorted(all_symbols),
            "dates": sorted(days.keys()),
        },
        "days": days,
        "positions": positions,
    }

    js = "window.APP_DATA = " + json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(js)

    # 更新 index.html 中的 data.js 引用，加入时间戳防止缓存
    index_html = os.path.join(os.path.dirname(__file__), "index.html")
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    with open(index_html, "r", encoding="utf-8") as f:
        html = f.read()
    html = re.sub(r'src="data\.js(\?v=[^"]*)?"', f'src="data.js?v={ts}"', html)
    with open(index_html, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[dashboard] 生成完成: {data['meta']['total_entries']} 条, "
          f"{len(days)} 天, {len(all_symbols)} 品种 → {OUTPUT}")
    print(f"[dashboard] 品种: {', '.join(sorted(all_symbols))}")
    for d in sorted(days.keys()):
        day = days[d]
        if day["type"] == "log":
            print(f"  {d}: {len(day['entries'])} 条 [{', '.join(day['symbols'])}]")
        else:
            print(f"  {d}: 总结文档")


if __name__ == "__main__":
    main()
