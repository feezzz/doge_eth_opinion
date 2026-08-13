"""狗哥多品种全自动行情分析 — 多线程并行
流程：并行取K线 → 并行DeepSeek分析 → 汇总飞书 → Git push每日

前置条件：pip install 无需额外依赖（仅用标准库）
配置方式：复制 .env.example 为 .env，填入 DEEPSEEK_API_KEY 和 FEISHU_WEBHOOK_URL
"""
import subprocess, json, urllib.request, sys, os, re, time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── 路径（基于脚本所在目录） ────────────────────────────
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent

def load_env():
    """从 autopilot/.env 加载环境变量（不覆盖已有的）。"""
    env_file = HERE / ".env"
    if env_file.exists():
        with open(env_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip()
                if k not in os.environ:
                    os.environ[k] = v

load_env()

# ── 配置 ────────────────────────────────────────────
SYMBOLS        = ["ETHUSDT", "MUUSDT", "SNDKUSDT", "SKHYUSDT"]
KLINES_SCRIPT  = str(HERE / "doge_klines.py")
LOG_FILE       = str(HERE / "doge_autopilot.log")
DATA_DIR       = str(PROJECT_ROOT / "data")
ANALYSIS_DIR   = str(PROJECT_ROOT / "analysis")
MSG_PATH       = str(HERE / "feishu_msg.txt")
FEISHU_SEND    = str(HERE / "feishu_send.py")
GIT_REPO       = str(PROJECT_ROOT)
POSITION_FILE  = str(HERE / "positions.json")

DEEPSEEK_KEY   = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_URL   = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")


def log(msg):
    """同时输出到控制台和日志文件。"""
    print(msg, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

# ── 狗哥交易体系系统提示 ──────────────────────────
SYSTEM_PROMPT = """你是"狗哥(doge)"，加密货币交易员，主做ETH日内短线，纯技术流多级别联动分析。

## 分析风格
- 口语化中文，像交易员复盘，不是直播喊单
- 先讲核心变化/节奏，再点盯盘关键位
- 变化大详细展开，没变化简短带过，2-5句话
- 不做机械罗列，只点出现变化的周期
- 禁止编造数据中不存在的数字（如自己算MA值）

## 核心概念
- 级别: 5m/15m/30m/1H/4H
- 三线共振: 15m+30m+1H 同方向
- 四线共振: +4H 同方向
- 对抗行情: 相邻级别方向相反
- 换手K: 红转绿/绿转红的关键K线
- 漂: 本该成形的方向没走成
- MA45: 行情加速带，1H强实体突破即起飞
- MA5: 逃跑线/止盈线
- 量价背离、重心上移/下移、V反、假突破、多空双杀、射击之星、锤子线
- 分仓止盈: 到目标位平50-75%，留小部分博延续

## 纪律
- 最大回撤10%，永不扛单
- 宁可错过不可做错，不追行情
- 盯死关键位，位置不到不动手

## 输出格式
先输出分析正文，2-5句话。不要输出标题（###），不要用代码块。
正文直接以行情描述开头。不输出"兄弟们"等直播开场白。

正文后必须另起一行输出一条机器信号，格式严格为：
[[SIGNAL]]{"action":"WAIT","bias":"NEUTRAL","trial_zone":null,"trigger":"","confirm_price":null,"stop":null,"targets":[],"invalidation":"","allow_chase":false,"evidence":{"location":"UNKNOWN","resonance":"UNKNOWN","turnover":"UNKNOWN","room":"UNKNOWN"}}

字段约束：
- action 只能是 TRY_LONG / TRY_SHORT / PREPARE_LONG / PREPARE_SHORT / WAIT / NO_TRADE
- bias 只能是 LONG / SHORT / NEUTRAL
- trial_zone 只有在正文明确给出可试仓/可接/可空的价格或区间时填写 [低,高]，否则 null
- confirm_price 只有在正文明确给出确认价位时填写，否则 null
- stop、targets 只允许使用正文明确提到且来自输入K线/关键位的数据；没有就 null / []，绝不编造
- allow_chase 默认 false，只有正文明确允许追单才可 true
- evidence 四项没有依据就 UNKNOWN，不要为了填满而猜
- TRY_LONG / TRY_SHORT 只在当前最新价格已经进入或非常接近明确试仓区、且正文允许轻仓尝试时使用；价格尚未到计划区域时必须用 PREPARE_LONG / PREPARE_SHORT
- 如果只是“等确认、等回踩、位置不到”，有明确方向时用 PREPARE_LONG / PREPARE_SHORT，否则 WAIT；不要把所有情况都写成 NO_TRADE
- 机器信号不要解释，不要加 Markdown，不要再输出其他内容。
"""

# ── 函数 ────────────────────────────────────────────

def run_klines(symbol):
    """运行 doge_klines.py --align --symbol SYMBOL，返回 (stdout, stderr)。"""
    proc = subprocess.run(
        ["python", "-X", "utf8", KLINES_SCRIPT, "--align", "--symbol", symbol],
        capture_output=True, text=True, encoding="utf-8", timeout=120
    )
    if proc.returncode != 0:
        log(f"[autopilot] {symbol} klines 失败: {proc.stderr[:200]}")
        return None, None
    return proc.stdout, proc.stderr


def parse_scope(stderr):
    """从 stderr 提取 SCOPE 行。"""
    for line in stderr.strip().split("\n"):
        if line.startswith("SCOPE: "):
            return line.replace("SCOPE: ", "").strip()
    return ""


def parse_period_actual(stdout):
    """从 stdout 提取周期时间和实际时间。"""
    period = ""
    actual = ""
    for line in stdout.strip().split("\n"):
        if line.startswith("实际时间(北京): "):
            actual = line.replace("实际时间(北京): ", "").strip()
        if line.startswith("周期: "):
            period = line.replace("周期: ", "").strip()
    return period, actual


def load_positions():
    """读取持仓文件。"""
    try:
        with open(POSITION_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def call_deepseek(kline_data, symbol, scope, period):
    """调用 DeepSeek API 生成分析。"""
    if not DEEPSEEK_KEY:
        log(f"[autopilot] {symbol} DEEPSEEK_API_KEY 未设置，跳过 AI 分析")
        return None

    user_prompt = f"""品种: {symbol}
周期: {period}
数据范围: {scope}

K线数据:
{kline_data}

请按狗哥风格分析以上K线数据，2-5句话，并按系统要求在最后输出 [[SIGNAL]] JSON。"""

    req_data = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 600
    }

    req = urllib.request.Request(
        DEEPSEEK_URL,
        data=json.dumps(req_data).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DEEPSEEK_KEY}"
        }
    )

    try:
        resp = urllib.request.urlopen(req, timeout=60)
        result = json.loads(resp.read())
        return result["choices"][0]["message"]["content"].strip()
    except Exception as e:
        log(f"[autopilot] {symbol} DeepSeek API 失败: {e}")
        return None


def clean_analysis(analysis):
    """清理分析文本（去标题、去代码块）。"""
    analysis = re.sub(r'^###\s*[^\n]*\n?', '', analysis.strip(), flags=re.MULTILINE)
    return analysis.strip()



def normalize_signal(signal):
    """规范化 DeepSeek 的机器信号；缺失/异常字段安全回退。"""
    if not isinstance(signal, dict):
        signal = {}
    actions = {"TRY_LONG", "TRY_SHORT", "PREPARE_LONG", "PREPARE_SHORT", "WAIT", "NO_TRADE"}
    biases = {"LONG", "SHORT", "NEUTRAL"}
    out = {
        "action": signal.get("action") if signal.get("action") in actions else "WAIT",
        "bias": signal.get("bias") if signal.get("bias") in biases else "NEUTRAL",
        "trial_zone": None,
        "trigger": str(signal.get("trigger") or "")[:180],
        "confirm_price": None,
        "stop": None,
        "targets": [],
        "invalidation": str(signal.get("invalidation") or "")[:180],
        "allow_chase": bool(signal.get("allow_chase", False)),
        "evidence": {},
    }
    zone = signal.get("trial_zone")
    if isinstance(zone, (list, tuple)) and len(zone) == 2:
        try:
            lo, hi = float(zone[0]), float(zone[1])
            if lo > 0 and hi > 0:
                out["trial_zone"] = [min(lo, hi), max(lo, hi)]
        except Exception:
            pass
    for k in ("confirm_price", "stop"):
        try:
            v = signal.get(k)
            if v is not None and float(v) > 0:
                out[k] = float(v)
        except Exception:
            pass
    targets = signal.get("targets")
    if isinstance(targets, list):
        for v in targets[:3]:
            try:
                fv = float(v)
                if fv > 0:
                    out["targets"].append(fv)
            except Exception:
                pass
    allowed_evidence = {"GOOD", "OK", "BAD", "YES", "PARTIAL", "NO", "UNKNOWN"}
    ev = signal.get("evidence") if isinstance(signal.get("evidence"), dict) else {}
    for k in ("location", "resonance", "turnover", "room"):
        v = str(ev.get(k, "UNKNOWN")).upper()
        out["evidence"][k] = v if v in allowed_evidence else "UNKNOWN"
    return out


def split_analysis_signal(raw):
    """从模型输出末尾提取 [[SIGNAL]] JSON，返回(正文, signal)。"""
    raw = (raw or "").strip()
    marker = "[[SIGNAL]]"
    idx = raw.rfind(marker)
    if idx < 0:
        return clean_analysis(raw), normalize_signal({})
    body = clean_analysis(raw[:idx].strip())
    payload = raw[idx + len(marker):].strip()
    payload = re.sub(r'^```(?:json)?\s*|\s*```$', '', payload, flags=re.I | re.S).strip()
    try:
        signal = json.loads(payload)
    except Exception:
        m = re.search(r'\{.*\}', payload, flags=re.S)
        try:
            signal = json.loads(m.group(0)) if m else {}
        except Exception:
            signal = {}
    return body, normalize_signal(signal)


def process_symbol(symbol):
    """处理单个品种：取K线 → 分析，返回结果字典。"""
    log(f"[autopilot] [{symbol}] 开始...")
    t0 = time.time()

    stdout, stderr = run_klines(symbol)
    if not stdout:
        log(f"[autopilot] [{symbol}] 跳过（K线获取失败）")
        return None

    period, actual = parse_period_actual(stdout)
    scope = parse_scope(stderr)

    raw = call_deepseek(stdout, symbol, scope, period)
    analysis, signal = split_analysis_signal(raw)
    if not analysis:
        analysis = f"自动分析: {symbol} 周期 {period}, 详见K线数据。"
        signal = normalize_signal({})

    elapsed = time.time() - t0
    log(f"[autopilot] [{symbol}] 完成 ({elapsed:.0f}s)")

    return {
        "symbol": symbol,
        "analysis": analysis,
        "signal": signal,
        "period": period,
        "actual": actual,
        "scope": scope,
        "stdout": stdout,
    }


def write_daily_files(symbol, analysis, signal, period, actual, scope):
    """写入单个品种的每日日志（data/ + analysis/）。"""
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")

    header = f"### {symbol} 实际 {actual}"
    if period:
        header += f" | 周期 {period}"
    header += "（北京）"
    scope_line = f"\n\n数据范围: {scope}" if scope else ""
    signal_line = "\n\n<!--SIGNAL " + json.dumps(signal or normalize_signal({}), ensure_ascii=False, separators=(",", ":")) + "-->"

    summary = analysis.split("。")[0] + "。" if "。" in analysis else analysis[:80]
    daily_entry = f"\n\n## {now.strftime('%H:%M')} — {symbol}\n\n**摘要:** {summary}\n\n{header}{scope_line}{signal_line}\n\n{analysis}"

    for d in [DATA_DIR, ANALYSIS_DIR]:
        os.makedirs(d, exist_ok=True)
        daily_file = os.path.join(d, f"{today}.md")
        with open(daily_file, "a", encoding="utf-8") as f:
            f.write(daily_entry)


def build_feishu_msg(results, positions):
    """组装汇总飞书消息。"""
    lines = []
    actual = results[0]["actual"] if results else datetime.now().strftime("%H:%M:%S")
    period = results[0]["period"] if results else ""

    header = f"### 狗哥盯盘 实际 {actual}"
    if period:
        header += f" | 周期 {period}"
    header += "（北京）"
    lines.append(header)

    if positions:
        entries = positions.get("entries", [])
        if entries:
            avg = sum(e["price"] for e in entries) / len(entries)
            stop = positions.get("stop_loss", "N/A")
            targets = positions.get("targets", [])
            lines.append(f"\n> 持仓: ETH均价~{avg:.0f}, 止损{stop}, 目标{targets}")

    for r in results:
        symbol = r["symbol"]
        analysis = r["analysis"]
        scope = r.get("scope", "")
        scope_str = f" | {scope}" if scope else ""
        lines.append(f"\n---\n\n**{symbol}**{scope_str}\n\n{analysis}")

    return "\n".join(lines)


def git_push():
    """Git push 每日分析文件。"""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        daily_file = f"analysis/{today}.md"
        subprocess.run(["git", "-C", GIT_REPO, "add", daily_file, "dashboard/data.js", "dashboard/index.html"],
                       capture_output=True, timeout=15)
        subprocess.run(["git", "-C", GIT_REPO, "commit", "-m", f"{today} 狗哥多品种分析"],
                       capture_output=True, timeout=15)
        subprocess.run(["git", "-C", GIT_REPO, "push"],
                       capture_output=True, timeout=30)
        log("[autopilot] Git push 完成")
    except Exception as e:
        log(f"[autopilot] Git 失败: {e}")


def send_feishu():
    """发送飞书 webhook。"""
    try:
        subprocess.run(["python", FEISHU_SEND, MSG_PATH],
                       capture_output=True, timeout=15)
        log("[autopilot] 飞书发送完成")
    except Exception as e:
        log(f"[autopilot] 飞书失败: {e}")


# ── 主流程 ────────────────────────────────────────────

def main():
    if not DEEPSEEK_KEY:
        log("[autopilot] 错误: DEEPSEEK_API_KEY 未设置。请在 autopilot/.env 中配置。")
        return

    log(f"[autopilot] 启动 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ({len(SYMBOLS)}品种)")

    # 1. 并行处理所有品种
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_symbol, s): s for s in SYMBOLS}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    if not results:
        log("[autopilot] 所有品种都失败，退出")
        return

    # 按原始顺序排列
    symbol_order = {s: i for i, s in enumerate(SYMBOLS)}
    results.sort(key=lambda r: symbol_order.get(r["symbol"], 99))

    # 2. 写每日文件（每个品种）
    for r in results:
        write_daily_files(r["symbol"], r["analysis"], r.get("signal"), r["period"], r["actual"], r["scope"])

    # 3. 读持仓 + 组装飞书消息
    positions = load_positions()
    feishu_msg = build_feishu_msg(results, positions)

    with open(MSG_PATH, "w", encoding="utf-8") as f:
        f.write(feishu_msg)

    # 4. 刷新仪表盘数据（先生成，再一起 Git push）
    try:
        proc = subprocess.run(["python", os.path.join(GIT_REPO, "dashboard", "build_dashboard.py")],
                              timeout=30, capture_output=True, text=True, encoding="utf-8")
        if proc.returncode != 0:
            log(f"[autopilot] dashboard 刷新失败: {proc.stderr[:200]}")
    except Exception as e:
        log(f"[autopilot] dashboard 刷新失败: {e}")

    # 5. Git push（分析 + data.js + index.html）
    git_push()

    # 6. 飞书
    send_feishu()

    # 打印分析摘要
    log("[autopilot] === 分析摘要 ===")
    for r in results:
        log(f"  [{r['symbol']}] {r['analysis'][:120]}...")

    log(f"[autopilot] 完成 {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
