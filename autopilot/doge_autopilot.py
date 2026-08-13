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
MARKET_STATE_FILE = str(HERE / "market_state.json")

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
SYSTEM_PROMPT = """你是“狗哥(doge)”，负责 ETH、闪迪(SNDK)、SK海力士(SKHY)、镁光(MU) 的多周期行情分析。V7 的核心不是追逐 5 分钟涨跌，而是先确定大周期主线，再用小周期找执行位置。

## 周期权重（必须严格遵守）
1. 1D + 4H：决定“大周期主线”和市场阶段，权重最高。
2. 2H + 1H：决定当前主方向是否延续、回调还是转弱。
3. 30m + 15m：只负责判断结构、回踩/反抽、关键位是否成熟。
4. 5m：只负责换手K、收盘确认和精确入场，绝不能因为 5m 一两根K就把大周期从多翻空或从空翻多。

## 大周期稳定规则
- 每次会提供“上一轮大周期状态”。如果 1D/4H 的核心结构没有被明确破坏，macro_bias 必须继承上一轮。
- 若 5m/15m 与 4H/1D 反向，只描述为“短线回调/反弹/对抗”，不要直接改主线。
- macro_bias 只有在 1D/4H 或至少 4H+1H 出现明确结构性失效时才允许 FLIP；必须在正文说明失效依据。
- 默认不做逆大周期试仓。只有 macro_bias=NEUTRAL，或 4H 明确箱体且 2H/1H 同步反转时，才允许 countertrend。
- 重点回答：未来数小时到 1-2 天更应该偏向哪一边、哪里是主线失效位、回调到哪里值得等。

## 狗哥体系
- 收盘确认优先，盘中刺穿不算突破。
- 15m/30m/1H 共振是执行确认，4H/1D 决定方向背景。
- 换手K是入场时机，不是方向来源。
- 点差/空间不足就放弃；不追高、不追低、不扛单。
- 分仓止盈，到目标位先落袋大部分，留小仓博大周期延续。

## 正文格式
输出 4-7 句话，保持口语化，但顺序固定：
1. “大周期主线：……” —— 必须先说 1D/4H 结论。
2. “当前阶段：……” —— 说明趋势延续、回调、反弹、箱体或转折观察。
3. “短线状态：……” —— 15m/5m 只作为执行层，不可盖过主线。
4. “关键位置：……” —— 最重要的主线支撑/压力/失效位。
5. “接下来计划：……” —— 顺大周期优先，告诉用户等待什么位置和条件。
不要机械罗列全部K线，不要编造输入里没有的数值。

正文后另起一行输出机器信号：
[[SIGNAL]]{"action":"WAIT","bias":"NEUTRAL","macro_bias":"NEUTRAL","macro_regime":"RANGE","macro_change":"KEEP","macro_strength":"WEAK","macro_thesis":"","macro_invalidation":"","tactical_bias":"NEUTRAL","entry_mode":"NONE","countertrend":false,"trial_zone":null,"trigger":"","confirm_price":null,"stop":null,"targets":[],"invalidation":"","allow_chase":false,"evidence":{"location":"UNKNOWN","resonance":"UNKNOWN","turnover":"UNKNOWN","room":"UNKNOWN"}}

字段约束：
- action: TRY_LONG / TRY_SHORT / PREPARE_LONG / PREPARE_SHORT / WAIT / NO_TRADE
- bias: 当前执行方向 LONG / SHORT / NEUTRAL
- macro_bias: 大周期主线 LONG / SHORT / NEUTRAL
- macro_regime: TREND_UP / TREND_DOWN / RANGE / TRANSITION
- macro_change: KEEP / STRENGTHEN / WEAKEN / FLIP；没有 4H/1D 级别依据禁止 FLIP
- macro_strength: STRONG / MEDIUM / WEAK
- macro_thesis: 30-100字说明为什么当前大周期主线成立
- macro_invalidation: 主线失效条件，允许是自然语言；没有明确依据就空字符串
- tactical_bias: 1H/30m 当前执行层方向 LONG / SHORT / NEUTRAL
- entry_mode: PULLBACK / BREAKOUT / REBOUND / RANGE_EDGE / NONE
- countertrend: 是否逆大周期；默认 false
- trial_zone 仅在明确给出可尝试区域时填写 [低,高]
- TRY 只代表当前价格已到试仓区；未到用 PREPARE
- stop/targets/confirm_price 只能使用输入中明确可依据的价格，不能自己造
- allow_chase 默认 false
- evidence 无依据就 UNKNOWN
- 机器信号不要解释，不要 Markdown。
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




def load_market_state():
    try:
        with open(MARKET_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def save_market_state(state):
    try:
        with open(MARKET_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"[autopilot] market_state 保存失败: {e}")


def call_deepseek(kline_data, symbol, scope, period, previous_state=None):
    """调用 DeepSeek API 生成分析。"""
    if not DEEPSEEK_KEY:
        log(f"[autopilot] {symbol} DEEPSEEK_API_KEY 未设置，跳过 AI 分析")
        return None

    prev = previous_state or {}
    prev_text = json.dumps(prev, ensure_ascii=False, separators=(",", ":")) if prev else "无（首次建立大周期主线）"
    user_prompt = f"""品种: {symbol}
周期: {period}
数据范围: {scope}

上一轮大周期状态:
{prev_text}

K线数据（按 1D→4H→2H→1H→30m→15m→5m 排列）:
{kline_data}

请按 V7 大周期主导规则分析。先判断 1D/4H 主线是否延续，再看 2H/1H，最后才用 30m/15m/5m 讨论执行。不要因为短线一两根K反向就翻转 macro_bias。正文 4-7 句话，最后输出 [[SIGNAL]] JSON。"""

    req_data = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 1000
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
    """规范化 V7 结构化信号。"""
    if not isinstance(signal, dict):
        signal = {}
    actions = {"TRY_LONG", "TRY_SHORT", "PREPARE_LONG", "PREPARE_SHORT", "WAIT", "NO_TRADE"}
    biases = {"LONG", "SHORT", "NEUTRAL"}
    regimes = {"TREND_UP", "TREND_DOWN", "RANGE", "TRANSITION"}
    changes = {"KEEP", "STRENGTHEN", "WEAKEN", "FLIP"}
    strengths = {"STRONG", "MEDIUM", "WEAK"}
    entry_modes = {"PULLBACK", "BREAKOUT", "REBOUND", "RANGE_EDGE", "NONE"}
    out = {
        "action": signal.get("action") if signal.get("action") in actions else "WAIT",
        "bias": signal.get("bias") if signal.get("bias") in biases else "NEUTRAL",
        "macro_bias": signal.get("macro_bias") if signal.get("macro_bias") in biases else "NEUTRAL",
        "macro_regime": signal.get("macro_regime") if signal.get("macro_regime") in regimes else "TRANSITION",
        "macro_change": signal.get("macro_change") if signal.get("macro_change") in changes else "KEEP",
        "macro_strength": signal.get("macro_strength") if signal.get("macro_strength") in strengths else "WEAK",
        "macro_thesis": str(signal.get("macro_thesis") or "")[:220],
        "macro_invalidation": str(signal.get("macro_invalidation") or "")[:220],
        "tactical_bias": signal.get("tactical_bias") if signal.get("tactical_bias") in biases else "NEUTRAL",
        "entry_mode": signal.get("entry_mode") if signal.get("entry_mode") in entry_modes else "NONE",
        "countertrend": bool(signal.get("countertrend", False)),
        "trial_zone": None,
        "trigger": str(signal.get("trigger") or "")[:220],
        "confirm_price": None,
        "stop": None,
        "targets": [],
        "invalidation": str(signal.get("invalidation") or "")[:220],
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

    # 安全约束：逆大周期信号不能伪装成顺势。
    if out["bias"] in {"LONG", "SHORT"} and out["macro_bias"] in {"LONG", "SHORT"} and out["bias"] != out["macro_bias"]:
        out["countertrend"] = True
    if out["countertrend"] and out["action"].startswith("TRY_"):
        out["action"] = "WAIT"
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


def process_symbol(symbol, previous_state=None):
    """处理单品种：多周期K线 → V7分析 → 结构化信号。"""
    log(f"[autopilot] [{symbol}] 开始...")
    t0 = time.time()
    stdout, stderr = run_klines(symbol)
    if not stdout:
        log(f"[autopilot] [{symbol}] 跳过（K线获取失败）")
        return None
    period, actual = parse_period_actual(stdout)
    scope = parse_scope(stderr)
    raw = call_deepseek(stdout, symbol, scope, period, previous_state)
    if not raw:
        raw = f"大周期主线：{symbol} 本轮 AI 分析失败，沿用上一轮主线并等待下一轮数据。\n[[SIGNAL]]" + json.dumps(normalize_signal(previous_state or {}), ensure_ascii=False)
    analysis, signal = split_analysis_signal(raw)

    # 如果模型无充分依据却想翻转主线，优先继承上一轮，避免 5m 噪声造成来回翻。
    prev_bias = str((previous_state or {}).get("macro_bias") or "NEUTRAL")
    if prev_bias in {"LONG", "SHORT"} and signal["macro_bias"] != prev_bias and signal.get("macro_change") != "FLIP":
        signal["macro_bias"] = prev_bias
        signal["macro_change"] = "KEEP"
    if signal.get("macro_change") == "FLIP" and not signal.get("macro_invalidation"):
        signal["macro_bias"] = prev_bias if prev_bias in {"LONG", "SHORT"} else signal["macro_bias"]
        signal["macro_change"] = "KEEP"

    elapsed = time.time() - t0
    log(f"[autopilot] [{symbol}] 完成 ({elapsed:.0f}s) 主线={signal.get('macro_bias')} / 执行={signal.get('action')}")
    return {
        "symbol": symbol, "analysis": analysis, "signal": signal, "period": period,
        "actual": actual, "scope": scope, "stdout": stdout,
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

    # 1. 并行处理所有品种；上一轮大周期状态作为稳定锚点
    market_state = load_market_state()
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_symbol, s, market_state.get(s)): s for s in SYMBOLS}
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

    # 保存本轮大周期状态，供下一轮继承。
    for r in results:
        sig = r.get("signal") or {}
        market_state[r["symbol"]] = {
            "macro_bias": sig.get("macro_bias", "NEUTRAL"),
            "macro_regime": sig.get("macro_regime", "TRANSITION"),
            "macro_change": sig.get("macro_change", "KEEP"),
            "macro_strength": sig.get("macro_strength", "WEAK"),
            "macro_thesis": sig.get("macro_thesis", ""),
            "macro_invalidation": sig.get("macro_invalidation", ""),
            "tactical_bias": sig.get("tactical_bias", "NEUTRAL"),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    save_market_state(market_state)

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
