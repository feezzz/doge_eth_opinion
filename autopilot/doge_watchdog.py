"""狗哥盯盘 — 每15分钟对齐执行，配合 watchdog.bat 持续运行"""
import subprocess, time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
AUTOPILOT = str(HERE / "doge_autopilot.py")
WORK_DIR = str(HERE)

print(f"[watchdog] start {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

while True:
    now = time.time()
    grid = (now // 900) * 900 + 900
    wait = grid + 10 - time.time()
    if wait > 0:
        time.sleep(wait)

    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[watchdog] exec {ts}", flush=True)
    try:
        subprocess.run(["python", "-X", "utf8", AUTOPILOT],
                       cwd=WORK_DIR, timeout=360)
    except Exception as e:
        print(f"[watchdog] err: {e}", flush=True)
    print(f"[watchdog] done {datetime.now().strftime('%H:%M:%S')}", flush=True)
