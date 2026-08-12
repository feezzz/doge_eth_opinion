"""飞书群机器人推送 — 发送 feishu_msg.txt 到 webhook
用法: python feishu_send.py <消息文件路径>
配置: 环境变量 FEISHU_WEBHOOK_URL 或 autopilot/.env 中设置
"""
import json, sys, os, urllib.request
from pathlib import Path

# 加载 .env（如果存在）
HERE = Path(__file__).resolve().parent
env_file = HERE / ".env"
if env_file.exists():
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() not in os.environ:
                os.environ[k.strip()] = v.strip()

webhook_url = os.environ.get("FEISHU_WEBHOOK_URL", "")

if not webhook_url:
    print("[feishu_send] 错误: FEISHU_WEBHOOK_URL 未设置", file=sys.stderr)
    sys.exit(1)

if len(sys.argv) > 1:
    msg = open(sys.argv[1], encoding="utf-8").read().strip()
else:
    msg = sys.stdin.read().strip()

if not msg:
    print("[feishu_send] 警告: 消息为空，跳过发送", file=sys.stderr)
    sys.exit(0)

data = json.dumps({"msg_type": "text", "content": {"text": msg}}).encode("utf-8")
req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
print(urllib.request.urlopen(req).read().decode())
