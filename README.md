# doge_eth_opinion — 狗哥多品种行情分析

ETH + MU + SNDK + SKHY 永续合约行情分析，基于狗哥交易体系（纯技术流多级别联动）。每 5 分钟自动轮询、DeepSeek AI 分析、GitHub 归档 + 飞书推送。

## 系统架构

```
doge_watchdog.bat / doge_watchdog.py    → 5 分钟栅格守护进程
  └─ doge_autopilot.py                  → 并行取 K 线 + DeepSeek 分析
       └─ doge_klines.py                → Binance 多周期 K 线取数
       └─ feishu_send.py                → 飞书群机器人推送
```

- **数据来源**：Binance 合约 API（ETH/MU/SNDK/SKHY 永续 K 线，北京时间）
- **AI 分析**：DeepSeek API，狗哥体系提示词（口语化、重关键位和预案）
- **推送**：飞书群 webhook + GitHub 自动 commit

## 分析框架

- **观察周期**：5m / 15m / 30m / 1H / 4H
- **核心原则**：
  - 收盘确认：盘中刺穿 ≠ 突破，一切以 K 线收盘确认为准
  - 分仓止盈：到目标位平 50-75%，留小仓博延续
  - 宁可错过不可做错：假突破识别优先，位置不到不动手
  - 数据口径：仅已收盘 K 线用"收盘"表述，形成中标注"形成中"

## 看板

**[狗哥 ETH 行情决策看板](https://feezzz.github.io/doge_eth_opinion/dashboard/)**

- 实时 Binance 价格 + 15m K 线蜡烛图（EMA20）
- 多周期共振矩阵（5m→4H）
- 狗哥条件检查（收盘确认/共振/换手K/点差/仓位）
- 交易预案（突破/延续/破位三种情景）
- 智能决策推断（根据价格在区间位置自动判断）

另有分析日志看板：`dashboard/index.html`（替换为旧版）
旧版路径：`dashboard/v1.html`（待迁移）

## 本地运行

```bash
# 1. 配置密钥
cp autopilot/.env.example autopilot/.env
# 编辑 autopilot/.env，填入 DEEPSEEK_API_KEY 和 FEISHU_WEBHOOK_URL

# 2. 启动 5 分钟轮询
cd autopilot
python doge_watchdog.py          # 前台运行
# 或
doge_watchdog.bat                # Windows 后台循环
```

## 文档结构

```
autopilot/
  doge_autopilot.py   主流程：K线 → DeepSeek → 写文件 → 飞书 → Git
  doge_klines.py      Binance 合约多周期 K 线取数
  doge_watchdog.py    5 分钟栅格调度器
  doge_watchdog.bat   Windows 外层循环包装
  feishu_send.py      飞书群机器人 webhook 推送
  .env.example        密钥配置模板（复制为 .env 填入实际值）
  positions.json      持仓状态
data/
  kline-log.md        实时轮询日志（追加式，约 800KB）
analysis/
  2026-08-03.md       每日分析（初期为总结文档，后期为滚动日志）
  ...
  2026-08-12.md       最新（4 品种并行分析）
dashboard/
  index.html          实时决策看板 V5.1
  build_dashboard.py  数据构建脚本
  data.js             仪表盘数据
README.md
```

## 统计（截至 2026-08-12）

| 指标 | 值 |
|------|-----|
| 分析条目 | 2,100+ |
| 覆盖天数 | 9 天 |
| 品种 | ETHUSDT, MUUSDT, SNDKUSDT, SKHYUSDT |
| 轮询频率 | 5 分钟 |

---

> 免责声明：本仓库仅为个人行情分析记录，不构成任何投资建议。
