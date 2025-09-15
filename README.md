# mystock

脚本：抓取A股资金流与基本信息

- 路径：`scripts/fund_flow.py`
- 功能：
  - 抓取个股资金流（主力、超大单、大单、中单、小单）
  - 获取基本信息（名称、交易所、最新价、市值）
  - 支持自定义日期或日期区间过滤
  - 新增：分钟级实时资金流轮询与SQLite存储

使用示例

- 指定单日：

```
python scripts/fund_flow.py 600519 --date 2025-09-12
```

- 指定日期区间并输出区间内所有天：

```
python scripts/fund_flow.py 600519 --start 2025-09-01 --end 2025-09-12 --all-days
```

- 多只股票，JSON 行输出：

```
python scripts/fund_flow.py 600519 sz000001 --date 2025-09-12 --json
```

实时资金流（分钟）

- 单次轮询（只取最新一分钟）：

```
python scripts/realtime_fund_flow.py --once 000981.SZ 300661.SZ
```

- 持续轮询（5秒间隔，存SQLite）：

```
python scripts/realtime_fund_flow.py --interval 5 --db data/flows.db 000981.SZ 300661.SZ
```

- 指定 K 线粒度与代理：

```
export HTTPS_PROXY=http://your-proxy:port
python scripts/realtime_fund_flow.py --klt 5 --use-proxy --once 000981.SZ

RSS 输出（每10分钟，交易时段）

- 生成/更新 RSS 文件（默认 `data/fund_flow.rss`），避免重复项：

```
python scripts/rss_fund_flow.py --interval 10 山子高科=000981.SZ 圣邦股份=300661.SZ
```

- RSS 每条包含：最新价、涨跌幅、总市值（亿元），以及主力/超大单/大单/中单/小单（亿元）。

每日 16:00 全市场资金流入库（SQLite）

- 立即执行（指定日期或今天）：

```
python scripts/daily_bulk_flow.py --db data/stocks.db --date 2025-09-12
```

- 常驻调度（每个交易日 16:00 执行）：

```
python scripts/daily_bulk_flow.py --db data/stocks.db --schedule
```
```

输出字段说明

- `date`: 交易日期
- `code`: 6位代码
- `name`: 股票名称
- `exchange`: 交易所（SH/SZ）
- `price`: 最新价（当前）
- `pct_chg`: 当日涨跌幅（来自该日资金流数据）
- `market_cap`: 总市值（人民币）
- `main/ultra_large/large/medium/small`: 当日对应资金净额（人民币）

说明

- 数据来源：东方财富公开接口。
- 若所在环境有系统代理导致请求失败，脚本已内置关闭系统代理（不走代理）。

在 Ubuntu 上使用

- 准备环境：

```
sudo apt update
sudo apt install -y python3 python3-venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

- 运行示例：

```
python scripts/fund_flow.py 600519 --start 2025-09-01 --end 2025-09-12 --all-days
```

- 若需要走系统代理（如企业网络）：

```
export HTTPS_PROXY=http://your-proxy:port
export HTTP_PROXY=http://your-proxy:port
python scripts/fund_flow.py 600519 --date 2025-09-12 --use-proxy
```

- 调整请求超时：

```
python scripts/fund_flow.py 600519 --date 2025-09-12 --timeout 15
```
观察股票的资金流入情况以及其他指标，抓涨停板
