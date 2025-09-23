# mystock

抓取 A 股资金流（日级/分钟级），支持 SQLite 入库、RSS 订阅、多用户 Web 配置（登录仅用于配置，RSS 基于 token 无需登录）。

## 功能概览
- 日级资金流与基本信息：`scripts/fund_flow.py`
  - 主力/超大单/大单/中单/小单净额与占比（净额单位：亿元，保留 4 位小数；占比单位：%）
  - 收盘价、日涨跌幅（来自 AKShare `stock_individual_fund_flow`）
  - 雪球公司概况（`stock_individual_basic_info_xq`）入库至独立表
  - 支持自定义日期或区间；可保存到 MySQL（UTF-8 中文列名）
- 实时资金流排名：`scripts/realtime_fund_flow.py`
  - 调用 AKShare `stock_individual_fund_flow_rank` 按窗口（今日/3日/5日/10日）拉取榜单
  - 可关注指定股票或输出榜单前 N 名，支持入库 `fund_flow_rank`
- RSS（脚本）：`scripts/rss_fund_flow.py`
  - 交易时段每 10 分钟生成/更新 RSS 文件（含最新价/涨跌幅/总市值/五类资金，单位亿元）
- 全市场日终入库：`scripts/daily_bulk_flow.py`
  - 每交易日 16:00 抓取全 A 股当日资金流（日级）并入库
- 多用户 Web：`web/app.py`
  - 注册/登录管理“我的股票”；每用户专属 RSS 链接；支持 token 重置与仅存哈希模式；限速与 .env 配置
- 资金流 REST API：`web/fund_flow_api.py`
  - 支持 `/health`、`/api/fund-flow`、`/api/fund-flow/latest` JSON 接口，可通过 Cloudflare Tunnel 对外暴露
  - 通过 `web/config.json` 或环境变量配置监听端口与多数据库映射，兼容 SQLite 与 MySQL

## 安装
```
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 日级脚本
- 单日：
```
python scripts/fund_flow.py 600519 --date 2025-09-12
```
- 区间+保存到 SQLite：
```
export MYSQL_DSN="mysql://mystock:StrongPwd@127.0.0.1:3306/mystock?charset=utf8mb4"
python scripts/fund_flow.py 600519 --start 2025-09-01 --end 2025-09-12 --all-days --dsn "$MYSQL_DSN"
```
- JSON 输出：
```
python scripts/fund_flow.py 600519 sz000001 --date 2025-09-12 --json
```

## 实时榜单脚本
- 单次（关注个股）：
```
python scripts/realtime_fund_flow.py --once 山子高科=000981.SZ 圣邦股份=300661.SZ
```
- 持续轮询榜单 Top20 并入库（默认使用系统代理，可用 `--proxy none` 禁用）：
```
python scripts/realtime_fund_flow.py --interval 60 --db data/flows.db
```

## RSS（脚本生成文件）
- 每 10 分钟更新（示例）：
```
python scripts/rss_fund_flow.py --interval 10 山子高科=000981.SZ 圣邦股份=300661.SZ
```

## 全市场日终入库
- 指定日期：
```
python scripts/daily_bulk_flow.py --dsn "$MYSQL_DSN" --date 2025-09-12
```
- 常驻调度（每交易日 16:00）：
```
python scripts/daily_bulk_flow.py --dsn "$MYSQL_DSN" --schedule
```
- 自动补全历史到指定日期（示例：抓取最早可用数据至 2025-09-15）：
```
python scripts/daily_bulk_flow.py --dsn "$MYSQL_DSN" --fill-to 2025-09-15
```
  - 脚本会自动判断东方财富最早能提供的数据日期，并在抓取每个交易日后刷新一次代理 IP。
- 一次性抓取所有历史数据（谨慎使用，耗时较长）：
```
python scripts/daily_bulk_flow.py --dsn "$MYSQL_DSN" --full-history
```
  - 逐只股票下载全部历史资金流记录，并批量写入数据库。
- 可选参数：
  - `--workers 20` 调整并发抓取数量，默认 20（可通过 `.env` 的 `BULK_WORKERS` 覆盖）
  - `--refresh-codes` 强制刷新本地缓存的股票代码列表（默认缓存于 `data/all_codes.json`）

## 每日自动抓取（工作日 16:00）
- 脚本：`scripts/fetch_today_fund_flow.py`
  - 先判断目标日期是否为工作日，是则调用 `run_for_date` 写入配置的 MySQL 数据库
  - 金额自动按“亿元”存储，无需额外换算
- 命令示例：
  - 当日：`python scripts/fetch_today_fund_flow.py --dsn "$MYSQL_DSN"`
  - 指定日期：`python scripts/fetch_today_fund_flow.py --date 2025-09-19 --dsn "$MYSQL_DSN"`
- Cron 示例（工作日 16:00 执行，并写入日志）：
  ```
  0 16 * * 1-5 cd /home/cwj/code/mystock && ./venv/bin/python scripts/fetch_today_fund_flow.py --dsn "mysql://mystock:StrongPwd@127.0.0.1:3306/mystock?charset=utf8mb4" >> logs/fund_flow_daily.log 2>&1
  ```

## 迁移到 MySQL
- 依赖：`requirements.txt` 已包含 `PyMySQL`
- 创建 MySQL 数据库及账户（示例命令需要 root 权限）：
  ```bash
  sudo apt install mysql-server
  sudo mysql -e "CREATE DATABASE mystock CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
  sudo mysql -e "CREATE USER 'mystock'@'%' IDENTIFIED BY '强密码';"
  sudo mysql -e "GRANT ALL PRIVILEGES ON mystock.* TO 'mystock'@'%'; FLUSH PRIVILEGES;"
  ```
  - 修改 `/etc/mysql/mysql.conf.d/mysqld.cnf` 中的 `bind-address = 0.0.0.0`（或保持默认 loopback，并通过 SSH 隧道访问）
  - 确保服务器防火墙开放 3306 或只允许可信来源
- 数据迁移脚本（若仍持有旧的 SQLite 数据）：
  ```bash
  ./venv/bin/python scripts/migrate_sqlite_to_mysql.py \
      --mysql-host 127.0.0.1 \
      --mysql-user mystock \
      --mysql-password '强密码' \
      --mysql-db mystock \
      --chunk 5000
  ```
  - 执行后会在目标数据库中创建/更新 `fund_flow_daily` 与 `stock_basic_info_xq` 表，并保持主键约束
  - 净额仍保留“亿元”单位；如脚本重复运行会执行 upsert，不会产生重复主键
- Python 连接示例：
  ```python
  import pymysql

  conn = pymysql.connect(
      host="your-host",
      port=3306,
      user="mystock",
      password="强密码",
      database="mystock",
      charset="utf8mb4",
  )
  with conn.cursor() as cur:
      cur.execute("SELECT COUNT(*) FROM fund_flow_daily")
      print(cur.fetchone())
  conn.close()
  ```
- 远程编辑器（DataGrip、DBeaver、VSCode SQL 扩展等）可使用同样的连接参数；需要时建议配置只读账号或开启 SSL

## 资金流 API 服务
- 脚本：`web/fund_flow_api.py`
  - `/health` 返回默认数据库与可选数据库清单
  - `/api/fund-flow` 支持参数 `code`、`start`、`end`、`limit`、`exchange`、`db`
  - `/api/fund-flow/latest` 返回最新一条记录
  - 查询结果沿用数据库中的中文列名（净额单位“亿元”）
- 配置：
  - 默认读取 `web/config.json`（可参考 `web/config.example.json`），也可通过环境变量 `FUND_FLOW_CONFIG` 指向其他配置
  - 支持为多个数据库配置别名，通过 `?db=` 选择
- 启动示例：
  - `./venv/bin/python web/fund_flow_api.py`
  - 或 `FUND_FLOW_PORT=8800 FLASK_APP=web.fund_flow_api ./venv/bin/flask run --host 0.0.0.0 --port 8800`
- 对外发布：
  - 推荐结合 Cloudflare Tunnel（Zero Trust）添加 `fundapi.<your-domain>` 的 Public Hostname，将流量转发到 `http://127.0.0.1:8800`
  - 可再结合 Access Policy / Service Token 做鉴权与限速

## 多用户 Web（登录配置 + 每用户 RSS）
- 启动前请设置 MySQL 连接串：`export APP_MYSQL_DSN="mysql://cwjcw:bj210726@127.0.0.1:3306/mystock?charset=utf8mb4"`
  - 若已经在其他脚本中配置了 `MYSQL_DSN`，也可复用该环境变量
- 启动服务（端口 18888）：
```
python web/app.py
```
- 使用说明：
  - 登录仅用于“配置股票/重置 token”；RSS 订阅无需登录
  - 在“我的股票”里：每行一个；可填“名称=代码”或直接 6 位代码（自动识别沪/深）
  - 推荐外部 RSS 链接：`https://stock.yourdomin.cn/<RSS_PREFIX>/<rsstoken>.rss`
    - 默认 `<RSS_PREFIX>` 为“用户名”，可改为固定前缀（见 .env）
  - 本地调试：`/u/<rsstoken>.rss` 或 `/<RSS_PREFIX>/<rsstoken>.rss`
  - RSS 条目除了每只股票的分钟资金流，还会追加一条 “持仓与盈亏” 快照：
    - 包含周期已实现盈亏（含初始盈利）、当前持仓盈亏、当日盈亏（股票/基金拆分）
    - 前 10 项持仓的最新价、涨跌幅、市值、持仓盈亏、持仓盈亏%、当日收益；除涨跌幅外的数字均四舍五入为整数
  - 重置 Token：页面一键重置；旧链接立即失效
  - 更安全模式：设置 `RSS_TOKEN_HASH_ONLY=true`，注册/重置时仅显示一次 token，数据库存哈希
  - 刷新频率限制：默认每 Token/每 IP 每分钟 1 次（超限返回 429），可在 .env 调整

### Web 端交易录入
- 支持买入/卖出/分红三种方向
- 可单独填写手续费和印花税，系统会自动在成本与盈亏中扣除

### 代理
- 在 `.env` 中配置 `PROXY_API_URL`、`PROXY_USERNAME`、`PROXY_PASSWORD` 后，日终抓取脚本会自动向代理服务拉取 IP 并定期刷新。
  - 可额外设置 `PROXY_REFRESH_INTERVAL`（秒），默认每 900 秒（15 分钟）更新一次。
  - `BULK_WORKERS` 可设置默认并发数（默认 20）。

## .env 配置（项目根目录）
启动时自动读取 `.env`（也可用进程环境变量覆盖），示例：
```
# 外部域名与 RSS 路径前缀
PUBLIC_DOMAIN=stock.you r d o mian.cn
# 使用用户名作为前缀：username；或指定固定前缀，如 rss
RSS_PREFIX=username

# 仅存哈希、Token 只显示一次（true/false）
RSS_TOKEN_HASH_ONLY=true

# 限速：每窗口内允许的请求次数、窗口秒数（默认 1/60）
RSS_RATE_LIMIT=1
RSS_RATE_WINDOW=60

# Web 服务端口与调试开关
APP_PORT=18888
DEBUG=false

# Flask 会话密钥（强随机）
# 生产环境务必使用强随机值（不要用示例/默认）。
# 生成方法：
# - 通用（Python）：
#   python -c "import secrets; print(secrets.token_urlsafe(32))"
# - Linux/macOS：
#   openssl rand -base64 32
# - Windows PowerShell：
#   [Convert]::ToBase64String((New-Object System.Security.Cryptography.RNGCryptoServiceProvider).GetBytes(32))
SECRET_KEY=change-me-please
```

## Cloudflare Zero Trust（不暴露端口）
- 用 Cloudflare Tunnel 将 `https://stock.yourdomin.cn` 流量转发到本机 `127.0.0.1:18888`；无需开放入站端口
- 在 Zero Trust 后台创建 Tunnel → 添加 Public Hostname（Service 指向 `http://127.0.0.1:18888`）→ 部署 cloudflared 常驻
- 可对 `/*.rss` 路径配置访问策略/频控

## 说明
- 数据来源：东方财富公开接口
- 资金流五类净额统一存储为“亿元”（数据库保留 4 位小数；API 输出沿用原值），涨跌幅来自实时行情接口
- Windows 控制台若中文显示异常，建议使用 UTF-8 或在编辑器查看
