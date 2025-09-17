# mystock

抓取 A 股资金流（日级/分钟级），支持 SQLite 入库、RSS 订阅、多用户 Web 配置（登录仅用于配置，RSS 基于 token 无需登录）。

## 功能概览
- 日级资金流与基本信息：`scripts/fund_flow.py`
  - 主力/超大单/大单/中单/小单（单位：亿元，保留两位小数）
  - 最新价、涨跌幅（实时行情接口）、总市值（亿元）
  - 支持自定义日期或区间；可保存到 SQLite（中文列名）
- 分钟级实时资金流：`scripts/realtime_fund_flow.py`
  - 拉取最新一分钟或全量；可入库 `fund_flow_minute`（中文列名）
- RSS（脚本）：`scripts/rss_fund_flow.py`
  - 交易时段每 10 分钟生成/更新 RSS 文件（含最新价/涨跌幅/总市值/五类资金，单位亿元）
- 全市场日终入库：`scripts/daily_bulk_flow.py`
  - 每交易日 16:00 抓取全 A 股当日资金流（日级）并入库
- 多用户 Web：`web/app.py`
  - 注册/登录管理“我的股票”；每用户专属 RSS 链接；支持 token 重置与仅存哈希模式；限速与 .env 配置

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
python scripts/fund_flow.py 600519 --start 2025-09-01 --end 2025-09-12 --all-days --db data/stocks.db
```
- JSON 输出：
```
python scripts/fund_flow.py 600519 sz000001 --date 2025-09-12 --json
```

## 分钟级脚本
- 单次（最新一分钟）：
```
python scripts/realtime_fund_flow.py --once 000981.SZ 300661.SZ
```
- 持续轮询并入库：
```
python scripts/realtime_fund_flow.py --interval 5 --db data/flows.db 000981.SZ 300661.SZ
```

## RSS（脚本生成文件）
- 每 10 分钟更新（示例）：
```
python scripts/rss_fund_flow.py --interval 10 山子高科=000981.SZ 圣邦股份=300661.SZ
```

## 全市场日终入库
- 指定日期：
```
python scripts/daily_bulk_flow.py --db data/stocks.db --date 2025-09-12
```
- 常驻调度（每交易日 16:00）：
```
python scripts/daily_bulk_flow.py --db data/stocks.db --schedule
```
- 自动补全历史到指定日期（示例：抓取最早可用数据至 2025-09-15）：
```
python scripts/daily_bulk_flow.py --db data/stocks.db --fill-to 2025-09-15
```
  - 脚本会自动判断东方财富最早能提供的数据日期，并在抓取每个交易日后刷新一次代理 IP。

## 多用户 Web（登录配置 + 每用户 RSS）
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
- 资金流五类与总市值输出单位为“亿元，两位小数”；涨跌幅来自实时行情接口
- Windows 控制台若中文显示异常，建议使用 UTF-8 或在编辑器查看
