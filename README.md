# mystock

轻量级的 A 股资金流与持仓跟踪工具。通过 Web 页面管理股票清单，并为每位用户生成带有分钟级资金流和持仓快照的 RSS 订阅。

## 快速开始

1. **准备环境**
   - Python 3.10+，MySQL 8.0。
   - 可选：Node/前端工具不是必需。
2. **安装依赖**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. **复制配置模板**
   ```bash
   cp .env.example .env
   ```
4. **在 `.env` 中设置关键变量**
   - `APP_MYSQL_DSN`：MySQL 连接串，例如 `mysql://user:pwd@127.0.0.1:3306/mystock?charset=utf8mb4`
   - `PUBLIC_DOMAIN`：RSS 对外访问域名，示例 `stock.example.com`
   - `SECRET_KEY`：Flask 会话密钥，生产环境务必使用随机值
   - 其余变量可按需保留默认值（端口、限流、代理等）
5. **确保数据库已创建并拥有账号**
   ```bash
   mysql -uroot -p -e "CREATE DATABASE mystock CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
   mysql -uroot -p -e "CREATE USER 'mystock'@'%' IDENTIFIED BY '强密码';"
   mysql -uroot -p -e "GRANT ALL PRIVILEGES ON mystock.* TO 'mystock'@'%'; FLUSH PRIVILEGES;"
   ```
6. **启动 Web 服务**
   ```bash
   ./venv/bin/python web/app.py
   ```
   首次启动会自动在数据库中创建所需表。
7. **访问管理页面**
   - 浏览器打开 `http://服务器IP:APP_PORT`
   - 注册登录后即可在“我的股票”中维护 watchlist。

## RSS 订阅

- 每个用户拥有唯一 Token：
  - `https://PUBLIC_DOMAIN/<用户名>/<token>.rss`（默认前缀为用户名，可在 `.env` 中改为固定值）
  - 本地调试：`http://127.0.0.1:18888/u/<token>.rss`
- RSS 内容
  - **资金流汇总条目**：展示 watchlist 中股票的最新价格、涨跌幅、总市值以及五类资金流（正数红、负数绿）。
  - **持仓与盈亏条目**：展示最近一次资金流查询时的前 10 个持仓，含日内涨跌与盈亏概览。正数红色、负数绿色，手机端阅读器会自适应窄屏。
- 超限保护：默认每个 Token/IP 每分钟允许 1 次请求，可在 `.env` 通过 `RSS_RATE_LIMIT`、`RSS_RATE_WINDOW` 调整。

## 常用操作

- **重置 Token**：登录 Web → “我的股票” → 点击“重置 RSS Token”。若开启 `RSS_TOKEN_HASH_ONLY=true`，新 Token 仅显示一次。
- **同步 watchlist**：每行一个条目，支持 `名称=代码` 或直接 6 位代码（自动识别沪/深）。
- **数据库测试**：
  ```bash
  mysqladmin -h127.0.0.1 -u用户名 -p ping
  ```
  若客户端提示 *Public Key Retrieval is not allowed*，在连接参数中添加 `allowPublicKeyRetrieval=true&useSSL=false` 或将账号改为 `mysql_native_password` 认证方式。

## 配置速查

| 变量 | 说明 |
| ---- | ---- |
| `PUBLIC_DOMAIN` | 对外访问域名，用于生成 RSS 链接 |
| `RSS_PREFIX` | RSS URL 前缀：`username`（默认，使用用户名）或固定字符串 |
| `APP_MYSQL_DSN` / `MYSQL_DSN` | MySQL 连接串，应用启动时需至少设置其一 |
| `APP_PORT` | Web 服务监听端口，默认为 18888 |
| `RSS_RATE_LIMIT` / `RSS_RATE_WINDOW` | RSS 限流配置，默认每 60 秒 1 次 |
| `SECRET_KEY` | Flask 会话密钥，生产环境必须替换 |
| `PROXY_*` | 抓取脚本可选的代理设置；若无需求可保留默认 |

## 可选脚本

项目仍保留一组命令行脚本（放在 `scripts/` 目录）用于批量抓取或调度：
- `fund_flow.py`：拉取某只股票在指定日期/区间的资金流并写入数据库。
- `realtime_fund_flow.py`：实时榜单/自选股资金流轮询。
- `daily_bulk_flow.py`：交易日结束后批量写入全市场数据，支持 `--schedule` 定时模式。
- `fetch_today_fund_flow.py`：作为 cron 任务的入口（示例见 `scripts` 内注释）。

这些脚本不影响 Web/RSS 的使用，如无批量需求可以忽略。

## 许可证

本项目遵循 MIT License 发布。
