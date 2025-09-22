# Flask API 配置说明

此目录提供了一个可扩展的 Flask API (`fund_flow_api.py`)，用于通过 HTTP 读取 SQLite 数据库中的资金流数据。支持通过配置文件接入多个数据库实例，便于后续扩展。

## 1. 配置文件

默认读取 `web/config.json`（可通过环境变量 `FUND_FLOW_CONFIG` 指向其他路径）。示例见 `web/config.example.json`：

```json
{
  "host": "0.0.0.0",
  "port": 8800,
  "default_db": "fund_flow",
  "databases": {
    "fund_flow": "data/stocks.db"
  }
}
```

字段含义：
- `host` / `port`：Flask 应用监听地址和端口。
- `default_db`：默认数据库标识，未在请求中指定 `db` 参数时使用。
- `databases`：`标识 -> SQLite 文件路径` 的映射，可扩展多个条目；路径支持 `~` 与环境变量。

如果 `config.json` 不存在，会自动使用默认配置并指向 `data/stocks.db`。

## 2. 启动服务

```bash
cp web/config.example.json web/config.json   # 如需自定义
./venv/bin/python web/fund_flow_api.py      # 或使用 flask run
```

环境变量覆盖：
- `FUND_FLOW_CONFIG`：配置文件路径。
- `FUND_FLOW_HOST` / `FUND_FLOW_PORT`：覆盖配置文件中的监听地址与端口。

## 3. API 说明

- `GET /health`：返回当前启用的数据库列表及默认值。
- `GET /api/fund-flow`：查询列表，支持参数：
  - `code`（必填）
  - `start` / `end`
  - `limit`（最大 1000）
  - `exchange`
  - `db`（可选，选择配置中的其他数据库）
- `GET /api/fund-flow/latest`：返回单条最新记录，参数同上。

## 4. 接入更多数据库

1. 在 `config.json` 的 `databases` 中新增一项，例如：
   ```json
   "databases": {
     "fund_flow": "data/stocks.db",
     "other_data": "/path/to/other.db"
   }
   ```
2. 请求时通过 `?db=other_data` 选择指定库；若表结构不同，可按同样模式新建 Blueprint / 视图函数以匹配新需求并注册到该 Flask 应用中。

通过这种方式，即使后续再增加新的 SQLite 数据库，也只需要更新配置并编写相应的路由逻辑即可。
