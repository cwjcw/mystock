"""Simple Flask API to expose fund flow data stored in MySQL."""
from __future__ import annotations

import os
from typing import Any, Dict, List

from flask import Flask, abort, g, jsonify, request
from pymysql.cursors import DictCursor

try:
    from .config import AppConfig, load_config
except ImportError:  # pragma: no cover - script mode fallback
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parent))
    from config import AppConfig, load_config  # type: ignore

try:
    from scripts.mysql_utils import connect_mysql
except ModuleNotFoundError:  # pragma: no cover
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1] / "scripts"))
    from mysql_utils import connect_mysql  # type: ignore


app = Flask(__name__)
app.config.setdefault("JSON_AS_ASCII", False)

APP_CONFIG: AppConfig = load_config()
MAX_LIMIT = 1000


def _resolve_db(key: str | None) -> tuple[str, str]:
    db_key = key or APP_CONFIG.default_db
    if db_key not in APP_CONFIG.databases:
        abort(400, description=f"未知的数据库标识: {db_key}")
    return db_key, APP_CONFIG.databases[db_key]


def get_conn(db_key: str | None = None):
    resolved_key, dsn = _resolve_db(db_key)
    cache = g.setdefault("_db_cache", {})  # type: ignore[var-annotated]
    if resolved_key not in cache:
        try:
            conn = connect_mysql(dsn, cursorclass=DictCursor)
        except Exception as exc:  # pragma: no cover
            abort(500, description=str(exc))
        cache[resolved_key] = conn
    return cache[resolved_key]


@app.teardown_appcontext
def close_conn(exception: Exception | None) -> None:  # noqa: D401
    cache = getattr(g, "_db_cache", None)
    if not cache:
        return
    for conn in cache.values():
        try:
            conn.close()
        except Exception:
            pass
    cache.clear()


def _normalize_code(raw_code: str) -> str:
    code = raw_code.strip().upper()
    if not code:
        abort(400, description="Stock code cannot be empty")
    if "." in code:
        code = code.split(".", 1)[0]
    if code.startswith(("SH", "SZ", "BJ")):
        code = code[2:]
    code = code[-6:]
    if len(code) != 6 or not code.isdigit():
        abort(400, description="Invalid stock code; expected a 6-digit code")
    return code


@app.errorhandler(400)
@app.errorhandler(404)
@app.errorhandler(500)
def handle_error(error):  # type: ignore[override]
    status = getattr(error, "code", 500)
    return (
        jsonify(
            {
                "error": getattr(error, "name", "Error"),
                "message": getattr(error, "description", str(error)),
                "status": status,
            }
        ),
        status,
    )


@app.route("/health", methods=["GET"])
def health() -> Any:
    return jsonify(
        {
            "status": "ok",
            "default_db": APP_CONFIG.default_db,
            "databases": {k: str(v) for k, v in APP_CONFIG.databases.items()},
        }
    )


@app.route("/api/fund-flow", methods=["GET"])
def fund_flow_list() -> Any:
    code_raw = request.args.get("code")
    if not code_raw:
        abort(400, description="Missing query parameter: code")
    code = _normalize_code(code_raw)

    db_key = request.args.get("db")
    start = request.args.get("start")
    end = request.args.get("end")
    limit = request.args.get("limit", default="100")
    exchange = request.args.get("exchange")

    try:
        limit_int = int(limit)
    except ValueError:
        abort(400, description="limit must be an integer")
    if limit_int <= 0:
        abort(400, description="limit must be greater than 0")
    limit_int = min(limit_int, MAX_LIMIT)

    clauses: List[str] = ["`代码` = %s"]
    params: List[Any] = [code]

    if exchange:
        exch = exchange.strip().upper()
        clauses.append("`交易所` = %s")
        params.append(exch)
    if start:
        clauses.append("`日期` >= %s")
        params.append(start)
    if end:
        clauses.append("`日期` <= %s")
        params.append(end)

    sql = (
        "SELECT `代码`,`交易所`,`日期`,`收盘价`,`涨跌幅`,"
        "`主力净流入-净额`,`主力净流入-净占比`,"
        "`超大单净流入-净额`,`超大单净流入-净占比`,"
        "`大单净流入-净额`,`大单净流入-净占比`,"
        "`中单净流入-净额`,`中单净流入-净占比`,"
        "`小单净流入-净额`,`小单净流入-净占比`,`名称` "
        "FROM `fund_flow_daily`"
    )
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY `日期` DESC, `交易所` LIMIT %s"
    params.append(limit_int)

    conn = get_conn(db_key)
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    return jsonify(rows)


@app.route("/api/fund-flow/latest", methods=["GET"])
def fund_flow_latest() -> Any:
    code_raw = request.args.get("code")
    if not code_raw:
        abort(400, description="Missing query parameter: code")
    code = _normalize_code(code_raw)

    db_key = request.args.get("db")
    exchange = request.args.get("exchange")

    clauses: List[str] = ["`代码` = %s"]
    params: List[Any] = [code]
    if exchange:
        exch = exchange.strip().upper()
        clauses.append("`交易所` = %s")
        params.append(exch)

    sql = (
        "SELECT `代码`,`交易所`,`日期`,`收盘价`,`涨跌幅`,"
        "`主力净流入-净额`,`主力净流入-净占比`,"
        "`超大单净流入-净额`,`超大单净流入-净占比`,"
        "`大单净流入-净额`,`大单净流入-净占比`,"
        "`中单净流入-净额`,`中单净流入-净占比`,"
        "`小单净流入-净额`,`小单净流入-净占比`,`名称` "
        "FROM `fund_flow_daily`"
    )
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY `日期` DESC LIMIT 1"

    conn = get_conn(db_key)
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()
    if not row:
        abort(404, description="No records found for the requested code")
    return jsonify(row)


if __name__ == "__main__":
    host = os.environ.get("FUND_FLOW_HOST", APP_CONFIG.host)
    port = int(os.environ.get("FUND_FLOW_PORT", APP_CONFIG.port))
    app.run(host=host, port=port)
