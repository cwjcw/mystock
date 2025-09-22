"""Simple Flask API to expose fund flow data stored in SQLite databases."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict

from flask import Flask, abort, g, jsonify, request

try:
    from .config import AppConfig, load_config
except ImportError:  # pragma: no cover - script mode fallback
    import sys

    sys.path.append(str(Path(__file__).resolve().parent))
    from config import AppConfig, load_config  # type: ignore


app = Flask(__name__)
app.config.setdefault("JSON_AS_ASCII", False)

APP_CONFIG: AppConfig = load_config()


def _resolve_db_path(key: str | None) -> tuple[str, Path]:
    db_key = key or APP_CONFIG.default_db
    if db_key not in APP_CONFIG.databases:
        abort(400, description=f"未知的数据库标识: {db_key}")
    return db_key, APP_CONFIG.databases[db_key]


def get_db(db_key: str | None = None) -> sqlite3.Connection:
    resolved_key, db_path = _resolve_db_path(db_key)
    cache = g.setdefault("_db_cache", {})  # type: ignore[var-annotated]
    if resolved_key not in cache:
        if not db_path.exists():
            abort(500, description=f"数据库文件不存在: {db_path}")
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cache[resolved_key] = conn
    return cache[resolved_key]


@app.teardown_appcontext
def close_db(exception: Exception | None) -> None:
    cache = getattr(g, "_db_cache", None)
    if not cache:
        return
    for conn in cache.values():
        try:
            conn.close()
        except Exception:
            pass
    cache.clear()


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


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
    response = {
        "error": getattr(error, "name", "Error"),
        "message": getattr(error, "description", str(error)),
        "status": getattr(error, "code", 500),
    }
    return jsonify(response), response["status"]


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
    try:
        limit_int = int(limit)
    except ValueError:
        abort(400, description="limit must be an integer")
    if limit_int <= 0:
        abort(400, description="limit must be greater than 0")
    limit_int = min(limit_int, 1000)

    exchange = request.args.get("exchange")
    if exchange:
        exchange = exchange.strip().upper()

    clauses: list[str] = ['"代码" = ?']
    params: list[Any] = [code]
    if exchange:
        clauses.append('"交易所" = ?')
        params.append(exchange)
    if start:
        clauses.append('"日期" >= ?')
        params.append(start)
    if end:
        clauses.append('"日期" <= ?')
        params.append(end)

    sql = (
        "SELECT "
        '"代码", "交易所", "日期", "收盘价", "涨跌幅", '
        '"主力净流入-净额", "主力净流入-净占比", '
        '"超大单净流入-净额", "超大单净流入-净占比", '
        '"大单净流入-净额", "大单净流入-净占比", '
        '"中单净流入-净额", "中单净流入-净占比", '
        '"小单净流入-净额", "小单净流入-净占比", "名称" '
        "FROM fund_flow_daily"
    )

    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += ' ORDER BY "日期" DESC, "交易所"'
    sql += " LIMIT ?"
    params.append(limit_int)

    cur = get_db(db_key).execute(sql, params)
    rows = cur.fetchall()
    return jsonify([_row_to_dict(row) for row in rows])


@app.route("/api/fund-flow/latest", methods=["GET"])
def fund_flow_latest() -> Any:
    code_raw = request.args.get("code")
    if not code_raw:
        abort(400, description="Missing query parameter: code")
    code = _normalize_code(code_raw)

    db_key = request.args.get("db")
    exchange = request.args.get("exchange")
    params: list[Any] = [code]
    clauses = ['"代码" = ?']
    if exchange:
        exchange = exchange.strip().upper()
        clauses.append('"交易所" = ?')
        params.append(exchange)

    sql = (
        "SELECT "
        '"代码", "交易所", "日期", "收盘价", "涨跌幅", '
        '"主力净流入-净额", "主力净流入-净占比", '
        '"超大单净流入-净额", "超大单净流入-净占比", '
        '"大单净流入-净额", "大单净流入-净占比", '
        '"中单净流入-净额", "中单净流入-净占比", '
        '"小单净流入-净额", "小单净流入-净占比", "名称" '
        "FROM fund_flow_daily"
    )
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += ' ORDER BY "日期" DESC LIMIT 1'

    cur = get_db(db_key).execute(sql, params)
    row = cur.fetchone()
    if not row:
        abort(404, description="No records found for the requested code")
    return jsonify(_row_to_dict(row))


if __name__ == "__main__":
    host = os.environ.get("FUND_FLOW_HOST", APP_CONFIG.host)
    port = int(os.environ.get("FUND_FLOW_PORT", APP_CONFIG.port))
    app.run(host=host, port=port)
