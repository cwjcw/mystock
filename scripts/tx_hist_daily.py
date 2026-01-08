"""Fetch Eastmoney A-share history via AKShare and store in MySQL."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import List, Optional, Tuple

import akshare as ak
import pandas as pd

try:
    from env_utils import load_env
    from mysql_utils import connect_mysql
except ImportError:  # pragma: no cover
    try:
        from .env_utils import load_env  # type: ignore
        from .mysql_utils import connect_mysql  # type: ignore
    except Exception as exc:
        raise SystemExit("Unable to import helpers; run from project root.") from exc


CODE_CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "all_codes.json"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `stock_daily_em` (
    `ts_code` VARCHAR(12) NOT NULL,
    `股票代码` VARCHAR(6) NOT NULL,
    `股票名称` VARCHAR(64) NULL,
    `日期` DATE NOT NULL,
    `开盘` DOUBLE NULL,
    `收盘` DOUBLE NULL,
    `最高` DOUBLE NULL,
    `最低` DOUBLE NULL,
    `成交量` DOUBLE NULL,
    `成交额` DOUBLE NULL,
    `振幅` DOUBLE NULL,
    `涨跌幅` DOUBLE NULL,
    `涨跌额` DOUBLE NULL,
    `换手率` DOUBLE NULL,
    `周期` VARCHAR(10) NOT NULL,
    `复权` VARCHAR(4) NOT NULL,
    PRIMARY KEY (`ts_code`, `日期`, `周期`, `复权`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def normalize_ts_code(code: str) -> Optional[str]:
    raw = code.strip().upper()
    if not raw:
        return None
    if "." in raw:
        parts = raw.split(".")
        if len(parts) != 2:
            return None
        return f"{parts[0][-6:]}.{parts[1].upper()}"
    cleaned = raw[-6:]
    if not cleaned.isdigit():
        return None
    if cleaned.startswith(("600", "601", "603", "605", "688")):
        return f"{cleaned}.SH"
    if cleaned.startswith(("000", "001", "002", "003", "300", "301")):
        return f"{cleaned}.SZ"
    if cleaned.startswith(
        ("430", "830", "831", "833", "835", "836", "838", "839", "870", "871", "872")
    ):
        return f"{cleaned}.BJ"
    return f"{cleaned}.SH"


def load_codes_from_cache() -> List[str]:
    if not CODE_CACHE_PATH.exists():
        return []
    try:
        data = json.loads(CODE_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    codes: List[str] = []
    for raw in data:
        if not isinstance(raw, str):
            continue
        ts_code = normalize_ts_code(raw)
        if ts_code:
            codes.append(ts_code)
    return sorted(set(codes))


def fetch_em_hist(
    code: str,
    *,
    period: str = "daily",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    adjust: str = "qfq",
    timeout: Optional[float] = None,
) -> pd.DataFrame:
    if start_date is None and end_date is None:
        china_tz = dt.timezone(dt.timedelta(hours=8))
        today = dt.datetime.now(china_tz).date()
        latest_trade = latest_trading_day(before=today)
        if latest_trade is None:
            return pd.DataFrame()
        start_date = latest_trade.strftime("%Y%m%d")
        end_date = latest_trade.strftime("%Y%m%d")
    df = ak.stock_zh_a_hist(
        symbol=code,
        period=period,
        start_date=start_date or "19000101",
        end_date=end_date or dt.date.today().strftime("%Y%m%d"),
        adjust=adjust,
        timeout=timeout,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    return df.copy()


def latest_trading_day(before: Optional[dt.date] = None) -> Optional[dt.date]:
    try:
        df = ak.tool_trade_date_hist_sina()
    except Exception:
        return None
    if df is None or df.empty:
        return None
    dates = pd.to_datetime(df["trade_date"], errors="coerce").dropna().dt.date
    if dates.empty:
        return None
    if before is None:
        return max(dates)
    filtered = [d for d in dates if d < before]
    return max(filtered) if filtered else None


def save_rows(dsn: str, rows: List[Tuple]) -> None:
    if not rows:
        return
    conn = connect_mysql(dsn, autocommit=False)
    try:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            sql = (
                "INSERT INTO `stock_daily_em` "
                "(`ts_code`,`股票代码`,`股票名称`,`日期`,`开盘`,`收盘`,`最高`,`最低`,"
                "`成交量`,`成交额`,`振幅`,`涨跌幅`,`涨跌额`,`换手率`,`周期`,`复权`) "
                "VALUES (" + ",".join(["%s"] * 16) + ") "
                "ON DUPLICATE KEY UPDATE "
                "`股票名称`=VALUES(`股票名称`),"
                "`开盘`=VALUES(`开盘`),`收盘`=VALUES(`收盘`),"
                "`最高`=VALUES(`最高`),`最低`=VALUES(`最低`),"
                "`成交量`=VALUES(`成交量`),`成交额`=VALUES(`成交额`),"
                "`振幅`=VALUES(`振幅`),`涨跌幅`=VALUES(`涨跌幅`),"
                "`涨跌额`=VALUES(`涨跌额`),`换手率`=VALUES(`换手率`)"
            )
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Eastmoney A-share history and store to MySQL")
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--start", default=None, help="Start date YYYYMMDD (default: previous trading day)")
    parser.add_argument("--end", default=None, help="End date YYYYMMDD (default: previous trading day)")
    parser.add_argument("--period", default="daily", help="Period: daily/weekly/monthly")
    parser.add_argument("--adjust", default="qfq", help="Adjust: qfq/hfq/empty")
    parser.add_argument("--timeout", type=float, default=None, help="Request timeout seconds")
    parser.add_argument("--codes", help="Comma separated ts_codes or 6-digit codes")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of stocks for testing")
    parser.add_argument("--skip", type=int, default=0, help="Skip first N stocks (resume)")
    parser.add_argument("--batch-size", type=int, default=2000, help="Rows per MySQL batch insert")
    parser.add_argument("--dry-run", action="store_true", help="Fetch only; do not write to MySQL")
    args = parser.parse_args()

    load_env()

    if not args.dsn:
        args.dsn = os.environ.get("MYSQL_DSN") or os.environ.get("APP_MYSQL_DSN")

    if not args.dsn and not args.dry_run:
        raise SystemExit("Missing MySQL DSN; set MYSQL_DSN or APP_MYSQL_DSN.")

    adjust = (args.adjust or "").strip()
    if adjust not in {"", "qfq", "hfq"}:
        raise SystemExit("adjust must be one of: '', qfq, hfq")
    adjust_label = {"": "不复权", "qfq": "前复权", "hfq": "后复权"}[adjust]

    period = (args.period or "daily").strip()
    if period not in {"daily", "weekly", "monthly"}:
        raise SystemExit("period must be one of: daily, weekly, monthly")

    if args.start is None and args.end is None:
        china_tz = dt.timezone(dt.timedelta(hours=8))
        today = dt.datetime.now(china_tz).date()
        latest_trade = latest_trading_day(before=today)
        if latest_trade is None:
            raise SystemExit("无法获取交易日历，已退出。")
        args.start = latest_trade.strftime("%Y%m%d")
        args.end = latest_trade.strftime("%Y%m%d")

    if args.codes:
        raw_codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        codes = [normalize_ts_code(c) for c in raw_codes]
        codes = [c for c in codes if c]
    else:
        codes = load_codes_from_cache()
    if args.skip:
        codes = codes[args.skip :]
    if args.limit:
        codes = codes[: args.limit]
    if not codes:
        raise SystemExit("No stock codes available; provide --codes or update data/all_codes.json")

    name_map: dict[str, str] = {}
    try:
        spot_df = ak.stock_zh_a_spot_em()
        if spot_df is not None and not spot_df.empty:
            for _, row in spot_df.iterrows():
                code = str(row.get("代码") or "").zfill(6)
                name = str(row.get("名称") or "").strip()
                if code and name:
                    name_map[code] = name
    except Exception:
        name_map = {}

    total_rows = 0
    start_time = dt.datetime.now()
    for idx, ts_code in enumerate(codes, 1):
        code = ts_code.split(".", 1)[0]
        try:
            df = fetch_em_hist(
                code,
                period=period,
                start_date=args.start,
                end_date=args.end,
                adjust=adjust,
                timeout=args.timeout,
            )
        except Exception as exc:
            print(f"[{idx}/{len(codes)}] {ts_code} fetch failed: {exc}")
            continue
        if df.empty:
            print(f"[{idx}/{len(codes)}] {ts_code} no data")
            continue
        name = name_map.get(code.zfill(6), "")
        rows: List[Tuple] = []
        for _, row in df.iterrows():
            row_code = str(row.get("股票代码") or code).zfill(6)
            rows.append(
                (
                    ts_code,
                    row_code,
                    name,
                    row.get("日期"),
                    row.get("开盘"),
                    row.get("收盘"),
                    row.get("最高"),
                    row.get("最低"),
                    row.get("成交量"),
                    row.get("成交额"),
                    row.get("振幅"),
                    row.get("涨跌幅"),
                    row.get("涨跌额"),
                    row.get("换手率"),
                    period,
                    adjust_label,
                )
            )
            if len(rows) >= args.batch_size:
                if not args.dry_run:
                    save_rows(args.dsn, rows)
                total_rows += len(rows)
                rows.clear()
        if rows:
            if not args.dry_run:
                save_rows(args.dsn, rows)
            total_rows += len(rows)
        if idx % 50 == 0:
            elapsed = (dt.datetime.now() - start_time).total_seconds()
            print(f"Processed {idx}/{len(codes)} stocks, rows={total_rows}, {elapsed:.1f}s")

    elapsed = (dt.datetime.now() - start_time).total_seconds()
    print(f"Done. stocks={len(codes)} rows={total_rows} time={elapsed:.1f}s")


if __name__ == "__main__":
    main()
