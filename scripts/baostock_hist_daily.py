"""Fetch Baostock daily history and store all fields into MySQL."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import baostock as bs
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
CREATE TABLE IF NOT EXISTS `stock_daily_baostock` (
    `ts_code` VARCHAR(12) NOT NULL,
    `股票代码` VARCHAR(6) NOT NULL,
    `股票名称` VARCHAR(64) NULL,
    `日期` DATE NOT NULL,
    `开盘` DOUBLE NULL,
    `最高` DOUBLE NULL,
    `最低` DOUBLE NULL,
    `收盘` DOUBLE NULL,
    `成交量` DOUBLE NULL,
    `成交额` DOUBLE NULL,
    `复权` VARCHAR(4) NULL,
    `换手率` DOUBLE NULL,
    `交易状态` VARCHAR(4) NULL,
    `涨跌幅` DOUBLE NULL,
    `市盈率_TTM` DOUBLE NULL,
    `市净率` DOUBLE NULL,
    `市销率_TTM` DOUBLE NULL,
    `市现率_TTM` DOUBLE NULL,
    `是否ST` VARCHAR(4) NULL,
    `写入时间` DATETIME NULL,
    PRIMARY KEY (`ts_code`, `日期`)
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


def ts_code_to_baostock(ts_code: str) -> Optional[str]:
    if "." not in ts_code:
        ts_code = normalize_ts_code(ts_code) or ""
    parts = ts_code.split(".")
    if len(parts) != 2:
        return None
    code, exch = parts
    exch = exch.lower()
    if exch not in {"sh", "sz"}:
        return None
    return f"{exch}.{code}"


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


def latest_trading_day(before: Optional[dt.date] = None) -> Optional[dt.date]:
    try:
        import akshare as ak
    except Exception:
        return None
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


def fetch_name_map() -> Dict[str, str]:
    try:
        import akshare as ak
    except Exception:
        return {}
    try:
        spot_df = ak.stock_zh_a_spot_em()
    except Exception:
        return {}
    if spot_df is None or spot_df.empty:
        return {}
    name_map: Dict[str, str] = {}
    for _, row in spot_df.iterrows():
        code = str(row.get("代码") or "").zfill(6)
        name = str(row.get("名称") or "").strip()
        if code and name:
            name_map[code] = name
    return name_map


def query_history(
    bs_code: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> Optional[pd.DataFrame]:
    fields = (
        "date,code,open,high,low,close,volume,amount,adjustflag,turn,"
        "tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
    )
    rs = bs.query_history_k_data_plus(
        bs_code,
        fields=fields,
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag=adjust,
    )
    if rs.error_code != "0":
        return None
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list:
        return pd.DataFrame()
    df = pd.DataFrame(data_list, columns=fields.split(","))
    return df


def save_rows(dsn: str, rows: List[Tuple]) -> None:
    if not rows:
        return
    conn = connect_mysql(dsn, autocommit=False)
    try:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            sql = (
                "INSERT INTO `stock_daily_baostock` ("
                "`ts_code`,`股票代码`,`股票名称`,`日期`,`开盘`,`最高`,`最低`,`收盘`,"
                "`成交量`,`成交额`,`复权`,`换手率`,`交易状态`,`涨跌幅`,"
                "`市盈率_TTM`,`市净率`,`市销率_TTM`,`市现率_TTM`,`是否ST`,`写入时间`"
                ") VALUES (" + ",".join(["%s"] * 20) + ") "
                "ON DUPLICATE KEY UPDATE "
                "`股票代码`=VALUES(`股票代码`),"
                "`股票名称`=VALUES(`股票名称`),"
                "`开盘`=VALUES(`开盘`),`最高`=VALUES(`最高`),`最低`=VALUES(`最低`),"
                "`收盘`=VALUES(`收盘`),`成交量`=VALUES(`成交量`),"
                "`成交额`=VALUES(`成交额`),`复权`=VALUES(`复权`),"
                "`换手率`=VALUES(`换手率`),`交易状态`=VALUES(`交易状态`),"
                "`涨跌幅`=VALUES(`涨跌幅`),`市盈率_TTM`=VALUES(`市盈率_TTM`),"
                "`市净率`=VALUES(`市净率`),`市销率_TTM`=VALUES(`市销率_TTM`),"
                "`市现率_TTM`=VALUES(`市现率_TTM`),`是否ST`=VALUES(`是否ST`),"
                "`写入时间`=VALUES(`写入时间`)"
            )
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


def to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "null":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Baostock daily history and store to MySQL")
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (default: previous trading day)")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD (default: previous trading day)")
    parser.add_argument(
        "--adjust",
        default="2",
        help="Adjust flag: 1=前复权, 2=后复权, 3=不复权",
    )
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
    if adjust not in {"1", "2", "3"}:
        raise SystemExit("adjust must be one of: 1(前复权), 2(后复权), 3(不复权)")
    adjust_label = {"1": "前复权", "2": "后复权", "3": "不复权"}[adjust]

    if args.start is None and args.end is None:
        china_tz = dt.timezone(dt.timedelta(hours=8))
        today = dt.datetime.now(china_tz).date()
        latest_trade = latest_trading_day(before=today)
        if latest_trade is None:
            raise SystemExit("无法获取交易日历，已退出。")
        args.start = latest_trade.strftime("%Y-%m-%d")
        args.end = latest_trade.strftime("%Y-%m-%d")

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

    name_map = fetch_name_map()

    login = bs.login()
    if login.error_code != "0":
        raise SystemExit(f"baostock login failed: {login.error_msg}")

    total_rows = 0
    start_time = dt.datetime.now()
    try:
        for idx, ts_code in enumerate(codes, 1):
            bs_code = ts_code_to_baostock(ts_code)
            if not bs_code:
                print(f"[{idx}/{len(codes)}] {ts_code} skipped (unsupported exchange)")
                continue
            try:
                df = query_history(bs_code, args.start, args.end, adjust)
            except Exception as exc:
                print(f"[{idx}/{len(codes)}] {ts_code} fetch failed: {exc}")
                continue
            if df is None or df.empty:
                print(f"[{idx}/{len(codes)}] {ts_code} no data")
                continue
            name = name_map.get(ts_code.split(".", 1)[0], "")
            rows: List[Tuple] = []
            for _, row in df.iterrows():
                rows.append(
                    (
                        ts_code,
                        ts_code.split(".", 1)[0],
                        name,
                        row.get("date"),
                        to_float(row.get("open")),
                        to_float(row.get("high")),
                        to_float(row.get("low")),
                        to_float(row.get("close")),
                        to_float(row.get("volume")),
                        to_float(row.get("amount")),
                        adjust_label,
                        to_float(row.get("turn")),
                        row.get("tradestatus"),
                        to_float(row.get("pctChg")),
                        to_float(row.get("peTTM")),
                        to_float(row.get("pbMRQ")),
                        to_float(row.get("psTTM")),
                        to_float(row.get("pcfNcfTTM")),
                        row.get("isST"),
                        dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
    finally:
        bs.logout()

    elapsed = (dt.datetime.now() - start_time).total_seconds()
    print(f"Done. stocks={len(codes)} rows={total_rows} time={elapsed:.1f}s")


if __name__ == "__main__":
    main()
