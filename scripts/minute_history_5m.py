"""Fetch A-share 5-minute history (OHLCV) and fund flow via public APIs."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import akshare as ak
import pandas as pd
import requests

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

EM_HEADERS = {
    "Referer": "https://quote.eastmoney.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
}

KLINE_FIELDS = ",".join(["f51", "f52", "f53", "f54", "f55", "f56", "f57", "f58"])

CREATE_MINUTE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `stock_minute_5m` (
    `ts_code` VARCHAR(12) NOT NULL,
    `stock_code` VARCHAR(6) NOT NULL,
    `stock_name` VARCHAR(255) NULL,
    `trade_time` DATETIME NOT NULL,
    `open` DOUBLE NULL,
    `close` DOUBLE NULL,
    `high` DOUBLE NULL,
    `low` DOUBLE NULL,
    `vol` DOUBLE NULL,
    `amount` DOUBLE NULL,
    `main` DOUBLE NULL,
    `ultra_large` DOUBLE NULL,
    `large` DOUBLE NULL,
    `medium` DOUBLE NULL,
    `small` DOUBLE NULL,
    PRIMARY KEY (`ts_code`, `trade_time`),
    INDEX `idx_trade_time` (`trade_time`),
    INDEX `idx_stock_code` (`stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


class RateLimiter:
    def __init__(self, max_calls: int, period_seconds: float) -> None:
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self.calls: List[float] = []

    def wait(self) -> None:
        while True:
            now = time.monotonic()
            self.calls = [t for t in self.calls if now - t < self.period_seconds]
            if len(self.calls) < self.max_calls:
                self.calls.append(now)
                return
            sleep_for = self.period_seconds - (now - self.calls[0]) + 0.01
            if sleep_for > 0:
                time.sleep(sleep_for)


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


def ts_code_to_stock_code(ts_code: str) -> str:
    return ts_code.split(".", 1)[0]


def ts_code_to_secid(ts_code: str) -> Optional[str]:
    if "." not in ts_code:
        ts_code = normalize_ts_code(ts_code) or ""
    parts = ts_code.split(".")
    if len(parts) != 2:
        return None
    code, exch = parts
    if exch == "SH":
        return f"1.{code}"
    return f"0.{code}"


def ts_code_to_sina_symbol(ts_code: str) -> Optional[str]:
    if "." not in ts_code:
        ts_code = normalize_ts_code(ts_code) or ""
    parts = ts_code.split(".")
    if len(parts) != 2:
        return None
    code, exch = parts
    return f"{exch.lower()}{code}"


def parse_datetime(value: str, fallback_time: str) -> dt.datetime:
    cleaned = value.strip()
    if " " in cleaned:
        return dt.datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
    return dt.datetime.strptime(f"{cleaned} {fallback_time}", "%Y-%m-%d %H:%M:%S")


def date_range_from_years(years: int) -> Tuple[dt.datetime, dt.datetime]:
    end = dt.datetime.now()
    start = end - dt.timedelta(days=365 * years)
    return start, end


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


def fetch_stock_name_map() -> Dict[str, str]:
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception as exc:
        print(f"Fetch stock name map failed: {exc}", file=sys.stderr)
        return {}
    if df is None or df.empty:
        return {}
    if "代码" not in df.columns or "名称" not in df.columns:
        return {}
    df = df[["代码", "名称"]].copy()
    df["代码"] = df["代码"].astype(str).str.zfill(6)
    df["名称"] = df["名称"].astype(str).str.strip()
    return dict(zip(df["代码"], df["名称"]))


def fetch_price_akshare(
    ts_code: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    period: str,
) -> pd.DataFrame:
    symbol = ts_code.split(".", 1)[0]
    df = ak.stock_zh_a_hist_min_em(
        symbol=symbol,
        period=period,
        start_date=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_date=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.rename(
        columns={
            "时间": "trade_time",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "vol",
            "成交额": "amount",
        },
        inplace=True,
    )
    df["trade_time"] = df["trade_time"].astype(str)
    df["ts_code"] = ts_code
    return df[["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"]]


def fetch_price_sina(
    ts_code: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    period: str,
) -> pd.DataFrame:
    symbol = ts_code_to_sina_symbol(ts_code)
    if symbol is None:
        return pd.DataFrame()
    df = ak.stock_zh_a_minute(symbol=symbol, period=str(period))
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.rename(
        columns={
            "day": "trade_time",
            "open": "open",
            "close": "close",
            "high": "high",
            "low": "low",
            "volume": "vol",
        },
        inplace=True,
    )
    df["trade_time"] = pd.to_datetime(df["trade_time"])
    df = df[(df["trade_time"] >= start_dt) & (df["trade_time"] <= end_dt)]
    if df.empty:
        return pd.DataFrame()
    df["trade_time"] = df["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["amount"] = None
    df["ts_code"] = ts_code
    return df[["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"]]


def fetch_price_fallback(
    ts_code: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    period: str,
    limit: int = 1000,
) -> pd.DataFrame:
    secid = ts_code_to_secid(ts_code)
    if secid is None:
        return pd.DataFrame()
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": KLINE_FIELDS,
        "klt": int(period),
        "fqt": 1,
        "beg": 0,
        "end": 20500101,
        "lmt": limit,
    }
    resp = requests.get(
        "https://push2his.eastmoney.com/api/qt/stock/kline/get",
        params=params,
        headers=EM_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json()
    data = (payload or {}).get("data") or {}
    klines = data.get("klines") or []
    if not klines:
        return pd.DataFrame()
    rows: List[Tuple[str, str, float, float, float, float, float, float]] = []
    for line in klines:
        parts = str(line).split(",")
        if len(parts) < 7:
            continue
        trade_time = parts[0].strip()
        if len(trade_time) == 16:
            trade_time = f"{trade_time}:00"
        try:
            trade_dt = dt.datetime.strptime(trade_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if trade_dt < start_dt or trade_dt > end_dt:
            continue
        rows.append(
            (
                ts_code,
                trade_dt.strftime("%Y-%m-%d %H:%M:%S"),
                float(parts[1]),
                float(parts[2]),
                float(parts[3]),
                float(parts[4]),
                float(parts[5]),
                float(parts[6]),
            )
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"])
    return df


def fetch_flow_1m(ts_code: str, limit: int = 2000) -> pd.DataFrame:
    secid = ts_code_to_secid(ts_code)
    if secid is None:
        return pd.DataFrame()
    params = {
        "secid": secid,
        "klt": "1",
        "lmt": limit,
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    }
    resp = requests.get(
        "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get",
        params=params,
        headers=EM_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json()
    data = (payload or {}).get("data") or {}
    klines = data.get("klines") or []
    if not klines:
        return pd.DataFrame()
    rows: List[Tuple[str, float, float, float, float, float]] = []
    for line in klines:
        parts = str(line).split(",")
        if len(parts) < 6:
            continue
        trade_time = parts[0].strip()
        if len(trade_time) == 16:
            trade_time = f"{trade_time}:00"
        rows.append(
            (
                trade_time,
                float(parts[1]),
                float(parts[2]),
                float(parts[3]),
                float(parts[4]),
                float(parts[5]),
            )
        )
    df = pd.DataFrame(
        rows, columns=["trade_time", "main", "ultra_large", "large", "medium", "small"]
    )
    df["trade_time"] = pd.to_datetime(df["trade_time"])
    df["ts_code"] = ts_code
    return df


def aggregate_flow_to_period(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df = df.set_index("trade_time")
    agg = df[["main", "ultra_large", "large", "medium", "small"]].resample(f"{minutes}min").sum()
    agg = agg.dropna(how="all").reset_index()
    agg["ts_code"] = df["ts_code"].iloc[0]
    agg["trade_time"] = agg["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return agg[["ts_code", "trade_time", "main", "ultra_large", "large", "medium", "small"]]


def save_rows(dsn: str, create_sql: str, sql: str, rows: List[Tuple]) -> None:
    if not rows:
        return
    conn = connect_mysql(dsn, autocommit=False)
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


def save_price_rows(dsn: str, rows: List[Tuple]) -> None:
    sql = (
        "INSERT INTO `stock_minute_5m` "
        "(`ts_code`,`stock_code`,`stock_name`,`trade_time`,"
        "`open`,`close`,`high`,`low`,`vol`,`amount`) "
        "VALUES (" + ",".join(["%s"] * 10) + ") "
        "ON DUPLICATE KEY UPDATE "
        "`stock_code`=VALUES(`stock_code`),`stock_name`=VALUES(`stock_name`),"
        "`open`=VALUES(`open`),`close`=VALUES(`close`),"
        "`high`=VALUES(`high`),`low`=VALUES(`low`),"
        "`vol`=VALUES(`vol`),`amount`=VALUES(`amount`)"
    )
    save_rows(dsn, CREATE_MINUTE_TABLE_SQL, sql, rows)


def save_flow_rows(dsn: str, rows: List[Tuple]) -> None:
    sql = (
        "INSERT INTO `stock_minute_5m` "
        "(`ts_code`,`stock_code`,`stock_name`,`trade_time`,"
        "`main`,`ultra_large`,`large`,`medium`,`small`) "
        "VALUES (" + ",".join(["%s"] * 9) + ") "
        "ON DUPLICATE KEY UPDATE "
        "`stock_code`=VALUES(`stock_code`),`stock_name`=VALUES(`stock_name`),"
        "`main`=VALUES(`main`),`ultra_large`=VALUES(`ultra_large`),"
        "`large`=VALUES(`large`),`medium`=VALUES(`medium`),"
        "`small`=VALUES(`small`)"
    )
    save_rows(dsn, CREATE_MINUTE_TABLE_SQL, sql, rows)


def run_fetch_range(
    dsn: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    codes: Iterable[str],
    *,
    name_map: Optional[Dict[str, str]] = None,
    with_flow: bool = True,
    rpm: int = 50,
    batch_size: int = 2000,
    dry_run: bool = False,
) -> None:
    code_list = list(codes)
    if not code_list:
        raise SystemExit("No stock codes available; provide --codes or update data/all_codes.json")
    if name_map is None:
        name_map = fetch_stock_name_map()

    limiter = RateLimiter(max(1, rpm), 60.0)
    total_rows = 0
    start_time = time.perf_counter()

    for idx, ts_code in enumerate(code_list, 1):
        limiter.wait()
        stock_code = ts_code_to_stock_code(ts_code)
        stock_name = (name_map or {}).get(stock_code) or stock_code
        try:
            df = fetch_price_sina(ts_code, start_dt, end_dt, "5")
        except Exception as exc:
            print(f"[{idx}/{len(code_list)}] {ts_code} sina failed: {exc}", file=sys.stderr)
            df = pd.DataFrame()

        if df.empty:
            try:
                df = fetch_price_akshare(ts_code, start_dt, end_dt, "5")
            except Exception as exc:
                print(f"[{idx}/{len(code_list)}] {ts_code} akshare em failed: {exc}", file=sys.stderr)
                df = pd.DataFrame()

        if df.empty:
            try:
                df = fetch_price_fallback(ts_code, start_dt, end_dt, "5")
            except Exception as exc:
                print(f"[{idx}/{len(code_list)}] {ts_code} em fallback failed: {exc}", file=sys.stderr)
                df = pd.DataFrame()

        if df.empty:
            print(f"[{idx}/{len(code_list)}] {ts_code} no price data")
        else:
            rows: List[Tuple] = []
            for _, row in df.iterrows():
                rows.append(
                    (
                        row.get("ts_code"),
                        stock_code,
                        stock_name,
                        row.get("trade_time"),
                        row.get("open"),
                        row.get("close"),
                        row.get("high"),
                        row.get("low"),
                        row.get("vol"),
                        row.get("amount"),
                    )
                )
                if len(rows) >= batch_size:
                    if not dry_run:
                        save_price_rows(dsn, rows)
                    total_rows += len(rows)
                    rows.clear()
            if rows:
                if not dry_run:
                    save_price_rows(dsn, rows)
                total_rows += len(rows)

        if with_flow:
            limiter.wait()
            try:
                flow_1m = fetch_flow_1m(ts_code)
            except Exception as exc:
                print(f"[{idx}/{len(code_list)}] {ts_code} flow failed: {exc}", file=sys.stderr)
                flow_1m = pd.DataFrame()

            if not flow_1m.empty:
                flow_1m = flow_1m[
                    (flow_1m["trade_time"] >= start_dt) & (flow_1m["trade_time"] <= end_dt)
                ]
            flow_5m = aggregate_flow_to_period(flow_1m, 5)

            if flow_5m.empty:
                print(f"[{idx}/{len(code_list)}] {ts_code} no flow data")
            else:
                flow_rows: List[Tuple] = []
                for _, row in flow_5m.iterrows():
                    flow_rows.append(
                        (
                            row.get("ts_code"),
                            stock_code,
                            stock_name,
                            row.get("trade_time"),
                            row.get("main"),
                            row.get("ultra_large"),
                            row.get("large"),
                            row.get("medium"),
                            row.get("small"),
                        )
                    )
                    if len(flow_rows) >= batch_size:
                        if not dry_run:
                            save_flow_rows(dsn, flow_rows)
                        flow_rows.clear()
                if flow_rows:
                    if not dry_run:
                        save_flow_rows(dsn, flow_rows)

        if idx % 50 == 0:
            elapsed = time.perf_counter() - start_time
            print(f"Processed {idx}/{len(code_list)} stocks, rows={total_rows}, {elapsed:.1f}s")

    elapsed = time.perf_counter() - start_time
    print(f"Done. stocks={len(code_list)} rows={total_rows} time={elapsed:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch A-share 5m history via public APIs")
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--start", help="Start date YYYY-MM-DD (default: now-10y)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--years", type=int, default=10, help="Default year range when start/end omitted")
    parser.add_argument("--codes", help="Comma separated ts_codes or 6-digit codes")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of stocks for testing")
    parser.add_argument("--skip", type=int, default=0, help="Skip first N stocks (resume)")
    parser.add_argument("--rpm", type=int, default=50, help="Max API calls per minute (default 50)")
    parser.add_argument("--batch-size", type=int, default=2000, help="Rows per MySQL batch insert")
    parser.add_argument("--no-flow", action="store_true", help="Skip fund flow fetch")
    parser.add_argument("--dry-run", action="store_true", help="Fetch only; do not write to MySQL")
    args = parser.parse_args()

    load_env()

    if not args.dsn:
        args.dsn = os.environ.get("MYSQL_DSN") or os.environ.get("APP_MYSQL_DSN")

    if not args.dsn and not args.dry_run:
        raise SystemExit("Missing MySQL DSN; set MYSQL_DSN or APP_MYSQL_DSN.")

    if args.start and args.end:
        start_dt = parse_datetime(args.start, "09:30:00")
        end_dt = parse_datetime(args.end, "15:00:00")
    else:
        start_dt, end_dt = date_range_from_years(args.years)

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

    run_fetch_range(
        args.dsn,
        start_dt,
        end_dt,
        codes,
        with_flow=not args.no_flow,
        rpm=args.rpm,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
