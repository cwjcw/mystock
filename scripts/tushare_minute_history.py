"""Fetch A-share 15-minute history via Tushare and write to MySQL."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

try:
    import tushare as ts
except Exception as exc:  # pragma: no cover - missing dependency
    raise SystemExit("Missing dependency: install tushare to run this script.") from exc

try:
    from mysql_utils import connect_mysql
except ImportError:  # pragma: no cover
    try:
        from .mysql_utils import connect_mysql  # type: ignore
    except Exception as exc:
        raise SystemExit("Unable to import mysql_utils; run from project root.") from exc


ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
CODE_CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "all_codes.json"


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `stock_minute_15m` (
    `ts_code` VARCHAR(12) NOT NULL,
    `trade_time` DATETIME NOT NULL,
    `open` DOUBLE NULL,
    `close` DOUBLE NULL,
    `high` DOUBLE NULL,
    `low` DOUBLE NULL,
    `vol` DOUBLE NULL,
    `amount` DOUBLE NULL,
    PRIMARY KEY (`ts_code`, `trade_time`),
    INDEX `idx_trade_time` (`trade_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def load_env() -> None:
    if not ENV_FILE.exists():
        return
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value and key not in os.environ:
            os.environ[key] = value


def get_tushare_token() -> str:
    token = os.environ.get("TUSHARE_TOKEN") or os.environ.get("TS_TOKEN")
    if not token:
        raise SystemExit("Missing Tushare token: set TUSHARE_TOKEN or TS_TOKEN.")
    return token


def normalize_date(date_str: str) -> str:
    cleaned = date_str.strip()
    if "-" in cleaned:
        return cleaned.replace("-", "")
    return cleaned


def default_date_range(years: int) -> Tuple[str, str]:
    end = dt.date.today()
    start = end - dt.timedelta(days=365 * years)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def code_to_ts_code(code: str) -> Optional[str]:
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
        ts_code = code_to_ts_code(raw)
        if ts_code:
            codes.append(ts_code)
    return sorted(set(codes))


def fetch_codes_from_tushare(pro) -> List[str]:
    df = pro.stock_basic(exchange="", list_status="L", fields="ts_code")
    if df is None or df.empty:
        return []
    return sorted(df["ts_code"].astype(str).tolist())


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


def normalize_trade_time(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if " " in text:
        if len(text) == 16:
            return f"{text}:00"
        return text
    if text.isdigit():
        if len(text) == 14:
            dt_val = dt.datetime.strptime(text, "%Y%m%d%H%M%S")
            return dt_val.strftime("%Y-%m-%d %H:%M:%S")
        if len(text) == 12:
            dt_val = dt.datetime.strptime(text, "%Y%m%d%H%M")
            return dt_val.strftime("%Y-%m-%d %H:%M:%S")
        if len(text) == 8:
            dt_val = dt.datetime.strptime(text, "%Y%m%d")
            return dt_val.strftime("%Y-%m-%d 00:00:00")
    return text


def iter_rows(df) -> Iterable[Tuple]:
    time_col = "trade_time" if "trade_time" in df.columns else "trade_date"
    for _, row in df.iterrows():
        trade_time = normalize_trade_time(row.get(time_col))
        if not trade_time:
            continue
        yield (
            row.get("ts_code"),
            trade_time,
            row.get("open"),
            row.get("close"),
            row.get("high"),
            row.get("low"),
            row.get("vol"),
            row.get("amount"),
        )


def save_rows(dsn: str, rows: List[Tuple]) -> None:
    if not rows:
        return
    conn = connect_mysql(dsn, autocommit=False)
    try:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            sql = (
                "INSERT INTO `stock_minute_15m` "
                "(`ts_code`,`trade_time`,`open`,`close`,`high`,`low`,`vol`,`amount`) "
                "VALUES (" + ",".join(["%s"] * 8) + ") "
                "ON DUPLICATE KEY UPDATE "
                "`open`=VALUES(`open`),`close`=VALUES(`close`),"
                "`high`=VALUES(`high`),`low`=VALUES(`low`),"
                "`vol`=VALUES(`vol`),`amount`=VALUES(`amount`)"
            )
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch 15-min A-share history via Tushare")
    parser.add_argument("--dsn", default=os.environ.get("MYSQL_DSN") or os.environ.get("APP_MYSQL_DSN"))
    parser.add_argument("--start", help="Start date YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--years", type=int, default=2, help="Default year range when start/end omitted")
    parser.add_argument("--freq", default="15MIN", help="Minute frequency: 1MIN/5MIN/15MIN/30MIN/60MIN")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of stocks for testing")
    parser.add_argument("--skip", type=int, default=0, help="Skip first N stocks (resume)")
    parser.add_argument("--from-tushare", action="store_true", help="Fetch code list from Tushare")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per MySQL batch insert")
    parser.add_argument("--rpm", type=int, default=50, help="Max API calls per minute (default 50)")
    args = parser.parse_args()

    if not args.dsn:
        raise SystemExit("Missing MySQL DSN; set MYSQL_DSN or APP_MYSQL_DSN.")

    load_env()
    token = get_tushare_token()
    ts.set_token(token)
    pro = ts.pro_api()

    if args.start and args.end:
        start_date = normalize_date(args.start)
        end_date = normalize_date(args.end)
    else:
        start_date, end_date = default_date_range(args.years)

    freq = args.freq.strip().upper()
    freq_map = {
        "1MIN": "1min",
        "5MIN": "5min",
        "15MIN": "15min",
        "30MIN": "30min",
        "60MIN": "60min",
    }
    if freq not in freq_map:
        raise SystemExit(f"Unsupported freq: {freq}")
    bar_freq = freq_map[freq]

    if args.from_tushare:
        codes = fetch_codes_from_tushare(pro)
    else:
        codes = load_codes_from_cache()
    if not codes:
        raise SystemExit("No stock codes available. Use --from-tushare or update data/all_codes.json")
    if args.skip:
        codes = codes[args.skip :]
    if args.limit:
        codes = codes[: args.limit]

    limiter = RateLimiter(max(1, args.rpm), 60.0)
    total_rows = 0
    total_calls = 0
    start_time = time.perf_counter()

    for idx, ts_code in enumerate(codes, 1):
        limiter.wait()
        total_calls += 1
        try:
            df = ts.pro_bar(
                ts_code=ts_code,
                asset="E",
                freq=bar_freq,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_time,open,high,low,close,vol,amount",
            )
        except Exception as exc:
            print(f"[{idx}/{len(codes)}] {ts_code} fetch failed: {exc}", file=sys.stderr)
            continue
        if df is None or df.empty:
            print(f"[{idx}/{len(codes)}] {ts_code} no data")
            continue

        rows: List[Tuple] = []
        for row in iter_rows(df):
            rows.append(row)
            if len(rows) >= args.batch_size:
                save_rows(args.dsn, rows)
                total_rows += len(rows)
                rows.clear()
        if rows:
            save_rows(args.dsn, rows)
            total_rows += len(rows)

        if idx % 50 == 0:
            elapsed = time.perf_counter() - start_time
            print(f"Processed {idx}/{len(codes)} stocks, rows={total_rows}, calls={total_calls}, {elapsed:.1f}s")

    elapsed = time.perf_counter() - start_time
    print(f"Done. stocks={len(codes)} rows={total_rows} calls={total_calls} time={elapsed:.1f}s")


if __name__ == "__main__":
    main()
