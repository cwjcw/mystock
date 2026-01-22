"""Fetch today's 5-minute data (price + fund flow) and store into MySQL."""
from __future__ import annotations

import argparse
import datetime as dt
import os
import pathlib
import sys
from typing import List


if __package__ is None or __package__ == "":
    project_root = pathlib.Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from scripts.daily_bulk_flow import is_trading_day
    from scripts.env_utils import load_env
    from scripts.minute_history_5m import load_codes_from_cache, normalize_ts_code, run_fetch_range
else:
    from .daily_bulk_flow import is_trading_day
    from .env_utils import load_env
    from .minute_history_5m import load_codes_from_cache, normalize_ts_code, run_fetch_range


def main() -> None:
    load_env()
    parser = argparse.ArgumentParser(description="Fetch today's 5m history if it is a trading day")
    parser.add_argument("--date", help="Date to fetch in YYYY-MM-DD (default: today)")
    parser.add_argument(
        "--dsn",
        default=None,
        help="MySQL DSN (默认读取环境变量 MYSQL_DSN)",
    )
    parser.add_argument("--codes", help="Comma separated ts_codes or 6-digit codes")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of stocks for testing")
    parser.add_argument("--skip", type=int, default=0, help="Skip first N stocks (resume)")
    parser.add_argument("--rpm", type=int, default=50, help="Max API calls per minute (default 50)")
    parser.add_argument("--batch-size", type=int, default=2000, help="Rows per MySQL batch insert")
    parser.add_argument("--no-flow", action="store_true", help="Skip fund flow fetch")
    parser.add_argument("--dry-run", action="store_true", help="Fetch only; do not write to MySQL")
    args = parser.parse_args()

    CHINA_TZ = dt.timezone(dt.timedelta(hours=8))

    if args.date:
        try:
            target_date = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise SystemExit(f"Invalid date format: {args.date}") from exc
    else:
        target_date = dt.datetime.now(CHINA_TZ).date()

    if not is_trading_day(target_date):
        print(f"{target_date} 非交易日，跳过读取。")
        return

    if not args.dsn:
        args.dsn = os.environ.get("MYSQL_DSN") or os.environ.get("APP_MYSQL_DSN")
    if not args.dsn and not args.dry_run:
        raise SystemExit("缺少 MySQL DSN，请通过 --dsn 或环境变量 MYSQL_DSN 提供")

    if args.codes:
        raw_codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        codes: List[str] = [normalize_ts_code(c) for c in raw_codes]
        codes = [c for c in codes if c]
    else:
        codes = load_codes_from_cache()

    if args.skip:
        codes = codes[args.skip :]
    if args.limit:
        codes = codes[: args.limit]

    start_dt = dt.datetime.combine(target_date, dt.time(9, 30, 0))
    end_dt = dt.datetime.combine(target_date, dt.time(15, 0, 0))

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
