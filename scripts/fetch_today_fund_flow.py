"""Fetch fund flow data for today (or a given date) if it is a trading day."""
from __future__ import annotations

import argparse
import datetime as dt
import os

from scripts.daily_bulk_flow import BULK_WORKERS_DEFAULT, is_trading_day, run_for_date


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch fund flow data for a date if it is a trading day")
    parser.add_argument(
        "--date",
        help="Date to fetch in YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("MYSQL_DSN"),
        help="MySQL DSN (默认读取环境变量 MYSQL_DSN)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker threads (default: daily_bulk_flow default)",
    )
    args = parser.parse_args()

    if args.date:
        try:
            target_date = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise SystemExit(f"Invalid date format: {args.date}") from exc
    else:
        target_date = dt.date.today()

    if not is_trading_day(target_date):
        print(f"{target_date} 非交易日，跳过读取。")
        return

    if not args.dsn:
        raise SystemExit("缺少 MySQL DSN，请通过 --dsn 或环境变量 MYSQL_DSN 提供")

    run_for_date(
        args.dsn,
        the_date=target_date.strftime("%Y-%m-%d"),
        workers=args.workers or BULK_WORKERS_DEFAULT,
    )


if __name__ == "__main__":
    main()
