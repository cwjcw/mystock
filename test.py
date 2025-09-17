#!/usr/bin/env python3
"""Quick AKShare fund-flow test script.

Usage examples::

    python test.py --code 600410 --market sh --date 20250916

When ``--date`` is supplied, only the matching trading day's row is printed;
otherwise the most recent records are shown. This script is intended to verify
that AKShare can fetch post-close (T+0)资金流数据 for a specific股票.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys


def fetch_fund_flow(code: str, market: str, date: str | None):
    try:
        import akshare as ak  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime check
        raise RuntimeError(
            "AKShare is not installed; run `pip install akshare` inside your env."
        ) from exc

    df = ak.stock_individual_fund_flow(stock=code, market=market)
    if date:
        df = df[df["日期"] == date]
    return df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Test AKShare single-stock fund-flow API")
    parser.add_argument("--code", default="600410", help="Stock code without prefix, e.g. 600410")
    parser.add_argument("--market", default="sh", choices={"sh", "sz"}, help="Exchange prefix: sh or sz")
    parser.add_argument(
        "--date",
        help="Trading day in YYYYMMDD format. If omitted, show latest rows",
    )
    args = parser.parse_args(argv)

    date = None
    if args.date:
        try:
            dt.datetime.strptime(args.date, "%Y%m%d")
        except ValueError as exc:
            raise SystemExit(f"Invalid date format: {args.date}") from exc
        date = dt.datetime.strptime(args.date, "%Y%m%d").strftime("%Y-%m-%d")

    df = fetch_fund_flow(args.code, args.market, date)
    if df.empty:
        print("No data returned; check if the date is a trading day or try a different code.")
    else:
        print(df.head())
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
