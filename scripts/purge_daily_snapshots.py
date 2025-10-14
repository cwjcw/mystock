#!/usr/bin/env python
"""
Purge unwanted daily profit snapshot records for non-trading days.

Usage:
    python scripts/purge_daily_snapshots.py \
        --dates 2024-09-27 2024-09-28 2024-10-01 2024-10-02 ...

If --dates is omitted, a default list is used (see DEFAULT_DATES).
Set --dry-run to preview deletions without applying changes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path
from typing import Iterable, List

from mysql_utils import MySQLConfigError, connect_mysql


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

# Dates provided by the user (2024 Mainland holiday window)
DEFAULT_DATES = [
    dt.date(2024, 9, 27),
    dt.date(2024, 9, 28),
    dt.date(2024, 10, 1),
    dt.date(2024, 10, 2),
    dt.date(2024, 10, 3),
    dt.date(2024, 10, 4),
    dt.date(2024, 10, 5),
    dt.date(2024, 10, 6),
    dt.date(2024, 10, 7),
    dt.date(2024, 10, 8),
    dt.date(2024, 10, 11),
    dt.date(2024, 10, 12),
]


def load_env() -> None:
    if not ENV_FILE.exists():
        return
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        val = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def parse_dates(values: Iterable[str]) -> List[dt.date]:
    dates: List[dt.date] = []
    for item in values:
        try:
            dates.append(dt.date.fromisoformat(item))
        except ValueError:
            raise argparse.ArgumentTypeError(f"无效的日期格式: {item}，请使用 YYYY-MM-DD") from None
    return dates


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="删除 daily_profit_snapshots 表中指定日期的记录")
    parser.add_argument(
        "--dates",
        nargs="+",
        metavar="YYYY-MM-DD",
        help="需要删除的日期列表（默认使用脚本内置的假期日期）",
    )
    parser.add_argument("--dry-run", action="store_true", help="仅预览删除结果，不执行删除")
    args = parser.parse_args(argv)

    load_env()
    dsn = os.environ.get("APP_MYSQL_DSN") or os.environ.get("MYSQL_DSN")
    if not dsn:
        print("未找到 APP_MYSQL_DSN / MYSQL_DSN 环境变量，无法连接数据库。", file=sys.stderr)
        return 2

    if args.dates:
        dates = parse_dates(args.dates)
    else:
        dates = DEFAULT_DATES
    if not dates:
        print("未指定任何日期，退出。")
        return 0

    placeholders = ", ".join(["%s"] * len(dates))
    select_sql = (
        f"SELECT user_id, snapshot_date, amount, ratio, total_market_value FROM daily_profit_snapshots "
        f"WHERE snapshot_date IN ({placeholders}) ORDER BY snapshot_date, user_id"
    )
    delete_sql = (
        f"DELETE FROM daily_profit_snapshots WHERE snapshot_date IN ({placeholders})"
    )

    try:
        conn = connect_mysql(dsn, autocommit=False)
    except MySQLConfigError as exc:
        print(f"连接数据库失败: {exc}", file=sys.stderr)
        return 3

    try:
        with conn.cursor() as cursor:  # type: ignore[call-arg]
            cursor.execute(select_sql, [d.isoformat() for d in dates])
            rows = cursor.fetchall()
            if not rows:
                print("未找到任何匹配的记录。")
                conn.rollback()
                return 0
            print(f"即将删除 {len(rows)} 条记录：")
            for row in rows:
                uid, snapshot_date, amount, ratio, total_mv = row
                print(
                    f"  用户 {uid} | 日期 {snapshot_date} | 日盈亏 {amount} | 日收益率 {ratio} | 总市值 {total_mv}"
                )
            if args.dry_run:
                print("dry-run 模式，未执行删除。")
                conn.rollback()
                return 0

            cursor.execute(delete_sql, [d.isoformat() for d in dates])
            deleted = cursor.rowcount
            conn.commit()
            print(f"已删除 {deleted} 条记录。")
    except Exception as exc:  # pragma: no cover - runtime errors
        conn.rollback()
        print(f"执行过程中出错，已回滚: {exc}", file=sys.stderr)
        return 4
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
