"""Migrate data from the local SQLite database to a MySQL instance."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence, Tuple

import pymysql

DEFAULT_SQLITE = Path(__file__).resolve().parents[1] / "data" / "stocks.db"

FUND_FLOW_COLUMNS = [
    "代码",
    "交易所",
    "日期",
    "收盘价",
    "涨跌幅",
    "主力净流入-净额",
    "主力净流入-净占比",
    "超大单净流入-净额",
    "超大单净流入-净占比",
    "大单净流入-净额",
    "大单净流入-净占比",
    "中单净流入-净额",
    "中单净流入-净占比",
    "小单净流入-净额",
    "小单净流入-净占比",
    "名称",
]

BASIC_INFO_COLUMNS = ["代码", "交易所", "字段", "值", "更新时间"]


def _quoted(columns: Sequence[str]) -> str:
    return ",".join(f'`{c}`' for c in columns)


def create_mysql_schema(conn: pymysql.connections.Connection, db_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
    conn.commit()
    conn.select_db(db_name)

    fund_flow_sql = f"""
    CREATE TABLE IF NOT EXISTS `fund_flow_daily` (
        `代码` VARCHAR(6) NOT NULL,
        `交易所` VARCHAR(4) NOT NULL,
        `日期` DATE NOT NULL,
        `收盘价` DOUBLE NULL,
        `涨跌幅` DOUBLE NULL,
        `主力净流入-净额` DOUBLE NULL,
        `主力净流入-净占比` DOUBLE NULL,
        `超大单净流入-净额` DOUBLE NULL,
        `超大单净流入-净占比` DOUBLE NULL,
        `大单净流入-净额` DOUBLE NULL,
        `大单净流入-净占比` DOUBLE NULL,
        `中单净流入-净额` DOUBLE NULL,
        `中单净流入-净占比` DOUBLE NULL,
        `小单净流入-净额` DOUBLE NULL,
        `小单净流入-净占比` DOUBLE NULL,
        `名称` VARCHAR(255) NULL,
        PRIMARY KEY (`代码`, `交易所`, `日期`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    basic_info_sql = f"""
    CREATE TABLE IF NOT EXISTS `stock_basic_info_xq` (
        `代码` VARCHAR(6) NOT NULL,
        `交易所` VARCHAR(4) NOT NULL,
        `字段` VARCHAR(255) NOT NULL,
        `值` TEXT NULL,
        `更新时间` DATETIME NULL,
        PRIMARY KEY (`代码`, `交易所`, `字段`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    with conn.cursor() as cur:
        cur.execute(fund_flow_sql)
        cur.execute(basic_info_sql)
    conn.commit()


def migrate_table(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: pymysql.connections.Connection,
    table: str,
    columns: Sequence[str],
    chunk_size: int,
) -> Tuple[int, int]:
    placeholders = ",".join(["%s"] * len(columns))
    insert_sql = (
        f"INSERT INTO `{table}` ({_quoted(columns)}) VALUES ({placeholders}) "
        + "ON DUPLICATE KEY UPDATE "
        + ",".join(f"`{col}`=VALUES(`{col}`)" for col in columns if col not in {"代码", "交易所", "日期"})
    )
    total_rows = 0
    batches = 0

    sqlite_conn.row_factory = sqlite3.Row
    query = f'SELECT {_quoted(columns)} FROM {table}'
    cur = sqlite_conn.execute(query)

    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            break
        payload = [tuple(row[col] for col in columns) for row in rows]
        with mysql_conn.cursor() as mysql_cursor:
            mysql_cursor.executemany(insert_sql, payload)
        mysql_conn.commit()
        total_rows += len(payload)
        batches += 1
        print(f"{table}: migrated {total_rows} rows (batch {batches})")

    return total_rows, batches


def migrate_basic_info(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: pymysql.connections.Connection,
    chunk_size: int,
) -> Tuple[int, int]:
    placeholders = ",".join(["%s"] * len(BASIC_INFO_COLUMNS))
    insert_sql = (
        f"INSERT INTO `stock_basic_info_xq` ({_quoted(BASIC_INFO_COLUMNS)}) VALUES ({placeholders}) "
        + "ON DUPLICATE KEY UPDATE "
        + ",".join(
            f"`{col}`=VALUES(`{col}`)" for col in BASIC_INFO_COLUMNS if col not in {"代码", "交易所", "字段"}
        )
    )
    total_rows = 0
    batches = 0

    sqlite_conn.row_factory = sqlite3.Row
    query = f'SELECT {_quoted(BASIC_INFO_COLUMNS)} FROM stock_basic_info_xq'
    cur = sqlite_conn.execute(query)

    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            break
        payload = [tuple(row[col] for col in BASIC_INFO_COLUMNS) for row in rows]
        with mysql_conn.cursor() as mysql_cursor:
            mysql_cursor.executemany(insert_sql, payload)
        mysql_conn.commit()
        total_rows += len(payload)
        batches += 1
        print(f"stock_basic_info_xq: migrated {total_rows} rows (batch {batches})")

    return total_rows, batches


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate data from SQLite to MySQL")
    parser.add_argument("--sqlite", default=str(DEFAULT_SQLITE), help="Path to SQLite file (default: data/stocks.db)")
    parser.add_argument("--mysql-host", default="127.0.0.1", help="MySQL host")
    parser.add_argument("--mysql-port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--mysql-user", required=True, help="MySQL username")
    parser.add_argument("--mysql-password", required=True, help="MySQL password")
    parser.add_argument("--mysql-db", default="mystock", help="Target MySQL database name")
    parser.add_argument("--chunk", type=int, default=2000, help="Batch size for inserts (default: 2000)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    sqlite_path = Path(args.sqlite)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite 数据库不存在: {sqlite_path}")

    sqlite_conn = sqlite3.connect(str(sqlite_path))

    mysql_conn = pymysql.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        charset="utf8mb4",
        autocommit=False,
    )

    try:
        create_mysql_schema(mysql_conn, args.mysql_db)

        ff_count, ff_batches = migrate_table(
            sqlite_conn,
            mysql_conn,
            "fund_flow_daily",
            FUND_FLOW_COLUMNS,
            args.chunk,
        )
        print(f"fund_flow_daily migrated: {ff_count} rows in {ff_batches} batches")

        bi_count, bi_batches = migrate_basic_info(sqlite_conn, mysql_conn, args.chunk)
        print(f"stock_basic_info_xq migrated: {bi_count} rows in {bi_batches} batches")
    finally:
        mysql_conn.close()
        sqlite_conn.close()


if __name__ == "__main__":
    main()
