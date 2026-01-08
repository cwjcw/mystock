"""Quick inspection tool for MySQL fund_flow_daily contents."""
from __future__ import annotations

import os

from scripts.env_utils import load_env
from scripts.mysql_utils import connect_mysql


def main() -> None:
    load_env()
    dsn = os.environ.get("MYSQL_DSN") or os.environ.get("APP_MYSQL_DSN")
    if not dsn:
        raise SystemExit("请先在环境变量 MYSQL_DSN 中配置 MySQL 连接串")

    conn = connect_mysql(dsn, cursorclass=None)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM `fund_flow_daily`")
            total_rows = cur.fetchone()[0]
        print("total rows:", total_rows)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT `代码`,`交易所`,`日期`,`收盘价`,`主力净流入-净额`,`名称` "
                "FROM `fund_flow_daily` ORDER BY `日期`,`代码` LIMIT 10"
            )
            for row in cur.fetchall():
                print(row)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
