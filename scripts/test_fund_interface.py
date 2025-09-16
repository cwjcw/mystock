import sqlite3
from pathlib import Path

db_path = Path("data/stocks.db")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

with conn:
    cur = conn.execute("SELECT COUNT(*) FROM fund_flow_daily;")
    total_rows = cur.fetchone()[0]
    print("total rows:", total_rows)

    cur = conn.execute(
        """
        SELECT *
        FROM fund_flow_daily
        ORDER BY "日期", "代码"
        LIMIT 100
        """
    )
    rows = cur.fetchall()
    for row in rows:
        print(dict(row))

conn.close()
