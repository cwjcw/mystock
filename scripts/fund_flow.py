import argparse
import datetime as dt
import json
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

import requests


EM_REFERER = {"Referer": "https://quote.eastmoney.com/"}
SESSION = requests.Session()
# Default: avoid system proxy unless explicitly enabled via CLI
SESSION.trust_env = False
DEFAULT_TIMEOUT = 10


def normalize_code(code: str) -> str:
    """
    Normalize input stock code to eastmoney secid format: "1.600519" or "0.000001".
    Accepts forms like "600519", "sh600519", "sz000001".
    """
    c = code.strip().lower()
    if c.startswith("sh"):
        return f"1.{c[2:]}"
    if c.startswith("sz"):
        return f"0.{c[2:]}"
    # Infer by leading digits
    if c.startswith(("600", "601", "603", "605", "688")):
        return f"1.{c}"
    elif c.startswith(("000", "001", "002", "003", "300", "301")):
        return f"0.{c}"
    # Fallback: assume 6-digit SH if unknown
    if len(c) == 6 and c.isdigit():
        return f"1.{c}"
    raise ValueError(f"Unrecognized stock code: {code}")


def fetch_basic_info(code: str, session: Optional[requests.Session] = None) -> Dict:
    """
    Fetch basic quote and market cap info from Eastmoney.
    Returns keys: name, code, exchange, price, change, change_pct, market_cap, float_market_cap.
    """
    secid = normalize_code(code)
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get?"
        "fields=f58,f116,f117,f43,f170,f169,f152&secid=" + secid
    )
    sess = session or SESSION
    r = sess.get(url, headers=EM_REFERER, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    data = r.json().get("data") or {}
    name = data.get("f58")
    price = (data.get("f43") or 0) / 100.0
    change = (data.get("f169") or 0) / 100.0
    change_pct = (data.get("f170") or 0) / 100.0
    market_cap = data.get("f116")  # RMB
    float_market_cap = data.get("f117")  # RMB
    exch = "SH" if secid.startswith("1.") else "SZ"
    return {
        "code": secid.split(".")[1],
        "exchange": exch,
        "name": name,
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "market_cap": market_cap,
        "float_market_cap": float_market_cap,
    }


def fetch_fund_flow_dayk(
    code: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> List[Dict]:
    """
    Fetch daily funds flow breakdown (main, ultra-large, large, medium, small) from Eastmoney.
    Returns list of dicts with keys: date, main, ultra_large, large, medium, small, pct_chg.
    start/end are strings YYYY-MM-DD (inclusive). If omitted, returns all available and caller filters.
    """
    secid = normalize_code(code)
    # Use historical endpoint which works without ut when Referer is set
    params = [
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?",
        "fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58",
    ]

    if start:
        params.append(f"beg={start.replace('-', '')}")
    if end:
        params.append(f"end={end.replace('-', '')}")
    params.append("lmt=0")

    params.append("klt=101")
    params.append(f"secid={secid}")
    url = "&".join(params)
    sess = session or SESSION
    r = sess.get(url, headers=EM_REFERER, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    j = r.json()
    data = (j.get("data") or {}).get("klines") or []
    rows: List[Dict] = []
    for line in data:
        parts = line.split(",")
        if len(parts) < 8:
            continue
        d, main, ultra, large, medium, small, pct_chg, _last = parts[:8]
        rows.append(
            {
                "date": d,
                "main": float(main),
                "ultra_large": float(ultra),
                "large": float(large),
                "medium": float(medium),
                "small": float(small),
                "pct_chg": float(pct_chg),
            }
        )
    # Filter by date range if provided
    def to_date(s: str) -> dt.date:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()

    if start:
        sdt = to_date(start)
        rows = [r for r in rows if to_date(r["date"]) >= sdt]
    if end:
        edt = to_date(end)
        rows = [r for r in rows if to_date(r["date"]) <= edt]

    return rows


def earliest_fund_flow_date(code: str) -> Optional[str]:
    """Return the earliest available trading date for the given stock."""
    rows = fetch_fund_flow_dayk(code)
    if not rows:
        return None
    return min(r["date"] for r in rows)


def merge_latest_on_date(code: str, date: Optional[str]) -> Dict:
    """
    Return a single merged record for the specified date (or the latest if date is None):
    Includes basic info and fund flow figures for that date.
    """
    base = fetch_basic_info(code)
    flows = fetch_fund_flow_dayk(code, start=date, end=date) if date else fetch_fund_flow_dayk(code)
    flow = None
    if date:
        flow = flows[0] if flows else None
    else:
        flow = flows[-1] if flows else None
    result = {
        **base,
        "date": (flow or {}).get("date"),
        "main": (flow or {}).get("main"),
        "ultra_large": (flow or {}).get("ultra_large"),
        "large": (flow or {}).get("large"),
        "medium": (flow or {}).get("medium"),
        "small": (flow or {}).get("small"),
        "pct_chg": (flow or {}).get("pct_chg", base.get("change_pct")),
    }
    return result


def _init_db(conn: sqlite3.Connection):
    # 使用中文列名，并使用双引号包裹以确保兼容性
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_basics (
            "代码" TEXT NOT NULL,
            "交易所" TEXT NOT NULL,
            "名称" TEXT,
            "最新价" REAL,
            "总市值" REAL,
            "流通市值" REAL,
            "更新时间" TEXT,
            PRIMARY KEY ("代码", "交易所")
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fund_flow_daily (
            "代码" TEXT NOT NULL,
            "交易所" TEXT NOT NULL,
            "日期" TEXT NOT NULL,
            "主力" REAL,
            "超大单" REAL,
            "大单" REAL,
            "中单" REAL,
            "小单" REAL,
            "涨跌幅" REAL,
            PRIMARY KEY ("代码", "交易所", "日期")
        )
        """
    )
    cols = {row[1] for row in conn.execute('PRAGMA table_info("fund_flow_daily")')}
    if "名称" not in cols:
        conn.execute('ALTER TABLE fund_flow_daily ADD COLUMN "名称" TEXT')
    if "总市值" not in cols:
        conn.execute('ALTER TABLE fund_flow_daily ADD COLUMN "总市值" REAL')


def save_to_sqlite(results: List[Dict], db_path: str):
    # Ensure directory exists
    p = Path(db_path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    try:
        _init_db(conn)
        now_iso = dt.datetime.now().isoformat(timespec="seconds")
        # Upsert basics
        def sc(x):
            return round(float(x) / 1e8, 2) if x is not None else None
        basics_rows = [
            (
                r.get("code"),
                r.get("exchange"),
                r.get("name"),
                r.get("price"),
                sc(r.get("market_cap")),
                sc(r.get("float_market_cap")),
                now_iso,
            )
            for r in results
        ]
        conn.executemany(
            """
            INSERT INTO stock_basics ("代码","交易所","名称","最新价","总市值","流通市值","更新时间")
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT("代码","交易所") DO UPDATE SET
                "名称"=excluded."名称",
                "最新价"=excluded."最新价",
                "总市值"=excluded."总市值",
                "流通市值"=excluded."流通市值",
                "更新时间"=excluded."更新时间"
            """,
            basics_rows,
        )

        # Upsert fund flow per date if present
        flow_rows = []
        for r in results:
            if not r.get("date"):
                continue
            # scale to 亿元 with 2 decimals
            def sc(x):
                return round(float(x) / 1e8, 2) if x is not None else None
            market_cap_scaled = sc(r.get("market_cap"))
            try:
                pct = float(r.get("pct_chg"))
            except (TypeError, ValueError):
                pct = None
            if pct is not None:
                pct = round(pct, 2)
            flow_rows.append(
                (
                    r.get("code"),
                    r.get("exchange"),
                    r.get("date"),
                    sc(r.get("main")),
                    sc(r.get("ultra_large")),
                    sc(r.get("large")),
                    sc(r.get("medium")),
                    sc(r.get("small")),
                    pct,
                    r.get("name"),
                    market_cap_scaled,
                )
            )
        if flow_rows:
            conn.executemany(
                """
                INSERT INTO fund_flow_daily (
                    "代码","交易所","日期","主力","超大单","大单","中单","小单","涨跌幅","名称","总市值"
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT("代码","交易所","日期") DO UPDATE SET
                    "主力"=excluded."主力",
                    "超大单"=excluded."超大单",
                    "大单"=excluded."大单",
                    "中单"=excluded."中单",
                    "小单"=excluded."小单",
                    "涨跌幅"=excluded."涨跌幅",
                    "名称"=excluded."名称",
                    "总市值"=excluded."总市值"
                """,
                flow_rows,
            )

        conn.commit()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Fetch A-share fund flow and basics from Eastmoney")
    parser.add_argument("codes", nargs="+", help="Stock codes like 600519, sh600519, sz000001")
    parser.add_argument("--date", dest="date", help="Specific date YYYY-MM-DD")
    parser.add_argument("--start", dest="start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", dest="end", help="End date YYYY-MM-DD")
    parser.add_argument("--all-days", action="store_true", help="Output all days in range instead of latest")
    parser.add_argument("--json", action="store_true", help="Output JSON lines instead of table")
    parser.add_argument("--db", dest="db_path", help="SQLite file path to save results")
    parser.add_argument("--use-proxy", action="store_true", help="Use system proxy settings (HTTP[S]_PROXY)")
    parser.add_argument("--timeout", type=float, default=None, help="HTTP timeout seconds (default 10)")
    parser.add_argument("--earliest", action="store_true", help="Only print earliest available date for each code")
    args = parser.parse_args()

    # Configure session per CLI
    if args.use_proxy:
        SESSION.trust_env = True
    if args.timeout is not None:
        global DEFAULT_TIMEOUT
        DEFAULT_TIMEOUT = args.timeout

    if args.earliest:
        for c in args.codes:
            code_only = c[-6:] if len(c) > 6 else c
            earliest = earliest_fund_flow_date(code_only)
            print(f"{c}: earliest date = {earliest}")
        return

    results: List[Dict] = []
    for c in args.codes:
        if args.all_days or args.start or args.end:
            flows = fetch_fund_flow_dayk(c, start=args.start or args.date, end=args.end or args.date)
            base = fetch_basic_info(c)
            for f in flows:
                results.append({**base, **f})
        else:
            results.append(merge_latest_on_date(c, args.date))

    if args.db_path:
        save_to_sqlite(results, args.db_path)

    # 中文列名映射与输出
    def _scale(v: Optional[float]) -> Optional[float]:
        if v is None:
            return None
        try:
            return round(float(v) / 1e8, 2)
        except Exception:
            return None

    def to_cn_record(r: Dict) -> Dict:
        try:
            pct = float(r.get("pct_chg")) if r.get("pct_chg") is not None else None
        except (TypeError, ValueError):
            pct = None
        if pct is not None:
            pct = round(pct, 2)
        return {
            "日期": r.get("date"),
            "代码": r.get("code"),
            "名称": r.get("name"),
            "交易所": r.get("exchange"),
            "最新价": r.get("price"),
            "涨跌幅": pct,
            "总市值": _scale(r.get("market_cap")),
            "主力": _scale(r.get("main")),
            "超大单": _scale(r.get("ultra_large")),
            "大单": _scale(r.get("large")),
            "中单": _scale(r.get("medium")),
            "小单": _scale(r.get("small")),
        }

    if args.json:
        for r in results:
            print(json.dumps(to_cn_record(r), ensure_ascii=False))
        return

    # Pretty table output
    cols = [
        "日期",
        "代码",
        "名称",
        "交易所",
        "最新价",
        "涨跌幅",
        "总市值",
        "主力",
        "超大单",
        "大单",
        "中单",
        "小单",
    ]
    header = "\t".join(cols)
    print(header)
    for r in results:
        cn = to_cn_record(r)
        row = [cn.get(k, "") for k in cols]
        # format numbers lightly
        out = []
        for k, v in zip(cols, row):
            if isinstance(v, float):
                if k == "涨跌幅":
                    out.append(f"{v:.2f}%")
                elif k in {"最新价"}:
                    out.append(f"{v:.2f}")
                else:
                    out.append(f"{v:.2f}")
            elif isinstance(v, (int,)):
                out.append(str(v))
            else:
                out.append(str(v) if v is not None else "")
        print("\t".join(out))


if __name__ == "__main__":
    main()
