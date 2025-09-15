import asyncio
import aiohttp
import time
import argparse
import pandas as pd
from typing import Dict, Optional, Tuple, List
import sqlite3
from pathlib import Path
import datetime as dt


DEFAULT_HEADERS = {
    "Referer": "https://quote.eastmoney.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}


def symbol_to_secid(symbol: str) -> str:
    code, exch = symbol.upper().split(".")
    return ("1." if exch == "SH" else "0.") + code


def parse_kline_line(line: str) -> Dict[str, Optional[float]]:
    """
    Eastmoney fflow kline fields mapping:
    fields2=f51,f52,f53,f54,f55,f56,f57,f58
    f51=time, f52=main, f53=ultra, f54=large, f55=medium, f56=small
    """
    parts = line.split(",")

    def to_float(x: str):
        try:
            return float(x)
        except Exception:
            return None

    return {
        "time": parts[0] if len(parts) > 0 else None,
        "主力": to_float(parts[1]) if len(parts) > 1 else None,
        "超大单": to_float(parts[2]) if len(parts) > 2 else None,
        "大单": to_float(parts[3]) if len(parts) > 3 else None,
        "中单": to_float(parts[4]) if len(parts) > 4 else None,
        "小单": to_float(parts[5]) if len(parts) > 5 else None,
    }


async def fetch_flow_min_kline(
    session: aiohttp.ClientSession,
    secid: str,
    *,
    klt: int = 1,
    latest_only: bool = True,
    timeout: float = 10,
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    url = "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": str(klt),
        "lmt": "1" if latest_only else "0",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    }
    try:
        async with session.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout) as resp:
            if resp.status != 200:
                return None, None
            j = await resp.json(content_type=None)
    except Exception:
        return None, None

    data = (j or {}).get("data") or {}
    name = data.get("name")
    kl = data.get("klines") or []
    if not kl:
        return name, None

    rows = [parse_kline_line(line) for line in kl]
    df = pd.DataFrame(rows)
    return name, df


def latest_row(df: Optional[pd.DataFrame]) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    return df.iloc[-1]


def fmt_scaled(v: Optional[float]) -> str:
    if v is None:
        return "-"
    return f"{(v/100000000):.2f}"


def _init_db(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fund_flow_minute (
            "代码" TEXT NOT NULL,
            "交易所" TEXT NOT NULL,
            "时间" TEXT NOT NULL,
            "主力" REAL,
            "超大单" REAL,
            "大单" REAL,
            "中单" REAL,
            "小单" REAL,
            PRIMARY KEY ("代码", "交易所", "时间")
        )
        """
    )


def save_minute_row(conn: sqlite3.Connection, code: str, exchange: str, row: pd.Series):
    conn.execute(
        """
        INSERT INTO fund_flow_minute ("代码","交易所","时间","主力","超大单","大单","中单","小单")
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT("代码","交易所","时间") DO UPDATE SET
            "主力"=excluded."主力",
            "超大单"=excluded."超大单",
            "大单"=excluded."大单",
            "中单"=excluded."中单",
            "小单"=excluded."小单"
        """,
        (
            code,
            exchange,
            str(row.get("time")),
            None if row.get("主力") is None else round(float(row.get("主力")) / 1e8, 2),
            None if row.get("超大单") is None else round(float(row.get("超大单")) / 1e8, 2),
            None if row.get("大单") is None else round(float(row.get("大单")) / 1e8, 2),
            None if row.get("中单") is None else round(float(row.get("中单")) / 1e8, 2),
            None if row.get("小单") is None else round(float(row.get("小单")) / 1e8, 2),
        ),
    )


async def poll_realtime_flows(
    name_to_symbol: Dict[str, str],
    *,
    interval_sec: int = 5,
    klt: int = 1,
    latest_only: bool = True,
    timeout: float = 10,
    db_path: Optional[str] = None,
    use_proxy: bool = False,
    once: bool = False,
):
    connector = aiohttp.TCPConnector(limit=20)
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg, trust_env=use_proxy) as session:
        conn: Optional[sqlite3.Connection] = None
        if db_path:
            p = Path(db_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(p))
            _init_db(conn)

        try:
            while True:
                start = time.time()
                name_to_secid = {n: symbol_to_secid(s) for n, s in name_to_symbol.items()}
                tasks = [
                    fetch_flow_min_kline(session, secid, klt=klt, latest_only=latest_only, timeout=timeout)
                    for secid in name_to_secid.values()
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                print(time.strftime("\n== %Y-%m-%d %H:%M:%S =="))
                for (cname, symbol), result in zip(name_to_secid.items(), results):
                    if isinstance(result, Exception):
                        print(f"[{cname}] 请求失败: {result}")
                        continue
                    ret_name, df = result
                    if df is None or df.empty:
                        print(f"[{cname}] 无数据")
                        continue
                    last = latest_row(df)
                    if last is None:
                        print(f"[{cname}] 无数据")
                        continue
                    print(
                        f"[{cname} / {ret_name or '-'}] {last['time']} | "
                        f"主力:{fmt_scaled(last['主力'])} "
                        f"超大单:{fmt_scaled(last['超大单'])} "
                        f"大单:{fmt_scaled(last['大单'])} "
                        f"中单:{fmt_scaled(last['中单'])} "
                        f"小单:{fmt_scaled(last['小单'])}"
                    )

                    if conn is not None:
                        code, exch = symbol.split(".")
                        save_minute_row(conn, code, exch, last)

                if conn is not None:
                    conn.commit()

                if once:
                    break

                elapsed = time.time() - start
                await asyncio.sleep(max(0, interval_sec - elapsed))
        finally:
            if conn is not None:
                conn.close()


def build_default_watchlist() -> Dict[str, str]:
    return {
        "山子高科": "000981.SZ",
        "圣邦股份": "300661.SZ",
        "中科曙光": "603019.SH",
        "阿尔特": "300825.SZ",
        "三博脑科": "301293.SZ",
    }


def parse_name_symbol_pairs(pairs: List[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for p in pairs:
        if "=" in p:
            name, sym = p.split("=", 1)
            mapping[name] = sym
        else:
            # If only symbol provided, use it as name
            mapping[p] = p
    return mapping


def main():
    parser = argparse.ArgumentParser(description="Realtime A-share fund flow (minute) via Eastmoney")
    parser.add_argument("pairs", nargs="*", help="Name=Symbol pairs, e.g. 山子高科=000981.SZ 或直接 000981.SZ")
    parser.add_argument("--interval", type=int, default=5, help="Polling interval seconds")
    parser.add_argument("--klt", type=int, default=1, choices=[1, 5], help="K-line period: 1 or 5 minutes")
    parser.add_argument("--all", dest="latest_only", action="store_false", help="Fetch full period instead of latest only")
    parser.add_argument("--timeout", type=float, default=10, help="HTTP timeout seconds")
    parser.add_argument("--db", dest="db_path", help="SQLite db file to store minute flows")
    parser.add_argument("--use-proxy", action="store_true", help="Use system proxy (HTTP[S]_PROXY)")
    parser.add_argument("--once", action="store_true", help="Run one iteration and exit")
    args = parser.parse_args()

    watchlist = parse_name_symbol_pairs(args.pairs) if args.pairs else build_default_watchlist()

    try:
        asyncio.run(
            poll_realtime_flows(
                watchlist,
                interval_sec=args.interval,
                klt=args.klt,
                latest_only=args.latest_only,
                timeout=args.timeout,
                db_path=args.db_path,
                use_proxy=args.use_proxy,
                once=args.once,
            )
        )
    except KeyboardInterrupt:
        print("\n已停止。")


if __name__ == "__main__":
    main()
