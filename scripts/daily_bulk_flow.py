import argparse
import datetime as dt
from typing import List, Dict, Optional
import requests

# Robust import to support both `python scripts/daily_bulk_flow.py` and `python -m scripts.daily_bulk_flow`
try:
    from scripts.fund_flow import fetch_fund_flow_dayk, fetch_basic_info, save_to_sqlite  # type: ignore
except ModuleNotFoundError:
    import os, sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
    from fund_flow import fetch_fund_flow_dayk, fetch_basic_info, save_to_sqlite  # type: ignore


EM_HEADERS = {"Referer": "https://quote.eastmoney.com/"}
SESSION = requests.Session()
SESSION.trust_env = False


def fetch_all_stock_codes() -> List[str]:
    """Fetch all A-share 6-digit codes (both SH and SZ)."""
    codes: List[str] = []
    pn = 1
    while True:
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get?"
            f"pn={pn}&pz=500&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:0,m:1&fields=f12"
        )
        r = SESSION.get(url, headers=EM_HEADERS, timeout=10)
        r.raise_for_status()
        j = r.json()
        data = (j.get("data") or {}).get("diff") or []
        if not data:
            break
        for d in data:
            code = d.get("f12")
            if code and len(code) == 6 and code.isdigit():
                codes.append(code)
        pn += 1
        if pn > 200:  # safety cap
            break
    # de-dup
    codes = sorted(set(codes))
    return codes


def run_for_date(db_path: str, the_date: Optional[str] = None, limit: Optional[int] = None):
    codes = fetch_all_stock_codes()
    if limit:
        codes = codes[:limit]
    results: List[Dict] = []
    for code in codes:
        try:
            flows = fetch_fund_flow_dayk(code, start=the_date, end=the_date)
            if not flows:
                continue
            base = fetch_basic_info(code)
            for f in flows:
                results.append({**base, **f})
        except Exception:
            continue
    if results:
        save_to_sqlite(results, db_path)


def is_trading_day(d: dt.date) -> bool:
    return d.weekday() < 5


def scheduler_loop(db_path: str):
    """Run daily at 16:00 local time on trading days."""
    while True:
        now = dt.datetime.now()
        target = now.replace(hour=16, minute=0, second=0, microsecond=0)
        if now > target:
            # move to next day
            target = target + dt.timedelta(days=1)
        sleep_sec = (target - now).total_seconds()
        if sleep_sec > 0:
            try:
                import time
                time.sleep(sleep_sec)
            except KeyboardInterrupt:
                break
        # When wake up
        today = dt.date.today()
        if is_trading_day(today):
            run_for_date(db_path, the_date=today.strftime("%Y-%m-%d"))


def main():
    parser = argparse.ArgumentParser(description="Daily bulk fund flow to SQLite at 16:00")
    parser.add_argument("--db", required=True, help="SQLite db path")
    parser.add_argument("--date", help="Run once for date YYYY-MM-DD (no schedule)")
    parser.add_argument("--limit", type=int, help="Limit number of stocks for testing")
    parser.add_argument("--schedule", action="store_true", help="Run scheduler (16:00 every trading day)")
    args = parser.parse_args()

    if args.date:
        run_for_date(args.db, the_date=args.date, limit=args.limit)
    elif args.schedule:
        scheduler_loop(args.db)
    else:
        # default: run once for today
        run_for_date(args.db, the_date=dt.date.today().strftime("%Y-%m-%d"), limit=args.limit)


if __name__ == "__main__":
    main()
