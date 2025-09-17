import argparse
import datetime as dt
import os
import random
import time
from pathlib import Path
from typing import List, Dict, Optional

import requests

# Robust import to support both `python scripts/daily_bulk_flow.py` and `python -m scripts.daily_bulk_flow`
try:
    from scripts.fund_flow import (
        fetch_fund_flow_dayk,
        fetch_basic_info,
        save_to_sqlite,
        earliest_fund_flow_date,
    )  # type: ignore
except ModuleNotFoundError:
    import os, sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
    from fund_flow import fetch_fund_flow_dayk, fetch_basic_info, save_to_sqlite, earliest_fund_flow_date  # type: ignore


EM_HEADERS = {
    "Referer": "https://quote.eastmoney.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
}
SESSION = requests.Session()
SESSION.trust_env = False


PROXY_API_URL = None
PROXY_USERNAME = None
PROXY_PASSWORD = None


ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
if ENV_FILE.exists():
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        os.environ.setdefault(k.strip(), v.strip())
PROXY_API_URL = os.getenv("PROXY_API_URL")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")


def _refresh_proxy():
    """(Re)configure SESSION proxies using the configured API."""
    if not PROXY_API_URL:
        return
    try:
        proxy_ip = requests.get(PROXY_API_URL, timeout=10).text.strip()
    except requests.RequestException:
        return
    if not proxy_ip:
        return
    if PROXY_USERNAME and PROXY_PASSWORD:
        proxy_auth = f"{PROXY_USERNAME}:{PROXY_PASSWORD}@{proxy_ip}"
    else:
        proxy_auth = proxy_ip
    proxy_url = f"http://{proxy_auth}/"
    SESSION.proxies.update({"http": proxy_url, "https": proxy_url})


_refresh_proxy()


def fetch_all_stock_codes() -> List[str]:
    """Fetch all A-share 6-digit codes (both SH and SZ)."""
    codes: List[str] = []
    pn = 1
    while True:
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get?"
            f"pn={pn}&pz=500&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:0,m:1&fields=f12"
        )
        delay = 1.0
        for attempt in range(5):
            try:
                r = SESSION.get(url, headers=EM_HEADERS, timeout=10)
                r.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 4:
                    raise
                time.sleep(delay + random.uniform(0, 0.5))
                delay = min(delay * 2, 8)
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


def run_for_date(
    db_path: str,
    the_date: Optional[str] = None,
    limit: Optional[int] = None,
    codes_override: Optional[List[str]] = None,
):
    start_time = time.perf_counter()
    codes = codes_override or fetch_all_stock_codes()
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
    elapsed = time.perf_counter() - start_time
    date_label = the_date or dt.date.today().strftime("%Y-%m-%d")
    print(
        f"run_for_date({date_label}) processed {len(results)} records in {elapsed:.2f}s"
    )
    _refresh_proxy()


def run_full_range(db_path: str, end_date_str: str, limit: Optional[int] = None):
    try:
        end_date = dt.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid end date format: {end_date_str}") from exc

    codes = fetch_all_stock_codes()
    if limit:
        codes = codes[:limit]
    if not codes:
        print("No stock codes retrieved; aborting.")
        return

    sample_code = codes[0]
    start_str = earliest_fund_flow_date(sample_code)
    if not start_str:
        print("Unable to determine earliest available date; aborting.")
        return
    start_date = dt.datetime.strptime(start_str, "%Y-%m-%d").date()
    print(f"Fetching fund flow from {start_date} to {end_date}...")

    current = start_date
    while current <= end_date:
        if is_trading_day(current):
            run_for_date(db_path, current.strftime("%Y-%m-%d"), limit=limit, codes_override=codes)
        current += dt.timedelta(days=1)


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
    parser.add_argument(
        "--fill-to",
        help="Fetch from earliest available date up to YYYY-MM-DD",
    )
    args = parser.parse_args()

    if args.fill_to:
        run_full_range(args.db, args.fill_to, limit=args.limit)
    elif args.date:
        run_for_date(args.db, the_date=args.date, limit=args.limit)
    elif args.schedule:
        scheduler_loop(args.db)
    else:
        # default: run once for today
        run_for_date(args.db, the_date=dt.date.today().strftime("%Y-%m-%d"), limit=args.limit)


if __name__ == "__main__":
    main()
