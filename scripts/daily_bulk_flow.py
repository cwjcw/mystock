import argparse
import datetime as dt
import json
import os
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import requests

# Robust import to support both `python scripts/daily_bulk_flow.py` and `python -m scripts.daily_bulk_flow`
try:
    from scripts.fund_flow import (
        fetch_fund_flow_dayk,
        save_to_sqlite,
        earliest_fund_flow_date,
        fetch_basic_profile,
        extract_stock_name,
        parse_stock_code,
    )  # type: ignore
except ModuleNotFoundError:
    import os, sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
    from fund_flow import (  # type: ignore
        fetch_fund_flow_dayk,
        save_to_sqlite,
        earliest_fund_flow_date,
        fetch_basic_profile,
        extract_stock_name,
        parse_stock_code,
    )


EM_HEADERS = {
    "Referer": "https://quote.eastmoney.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
}
# 仅查询沪深 A 股（含创业板、科创板），剔除指数、债券等无日度资金流数据的标的
EM_FS_FILTERS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
SESSION = requests.Session()
SESSION.trust_env = False


LOG_PATH = Path(__file__).resolve().parents[1] / "bulk.log"
LOGGER = logging.getLogger("daily_bulk_flow")
if not LOGGER.handlers:
    LOGGER.setLevel(logging.INFO)
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        LOGGER.addHandler(handler)
    except OSError:
        # 如果日志目录无法创建，退回到默认配置但不阻塞任务
        logging.basicConfig(level=logging.INFO)
    if LOGGER.handlers:
        LOGGER.propagate = False


PROXY_API_URL = None
PROXY_USERNAME = None
PROXY_PASSWORD = None
PROXY_REFRESH_INTERVAL = 900
_proxy_timestamp: Optional[float] = None


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
if os.getenv("PROXY_REFRESH_INTERVAL"):
    try:
        PROXY_REFRESH_INTERVAL = int(os.getenv("PROXY_REFRESH_INTERVAL"))
    except ValueError:
        pass
BULK_WORKERS_DEFAULT = 20
if os.getenv("BULK_WORKERS"):
    try:
        BULK_WORKERS_DEFAULT = max(1, int(os.getenv("BULK_WORKERS")))
    except ValueError:
        pass
CODE_CACHE_PATH = Path(__file__).resolve().parents[1] / 'data' / 'all_codes.json'


def _refresh_proxy():
    """(Re)configure SESSION proxies using the configured API."""
    global _proxy_timestamp
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
    _proxy_timestamp = time.monotonic()


def _ensure_proxy():
    if not PROXY_API_URL:
        return
    global _proxy_timestamp
    now = time.monotonic()
    if _proxy_timestamp is None or (now - _proxy_timestamp) > PROXY_REFRESH_INTERVAL:
        _refresh_proxy()


_refresh_proxy()


def fetch_all_stock_codes(force_refresh: bool = False) -> List[str]:
    """Fetch all A-share stock codes (沪深、北交等)。"""
    if not force_refresh and CODE_CACHE_PATH.exists():
        try:
            data = json.loads(CODE_CACHE_PATH.read_text(encoding='utf-8'))
            if isinstance(data, list) and data:
                return data
        except Exception:
            pass

    codes: List[str] = []
    pn = 1
    while True:
        _ensure_proxy()
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get?"
            f"pn={pn}&pz=500&po=1&np=1&fltt=2&invt=2&fid=f3&fs={EM_FS_FILTERS}&fields=f12"
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
    try:
        CODE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CODE_CACHE_PATH.write_text(json.dumps(codes, ensure_ascii=False), encoding='utf-8')
    except Exception:
        pass
    return codes


def run_for_date(
    db_path: str,
    the_date: Optional[str] = None,
    limit: Optional[int] = None,
    codes_override: Optional[List[str]] = None,
    workers: int = BULK_WORKERS_DEFAULT,
    force_refresh: bool = False,
):
    start_time = time.perf_counter()
    _ensure_proxy()
    codes = codes_override or fetch_all_stock_codes(force_refresh=force_refresh)
    if limit:
        codes = codes[:limit]
    flows_collected: List[Dict] = []
    profile_map: Dict[Tuple[str, str], Dict[str, str]] = {}
    date_label = the_date or dt.date.today().strftime("%Y-%m-%d")
    print(f"正在读取 {date_label} …")

    def _worker(code: str) -> Tuple[List[Dict], Optional[Tuple[str, str, Dict[str, str]]]]:
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                flows = fetch_fund_flow_dayk(code, start=the_date, end=the_date)
                profile = fetch_basic_profile(code)
                stock, _market, exchange = parse_stock_code(code)
                name = extract_stock_name(profile)
                enriched = []
                for item in flows:
                    enriched.append({**item, "name": name})
                if not enriched:
                    LOGGER.warning("no flow data returned for %s on %s", code, the_date)
                return enriched, (stock, exchange, profile)
            except Exception as exc:
                last_exc = exc
                time.sleep(1 + attempt)
        if last_exc is not None:
            LOGGER.warning("failed to fetch %s for %s: %s", code, the_date, last_exc)
        return [], None

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(_worker, code): code for code in codes}
        for future in as_completed(futures):
            batch, profile_entry = future.result()
            if profile_entry is not None:
                stock, exchange, profile = profile_entry
                profile_map[(stock, exchange)] = profile
            if batch:
                flows_collected.extend(batch)
    if flows_collected or profile_map:
        save_to_sqlite(flows_collected, profile_map, db_path)
    elapsed = time.perf_counter() - start_time
    print(
        f"run_for_date({date_label}) processed {len(flows_collected)} records in {elapsed:.2f}s"
    )
    _refresh_proxy()


def run_full_range(
    db_path: str,
    end_date_str: str,
    limit: Optional[int] = None,
    workers: int = BULK_WORKERS_DEFAULT,
    force_refresh_codes: bool = False,
):
    try:
        end_date = dt.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid end date format: {end_date_str}") from exc

    codes = fetch_all_stock_codes(force_refresh=force_refresh_codes)
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
            run_for_date(
                db_path,
                current.strftime("%Y-%m-%d"),
                limit=limit,
                codes_override=codes,
                workers=workers,
            )
        current += dt.timedelta(days=1)


def run_full_history(
    db_path: str,
    limit: Optional[int] = None,
    workers: int = BULK_WORKERS_DEFAULT,
    force_refresh_codes: bool = False,
):
    _ensure_proxy()
    codes = fetch_all_stock_codes(force_refresh=force_refresh_codes)
    if limit:
        codes = codes[:limit]
    total = len(codes)

    def _worker(code: str) -> Tuple[List[Dict], Optional[Tuple[str, str, Dict[str, str]]]]:
        try:
            flows = fetch_fund_flow_dayk(code)
            profile = fetch_basic_profile(code)
            stock, _market, exchange = parse_stock_code(code)
            name = extract_stock_name(profile)
            enriched = [{**item, "name": name} for item in flows]
            return enriched, (stock, exchange, profile)
        except Exception as exc:
            LOGGER.warning("failed to fetch history for %s: %s", code, exc)
            return [], None

    flows_batch: List[Dict] = []
    profile_batch: Dict[Tuple[str, str], Dict[str, str]] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(_worker, code): code for code in codes}
        for idx, future in enumerate(as_completed(futures), 1):
            data, profile_entry = future.result()
            if profile_entry is not None:
                stock, exchange, profile = profile_entry
                profile_batch[(stock, exchange)] = profile
            if data:
                flows_batch.extend(data)
            if flows_batch and len(flows_batch) > 2000:
                save_to_sqlite(flows_batch, profile_batch, db_path)
                flows_batch.clear()
                profile_batch.clear()
            if idx % 200 == 0:
                print(f"Fetched {idx}/{total} stocks...")
    if flows_batch or profile_batch:
        save_to_sqlite(flows_batch, profile_batch, db_path)


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
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Fetch complete history for all codes in one pass",
    )
    parser.add_argument(
        "--workers",
        type=int,
        help="Number of concurrent workers (default 20)",
    )
    parser.add_argument(
        "--refresh-codes",
        action="store_true",
        help="Force refresh cached stock code list",
    )
    args = parser.parse_args()

    workers = max(1, args.workers or BULK_WORKERS_DEFAULT)
    if args.full_history:
        run_full_history(args.db, limit=args.limit, workers=workers, force_refresh_codes=args.refresh_codes)
    elif args.fill_to:
        run_full_range(args.db, args.fill_to, limit=args.limit, workers=workers, force_refresh_codes=args.refresh_codes)
    elif args.date:
        run_for_date(args.db, the_date=args.date, limit=args.limit, workers=workers, force_refresh=args.refresh_codes)
    elif args.schedule:
        scheduler_loop(args.db)
    else:
        # default: run once for today
        run_for_date(
            args.db,
            the_date=dt.date.today().strftime("%Y-%m-%d"),
            limit=args.limit,
            workers=workers,
            force_refresh=args.refresh_codes,
        )


if __name__ == "__main__":
    main()
