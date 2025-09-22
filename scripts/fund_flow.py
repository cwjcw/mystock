import argparse
import datetime as dt
import json
import os
import sqlite3
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple, TypeVar

import akshare as ak
import requests


T = TypeVar("T")


def _has_proxy_env() -> bool:
    for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
        if os.environ.get(key):
            return True
    return False


def _disable_proxies() -> None:
    for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
        os.environ.pop(key, None)
    os.environ.setdefault("NO_PROXY", "localhost,127.0.0.1")
    os.environ.setdefault("no_proxy", "localhost,127.0.0.1")


def _call_with_proxy_retry(fn: Callable[[], T], description: str) -> T:
    try:
        return fn()
    except requests.exceptions.ProxyError:
        if _has_proxy_env():
            print(f"检测到代理访问 {description} 失败，自动禁用代理后重试…")
            _disable_proxies()
            return fn()
        raise
    except requests.exceptions.RequestException:
        if _has_proxy_env():
            print(f"访问 {description} 出现网络异常，自动禁用代理后重试…")
            _disable_proxies()
            return fn()
        raise


def parse_stock_code(code: str) -> Tuple[str, str, str]:
    """Return (stock, market, exchange) for AKShare interfaces."""
    raw = code.strip()
    if not raw:
        raise ValueError("Empty stock code")

    stock: Optional[str] = None
    exchange: Optional[str] = None

    if "." in raw:
        parts = raw.split(".")
        if len(parts) != 2:
            raise ValueError(f"Unrecognized stock code: {code}")
        stock_part, exch_part = parts
        stock = stock_part[-6:]
        exchange = exch_part.upper()
    else:
        lowered = raw.lower()
        if lowered.startswith("sh") or lowered.startswith("sz") or lowered.startswith("bj"):
            exchange = lowered[:2].upper()
            stock = raw[2:][-6:]
        else:
            cleaned = raw[-6:]
            if not cleaned.isdigit():
                raise ValueError(f"Unrecognized stock code: {code}")
            stock = cleaned
            if cleaned.startswith(("600", "601", "603", "605", "688")):
                exchange = "SH"
            elif cleaned.startswith(("000", "001", "002", "003", "300", "301")):
                exchange = "SZ"
            elif cleaned.startswith(("430", "688", "830", "831", "833", "835", "836", "838", "839", "870", "871", "872")):
                exchange = "BJ"
            else:
                exchange = "SH"

    if not stock or len(stock) != 6 or not stock.isdigit():
        raise ValueError(f"Unrecognized stock code: {code}")
    if exchange not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"Unsupported exchange for code {code}")

    market_map = {"SH": "sh", "SZ": "sz", "BJ": "bj"}
    return stock, market_map[exchange], exchange


def fetch_fund_flow_dayk(
    code: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[Dict]:
    """
    Fetch daily funds flow via AKShare stock_individual_fund_flow.
    Returns list of dicts per trading day containing flow metrics (单位: 元 / %).
    """
    stock, market, exchange = parse_stock_code(code)
    df = _call_with_proxy_retry(
        lambda: ak.stock_individual_fund_flow(stock=stock, market=market),
        description="AKShare stock_individual_fund_flow",
    )
    if df is None or df.empty:
        return []

    df = df.copy()
    df["日期"] = df["日期"].astype(str)
    if start:
        df = df[df["日期"] >= start]
    if end:
        df = df[df["日期"] <= end]

    df = df.sort_values("日期")

    records: List[Dict] = []
    for row in df.to_dict(orient="records"):
        records.append(
            {
                "code": stock,
                "exchange": exchange,
                "date": row.get("日期"),
                "close": row.get("收盘价"),
                "pct_chg": row.get("涨跌幅"),
                "main": row.get("主力净流入-净额"),
                "main_ratio": row.get("主力净流入-净占比"),
                "ultra_large": row.get("超大单净流入-净额"),
                "ultra_large_ratio": row.get("超大单净流入-净占比"),
                "large": row.get("大单净流入-净额"),
                "large_ratio": row.get("大单净流入-净占比"),
                "medium": row.get("中单净流入-净额"),
                "medium_ratio": row.get("中单净流入-净占比"),
                "small": row.get("小单净流入-净额"),
                "small_ratio": row.get("小单净流入-净占比"),
            }
        )

    return records

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


def _init_db(conn: sqlite3.Connection):
    # 资金流表 + 基本信息表（雪球字段）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_basic_info_xq (
            "代码" TEXT NOT NULL,
            "交易所" TEXT NOT NULL,
            "字段" TEXT NOT NULL,
            "值" TEXT,
            "更新时间" TEXT,
            PRIMARY KEY ("代码", "交易所", "字段")
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fund_flow_daily (
            "代码" TEXT NOT NULL,
            "交易所" TEXT NOT NULL,
            "日期" TEXT NOT NULL,
            "收盘价" REAL,
            "涨跌幅" REAL,
            "主力净流入-净额" REAL,
            "主力净流入-净占比" REAL,
            "超大单净流入-净额" REAL,
            "超大单净流入-净占比" REAL,
            "大单净流入-净额" REAL,
            "大单净流入-净占比" REAL,
            "中单净流入-净额" REAL,
            "中单净流入-净占比" REAL,
            "小单净流入-净额" REAL,
            "小单净流入-净占比" REAL,
            "名称" TEXT,
            PRIMARY KEY ("代码", "交易所", "日期")
        )
        """
    )

    cols = {row[1] for row in conn.execute('PRAGMA table_info("fund_flow_daily")')}
    rename_map = {
        "主力": "主力净流入-净额",
        "超大单": "超大单净流入-净额",
        "大单": "大单净流入-净额",
        "中单": "中单净流入-净额",
        "小单": "小单净流入-净额",
    }
    for old, new in rename_map.items():
        if old in cols and new not in cols:
            conn.execute(f'ALTER TABLE fund_flow_daily RENAME COLUMN "{old}" TO "{new}"')

    def _drop_column(column: str) -> None:
        try:
            conn.execute(f'ALTER TABLE fund_flow_daily DROP COLUMN "{column}"')
        except sqlite3.OperationalError:
            pass

    # Remove obsolete columns if they still exist
    _drop_column("总市值")
    _drop_column("最新价")

    cols = {row[1] for row in conn.execute('PRAGMA table_info("fund_flow_daily")')}
    required_cols = {
        "收盘价": "REAL",
        "涨跌幅": "REAL",
        "主力净流入-净额": "REAL",
        "主力净流入-净占比": "REAL",
        "超大单净流入-净额": "REAL",
        "超大单净流入-净占比": "REAL",
        "大单净流入-净额": "REAL",
        "大单净流入-净占比": "REAL",
        "中单净流入-净额": "REAL",
        "中单净流入-净占比": "REAL",
        "小单净流入-净额": "REAL",
        "小单净流入-净占比": "REAL",
        "名称": "TEXT",
    }
    for col, col_type in required_cols.items():
        if col not in cols:
            conn.execute(f'ALTER TABLE fund_flow_daily ADD COLUMN "{col}" {col_type}')

def fetch_basic_profile(
    code: str,
    *,
    token: Optional[str] = None,
    timeout: Optional[float] = None,
) -> Dict[str, str]:
    stock, _market, exchange = parse_stock_code(code)
    symbol = f"{exchange}{stock}"
    try:
        df = _call_with_proxy_retry(
            lambda: ak.stock_individual_basic_info_xq(symbol=symbol, token=token, timeout=timeout),
            description="AKShare stock_individual_basic_info_xq",
        )
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    profile = {}
    for record in df.to_dict(orient="records"):
        item = str(record.get("item"))
        value = record.get("value")
        profile[item] = "" if value is None else str(value)
    return profile


def extract_stock_name(profile: Dict[str, str]) -> Optional[str]:
    preferred_keys = [
        "org_short_name_cn",
        "org_name_cn",
        "org_short_name_en",
        "org_name_en",
    ]
    for key in preferred_keys:
        if profile.get(key):
            return profile[key]
    return None


def save_to_sqlite(
    flows: Iterable[Dict],
    profiles: Dict[Tuple[str, str], Dict[str, str]],
    db_path: str,
):
    flow_list = list(flows)
    if not flow_list and not profiles:
        return

    p = Path(db_path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(p))
    try:
        _init_db(conn)
        now_iso = dt.datetime.now().isoformat(timespec="seconds")

        if profiles:
            basic_rows = []
            for (code, exchange), data in profiles.items():
                for field, value in data.items():
                    basic_rows.append((code, exchange, field, value, now_iso))
            if basic_rows:
                conn.executemany(
                    """
                    INSERT INTO stock_basic_info_xq ("代码","交易所","字段","值","更新时间")
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT("代码","交易所","字段") DO UPDATE SET
                        "值"=excluded."值",
                        "更新时间"=excluded."更新时间"
                    """,
                    basic_rows,
                )

        def to_float(val: Optional[float]) -> Optional[float]:
            if val is None:
                return None
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        def to_pct(val: Optional[float]) -> Optional[float]:
            fval = to_float(val)
            if fval is None:
                return None
            return round(fval, 2)

        def to_amount(val: Optional[float]) -> Optional[float]:
            fval = to_float(val)
            if fval is None:
                return None
            return round(fval / 1e8, 4)

        flow_rows = []
        for row in flow_list:
            if not row.get("date"):
                continue
            flow_rows.append(
                (
                    row.get("code"),
                    row.get("exchange"),
                    row.get("date"),
                    to_float(row.get("close")),
                    to_pct(row.get("pct_chg")),
                    to_amount(row.get("main")),
                    to_pct(row.get("main_ratio")),
                    to_amount(row.get("ultra_large")),
                    to_pct(row.get("ultra_large_ratio")),
                    to_amount(row.get("large")),
                    to_pct(row.get("large_ratio")),
                    to_amount(row.get("medium")),
                    to_pct(row.get("medium_ratio")),
                    to_amount(row.get("small")),
                    to_pct(row.get("small_ratio")),
                    row.get("name"),
                )
            )

        if flow_rows:
            conn.executemany(
                """
                INSERT INTO fund_flow_daily (
                    "代码","交易所","日期","收盘价","涨跌幅",
                    "主力净流入-净额","主力净流入-净占比",
                    "超大单净流入-净额","超大单净流入-净占比",
                    "大单净流入-净额","大单净流入-净占比",
                    "中单净流入-净额","中单净流入-净占比",
                    "小单净流入-净额","小单净流入-净占比",
                    "名称"
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT("代码","交易所","日期") DO UPDATE SET
                    "收盘价"=excluded."收盘价",
                    "涨跌幅"=excluded."涨跌幅",
                    "主力净流入-净额"=excluded."主力净流入-净额",
                    "主力净流入-净占比"=excluded."主力净流入-净占比",
                    "超大单净流入-净额"=excluded."超大单净流入-净额",
                    "超大单净流入-净占比"=excluded."超大单净流入-净占比",
                    "大单净流入-净额"=excluded."大单净流入-净额",
                    "大单净流入-净占比"=excluded."大单净流入-净占比",
                    "中单净流入-净额"=excluded."中单净流入-净额",
                    "中单净流入-净占比"=excluded."中单净流入-净占比",
                    "小单净流入-净额"=excluded."小单净流入-净额",
                    "小单净流入-净占比"=excluded."小单净流入-净占比",
                    "名称"=excluded."名称"
                """,
                flow_rows,
            )

        conn.commit()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Fetch A-share fund flow via AKShare")
    parser.add_argument("codes", nargs="+", help="Stock codes like 600519, sh600519, 000001.SZ")
    parser.add_argument("--date", dest="date", help="Specific date YYYY-MM-DD")
    parser.add_argument("--start", dest="start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", dest="end", help="End date YYYY-MM-DD")
    parser.add_argument("--all-days", action="store_true", help="Output all days in range instead of only the latest")
    parser.add_argument("--json", action="store_true", help="Output JSON lines instead of table")
    parser.add_argument("--db", dest="db_path", help="SQLite file path to save results")
    parser.add_argument("--xq-token", dest="xq_token", help="Override Xueqiu token for basic info")
    parser.add_argument("--timeout", type=float, default=None, help="Request timeout for Xueqiu basic info")
    parser.add_argument("--earliest", action="store_true", help="Only print earliest available date for each code")
    args = parser.parse_args()

    start = args.start or args.date
    end = args.end or args.date

    if args.earliest:
        for c in args.codes:
            earliest = earliest_fund_flow_date(c)
            print(f"{c}: earliest date = {earliest}")
        return

    flows_for_output: List[Dict] = []
    profile_map: Dict[Tuple[str, str], Dict[str, str]] = {}

    for code_input in args.codes:
        stock, _market, exchange = parse_stock_code(code_input)
        profile = fetch_basic_profile(code_input, token=args.xq_token, timeout=args.timeout)
        profile_map[(stock, exchange)] = profile
        name = extract_stock_name(profile)

        flows = fetch_fund_flow_dayk(code_input, start=start, end=end)
        if not args.all_days and not (start or end):
            flows = flows[-1:] if flows else []

        if not flows:
            flows_for_output.append(
                {
                    "code": stock,
                    "exchange": exchange,
                    "date": start if start == end else None,
                    "close": None,
                    "pct_chg": None,
                    "main": None,
                    "main_ratio": None,
                    "ultra_large": None,
                    "ultra_large_ratio": None,
                    "large": None,
                    "large_ratio": None,
                    "medium": None,
                    "medium_ratio": None,
                    "small": None,
                    "small_ratio": None,
                    "name": name,
                }
            )
            continue

        for f in flows:
            flows_for_output.append({**f, "name": name})

    if args.db_path:
        save_to_sqlite(flows_for_output, profile_map, args.db_path)

    def _to_float(value: Optional[float]) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _to_pct(value: Optional[float]) -> Optional[float]:
        val = _to_float(value)
        if val is None:
            return None
        return round(val, 2)

    def to_cn_record(r: Dict) -> Dict:
        return {
            "日期": r.get("date"),
            "代码": r.get("code"),
            "名称": r.get("name"),
            "交易所": r.get("exchange"),
            "收盘价": _to_float(r.get("close")),
            "涨跌幅": _to_pct(r.get("pct_chg")),
            "主力净流入-净额": _to_float(r.get("main")),
            "主力净流入-净占比": _to_pct(r.get("main_ratio")),
            "超大单净流入-净额": _to_float(r.get("ultra_large")),
            "超大单净流入-净占比": _to_pct(r.get("ultra_large_ratio")),
            "大单净流入-净额": _to_float(r.get("large")),
            "大单净流入-净占比": _to_pct(r.get("large_ratio")),
            "中单净流入-净额": _to_float(r.get("medium")),
            "中单净流入-净占比": _to_pct(r.get("medium_ratio")),
            "小单净流入-净额": _to_float(r.get("small")),
            "小单净流入-净占比": _to_pct(r.get("small_ratio")),
        }

    if args.json:
        for r in flows_for_output:
            print(json.dumps(to_cn_record(r), ensure_ascii=False))
        return

    cols = [
        "日期",
        "代码",
        "名称",
        "交易所",
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
    ]
    header = "\t".join(cols)
    print(header)
    for r in flows_for_output:
        cn = to_cn_record(r)
        row = [cn.get(k, "") for k in cols]
        out = []
        for k, v in zip(cols, row):
            if isinstance(v, float):
                if k == "涨跌幅" or "占比" in k:
                    out.append(f"{v:.2f}%")
                else:
                    out.append(f"{v:.2f}")
            elif isinstance(v, (int,)):
                out.append(str(v))
            else:
                out.append(str(v) if v is not None else "")
        print("\t".join(out))


if __name__ == "__main__":
    main()
