import argparse
import datetime as dt
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
import requests

try:
    from akshare.utils import tqdm as ak_tqdm  # type: ignore

    ak_tqdm.get_tqdm = lambda enable=True: (lambda iterable, *args, **kwargs: iterable)
except Exception:
    pass

try:
    from akshare.stock import stock_fund_em  # type: ignore

    stock_fund_em.get_tqdm = lambda enable=True: (lambda iterable, *args, **kwargs: iterable)
except Exception:
    pass


INDICATOR_CHOICES = ["今日", "3日", "5日", "10日"]

EM_HEADERS = {
    "Referer": "https://quote.eastmoney.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
}
# 仅查询沪深 A 股（含创业板、科创板），剔除指数、债券等
EM_FS_FILTERS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"


def disable_proxies() -> None:
    for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
        os.environ.pop(key, None)
    os.environ.setdefault("NO_PROXY", "*")
    os.environ.setdefault("no_proxy", "*")


def set_custom_proxy(proxy: str) -> None:
    if not proxy:
        return
    for key in ["http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY"]:
        os.environ[key] = proxy
    # allow localhost bypass
    os.environ.setdefault("NO_PROXY", "localhost,127.0.0.1")
    os.environ.setdefault("no_proxy", "localhost,127.0.0.1")


def normalize_code(code: str) -> str:
    cleaned = code.strip().upper()
    for suffix in (".SH", ".SZ", ".BJ"):
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-3]
    if cleaned.startswith("SH") or cleaned.startswith("SZ") or cleaned.startswith("BJ"):
        cleaned = cleaned[2:]
    return cleaned.zfill(6)


def parse_targets(pairs: List[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for item in pairs:
        if "=" in item:
            alias, code = item.split("=", 1)
            mapping[alias.strip()] = normalize_code(code)
        else:
            code = normalize_code(item)
            mapping[code] = code
    return mapping


def _has_proxy_env() -> bool:
    for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
        if os.environ.get(key):
            return True
    return False


def fetch_rank(indicator: str, *, _fallback: bool = True) -> pd.DataFrame:
    try:
        df = ak.stock_individual_fund_flow_rank(indicator=indicator)
    except requests.exceptions.ProxyError as exc:
        if _fallback and _has_proxy_env():
            print("检测到代理连接失败，自动禁用代理后重试…")
            disable_proxies()
            return fetch_rank(indicator, _fallback=False)
        raise
    except requests.exceptions.RequestException as exc:
        if _fallback and _has_proxy_env():
            print("检测到网络异常，自动禁用代理后重试…")
            disable_proxies()
            return fetch_rank(indicator, _fallback=False)
        raise

    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    rename_map: Dict[str, str] = {}
    for col in df.columns:
        for prefix in INDICATOR_CHOICES:
            if col.startswith(prefix) and col not in {"序号", "代码", "名称", "最新价"}:
                rename_map[col] = col[len(prefix) :]
                break
    df.rename(columns=rename_map, inplace=True)
    df["代码"] = df["代码"].astype(str).apply(normalize_code)
    for col in df.columns:
        if col in {"代码", "名称"}:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["指标"] = indicator
    return df


def fetch_all_realtime_flow(page_size: int = 500, *, _fallback: bool = True) -> pd.DataFrame:
    def _fetch_all_pages() -> pd.DataFrame:
        rows: List[Dict] = []
        pn = 1
        total: Optional[int] = None
        while True:
            params = {
                "pn": pn,
                "pz": page_size,
                "po": 1,
                "np": 1,
                "fltt": 2,
                "invt": 2,
                "fid": "f62",
                "fs": EM_FS_FILTERS,
                "fields": ",".join(
                    [
                        "f12",  # code
                        "f14",  # name
                        "f2",  # last price
                        "f3",  # pct chg
                        "f62",  # main net
                        "f184",  # main ratio
                        "f66",  # ultra large net
                        "f69",  # ultra large ratio
                        "f72",  # large net
                        "f75",  # large ratio
                        "f78",  # medium net
                        "f81",  # medium ratio
                        "f84",  # small net
                        "f87",  # small ratio
                    ]
                ),
            }
            r = requests.get(
                "https://push2.eastmoney.com/api/qt/clist/get",
                params=params,
                headers=EM_HEADERS,
                timeout=10,
            )
            r.raise_for_status()
            payload = r.json()
            data = (payload or {}).get("data") or {}
            diff = data.get("diff") or []
            if not diff:
                break
            rows.extend(diff)
            if total is None:
                try:
                    total = int(data.get("total")) if data.get("total") is not None else None
                except (TypeError, ValueError):
                    total = None
            if total is not None:
                pages = (total + page_size - 1) // page_size
                if pn >= pages:
                    break
            pn += 1

        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        rename_map = {
            "f12": "代码",
            "f14": "名称",
            "f2": "最新价",
            "f3": "涨跌幅",
            "f62": "主力净流入-净额",
            "f184": "主力净流入-净占比",
            "f66": "超大单净流入-净额",
            "f69": "超大单净流入-净占比",
            "f72": "大单净流入-净额",
            "f75": "大单净流入-净占比",
            "f78": "中单净流入-净额",
            "f81": "中单净流入-净占比",
            "f84": "小单净流入-净额",
            "f87": "小单净流入-净占比",
        }
        df.rename(columns=rename_map, inplace=True)
        df.insert(0, "序号", range(1, len(df) + 1))
        df["代码"] = df["代码"].astype(str).apply(normalize_code)
        for col in df.columns:
            if col in {"序号", "代码", "名称"}:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["指标"] = "今日"
        return df

    try:
        return _fetch_all_pages()
    except requests.exceptions.ProxyError:
        if _fallback and _has_proxy_env():
            print("检测到代理连接失败，自动禁用代理后重试…")
            disable_proxies()
            return fetch_all_realtime_flow(page_size, _fallback=False)
        raise
    except requests.exceptions.RequestException:
        if _fallback and _has_proxy_env():
            print("检测到网络异常，自动禁用代理后重试…")
            disable_proxies()
            return fetch_all_realtime_flow(page_size, _fallback=False)
        raise


def filter_for_targets(df: pd.DataFrame, targets: Dict[str, str]) -> pd.DataFrame:
    if df.empty or not targets:
        return df
    codes = list(targets.values())
    flt = df[df["代码"].isin(codes)].copy()
    return flt


def render_targets(df: pd.DataFrame, targets: Dict[str, str]) -> None:
    if not targets:
        return
    if df.empty:
        print("暂无排名数据；稍后再试。")
        return
    key_cols = [
        "序号",
        "代码",
        "名称",
        "最新价",
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
    display_df = filter_for_targets(df, targets)
    for alias, code in targets.items():
        row = display_df[display_df["代码"] == code]
        if row.empty:
            print(f"[{alias}] {code}: 未进入榜单")
            continue
        r = row.iloc[0]
        parts = [
            f"[{alias}] 序号:{int(r['序号'])}",
            f"名称:{r['名称']}",
            f"价格:{r['最新价']:.2f}" if pd.notna(r['最新价']) else "价格:-",
            f"涨跌幅:{r['涨跌幅']:.2f}%" if pd.notna(r['涨跌幅']) else "涨跌幅:-",
            f"主力净额:{r['主力净流入-净额']/1e8:.2f}亿" if pd.notna(r['主力净流入-净额']) else "主力净额:-",
            f"主力占比:{r['主力净流入-净占比']:.2f}%" if pd.notna(r['主力净流入-净占比']) else "主力占比:-",
            f"超大单:{r['超大单净流入-净额']/1e8:.2f}亿" if pd.notna(r['超大单净流入-净额']) else "超大单:-",
            f"大单:{r['大单净流入-净额']/1e8:.2f}亿" if pd.notna(r['大单净流入-净额']) else "大单:-",
            f"中单:{r['中单净流入-净额']/1e8:.2f}亿" if pd.notna(r['中单净流入-净额']) else "中单:-",
            f"小单:{r['小单净流入-净额']/1e8:.2f}亿" if pd.notna(r['小单净流入-净额']) else "小单:-",
        ]
        print(" | ".join(parts))


def render_top(df: pd.DataFrame, top: int) -> None:
    if df.empty:
        print("暂无排名数据；稍后再试。")
        return
    subset = df.head(top)
    cols = [
        "序号",
        "代码",
        "名称",
        "最新价",
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
    for _, row in subset.iterrows():
        out: List[str] = []
        for col in cols:
            val = row.get(col)
            if pd.isna(val):
                out.append("-")
            elif col in {"序号"}:
                out.append(str(int(val)))
            elif col in {"代码", "名称"}:
                out.append(str(val))
            elif col in {"最新价"}:
                out.append(f"{float(val):.2f}")
            elif col in {
                "涨跌幅",
                "主力净流入-净占比",
                "超大单净流入-净占比",
                "大单净流入-净占比",
                "中单净流入-净占比",
                "小单净流入-净占比",
            }:
                out.append(f"{float(val):.2f}%")
            else:
                out.append(f"{float(val)/1e8:.2f}亿")
        print("\t".join(out))


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fund_flow_rank (
            "采集时间" TEXT NOT NULL,
            "指标" TEXT NOT NULL,
            "序号" INTEGER,
            "代码" TEXT NOT NULL,
            "名称" TEXT,
            "最新价" REAL,
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
            PRIMARY KEY ("采集时间", "指标", "代码")
        )
        """
    )


def save_rank_to_db(df: pd.DataFrame, indicator: str, db_path: str) -> None:
    if df.empty:
        return
    p = Path(db_path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    try:
        _init_db(conn)
        ts = dt.datetime.now().isoformat(timespec="seconds")
        rows = []
        cols = [
            "序号",
            "代码",
            "名称",
            "最新价",
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
        for _, row in df.iterrows():
            payload = [ts, indicator]
            for col in cols:
                payload.append(row.get(col))
            rows.append(tuple(payload))
        conn.executemany(
            """
            INSERT INTO fund_flow_rank (
                "采集时间","指标","序号","代码","名称","最新价","涨跌幅",
                "主力净流入-净额","主力净流入-净占比",
                "超大单净流入-净额","超大单净流入-净占比",
                "大单净流入-净额","大单净流入-净占比",
                "中单净流入-净额","中单净流入-净占比",
                "小单净流入-净额","小单净流入-净占比"
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT("采集时间","指标","代码") DO UPDATE SET
                "名称"=excluded."名称",
                "最新价"=excluded."最新价",
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
                "小单净流入-净占比"=excluded."小单净流入-净占比"
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Realtime fund-flow ranking via AKShare stock_individual_fund_flow_rank"
    )
    parser.add_argument(
        "targets",
        nargs="*",
        help="Codes or alias=code pairs (e.g. 中科曙光=603019). If omitted, show top list.",
    )
    parser.add_argument(
        "--indicator",
        choices=INDICATOR_CHOICES,
        default="今日",
        help="Ranking indicator window (default: 今日)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch all A-share realtime fund flow via Eastmoney list API (today only)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=500,
        help="Page size for --all (default: 500)",
    )
    parser.add_argument("--top", type=int, default=20, help="Rows to display when no targets provided")
    parser.add_argument("--interval", type=int, default=60, help="Polling interval seconds")
    parser.add_argument("--once", action="store_true", help="Fetch once and exit")
    parser.add_argument("--db", dest="db_path", help="Optional SQLite database to store rankings")
    parser.add_argument(
        "--proxy",
        default="system",
        help="Proxy setting: system (default), none, or http[s]://host:port",
    )
    args = parser.parse_args()

    targets = parse_targets(args.targets)

    proxy_arg = (args.proxy or "system").strip()
    if proxy_arg.lower() == "none":
        disable_proxies()
    elif proxy_arg.lower() == "system":
        # leave environment untouched
        pass
    else:
        set_custom_proxy(proxy_arg)

    while True:
        start = time.time()
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n== {timestamp} {args.indicator} ==")
        try:
            if args.all:
                if args.indicator != "今日":
                    print("提示: --all 仅支持今日实时资金流，已自动切换为 今日。")
                    args.indicator = "今日"
                df = fetch_all_realtime_flow(page_size=max(1, args.page_size))
            else:
                df = fetch_rank(args.indicator)
        except Exception as exc:
            print(f"获取排名数据失败: {exc}")
            df = pd.DataFrame()

        if targets:
            render_targets(df, targets)
        else:
            render_top(df, args.top)

        if args.db_path:
            try:
                save_rank_to_db(df, args.indicator, args.db_path)
            except Exception as exc:
                print(f"写入数据库失败: {exc}")

        if args.once:
            break

        elapsed = time.time() - start
        sleep_sec = max(0, args.interval - elapsed)
        time.sleep(sleep_sec)


if __name__ == "__main__":
    main()
