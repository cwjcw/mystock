import argparse
import asyncio
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Optional, Tuple, List
import aiohttp
import pandas as pd
import datetime as dt


DEFAULT_HEADERS = {
    "Referer": "https://quote.eastmoney.com/",
    "User-Agent": "Mozilla/5.0"
}


def symbol_to_secid(symbol: str) -> str:
    code, exch = symbol.upper().split(".")
    return ("1." if exch == "SH" else "0.") + code


def parse_kline_line(line: str):
    parts = line.split(",")
    def to_float(x):
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


async def fetch_latest_minute(session: aiohttp.ClientSession, secid: str) -> Tuple[Optional[str], Optional[dict]]:
    url = "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "1",
        "lmt": "1",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    }
    try:
        async with session.get(url, params=params, headers=DEFAULT_HEADERS, timeout=10) as resp:
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
    row = parse_kline_line(kl[-1])
    # scale to 亿元 with 2 decimals
    for k in ["主力", "超大单", "大单", "中单", "小单"]:
        v = row.get(k)
        row[k] = None if v is None else round(v / 1e8, 2)
    return name, row


async def fetch_quote_basic(session: aiohttp.ClientSession, secid: str) -> Optional[dict]:
    """Fetch realtime quote basics: latest price, change pct, total mkt cap."""
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": secid,
        "fields": "f58,f116,f117,f43,f170,f169,f152",
    }
    try:
        async with session.get(url, params=params, headers=DEFAULT_HEADERS, timeout=10) as resp:
            if resp.status != 200:
                return None
            j = await resp.json(content_type=None)
    except Exception:
        return None
    d = (j or {}).get("data") or {}
    price = (d.get("f43") or 0) / 100.0
    change_pct = (d.get("f170") or 0) / 100.0
    mcap = d.get("f116")  # RMB
    name = d.get("f58")
    return {"name": name, "price": price, "change_pct": change_pct, "market_cap": mcap}





def ensure_channel(tree: Optional[ET.ElementTree], title: str, link: str, desc: str) -> ET.ElementTree:
    if tree is None:
        rss = ET.Element("rss", version="2.0")
        channel = ET.SubElement(rss, "channel")
        ET.SubElement(channel, "title").text = title
        ET.SubElement(channel, "link").text = link
        ET.SubElement(channel, "description").text = desc
        ET.SubElement(channel, "lastBuildDate").text = time.strftime("%a, %d %b %Y %H:%M:%S %z")
        return ET.ElementTree(rss)
    else:
        return tree


def read_existing_guids(path: Path) -> set:
    if not path.exists():
        return set()
    try:
        tree = ET.parse(str(path))
        root = tree.getroot()
        guids = set()
        for item in root.findall("./channel/item"):
            guid = item.findtext("guid")
            if guid:
                guids.add(guid)
        return guids
    except Exception:
        return set()


def append_items(path: Path, items: List[dict], feed_title: str = "资金流RSS"):
    existing = None
    if path.exists():
        try:
            existing = ET.parse(str(path))
        except Exception:
            existing = None
    tree = ensure_channel(existing, feed_title, "https://quote.eastmoney.com/", "A股分钟级资金流")
    root = tree.getroot()
    channel = root.find("channel")
    seen = read_existing_guids(path)

    for it in items:
        if it["guid"] in seen:
            continue
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = it["title"]
        ET.SubElement(item, "description").text = it["description"]
        ET.SubElement(item, "guid").text = it["guid"]
        ET.SubElement(item, "pubDate").text = it["pubDate"]

    # Trim size
    max_items = 500
    items_xml = channel.findall("item")
    if len(items_xml) > max_items:
        for old in items_xml[: len(items_xml) - max_items]:
            channel.remove(old)

    channel.find("lastBuildDate").text = time.strftime("%a, %d %b %Y %H:%M:%S %z")
    path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(str(path), encoding="utf-8", xml_declaration=True)


async def run_once(symbols: Dict[str, str], rss_path: str, use_proxy: bool = False) -> None:
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector, trust_env=use_proxy) as session:
        tasks = []
        mapping = {name: symbol_to_secid(sym) for name, sym in symbols.items()}
        for name, secid in mapping.items():
            tasks.append(fetch_latest_minute(session, secid))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # fetch quotes in parallel
        qtasks = [fetch_quote_basic(session, secid) for secid in mapping.values()]
        qresults = await asyncio.gather(*qtasks, return_exceptions=True)

    now = dt.datetime.now()
    items = []
    for (name, symbol), result, q in zip(symbols.items(), results, qresults):
        if isinstance(result, Exception):
            continue
        ret_name, row = result
        if not row:
            continue
        if isinstance(q, Exception) or q is None:
            quote = {"price": None, "change_pct": None, "market_cap": None}
        else:
            quote = q
        guid = f"{symbol}_{row['time']}"
        title = f"{name} / {ret_name or quote.get('name') or ''} {row['time']}"
        mcap_yi = None if quote.get("market_cap") is None else round(quote["market_cap"] / 1e8, 2)
        price = quote.get("price")
        chg = quote.get("change_pct")
        chg_txt = "-" if chg is None else f"{chg:.2f}%"
        price_txt = "-" if price is None else f"{price:.2f}"
        mcap_txt = "-" if mcap_yi is None else f"{mcap_yi:.2f}亿"
        # 按你的要求修正标签与数值的对应关系：
        # 顺序仍为：主力，超大单，大单，中单，小单
        # 但数值映射为：
        #   主力 -> row['主力']
        #   超大单 -> row['小单']
        #   大单 -> row['中单']
        #   中单 -> row['大单']
        #   小单 -> row['超大单']
        desc = (
            f"最新价: {price_txt}\n"
            f"涨跌幅: {chg_txt}\n"
            f"总市值: {mcap_txt}\n"
            f"主力: {row['主力']} 亿元\n"
            f"超大单: {row['小单']} 亿元\n"
            f"大单: {row['中单']} 亿元\n"
            f"中单: {row['大单']} 亿元\n"
            f"小单: {row['超大单']} 亿元"
        )
        pub = now.strftime("%a, %d %b %Y %H:%M:%S %z")
        items.append({"guid": guid, "title": title, "description": desc, "pubDate": pub})

    append_items(Path(rss_path), items)





def parse_pairs(pairs: List[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for p in pairs:
        if "=" in p:
            n, s = p.split("=", 1)
            m[n] = s
        else:
            m[p] = p
    return m


def main():
    parser = argparse.ArgumentParser(description="一次性抓取并生成RSS（分钟资金流）")
    parser.add_argument("pairs", nargs="*", help="Name=Symbol or Symbol (e.g., 山子高科=000981.SZ)")
    parser.add_argument("--rss", default="data/fund_flow.rss", help="RSS输出文件路径")
    parser.add_argument("--use-proxy", action="store_true", help="使用系统代理")
    args = parser.parse_args()

    symbols = parse_pairs(args.pairs) if args.pairs else {
        "山子高科": "000981.SZ",
        "圣邦股份": "300661.SZ",
        "中科曙光": "603019.SH",
        "阿尔特": "300825.SZ",
        "三博脑科": "301293.SZ",
    }

    asyncio.run(run_once(symbols, args.rss, use_proxy=args.use_proxy))


if __name__ == "__main__":
    main()
