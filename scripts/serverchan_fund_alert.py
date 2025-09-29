"""Monitor main fund inflow and push ServerChan alerts."""

from __future__ import annotations

import datetime as dt
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import akshare as ak
import pandas as pd
import requests

# MySQL access for watchlist
try:
    from mysql_utils import connect_mysql  # type: ignore
except ImportError:  # pragma: no cover - fallback when running as module
    try:
        from .mysql_utils import connect_mysql  # type: ignore
    except ImportError:  # pragma: no cover
        connect_mysql = None  # type: ignore

try:
    import pymysql  # type: ignore
    from pymysql.cursors import DictCursor  # type: ignore
except Exception:  # pragma: no cover - pymysql optional for read-only mode
    pymysql = None  # type: ignore
    DictCursor = None  # type: ignore

REFRESH_INTERVAL_SECONDS = int(os.environ.get("FUND_ALERT_INTERVAL", "600"))

TRADING_SESSIONS: Tuple[Tuple[dt.time, dt.time], ...] = (
    (dt.time(hour=9, minute=30), dt.time(hour=11, minute=30)),
    (dt.time(hour=13, minute=0), dt.time(hour=15, minute=0)),
)

ENV_FILE_CANDIDATES = (
    Path(".env"),
    Path(__file__).resolve().parent.parent / ".env",
)


@dataclass
class StockSnapshot:
    code: str
    name: str
    amount: Optional[float]
    pct: Optional[float]

    def amount_text(self) -> str:
        if self.amount is None:
            return "-"
        return f"{self.amount / 1e8:.2f} 亿"

    def pct_text(self) -> str:
        if self.pct is None:
            return "-"
        return f"{self.pct:+.2f}%"


@dataclass
class WatchItem:
    symbol: str
    code: str
    name: str


@dataclass
class UserWatchConfig:
    user_id: int
    send_key: Optional[str]
    items: List[WatchItem]


def normalize_code(code: str) -> str:
    cleaned = code.strip().upper()
    if not cleaned:
        return ""
    if "." in cleaned:
        parts = cleaned.split(".")
        if len(parts) == 2:
            cleaned = parts[0][-6:]
    for prefix in ("SH", "SZ", "BJ"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :]
    return cleaned[-6:].zfill(6)


def parse_stock_code(code: str) -> Tuple[str, str, str]:
    raw = code.strip().upper()
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
        if lowered.startswith(("sh", "sz", "bj")):
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
            elif cleaned.startswith(("430", "830", "831", "833", "835", "836", "838", "839", "870", "871", "872")):
                exchange = "BJ"
            else:
                exchange = "SH"

    if not stock or len(stock) != 6 or not stock.isdigit():
        raise ValueError(f"Unrecognized stock code: {code}")
    if exchange not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"Unsupported exchange for code {code}")

    market_map = {"SH": "sh", "SZ": "sz", "BJ": "bj"}
    return stock, market_map[exchange], exchange


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


def load_env() -> None:
    for path in ENV_FILE_CANDIDATES:
        if not path.exists():
            continue
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and value and key not in os.environ:
                os.environ[key] = value


def load_user_watch_configs() -> List[UserWatchConfig]:
    global _SEND_KEY_COLUMN_WARNED, _SEND_KEY_COLUMN_ATTEMPTED
    dsn = os.environ.get("APP_MYSQL_DSN")
    if not dsn or connect_mysql is None or pymysql is None or DictCursor is None:
        return []
    try:
        conn = connect_mysql(dsn, cursorclass=DictCursor)
    except Exception as exc:  # pragma: no cover - MySQL unavailable
        print(f"读取自选股列表失败: {exc}", file=sys.stderr)
        return []

    users: Dict[int, UserWatchConfig] = {}
    try:
        with conn.cursor() as cursor:  # type: ignore[call-arg]
            query = (
                """
                SELECT u.id AS user_id,
                       u.serverchan_send_key AS send_key,
                       w.symbol,
                       w.name
                FROM `users` AS u
                LEFT JOIN `watchlist` AS w ON w.user_id = u.id
                ORDER BY u.id, w.id
                """
            )
            try:
                cursor.execute(query)
            except Exception as exc:  # pragma: no cover - 表不存在或无权限
                message = str(exc)
                if "serverchan_send_key" in message and not _SEND_KEY_COLUMN_ATTEMPTED:
                    _SEND_KEY_COLUMN_ATTEMPTED = True
                    try:
                        cursor.execute(
                            "ALTER TABLE `users` ADD COLUMN `serverchan_send_key` VARCHAR(255)"
                        )
                        conn.commit()
                        cursor.execute(query)
                    except Exception as alter_exc:  # pragma: no cover - 权限不足
                        if not _SEND_KEY_COLUMN_WARNED:
                            print(
                                "查询自选股失败: 数据库缺少 serverchan_send_key 列且自动补全失败，请先更新 Web 应用数据库结构。",
                                file=sys.stderr,
                            )
                            _SEND_KEY_COLUMN_WARNED = True
                        return []
                else:
                    if "serverchan_send_key" in message and not _SEND_KEY_COLUMN_WARNED:
                        print(
                            "查询自选股失败: 数据库缺少 serverchan_send_key 列，请先更新 Web 应用数据库结构。",
                            file=sys.stderr,
                        )
                        _SEND_KEY_COLUMN_WARNED = True
                    elif "serverchan_send_key" not in message:
                        print(f"查询自选股失败: {exc}", file=sys.stderr)
                    return []
            for row in cursor.fetchall():
                user_id = int(row.get("user_id"))
                config = users.get(user_id)
                if config is None:
                    send_key = (row.get("send_key") or "").strip() or None
                    config = UserWatchConfig(user_id=user_id, send_key=send_key, items=[])
                    users[user_id] = config
                raw_symbol = str(row.get("symbol") or "").strip()
                if not raw_symbol:
                    continue
                code = normalize_code(raw_symbol)
                if not code:
                    continue
                name = str(row.get("name") or code)
                if any(item.code == code for item in config.items):
                    continue
                config.items.append(WatchItem(symbol=raw_symbol or code, code=code, name=name))
    finally:
        conn.close()
    return list(users.values())


_TRADING_CAL_CACHE: Optional[Set[str]] = None
_SEND_KEY_COLUMN_WARNED = False
_SEND_KEY_COLUMN_ATTEMPTED = False


def is_trading_day(date: dt.date) -> bool:
    if date.weekday() >= 5:
        return False
    date_str = date.strftime("%Y-%m-%d")
    global _TRADING_CAL_CACHE
    if _TRADING_CAL_CACHE is None:
        try:
            df = ak.tool_trade_date_hist_sina()
        except Exception as exc:  # pragma: no cover - akshare failure path
            print(f"无法确认交易日信息 ({exc}); 默认继续运行。", file=sys.stderr)
            return True
        _TRADING_CAL_CACHE = set(df["trade_date"].astype(str))
    return date_str in _TRADING_CAL_CACHE


def fetch_fund_flow() -> pd.DataFrame:
    try:
        df = ak.stock_individual_fund_flow_rank(indicator="今日")
    except requests.exceptions.ProxyError as exc:
        if _has_proxy_env():
            print("检测到代理连接失败，自动禁用代理后重试…")
            _disable_proxies()
            df = ak.stock_individual_fund_flow_rank(indicator="今日")
        else:
            raise
    except requests.exceptions.RequestException as exc:
        if _has_proxy_env():
            print("检测到网络异常，自动禁用代理后重试…")
            _disable_proxies()
            df = ak.stock_individual_fund_flow_rank(indicator="今日")
        else:
            raise
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    rename_map: Dict[str, str] = {}
    for col in df.columns:
        for prefix in ("今日", "3日", "5日", "10日"):
            if col.startswith(prefix) and col not in {"序号", "代码", "名称", "最新价"}:
                rename_map[col] = col[len(prefix) :]
                break
    if rename_map:
        df.rename(columns=rename_map, inplace=True)
    df["代码"] = df["代码"].astype(str).str.strip().str.zfill(6)
    df["主力净流入-净额"] = pd.to_numeric(df["主力净流入-净额"], errors="coerce")
    if "涨跌幅" in df.columns:
        df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
    return df


def dataframe_snapshots(df: pd.DataFrame) -> Dict[str, StockSnapshot]:
    snapshots: Dict[str, StockSnapshot] = {}
    if df.empty:
        return snapshots
    for _, row in df.iterrows():
        code = normalize_code(str(row.get("代码", "")))
        if not code:
            continue
        name = str(row.get("名称") or code)
        amount = row.get("主力净流入-净额")
        pct = row.get("涨跌幅")
        amount_val = float(amount) if pd.notna(amount) else None
        pct_val = float(pct) if pd.notna(pct) else None
        snapshots[code] = StockSnapshot(code=code, name=name, amount=amount_val, pct=pct_val)
    return snapshots


def fetch_single_snapshot(item: WatchItem) -> StockSnapshot:
    try:
        stock, market, _ = parse_stock_code(item.symbol or item.code)
    except ValueError as exc:
        print(f"解析股票代码 {item.symbol} 失败: {exc}", file=sys.stderr)
        return StockSnapshot(code=item.code, name=item.name, amount=None, pct=None)
    try:
        df = ak.stock_individual_fund_flow(stock=stock, market=market)
    except Exception as exc:  # pragma: no cover - 请求异常
        print(f"获取 {item.symbol} 资金流向失败: {exc}", file=sys.stderr)
        return StockSnapshot(code=item.code, name=item.name, amount=None, pct=None)
    if df is None or df.empty:
        return StockSnapshot(code=item.code, name=item.name, amount=None, pct=None)
    row = df.iloc[-1]
    amount = row.get("主力净流入-净额")
    pct = row.get("涨跌幅")
    amount_val = float(amount) if pd.notna(amount) else None
    pct_val = float(pct) if pd.notna(pct) else None
    return StockSnapshot(code=item.code, name=item.name, amount=amount_val, pct=pct_val)


def build_notification_payload(
    updates: List[StockSnapshot],
    watch_snaps: List[StockSnapshot],
) -> Tuple[str, str]:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    title = f"资金流入提醒（{len(updates)}）"
    lines: List[str] = [f"时间: {ts}"]
    update_codes = {snap.code for snap in updates}
    if updates:
        lines.append("\n重点资金流入:")
        for snap in updates:
            lines.append(
                f"- {snap.code} {snap.name} 涨跌幅 {snap.pct_text()} 主力净流入 {snap.amount_text()}"
            )
    if watch_snaps:
        lines.append("\n重点关注:")
        for snap in watch_snaps:
            tag = " (高流入)" if snap.code in update_codes else ""
            lines.append(
                f"- {snap.code} {snap.name} 涨跌幅 {snap.pct_text()} 主力净流入 {snap.amount_text()}{tag}"
            )
    desp = "\n".join(lines)
    return title, desp


def send_serverchan(send_key: str, title: str, desp: str) -> None:
    url = f"https://sctapi.ftqq.com/{send_key}.send"
    try:
        resp = requests.post(url, data={"title": title, "desp": desp}, timeout=10)
        if resp.status_code != 200:
            print(f"ServerChan 请求失败: HTTP {resp.status_code} - {resp.text}", file=sys.stderr)
            return
        data = resp.json()
        if data.get("code") != 0:
            print(f"ServerChan 响应异常: {data}", file=sys.stderr)
    except requests.RequestException as exc:
        print(f"ServerChan 请求异常: {exc}", file=sys.stderr)


def poll_once(notified: Dict[str, int], configs: List[UserWatchConfig]) -> None:
    if not configs:
        print("当前没有配置有效的 Server酱 SendKey，跳过本轮推送。")
        return
    try:
        df = fetch_fund_flow()
    except Exception as exc:  # pragma: no cover - akshare failure path
        print(f"获取资金流向数据失败: {exc}", file=sys.stderr)
        return
    snapshots = dataframe_snapshots(df)

    triggered: List[StockSnapshot] = []
    for snap in snapshots.values():
        if snap.amount is None:
            continue
        current_floor = int(snap.amount // 100_000_000)
        if current_floor <= 0:
            continue
        last_floor = notified.get(snap.code, 0)
        if current_floor > last_floor:
            triggered.append(snap)
            notified[snap.code] = current_floor

    if not triggered:
        return

    for config in configs:
        send_key = (config.send_key or "").strip()
        if not send_key:
            continue
        watch_snaps: List[StockSnapshot] = []
        for item in config.items:
            snap = snapshots.get(item.code)
            if snap is None:
                snap = fetch_single_snapshot(item)
            else:
                snap = StockSnapshot(
                    code=snap.code,
                    name=item.name or snap.name,
                    amount=snap.amount,
                    pct=snap.pct,
                )
            watch_snaps.append(snap)

        title, desp = build_notification_payload(triggered, watch_snaps)
        send_serverchan(send_key, title, desp)
        print(f"已推送用户 {config.user_id} {len(triggered)} 条资金流入提醒。")


def wait_until(target: dt.datetime) -> None:
    while True:
        now = dt.datetime.now()
        if now >= target:
            return
        remaining = (target - now).total_seconds()
        sleep_seconds = max(1, min(REFRESH_INTERVAL_SECONDS, int(remaining)))
        time.sleep(sleep_seconds)


def run_session(
    notified: Dict[str, int],
    start: dt.time,
    end: dt.time,
) -> None:
    today = dt.date.today()
    start_dt = dt.datetime.combine(today, start)
    end_dt = dt.datetime.combine(today, end)

    now = dt.datetime.now()
    if now >= end_dt or now.date() != today:
        return
    if now < start_dt:
        wait_until(start_dt)

    print(f"进入监控时段 {start.strftime('%H:%M')} - {end.strftime('%H:%M')}。")
    while True:
        now = dt.datetime.now()
        if now.date() != today or now >= end_dt:
            print(f"{end.strftime('%H:%M')} 时段结束，抓取最新数据。")
            configs = [cfg for cfg in load_user_watch_configs() if cfg.send_key]
            poll_once(notified, configs)
            break

        configs = [cfg for cfg in load_user_watch_configs() if cfg.send_key]
        if configs:
            poll_once(notified, configs)
        else:
            print("未发现配置 Server酱 SendKey 的用户，暂停推送。")

        now = dt.datetime.now()
        remaining = int((end_dt - now).total_seconds())
        if remaining <= 0:
            continue
        sleep_seconds = max(1, min(REFRESH_INTERVAL_SECONDS, remaining))
        time.sleep(sleep_seconds)


def monitor() -> None:
    today = dt.date.today()
    if not is_trading_day(today):
        print(f"{today} 非交易日，结束脚本。")
        return

    print(f"开始监控 {today} 主力资金流入阈值，间隔 {REFRESH_INTERVAL_SECONDS} 秒。")
    notified: Dict[str, int] = {}
    configs = [cfg for cfg in load_user_watch_configs() if cfg.send_key]
    if configs:
        print(f"已加载 {len(configs)} 个 Server酱 推送配置。")
    else:
        print("尚未配置任何 Server酱 SendKey，等待用户配置后继续。")

    for start, end in TRADING_SESSIONS:
        run_session(notified, start, end)

    print("今日监控完成，脚本退出。")


def main() -> None:
    load_env()
    try:
        monitor()
    except KeyboardInterrupt:
        print("用户中断，退出。")


if __name__ == "__main__":
    main()
