import os
import secrets
import hashlib
import time
import threading
import math
from collections import defaultdict, deque
import sqlite3
from pathlib import Path
from typing import List, Dict

from flask import Flask, g, render_template, request, redirect, url_for, flash, make_response, abort
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    login_required,
    current_user,
    logout_user,
)
from werkzeug.security import generate_password_hash, check_password_hash

# Reuse fetchers from scripts for RSS
try:
    from scripts.rss_fund_flow import fetch_latest_minute, fetch_quote_basic, fetch_fund_quote, symbol_to_secid
except ModuleNotFoundError:
    import sys
    sys.path.append(str(Path(__file__).resolve().parents[1] / 'scripts'))
    from rss_fund_flow import fetch_latest_minute, fetch_quote_basic, fetch_fund_quote, symbol_to_secid  # type: ignore

import asyncio
import aiohttp
import datetime as dt


CHINA_TZ = dt.timezone(dt.timedelta(hours=8))
import xml.etree.ElementTree as ET


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR.parent / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / 'app.db'
PUBLIC_DOMAIN = os.environ.get('PUBLIC_DOMAIN', 'stock.cuixiaoyuan.cn')
# If RSS_PREFIX == 'username' (default), the URL pattern is /<username>/<token>.rss
# Otherwise, enforce a fixed prefix string
RSS_PREFIX = os.environ.get('RSS_PREFIX', 'username')
RSS_TOKEN_HASH_ONLY = os.environ.get('RSS_TOKEN_HASH_ONLY', 'false').lower() in {'1', 'true', 'yes'}

# Load environment-style config from project root .env (KEY=VALUE), if present
PROJECT_ROOT = APP_DIR.parent
ENV_FILE = PROJECT_ROOT / '.env'
if ENV_FILE.exists():
    for line in ENV_FILE.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        k, v = line.split('=', 1)
        os.environ[k.strip()] = v.strip()

# Re-resolve settings (allow .env to override defaults)
PUBLIC_DOMAIN = os.environ.get('PUBLIC_DOMAIN', PUBLIC_DOMAIN)
RSS_PREFIX = os.environ.get('RSS_PREFIX', RSS_PREFIX)
RSS_TOKEN_HASH_ONLY = os.environ.get('RSS_TOKEN_HASH_ONLY', 'false').lower() in {'1', 'true', 'yes'}
APP_PORT = int(os.environ.get('APP_PORT', '18888'))
DEBUG_FLAG = os.environ.get('DEBUG', 'false').lower() in {'1', 'true', 'yes'}

# Rate limit defaults: 1 request per 60 seconds
RATE_LIMIT_REQUESTS = int(os.environ.get('RSS_RATE_LIMIT', '1'))  # requests per window
RATE_LIMIT_WINDOW = int(os.environ.get('RSS_RATE_WINDOW', '60'))   # seconds

# Simple in-memory rate limiter: key -> deque[timestamps]
_RL_LOCK = threading.Lock()
_RL_BUCKETS: dict[str, deque] = defaultdict(deque)


def _rate_check_and_consume(key: str):
    """Return (ok: bool, retry_after: int)."""
    now = time.time()
    with _RL_LOCK:
        q = _RL_BUCKETS[key]
        # drop old
        while q and (now - q[0]) > RATE_LIMIT_WINDOW:
            q.popleft()
        if len(q) >= RATE_LIMIT_REQUESTS:
            retry = int(RATE_LIMIT_WINDOW - (now - q[0])) + 1
            return False, max(retry, 1)
        q.append(now)
        return True, 0


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(str(DB_PATH))
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    db = sqlite3.connect(str(DB_PATH))
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            rss_token TEXT UNIQUE,
            rss_token_hash TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS trade_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            trade_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            quantity REAL NOT NULL,
            price REAL NOT NULL,
            fee REAL NOT NULL DEFAULT 0,
            stamp_tax REAL NOT NULL DEFAULT 0,
            asset_type TEXT NOT NULL DEFAULT 'stock',
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS initial_profits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            profit_date TEXT NOT NULL,
            amount REAL NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    # Backfill schema if old DB exists without rss_token_hash
    try:
        cols = [r[1] for r in db.execute('PRAGMA table_info(users)').fetchall()]
        if 'rss_token_hash' not in cols:
            db.execute('ALTER TABLE users ADD COLUMN rss_token_hash TEXT')
        trade_cols = [r[1] for r in db.execute('PRAGMA table_info(trade_logs)').fetchall()]
        if 'asset_type' not in trade_cols:
            db.execute("ALTER TABLE trade_logs ADD COLUMN asset_type TEXT NOT NULL DEFAULT 'stock'")
        if 'stamp_tax' not in trade_cols:
            db.execute("ALTER TABLE trade_logs ADD COLUMN stamp_tax REAL NOT NULL DEFAULT 0")
    except Exception:
        pass
    db.commit()
    db.close()


app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')
login_manager = LoginManager(app)
login_manager.login_view = 'login'


class User(UserMixin):
    def __init__(self, id, username, password_hash, rss_token):
        self.id = id
        self.username = username
        self.password_hash = password_hash
        self.rss_token = rss_token


@login_manager.user_loader
def load_user(user_id):
    db = get_db()
    row = db.execute('SELECT id, username, password_hash, rss_token FROM users WHERE id = ?', (user_id,)).fetchone()
    if row:
        return User(row['id'], row['username'], row['password_hash'], row['rss_token'])
    return None


# Flask 3.0 移除了 before_first_request，直接在模块加载时初始化数据库
init_db()


@app.teardown_appcontext
def _teardown(exception=None):
    close_db()


@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('watchlist_view'))
    return redirect(url_for('login'))


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        if not username or not password:
            flash('请输入用户名和密码', 'error')
            return render_template('register.html')
        db = get_db()
        if db.execute('SELECT 1 FROM users WHERE username = ?', (username,)).fetchone():
            flash('用户名已存在', 'error')
            return render_template('register.html')
        token = secrets.token_urlsafe(24)
        token_hash = hashlib.sha256(token.encode('utf-8')).hexdigest()
        db.execute(
            'INSERT INTO users (username, password_hash, rss_token, rss_token_hash, created_at) VALUES (?, ?, ?, ?, ?)',
            (
                username,
                generate_password_hash(password),
                None if RSS_TOKEN_HASH_ONLY else token,
                token_hash if RSS_TOKEN_HASH_ONLY else None,
                dt.datetime.now().isoformat(timespec='seconds'),
            ),
        )
        db.commit()
        if RSS_TOKEN_HASH_ONLY:
            flash(f'注册成功，请妥善保存你的RSS Token（只显示一次）：{token}', 'success')
            flash('下次若遗失可在“我的股票”页面重置 Token。', 'success')
            # 直接登录并跳转到watchlist，方便复制
            row = db.execute('SELECT id, username, password_hash, rss_token, rss_token_hash FROM users WHERE username = ?', (username,)).fetchone()
            user = User(row['id'], row['username'], row['password_hash'], row['rss_token'])
            login_user(user)
            return redirect(url_for('watchlist_view'))
        else:
            flash('注册成功，请登录', 'success')
            return redirect(url_for('login'))
    return render_template('register.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        db = get_db()
        row = db.execute('SELECT id, username, password_hash, rss_token FROM users WHERE username = ?', (username,)).fetchone()
        if not row or not check_password_hash(row['password_hash'], password):
            flash('用户名或密码错误', 'error')
            return render_template('login.html')
        user = User(row['id'], row['username'], row['password_hash'], row['rss_token'])
        login_user(user)
        return redirect(url_for('watchlist_view'))
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


def _parse_symbols(text: str) -> List[Dict[str, str]]:
    """Parse user input: each line supports Name=Symbol or Symbol."""
    out: List[Dict[str, str]] = []
    def normalize_symbol(sym: str) -> str:
        s = sym.strip().upper()
        # support sh600519 / sz000001
        if s.startswith('SH') and len(s) == 8 and s[2:].isdigit():
            return s[2:] + '.SH'
        if s.startswith('SZ') and len(s) == 8 and s[2:].isdigit():
            return s[2:] + '.SZ'
        # already has suffix
        if '.' in s:
            return s
        # plain 6-digit -> infer exchange
        if len(s) == 6 and s.isdigit():
            if s.startswith(("600", "601", "603", "605", "688")):
                return s + '.SH'
            else:
                return s + '.SZ'
        return s
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if '=' in line:
            name, sym = line.split('=', 1)
            out.append({'name': name.strip(), 'symbol': normalize_symbol(sym)})
        else:
            sym = normalize_symbol(line)
            out.append({'name': sym, 'symbol': sym})
    return out


def _normalize_trade_symbol(value: str, asset_type: str) -> str | None:
    s = value.strip().upper()
    if not s:
        return None
    if asset_type == 'fund':
        if '.' not in s:
            s += '.OF'
        elif not s.endswith('.OF'):
            base = s.split('.', 1)[0]
            s = base + '.OF'
        return s
    # fallback to stock normalization
    parsed = _parse_symbols(s)
    return parsed[0]['symbol'] if parsed else None


def _parse_iso_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None


def _build_portfolio(rows, start_date: dt.date, end_date: dt.date, name_cache: Dict[str, str]):
    holdings: Dict[str, dict] = {}
    realized_period: Dict[str, float] = defaultdict(float)
    realized_total_period = 0.0
    realized_all_time = 0.0

    for row in rows:
        symbol = row['symbol']
        action = row['action']
        quantity = float(row['quantity'])
        price = float(row['price'])
        fee = float(row['fee'] or 0.0)
        stamp_tax = float(row['stamp_tax'] or 0.0)
        trade_day = _parse_iso_date(row['trade_date']) or start_date
        try:
            asset_type = row['asset_type'] or 'stock'
        except (KeyError, IndexError, TypeError):
            asset_type = 'stock'

        entry = holdings.setdefault(symbol, {'qty': 0.0, 'avg_cost': 0.0, 'asset_type': asset_type, 'name': name_cache.get(symbol)})
        entry['asset_type'] = asset_type or entry.get('asset_type', 'stock')
        if action == 'buy':
            total_cost = entry['avg_cost'] * entry['qty'] + quantity * price + fee + stamp_tax
            entry['qty'] += quantity
            if entry['qty'] > 0:
                entry['avg_cost'] = total_cost / entry['qty']
            else:
                entry['avg_cost'] = 0.0
        elif action == 'sell':
            available_qty = entry['qty']
            avg_cost = entry['avg_cost'] if available_qty > 0 else 0.0
            proceeds = quantity * price - fee - stamp_tax
            sell_qty = quantity if available_qty <= 0 else min(quantity, available_qty)
            cost_basis = sell_qty * avg_cost
            profit = proceeds - cost_basis
            realized_all_time += profit
            if start_date <= trade_day <= end_date:
                realized_period[symbol] += profit
                realized_total_period += profit
            entry['qty'] = available_qty - quantity
            if entry['qty'] <= 0:
                entry['qty'] = 0.0
                entry['avg_cost'] = 0.0
        elif action == 'dividend':
            proceeds = quantity * price - fee - stamp_tax
            realized_all_time += proceeds
            if start_date <= trade_day <= end_date:
                realized_period[symbol] += proceeds
                realized_total_period += proceeds

    # Remove empty holdings
    holdings = {sym: data for sym, data in holdings.items() if data['qty'] > 0}
    return {
        'holdings': holdings,
        'realized_period': realized_period,
        'realized_total': realized_total_period,
        'realized_all_time': realized_all_time,
    }


def _fetch_quotes_for_symbols(entries: List[tuple[str, str]]) -> Dict[str, dict]:
    if not entries:
        return {}

    async def gather():
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            async def fetch_one(sym: str, asset_type: str):
                if asset_type == 'fund':
                    return await fetch_fund_quote(session, sym)
                try:
                    secid = symbol_to_secid(sym)
                except Exception:
                    return None
                return await fetch_quote_basic(session, secid)

            tasks = [fetch_one(sym, asset_type) for sym, asset_type in entries]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        out: Dict[str, dict] = {}
        for (sym, _asset_type), res in zip(entries, results):
            if isinstance(res, Exception) or res is None:
                continue
            out[sym] = res
        return out

    try:
        return asyncio.run(gather())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(gather())
        finally:
            loop.close()


@app.route('/watchlist', methods=['GET', 'POST'])
@login_required
def watchlist_view():
    db = get_db()
    if request.method == 'POST':
        raw = request.form.get('symbols', '')
        items = _parse_symbols(raw)
        db.execute('DELETE FROM watchlist WHERE user_id = ?', (current_user.id,))
        for it in items:
            db.execute('INSERT INTO watchlist (user_id, symbol, name) VALUES (?, ?, ?)', (current_user.id, it['symbol'], it['name']))
        db.commit()
        flash('已保存股票列表', 'success')
        return redirect(url_for('watchlist_view'))

    rows = db.execute('SELECT symbol, name FROM watchlist WHERE user_id = ? ORDER BY id', (current_user.id,)).fetchall()
    text = '\n'.join([f"{r['name']}={r['symbol']}" if r['name'] != r['symbol'] else r['symbol'] for r in rows])
    # Resolve prefix for display
    resolved_prefix = current_user.username if RSS_PREFIX == 'username' else RSS_PREFIX
    return render_template(
        'watchlist.html',
        symbols_text=text,
        rss_token=current_user.rss_token,
        public_domain=PUBLIC_DOMAIN,
        rss_prefix=resolved_prefix,
        hash_only=RSS_TOKEN_HASH_ONLY,
    )


@app.route('/trades', methods=['GET', 'POST'])
@login_required
def trades_view():
    db = get_db()
    if request.method == 'POST':
        trade_date = request.form.get('trade_date', '').strip()
        symbol_input_raw = request.form.get('symbol', '')
        symbol_input = symbol_input_raw.strip()
        action = request.form.get('action', '').strip().lower()
        quantity_raw = request.form.get('quantity', '').strip()
        price_raw = request.form.get('price', '').strip()
        fee_raw = request.form.get('fee', '').strip()
        stamp_raw = request.form.get('stamp_tax', '').strip()
        asset_type = request.form.get('asset_type', 'stock').strip().lower()

        upper_symbol = symbol_input.upper()
        if upper_symbol.endswith('.OF'):
            asset_type = 'fund'

        if asset_type not in {'stock', 'fund'}:
            flash('请选择有效的资产类型。', 'error')
            return redirect(url_for('trades_view'))
        if not trade_date or not symbol_input or action not in {'buy', 'sell', 'dividend'}:
            flash('请完整填写日期/代码/方向。', 'error')
            return redirect(url_for('trades_view'))
        normalized_symbol = _normalize_trade_symbol(symbol_input, asset_type)
        if not normalized_symbol:
            flash('股票代码格式不正确。', 'error')
            return redirect(url_for('trades_view'))
        symbol = normalized_symbol
        try:
            quantity = float(quantity_raw)
            price = float(price_raw)
            if quantity <= 0 or price < 0:
                raise ValueError
            if action in {'buy', 'sell'} and price == 0:
                raise ValueError
        except ValueError:
            flash('数量与价格需为正数。', 'error')
            return redirect(url_for('trades_view'))
        try:
            fee = float(fee_raw) if fee_raw else 0.0
        except ValueError:
            flash('手续费需为数字。', 'error')
            return redirect(url_for('trades_view'))
        try:
            stamp_tax = float(stamp_raw) if stamp_raw else 0.0
        except ValueError:
            flash('印花税需为数字。', 'error')
            return redirect(url_for('trades_view'))

        db.execute(
            'INSERT INTO trade_logs (user_id, trade_date, symbol, action, quantity, price, fee, stamp_tax, asset_type, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (current_user.id, trade_date, symbol, action, quantity, price, fee, stamp_tax, asset_type, None),
        )
        db.commit()
        flash('已记录交易。', 'success')
        return redirect(url_for('trades_view'))

    page_raw = request.args.get('page', '1')
    try:
        page = int(page_raw)
    except ValueError:
        page = 1
    if page < 1:
        page = 1
    per_page = 50
    total = db.execute('SELECT COUNT(1) FROM trade_logs WHERE user_id = ?', (current_user.id,)).fetchone()[0]
    total = total or 0
    max_page = max(1, math.ceil(total / per_page)) if total else 1
    if page > max_page:
        page = max_page
    offset = (page - 1) * per_page
    trades = db.execute(
        'SELECT id, trade_date, symbol, action, quantity, price, fee, stamp_tax, asset_type, note, created_at FROM trade_logs WHERE user_id = ? ORDER BY trade_date DESC, id DESC LIMIT ? OFFSET ?',
        (current_user.id, per_page, offset),
    ).fetchall()

    name_map: Dict[str, str] = {}
    if trades:
        unique_entries = []
        seen = set()
        for row in trades:
            key = (row['symbol'], row['asset_type'])
            if key in seen:
                continue
            seen.add(key)
            unique_entries.append(key)
        quotes = _fetch_quotes_for_symbols(unique_entries)
        name_map = {sym: info.get('name') for sym, info in quotes.items() if info.get('name')}

    return render_template(
        'trades.html',
        trades=trades,
        page=page,
        pages=max_page,
        per_page=per_page,
        total=total,
        name_map=name_map,
    )


def _get_trade_or_404(trade_id: int):
    db = get_db()
    trade = db.execute(
        'SELECT id, trade_date, symbol, action, quantity, price, fee, stamp_tax, asset_type, note FROM trade_logs WHERE id = ? AND user_id = ?',
        (trade_id, current_user.id),
    ).fetchone()
    if not trade:
        abort(404)
    return trade


@app.route('/trades/<int:trade_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_trade(trade_id: int):
    trade = _get_trade_or_404(trade_id)
    db = get_db()
    if request.method == 'POST':
        trade_date = request.form.get('trade_date', '').strip()
        symbol_input_raw = request.form.get('symbol', '')
        symbol_input = symbol_input_raw.strip()
        action = request.form.get('action', '').strip().lower()
        quantity_raw = request.form.get('quantity', '').strip()
        price_raw = request.form.get('price', '').strip()
        fee_raw = request.form.get('fee', '').strip()
        stamp_raw = request.form.get('stamp_tax', '').strip()
        asset_type = request.form.get('asset_type', 'stock').strip().lower()

        upper_symbol = symbol_input.upper()
        if upper_symbol.endswith('.OF'):
            asset_type = 'fund'

        if asset_type not in {'stock', 'fund'}:
            flash('请选择有效的资产类型。', 'error')
            return redirect(url_for('edit_trade', trade_id=trade_id))
        if not trade_date or not symbol_input or action not in {'buy', 'sell', 'dividend'}:
            flash('请完整填写日期/代码/方向。', 'error')
            return redirect(url_for('edit_trade', trade_id=trade_id))
        normalized_symbol = _normalize_trade_symbol(symbol_input, asset_type)
        if not normalized_symbol:
            flash('股票代码格式不正确。', 'error')
            return redirect(url_for('edit_trade', trade_id=trade_id))
        try:
            quantity = float(quantity_raw)
            price = float(price_raw)
            if quantity <= 0 or price < 0:
                raise ValueError
            if action in {'buy', 'sell'} and price == 0:
                raise ValueError
        except ValueError:
            flash('数量与价格需为正数。', 'error')
            return redirect(url_for('edit_trade', trade_id=trade_id))
        try:
            fee = float(fee_raw) if fee_raw else 0.0
        except ValueError:
            flash('手续费需为数字。', 'error')
            return redirect(url_for('edit_trade', trade_id=trade_id))
        try:
            stamp_tax = float(stamp_raw) if stamp_raw else 0.0
        except ValueError:
            flash('印花税需为数字。', 'error')
            return redirect(url_for('edit_trade', trade_id=trade_id))

        db.execute(
            'UPDATE trade_logs SET trade_date=?, symbol=?, action=?, quantity=?, price=?, fee=?, stamp_tax=?, asset_type=?, note=NULL WHERE id=? AND user_id=?',
            (trade_date, normalized_symbol, action, quantity, price, fee, stamp_tax, asset_type, trade_id, current_user.id),
        )
        db.commit()
        flash('已更新交易。', 'success')
        return redirect(url_for('trades_view'))

    return render_template('trade_edit.html', trade=trade)


@app.route('/trades/<int:trade_id>/delete', methods=['POST'])
@login_required
def delete_trade(trade_id: int):
    db = get_db()
    cur = db.execute('DELETE FROM trade_logs WHERE id = ? AND user_id = ?', (trade_id, current_user.id))
    db.commit()
    if cur.rowcount:
        flash('已删除交易。', 'success')
    else:
        flash('未找到对应交易。', 'error')
    return redirect(url_for('trades_view'))


@app.route('/portfolio', methods=['GET'])
@login_required
def portfolio_view():
    db = get_db()
    today = dt.date.today()
    default_start = today.replace(month=1, day=1)
    start_raw = request.args.get('start') or default_start.isoformat()
    end_raw = request.args.get('end') or today.isoformat()
    start_date = _parse_iso_date(start_raw) or default_start
    end_date = _parse_iso_date(end_raw) or today
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    rows = db.execute(
        'SELECT trade_date, symbol, action, quantity, price, fee, stamp_tax, asset_type FROM trade_logs WHERE user_id = ? ORDER BY trade_date ASC, id ASC',
        (current_user.id,),
    ).fetchall()
    symbol_asset: Dict[str, str] = {}
    for r in rows:
        asset = r['asset_type'] or 'stock'
        symbol_asset[r['symbol']] = asset
    initial_quotes = _fetch_quotes_for_symbols(list(symbol_asset.items()))
    name_cache = {sym: info.get('name') for sym, info in initial_quotes.items() if info.get('name')}
    portfolio = _build_portfolio(rows, start_date, end_date, name_cache)
    holdings: Dict[str, dict] = portfolio['holdings']
    realized_period: Dict[str, float] = portfolio['realized_period']

    symbols = sorted(holdings.keys())
    quotes = _fetch_quotes_for_symbols([(sym, holdings[sym].get('asset_type', 'stock')) for sym in symbols])
    positions = []
    total_market_value = 0.0
    total_cost_basis = 0.0
    unrealized_total = 0.0
    daily_stock_pnl = 0.0
    daily_fund_pnl = 0.0

    for sym in symbols:
        data = holdings[sym]
        qty = data['qty']
        avg_cost = data['avg_cost']
        cost_basis = qty * avg_cost
        quote = quotes.get(sym)
        price = (quote or {}).get('price')
        change_pct_raw = (quote or {}).get('change_pct')
        try:
            change_pct = float(change_pct_raw)
        except (TypeError, ValueError):
            change_pct = None
        market_value = None if price is None else price * qty
        unrealized = None if market_value is None else market_value - cost_basis
        daily_change = None
        if price is not None and change_pct is not None and market_value is not None:
            c = change_pct / 100.0
            denom = 1.0 + c
            if abs(denom) > 1e-9:
                prev_price = price / denom
                daily_change = (price - prev_price) * qty
        if market_value is not None:
            total_market_value += market_value
        total_cost_basis += cost_basis
        if unrealized is not None:
            unrealized_total += unrealized
        asset_type_val = data.get('asset_type', 'stock')
        if asset_type_val == 'stock' and daily_change is not None:
            daily_stock_pnl += daily_change
        if asset_type_val == 'fund' and daily_change is not None:
            daily_fund_pnl += daily_change
        name = holdings[sym].get('name') or (quote or {}).get('name')
        positions.append({
            'symbol': sym,
            'name': name,
            'asset_type': holdings[sym].get('asset_type', 'stock'),
            'quantity': qty,
            'avg_cost': avg_cost,
            'price': price,
            'market_value': market_value,
            'unrealized': unrealized,
            'change_pct': change_pct,
            'daily_change': daily_change,
            'cost_basis': cost_basis,
        })

    realized_items = sorted(
        [{'symbol': sym, 'value': val} for sym, val in realized_period.items()],
        key=lambda x: x['symbol'],
    )

    profits = db.execute(
        'SELECT id, profit_date, amount, note FROM initial_profits WHERE user_id = ? ORDER BY profit_date DESC, id DESC',
        (current_user.id,),
    ).fetchall()
    initial_period_total = 0.0
    for row in profits:
        pdate = _parse_iso_date(row['profit_date'])
        if pdate and start_date <= pdate <= end_date:
            initial_period_total += float(row['amount'])

    realized_with_initial = portfolio['realized_total'] + initial_period_total
    combined_total = realized_with_initial + unrealized_total
    daily_total = daily_stock_pnl + daily_fund_pnl

    return render_template(
        'portfolio.html',
        positions=positions,
        realized_total=portfolio['realized_total'],
        realized_with_initial=realized_with_initial,
        realized_items=realized_items,
        realized_all_time=portfolio['realized_all_time'],
        unrealized_total=unrealized_total,
        total_market_value=total_market_value,
        total_cost_basis=total_cost_basis,
        combined_total=combined_total,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        initial_period_total=initial_period_total,
        initial_profits=profits,
        today_str=today.isoformat(),
        daily_stock_pnl=daily_stock_pnl,
        daily_fund_pnl=daily_fund_pnl,
        daily_total=daily_total,
    )


@app.route('/initial_profits', methods=['POST'])
@login_required
def add_initial_profit():
    amount_raw = request.form.get('amount', '').strip()
    note = request.form.get('note', '').strip() or None
    try:
        amount = float(amount_raw)
    except ValueError:
        flash('初始盈利需为数字。', 'error')
        return redirect(url_for('portfolio_view'))
    profit_date = dt.date.today().isoformat()
    db = get_db()
    db.execute(
        'INSERT INTO initial_profits (user_id, profit_date, amount, note) VALUES (?, ?, ?, ?)',
        (current_user.id, profit_date, amount, note),
    )
    db.commit()
    flash('已记录初始盈利。', 'success')
    return redirect(url_for('portfolio_view'))


@app.route('/initial_profits/<int:profit_id>/delete', methods=['POST'])
@login_required
def delete_initial_profit(profit_id: int):
    db = get_db()
    cur = db.execute('DELETE FROM initial_profits WHERE id = ? AND user_id = ?', (profit_id, current_user.id))
    db.commit()
    if cur.rowcount:
        flash('已删除初始盈利记录。', 'success')
    else:
        flash('未找到对应记录。', 'error')
    return redirect(url_for('portfolio_view'))


@app.route('/reset_token', methods=['POST'])
@login_required
def reset_token():
    db = get_db()
    token = secrets.token_urlsafe(24)
    token_hash = hashlib.sha256(token.encode('utf-8')).hexdigest()
    # 构造完整订阅链接
    resolved_prefix = current_user.username if RSS_PREFIX == 'username' else RSS_PREFIX
    rss_url = f"https://{PUBLIC_DOMAIN}/{resolved_prefix}/{token}.rss" if RSS_PREFIX else f"https://{PUBLIC_DOMAIN}/u/{token}.rss"
    if RSS_TOKEN_HASH_ONLY:
        db.execute('UPDATE users SET rss_token=NULL, rss_token_hash=? WHERE id=?', (token_hash, current_user.id))
        flash(f'新的RSS Token（只显示一次）：{token}', 'success')
        flash(f'你的专属RSS订阅链接（只显示一次）：{rss_url}', 'success')
    else:
        db.execute('UPDATE users SET rss_token=?, rss_token_hash=NULL WHERE id=?', (token, current_user.id))
        flash('已重置RSS Token。', 'success')
    db.commit()
    return redirect(url_for('watchlist_view'))


def _find_user_by_token(db, token: str):
    # match by plain token or sha256(token)
    row = db.execute('SELECT id, username FROM users WHERE rss_token = ?', (token,)).fetchone()
    if row:
        return row
    th = hashlib.sha256(token.encode('utf-8')).hexdigest()
    row = db.execute('SELECT id, username FROM users WHERE rss_token_hash = ?', (th,)).fetchone()
    return row


def generate_rss_response(token: str):
    # Rate limit by IP and by token
    ip_key = f"ip:{request.remote_addr or 'unknown'}"
    ok, retry = _rate_check_and_consume(ip_key)
    if not ok:
        resp = make_response('Too Many Requests (IP)', 429)
        resp.headers['Retry-After'] = str(retry)
        return resp
    tok_key = f"tok:{token}"
    ok, retry = _rate_check_and_consume(tok_key)
    if not ok:
        resp = make_response('Too Many Requests (Token)', 429)
        resp.headers['Retry-After'] = str(retry)
        return resp

    db = get_db()
    row = _find_user_by_token(db, token)
    if not row:
        return ('Not found', 404)
    items = db.execute('SELECT symbol, name FROM watchlist WHERE user_id = ? ORDER BY id', (row['id'],)).fetchall()
    mapping = { (r['name'] or r['symbol']): r['symbol'] for r in items }
    # Fetch latest minute + quote for mapping
    async def build_items():
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            name_to_secid = { name: symbol_to_secid(sym) for name, sym in mapping.items() }
            tasks = [fetch_latest_minute(session, secid) for secid in name_to_secid.values()]
            quotes = [fetch_quote_basic(session, secid) for secid in name_to_secid.values()]
            results = await asyncio.gather(*tasks)
            qresults = await asyncio.gather(*quotes)
        now = dt.datetime.now()
        out = []
        for (name, symbol), (nret, rowd), q in zip(name_to_secid.items(), results, qresults):
            if not rowd:
                continue
            price = q.get('price') if q else None
            chg = q.get('change_pct') if q else None
            mcap = q.get('market_cap') if q else None
            mcap_yi = None if mcap is None else round(mcap/1e8, 2)
            if chg is None or abs(chg) < 1e-8:
                chg_txt = '-'
            else:
                chg_txt = f"{chg:.2f}%"
            price_txt = '-' if price is None else f"{price:.2f}"
            mcap_txt = '-' if mcap_yi is None else f"{mcap_yi:.2f}亿"
            # Apply user-requested label/value mapping for RSS
            def color_num(val):
                try:
                    v = float(val)
                except Exception:
                    return f"<span>{val}</span>"
                color = 'green' if v >= 0 else 'red'
                return f'<span style="color:{color}">{v}</span>'

            desc = (
                f"最新价: {price_txt}<br>"
                f"涨跌幅: {chg_txt}<br>"
                f"总市值: {mcap_txt}<br>"
                f"主力: {color_num(rowd['主力'])} 亿元<br>"
                f"超大单: {color_num(rowd['小单'])} 亿元<br>"
                f"大单: {color_num(rowd['中单'])} 亿元<br>"
                f"中单: {color_num(rowd['大单'])} 亿元<br>"
                f"小单: {color_num(rowd['超大单'])} 亿元"
            )
            # Derive per-item metadata consumed by RSS readers
            base_time = rowd.get('time') or now.strftime('%Y-%m-%d %H:%M:%S')
            pub_dt = None
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
                try:
                    pub_dt = dt.datetime.strptime(base_time, fmt).replace(tzinfo=CHINA_TZ)
                    break
                except ValueError:
                    continue
            if pub_dt is None:
                pub_dt = now.astimezone(CHINA_TZ)
            guid_seed = f"{symbol}|{base_time}|{rowd.get('主力')}|{rowd.get('超大单')}|{rowd.get('大单')}|{rowd.get('中单')}|{rowd.get('小单')}"
            guid_hash = hashlib.sha1(guid_seed.encode('utf-8')).hexdigest()[:12]
            exch = symbol.split('.')[-1].lower()
            quote_link = f"https://quote.eastmoney.com/{exch}{symbol.split('.')[0].lower()}.html"
            item = {
                'guid': f"{symbol}_{base_time}_{guid_hash}",
                'title': f"{name} / {(nret or '')} {base_time}",
                'description': desc,
                'pubDate': pub_dt.strftime('%a, %d %b %Y %H:%M:%S %z'),
                'link': quote_link,
            }
            out.append(item)
        return out

    items = asyncio.run(build_items())
    # Build RSS XML
    rss = ET.Element('rss', version='2.0')
    ch = ET.SubElement(rss, 'channel')
    ET.SubElement(ch, 'title').text = f"资金流RSS - {row['username']}"
    ET.SubElement(ch, 'link').text = 'https://quote.eastmoney.com/'
    ET.SubElement(ch, 'description').text = 'A股分钟级资金流'
    ET.SubElement(ch, 'lastBuildDate').text = dt.datetime.now().strftime('%a, %d %b %Y %H:%M:%S %z')
    for it in items:
        item = ET.SubElement(ch, 'item')
        ET.SubElement(item, 'title').text = it['title']
        ET.SubElement(item, 'description').text = it['description']
        ET.SubElement(item, 'link').text = it['link']
        guid_el = ET.SubElement(item, 'guid')
        guid_el.text = it['guid']
        guid_el.set('isPermaLink', 'false')
        ET.SubElement(item, 'pubDate').text = it['pubDate']
    xml = ET.tostring(rss, encoding='utf-8', xml_declaration=True)
    resp = make_response(xml)
    resp.headers['Content-Type'] = 'application/rss+xml; charset=utf-8'
    return resp


@app.route('/u/<token>.rss')
def user_rss(token: str):
    return generate_rss_response(token)


@app.route('/<prefix>/<token>.rss')
def prefixed_rss(prefix: str, token: str):
    # Enforce prefix policy
    db = get_db()
    row = _find_user_by_token(db, token)
    if not row:
        return ('Not found', 404)
    if RSS_PREFIX == 'username':
        if prefix != row['username']:
            return ('Not found', 404)
    else:
        if prefix != RSS_PREFIX:
            return ('Not found', 404)
    return generate_rss_response(token)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=APP_PORT, debug=DEBUG_FLAG)
