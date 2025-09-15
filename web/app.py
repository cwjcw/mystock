import os
import secrets
import hashlib
import time
import threading
from collections import defaultdict, deque
import sqlite3
from pathlib import Path
from typing import List, Dict

from flask import Flask, g, render_template, request, redirect, url_for, flash, make_response
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
    from scripts.rss_fund_flow import fetch_latest_minute, fetch_quote_basic, symbol_to_secid
except ModuleNotFoundError:
    import sys
    sys.path.append(str(Path(__file__).resolve().parents[1] / 'scripts'))
    from rss_fund_flow import fetch_latest_minute, fetch_quote_basic, symbol_to_secid  # type: ignore

import asyncio
import aiohttp
import datetime as dt
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
        """
    )
    # Backfill schema if old DB exists without rss_token_hash
    try:
        cols = [r[1] for r in db.execute('PRAGMA table_info(users)').fetchall()]
        if 'rss_token_hash' not in cols:
            db.execute('ALTER TABLE users ADD COLUMN rss_token_hash TEXT')
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


@app.route('/reset_token', methods=['POST'])
@login_required
def reset_token():
    db = get_db()
    token = secrets.token_urlsafe(24)
    token_hash = hashlib.sha256(token.encode('utf-8')).hexdigest()
    if RSS_TOKEN_HASH_ONLY:
        db.execute('UPDATE users SET rss_token=NULL, rss_token_hash=? WHERE id=?', (token_hash, current_user.id))
        flash(f'新的RSS Token（只显示一次）：{token}', 'success')
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
            chg_txt = '-' if chg is None else f"{chg:.2f}%"
            price_txt = '-' if price is None else f"{price:.2f}"
            mcap_txt = '-' if mcap_yi is None else f"{mcap_yi:.2f}亿"
            # Apply user-requested label/value mapping for RSS
            desc = (
                f"最新价:{price_txt} 涨跌幅:{chg_txt} 总市值:{mcap_txt} | "
                f"主力:{rowd['主力']} 超大单:{rowd['小单']} 大单:{rowd['中单']} 中单:{rowd['大单']} 小单:{rowd['超大单']} (单位:亿元)"
            )
            item = {
                'guid': f"{symbol}_{rowd['time']}",
                'title': f"{name} / {(nret or '')} {rowd['time']}",
                'description': desc,
                'pubDate': now.strftime('%a, %d %b %Y %H:%M:%S %z')
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
        ET.SubElement(item, 'guid').text = it['guid']
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
