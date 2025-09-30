import os
import secrets
import hashlib
import time
import threading
import math
from functools import wraps
from collections import defaultdict, deque
from pathlib import Path
from typing import List, Dict, Optional

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
from pymysql.cursors import DictCursor

# Reuse fetchers from scripts for RSS
try:
    from scripts.rss_fund_flow import fetch_latest_minute, fetch_quote_basic, fetch_fund_quote, symbol_to_secid
except ModuleNotFoundError:
    import sys
    sys.path.append(str(Path(__file__).resolve().parents[1] / 'scripts'))
    from rss_fund_flow import fetch_latest_minute, fetch_quote_basic, fetch_fund_quote, symbol_to_secid  # type: ignore

try:
    from scripts.mysql_utils import connect_mysql, MySQLConfigError
except ModuleNotFoundError:  # pragma: no cover
    import sys
    sys.path.append(str(Path(__file__).resolve().parents[1] / 'scripts'))
    from mysql_utils import connect_mysql, MySQLConfigError  # type: ignore

import asyncio
import aiohttp
import datetime as dt
from html import escape


CHINA_TZ = dt.timezone(dt.timedelta(hours=8))


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR.parent / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)
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

APP_DB_DSN = os.environ.get('APP_MYSQL_DSN') or os.environ.get('MYSQL_DSN')
if not APP_DB_DSN:
    raise RuntimeError('未检测到数据库配置，请设置 APP_MYSQL_DSN 或 MYSQL_DSN')

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
        try:
            g.db = connect_mysql(APP_DB_DSN, cursorclass=DictCursor)
        except MySQLConfigError as exc:
            abort(500, description=str(exc))
    return g.db


def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        try:
            db.close()
        except Exception:
            pass


def manager_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or not getattr(current_user, 'is_manager', False):
            abort(403)
        return func(*args, **kwargs)

    return wrapper


def init_db():
    conn = connect_mysql(APP_DB_DSN, cursorclass=DictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `users` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `username` VARCHAR(255) NOT NULL UNIQUE,
                    `password_hash` VARCHAR(255) NOT NULL,
                    `rss_token` VARCHAR(255) UNIQUE,
                    `rss_token_hash` VARCHAR(255),
                    `role` VARCHAR(32) NOT NULL DEFAULT 'user',
                    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `watchlist` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `user_id` INT NOT NULL,
                    `symbol` VARCHAR(32) NOT NULL,
                    `name` VARCHAR(255),
                    CONSTRAINT `fk_watchlist_user` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `trade_logs` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `user_id` INT NOT NULL,
                    `trade_date` DATE NOT NULL,
                    `symbol` VARCHAR(32) NOT NULL,
                    `action` VARCHAR(32) NOT NULL,
                    `quantity` DOUBLE NOT NULL,
                    `price` DOUBLE NOT NULL,
                    `fee` DOUBLE NOT NULL DEFAULT 0,
                    `stamp_tax` DOUBLE NOT NULL DEFAULT 0,
                    `asset_type` VARCHAR(32) NOT NULL DEFAULT 'stock',
                    `note` TEXT,
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT `fk_trade_user` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `initial_profits` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `user_id` INT NOT NULL,
                    `profit_date` DATE NOT NULL,
                    `amount` DOUBLE NOT NULL,
                    `note` TEXT,
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT `fk_initial_profit_user` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `daily_profit_snapshots` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `user_id` INT NOT NULL,
                    `snapshot_date` DATE NOT NULL,
                    `amount` DOUBLE NOT NULL,
                    `ratio` DOUBLE DEFAULT NULL,
                    `total_market_value` DOUBLE DEFAULT NULL,
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY `uniq_user_date` (`user_id`, `snapshot_date`),
                    CONSTRAINT `fk_daily_profit_user` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )

        # Backfill columns if missing
        with conn.cursor() as cur:
            cur.execute("SHOW COLUMNS FROM `users`")
            user_cols = {row['Field'] for row in cur.fetchall()}
            if 'rss_token_hash' not in user_cols:
                cur.execute("ALTER TABLE `users` ADD COLUMN `rss_token_hash` VARCHAR(255)")
            if 'serverchan_send_key' not in user_cols:
                cur.execute("ALTER TABLE `users` ADD COLUMN `serverchan_send_key` VARCHAR(255)")
            if 'role' not in user_cols:
                cur.execute("ALTER TABLE `users` ADD COLUMN `role` VARCHAR(32) NOT NULL DEFAULT 'user'")

            cur.execute("SHOW COLUMNS FROM `trade_logs`")
            trade_cols = {row['Field'] for row in cur.fetchall()}
            if 'asset_type' not in trade_cols:
                cur.execute("ALTER TABLE `trade_logs` ADD COLUMN `asset_type` VARCHAR(32) NOT NULL DEFAULT 'stock'")
            if 'stamp_tax' not in trade_cols:
                cur.execute("ALTER TABLE `trade_logs` ADD COLUMN `stamp_tax` DOUBLE NOT NULL DEFAULT 0")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `fund_holdings` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `user_id` INT NOT NULL,
                    `shares` DOUBLE NOT NULL,
                    `cost_amount` DOUBLE NOT NULL,
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY `uniq_user` (`user_id`),
                    CONSTRAINT `fk_fund_holdings_user` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `fund_nav_history` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `snapshot_time` DATETIME NOT NULL,
                    `nav` DOUBLE NOT NULL,
                    `total_assets` DOUBLE NOT NULL,
                    `total_shares` DOUBLE NOT NULL,
                    `cash_amount` DOUBLE NOT NULL,
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `fund_position_history` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `nav_id` INT NOT NULL,
                    `symbol` VARCHAR(32) NOT NULL,
                    `name` VARCHAR(255),
                    `market_value` DOUBLE NOT NULL,
                    `weight` DOUBLE NOT NULL,
                    CONSTRAINT `fk_position_nav` FOREIGN KEY (`nav_id`) REFERENCES `fund_nav_history`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `fund_cash_adjustments` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `amount` DOUBLE NOT NULL,
                    `note` VARCHAR(255),
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    `operator_id` INT,
                    CONSTRAINT `fk_cash_operator` FOREIGN KEY (`operator_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS `fund_settings` (
                    `key` VARCHAR(64) PRIMARY KEY,
                    `value` TEXT,
                    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute('SELECT `id` FROM `users` WHERE `username` = %s', ('manager',))
            exists = cur.fetchone()
            if not exists:
                password = generate_password_hash('weijie81')
                cur.execute(
                    'INSERT INTO `users` (`username`, `password_hash`, `role`) VALUES (%s, %s, %s)',
                    ('manager', password, 'manager'),
                )
        conn.commit()
    finally:
        conn.close()


app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')
login_manager = LoginManager(app)
login_manager.login_view = 'login'


class User(UserMixin):
    def __init__(self, id, username, password_hash, rss_token, serverchan_send_key, role='user'):
        self.id = id
        self.username = username
        self.password_hash = password_hash
        self.rss_token = rss_token
        self.serverchan_send_key = serverchan_send_key
        self.role = role or 'user'

    @property
    def is_manager(self) -> bool:
        return self.role == 'manager'


@login_manager.user_loader
def load_user(user_id):
    row = db_query_one(
        'SELECT `id`, `username`, `password_hash`, `rss_token`, `serverchan_send_key`, `role` FROM `users` WHERE `id` = %s',
        (user_id,),
    )
    if row:
        return User(
            row['id'],
            row['username'],
            row['password_hash'],
            row['rss_token'],
            row.get('serverchan_send_key'),
            row.get('role', 'user'),
        )
    return None


# Flask 3.0 移除了 before_first_request，直接在模块加载时初始化数据库
init_db()


def db_query_one(sql: str, params: Optional[tuple] = None) -> Optional[Dict]:
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()


def db_query_all(sql: str, params: Optional[tuple] = None) -> List[Dict]:
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def db_execute(sql: str, params: Optional[tuple] = None) -> int:
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.rowcount


def db_executemany(sql: str, params_seq: List[tuple]) -> None:
    if not params_seq:
        return
    conn = get_db()
    with conn.cursor() as cur:
        cur.executemany(sql, params_seq)


def get_fund_setting(key: str) -> Optional[str]:
    row = db_query_one('SELECT `value` FROM `fund_settings` WHERE `key` = %s', (key,))
    if row:
        return row['value']
    return None


def set_fund_setting(key: str, value: str) -> None:
    db_execute(
        'INSERT INTO `fund_settings` (`key`, `value`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)',
        (key, value),
    )


def get_cash_balance() -> float:
    row = db_query_one('SELECT COALESCE(SUM(`amount`), 0) AS total FROM `fund_cash_adjustments`')
    if row and row['total'] is not None:
        return float(row['total'])
    return 0.0


def get_latest_nav_record() -> Optional[Dict]:
    return db_query_one(
        'SELECT `id`, `snapshot_time`, `nav`, `total_assets`, `total_shares`, `cash_amount` FROM `fund_nav_history` ORDER BY `snapshot_time` DESC LIMIT 1'
    )


def get_user_id_by_username(username: str) -> Optional[int]:
    row = db_query_one('SELECT `id` FROM `users` WHERE `username` = %s', (username,))
    if row:
        return int(row['id'])
    return None


def build_fund_snapshot() -> Optional[Dict]:
    base_user_id = get_user_id_by_username('cwjcw')
    if not base_user_id:
        return None

    context = _get_portfolio_context(base_user_id, dt.date(2000, 1, 1), dt.date.today())
    positions = context.get('positions', []) if context else []

    enriched_positions: List[Dict] = []
    total_stock_value = 0.0
    for pos in positions:
        market_value = pos.get('market_value')
        if market_value is None:
            market_value = 0.0
        total_stock_value += market_value
        enriched_positions.append(
            {
                'symbol': pos.get('symbol'),
                'name': pos.get('name') or pos.get('symbol'),
                'market_value': market_value,
            }
        )

    cash_balance = get_cash_balance()
    total_assets = total_stock_value + cash_balance

    row = db_query_one('SELECT COALESCE(SUM(`shares`), 0) AS total_shares FROM `fund_holdings`')
    total_shares = float(row['total_shares']) if row and row['total_shares'] is not None else 0.0
    nav = (total_assets / total_shares) if total_shares > 0 else 0.0

    total_denom = total_assets if total_assets > 0 else 1.0
    positions_with_weights: List[Dict] = []
    for pos in enriched_positions:
        positions_with_weights.append(
            {
                'symbol': pos['symbol'],
                'name': pos['name'],
                'market_value': pos['market_value'],
                'weight': pos['market_value'] / total_denom if total_denom else 0.0,
            }
        )

    positions_with_weights.append(
        {
            'symbol': 'CASH',
            'name': '现金',
            'market_value': cash_balance,
            'weight': cash_balance / total_denom if total_denom else 0.0,
        }
    )

    snapshot_time = dt.datetime.now(CHINA_TZ).replace(tzinfo=None)

    return {
        'snapshot_time': snapshot_time,
        'nav': nav,
        'total_assets': total_assets,
        'total_shares': total_shares,
        'cash_amount': cash_balance,
        'positions': positions_with_weights,
    }


def record_nav_snapshot(snapshot: Dict) -> Optional[int]:
    if not snapshot or snapshot.get('total_shares', 0) <= 0:
        return None
    conn = get_db()
    nav_id: Optional[int] = None
    with conn.cursor() as cur:
        cur.execute(
            'INSERT INTO `fund_nav_history` (`snapshot_time`, `nav`, `total_assets`, `total_shares`, `cash_amount`) '
            'VALUES (%s, %s, %s, %s, %s)',
            (
                snapshot['snapshot_time'],
                snapshot['nav'],
                snapshot['total_assets'],
                snapshot['total_shares'],
                snapshot['cash_amount'],
            ),
        )
        nav_id = cur.lastrowid
        positions = snapshot.get('positions') or []
        if positions:
            cur.executemany(
                'INSERT INTO `fund_position_history` (`nav_id`, `symbol`, `name`, `market_value`, `weight`) VALUES (%s, %s, %s, %s, %s)',
                [
                    (
                        nav_id,
                        pos['symbol'],
                        pos['name'],
                        pos['market_value'],
                        pos['weight'],
                    )
                    for pos in positions
                ],
            )
    return nav_id


def ensure_recent_nav_snapshot(max_age_minutes: int = 5) -> Optional[Dict]:
    latest = get_latest_nav_record()
    now = dt.datetime.now(CHINA_TZ).replace(tzinfo=None)
    if latest:
        snap_time = latest['snapshot_time']
        if isinstance(snap_time, dt.datetime):
            delta = now - snap_time
            if delta.total_seconds() <= max_age_minutes * 60:
                return latest
    snapshot = build_fund_snapshot()
    if snapshot and snapshot['total_shares'] > 0:
        nav_id = record_nav_snapshot(snapshot)
        if nav_id:
            set_fund_setting('fund_last_nav_id', str(nav_id))
            return get_latest_nav_record()
    return latest


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
        existing = db_query_one('SELECT 1 FROM `users` WHERE `username` = %s', (username,))
        if existing:
            flash('用户名已存在', 'error')
            return render_template('register.html')
        token = secrets.token_urlsafe(24)
        token_hash = hashlib.sha256(token.encode('utf-8')).hexdigest()
        db_execute(
            'INSERT INTO `users` (`username`, `password_hash`, `rss_token`, `rss_token_hash`) VALUES (%s, %s, %s, %s)',
            (
                username,
                generate_password_hash(password),
                None if RSS_TOKEN_HASH_ONLY else token,
                token_hash if RSS_TOKEN_HASH_ONLY else None,
            ),
        )
        if RSS_TOKEN_HASH_ONLY:
            flash(f'注册成功，请妥善保存你的RSS Token（只显示一次）：{token}', 'success')
            flash('下次若遗失可在“我的股票”页面重置 Token。', 'success')
            # 直接登录并跳转到watchlist，方便复制
            row = db_query_one(
                'SELECT `id`, `username`, `password_hash`, `rss_token`, `rss_token_hash`, `serverchan_send_key`, `role` FROM `users` WHERE `username` = %s',
                (username,),
            )
            user = User(
                row['id'],
                row['username'],
                row['password_hash'],
                row['rss_token'],
                row.get('serverchan_send_key'),
                row.get('role', 'user'),
            )
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
        row = db_query_one(
            'SELECT `id`, `username`, `password_hash`, `rss_token`, `serverchan_send_key`, `role` FROM `users` WHERE `username` = %s',
            (username,),
        )
        if not row or not check_password_hash(row['password_hash'], password):
            flash('用户名或密码错误', 'error')
            return render_template('login.html')
        user = User(
            row['id'],
            row['username'],
            row['password_hash'],
            row['rss_token'],
            row.get('serverchan_send_key'),
            row.get('role', 'user'),
        )
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


def _parse_iso_date(value: str | dt.date | dt.datetime | None) -> dt.date | None:
    if value is None or value == '':
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):  # already date object
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return dt.date.fromisoformat(text)
        except ValueError:
            return None
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


def _get_portfolio_context(user_id: int, start_date: dt.date, end_date: dt.date) -> dict:
    rows = db_query_all(
        'SELECT `trade_date`, `symbol`, `action`, `quantity`, `price`, `fee`, `stamp_tax`, `asset_type` '
        'FROM `trade_logs` WHERE `user_id` = %s ORDER BY `trade_date` ASC, `id` ASC',
        (user_id,),
    )

    symbol_asset: Dict[str, str] = {}
    for r in rows:
        asset = (r['asset_type'] or 'stock') if 'asset_type' in r.keys() else 'stock'
        symbol_asset[r['symbol']] = asset

    initial_quotes = _fetch_quotes_for_symbols(list(symbol_asset.items())) if symbol_asset else {}
    name_cache = {sym: info.get('name') for sym, info in initial_quotes.items() if info.get('name')}

    portfolio = _build_portfolio(rows, start_date, end_date, name_cache)
    holdings: Dict[str, dict] = portfolio['holdings']

    symbols = sorted(holdings.keys())
    quotes = _fetch_quotes_for_symbols(
        [(sym, holdings[sym].get('asset_type', 'stock')) for sym in symbols]
    ) if symbols else {}

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
            'asset_type': asset_type_val,
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
        [{'symbol': sym, 'value': val} for sym, val in portfolio['realized_period'].items()],
        key=lambda x: x['symbol'],
    )

    profits = db_query_all(
        'SELECT `id`, `profit_date`, `amount`, `note` FROM `initial_profits` '
        'WHERE `user_id` = %s ORDER BY `profit_date` DESC, `id` DESC',
        (user_id,),
    )
    initial_period_total = 0.0
    for row in profits:
        pdate = _parse_iso_date(row['profit_date'])
        if pdate and start_date <= pdate <= end_date:
            initial_period_total += float(row['amount'])

    realized_with_initial = portfolio['realized_total'] + initial_period_total
    combined_total = realized_with_initial + unrealized_total
    daily_total = daily_stock_pnl + daily_fund_pnl
    base_for_daily_ratio = total_market_value if abs(total_market_value) > 1e-9 else total_cost_basis
    daily_ratio = None
    if base_for_daily_ratio and abs(base_for_daily_ratio) > 1e-9:
        daily_ratio = (daily_total / base_for_daily_ratio) * 100
    unrealized_ratio = None
    if total_cost_basis and abs(total_cost_basis) > 1e-9:
        unrealized_ratio = (unrealized_total / total_cost_basis) * 100

    return {
        'positions': positions,
        'realized_items': realized_items,
        'realized_total': portfolio['realized_total'],
        'realized_all_time': portfolio['realized_all_time'],
        'realized_with_initial': realized_with_initial,
        'initial_period_total': initial_period_total,
        'initial_profits': profits,
        'unrealized_total': unrealized_total,
        'total_market_value': total_market_value,
        'total_cost_basis': total_cost_basis,
        'combined_total': combined_total,
        'daily_stock_pnl': daily_stock_pnl,
        'daily_fund_pnl': daily_fund_pnl,
        'daily_total': daily_total,
        'daily_ratio': daily_ratio,
        'unrealized_ratio': unrealized_ratio,
    }


def _record_daily_snapshot(user_id: int, snapshot: Optional[dict] = None, snapshot_date: Optional[dt.date] = None) -> bool:
    target_date = snapshot_date or dt.datetime.now(CHINA_TZ).date()
    data = snapshot
    if data is None:
        data = _get_portfolio_context(user_id, target_date.replace(month=1, day=1), target_date)
    if not data:
        return False
    db_execute(
        'INSERT INTO `daily_profit_snapshots` (`user_id`, `snapshot_date`, `amount`, `ratio`, `total_market_value`) '
        'VALUES (%s, %s, %s, %s, %s) '
        'ON DUPLICATE KEY UPDATE `amount` = VALUES(`amount`), `ratio` = VALUES(`ratio`), `total_market_value` = VALUES(`total_market_value`), `updated_at` = CURRENT_TIMESTAMP',
        (
            user_id,
            target_date,
            data.get('daily_total'),
            data.get('daily_ratio'),
            data.get('total_market_value'),
        ),
    )
    return True


def _record_daily_snapshots_for_all_users(snapshot_date: Optional[dt.date] = None) -> None:
    target_date = snapshot_date or dt.datetime.now(CHINA_TZ).date()
    users = db_query_all('SELECT `id` FROM `users`')
    for row in users:
        try:
            _record_daily_snapshot(row['id'], snapshot_date=target_date)
        except Exception:
            app.logger.exception('记录用户 %s 的每日盈亏数据失败', row['id'])


_DAILY_SNAPSHOT_THREAD_STARTED = False


def _start_daily_snapshot_worker() -> None:
    global _DAILY_SNAPSHOT_THREAD_STARTED
    if _DAILY_SNAPSHOT_THREAD_STARTED:
        return

    def _worker() -> None:
        last_recorded: Optional[dt.date] = None
        interval = max(120, int(os.environ.get('DAILY_SNAPSHOT_INTERVAL', '600')))
        while True:
            current = dt.datetime.now(CHINA_TZ).date()
            if current != last_recorded:
                try:
                    with app.app_context():
                        _record_daily_snapshots_for_all_users(current)
                        last_recorded = current
                except Exception:
                    app.logger.exception('每日盈亏写入线程执行失败')
            time.sleep(interval)

    thread = threading.Thread(target=_worker, name='DailySnapshotWorker', daemon=True)
    thread.start()
    _DAILY_SNAPSHOT_THREAD_STARTED = True


@app.route('/watchlist', methods=['GET', 'POST'])
@login_required
def watchlist_view():
    if request.method == 'POST':
        raw = request.form.get('symbols', '')
        send_key = request.form.get('serverchan_send_key', '').strip()
        items = _parse_symbols(raw)
        db_execute('DELETE FROM `watchlist` WHERE `user_id` = %s', (current_user.id,))
        payload = [(current_user.id, it['symbol'], it['name']) for it in items]
        db_executemany(
            'INSERT INTO `watchlist` (`user_id`, `symbol`, `name`) VALUES (%s, %s, %s)',
            payload,
        )
        db_execute(
            'UPDATE `users` SET `serverchan_send_key` = %s WHERE `id` = %s',
            (send_key or None, current_user.id),
        )
        current_user.serverchan_send_key = send_key or None
        flash('已保存股票列表', 'success')
        return redirect(url_for('watchlist_view'))

    rows = db_query_all(
        'SELECT `symbol`, `name` FROM `watchlist` WHERE `user_id` = %s ORDER BY `id`',
        (current_user.id,),
    )
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
        serverchan_send_key=current_user.serverchan_send_key or '',
    )


@app.route('/fund', methods=['GET'])
@login_required
def fund_overview_view():
    nav_record = ensure_recent_nav_snapshot()
    holding_row = db_query_one(
        'SELECT `shares`, `cost_amount` FROM `fund_holdings` WHERE `user_id` = %s',
        (current_user.id,),
    )
    shares = float(holding_row['shares']) if holding_row and holding_row['shares'] is not None else 0.0
    cost_amount = float(holding_row['cost_amount']) if holding_row and holding_row['cost_amount'] is not None else 0.0

    nav = float(nav_record['nav']) if nav_record and nav_record['nav'] is not None else 0.0
    total_assets = float(nav_record['total_assets']) if nav_record and nav_record['total_assets'] is not None else 0.0
    total_shares = float(nav_record['total_shares']) if nav_record and nav_record['total_shares'] is not None else 0.0
    cash_amount = float(nav_record['cash_amount']) if nav_record and nav_record['cash_amount'] is not None else get_cash_balance()

    current_value = shares * nav
    profit = current_value - cost_amount
    cost_per_share = (cost_amount / shares) if shares > 0 else None
    share_ratio = (shares / total_shares * 100) if total_shares > 0 else None

    nav_history_rows = db_query_all(
        'SELECT `snapshot_time`, `nav`, `total_assets`, `total_shares`, `cash_amount` FROM `fund_nav_history` ORDER BY `snapshot_time` DESC LIMIT 30'
    )
    nav_history = [
        {
            'snapshot_time': row['snapshot_time'],
            'nav': float(row['nav']) if row['nav'] is not None else 0.0,
            'total_assets': float(row['total_assets']) if row['total_assets'] is not None else 0.0,
            'total_shares': float(row['total_shares']) if row['total_shares'] is not None else 0.0,
            'cash_amount': float(row['cash_amount']) if row['cash_amount'] is not None else 0.0,
        }
        for row in nav_history_rows
    ]

    latest_positions: List[Dict] = []
    if nav_record:
        rows = db_query_all(
            'SELECT `symbol`, `name`, `market_value`, `weight` FROM `fund_position_history` WHERE `nav_id` = %s ORDER BY `weight` DESC',
            (nav_record['id'],),
        )
        for row in rows:
            latest_positions.append(
                {
                    'symbol': row['symbol'],
                    'name': row['name'],
                    'market_value': float(row['market_value']) if row['market_value'] is not None else 0.0,
                    'weight': float(row['weight']) if row['weight'] is not None else 0.0,
                    'weight_percent': (float(row['weight']) * 100) if row['weight'] is not None else 0.0,
                }
            )

    return render_template(
        'fund_overview.html',
        nav=nav,
        total_assets=total_assets,
        total_shares=total_shares,
        cash_amount=cash_amount,
        shares=shares,
        cost_amount=cost_amount,
        cost_per_share=cost_per_share,
        current_value=current_value,
        profit=profit,
        share_ratio=share_ratio,
        nav_history=nav_history,
        latest_positions=latest_positions,
    )


def _fetch_fund_dashboard_data() -> Dict:
    nav_record = ensure_recent_nav_snapshot()
    users = db_query_all('SELECT `id`, `username`, `role` FROM `users` ORDER BY `username`')
    holdings_rows = db_query_all('SELECT `user_id`, `shares`, `cost_amount` FROM `fund_holdings`')
    holdings_map = {row['user_id']: row for row in holdings_rows}
    cash_balance = get_cash_balance()
    nav_value = float(nav_record['nav']) if nav_record and nav_record['nav'] is not None else 1.0
    nav_total_shares = float(nav_record['total_shares']) if nav_record and nav_record['total_shares'] is not None else 0.0
    nav_total_assets = float(nav_record['total_assets']) if nav_record and nav_record['total_assets'] is not None else 0.0
    nav_cash = float(nav_record['cash_amount']) if nav_record and nav_record['cash_amount'] is not None else cash_balance

    total_cost = 0.0
    computed_total_shares = 0.0
    user_rows: List[Dict] = []
    for user in users:
        holding = holdings_map.get(user['id'])
        shares = float(holding['shares']) if holding and holding['shares'] is not None else 0.0
        cost_amount = float(holding['cost_amount']) if holding and holding['cost_amount'] is not None else 0.0
        current_value = shares * nav_value
        profit = current_value - cost_amount
        total_cost += cost_amount
        computed_total_shares += shares
        user_rows.append(
            {
                'id': user['id'],
                'username': user['username'],
                'role': user['role'],
                'shares': shares,
                'cost_amount': cost_amount,
                'current_value': current_value,
                'profit': profit,
            }
        )

    total_shares = nav_total_shares if nav_total_shares > 0 else computed_total_shares
    total_assets = nav_total_assets if nav_total_assets > 0 else (nav_value * total_shares + cash_balance)
    total_value = nav_value * total_shares
    total_profit = total_value - total_cost
    initialized_at = get_fund_setting('fund_initialized_at')
    initialized_by = get_fund_setting('fund_initialized_by')

    cash_logs = db_query_all(
        'SELECT `id`, `amount`, `note`, `created_at` FROM `fund_cash_adjustments` ORDER BY `created_at` DESC LIMIT 20'
    )

    return {
        'users': user_rows,
        'nav_record': nav_record,
        'nav': nav_value,
        'cash_balance': cash_balance,
        'nav_cash': nav_cash,
        'total_shares': total_shares,
        'total_assets': total_assets,
        'total_value': total_value,
        'total_cost': total_cost,
        'total_profit': total_profit,
        'initialized_at': initialized_at,
        'initialized_by': initialized_by,
        'cash_logs': cash_logs,
    }


@app.route('/manager/fund')
@login_required
@manager_required
def fund_manager_dashboard():
    data = _fetch_fund_dashboard_data()
    return render_template('fund_admin.html', **data)


@app.route('/manager/fund/holdings', methods=['POST'])
@login_required
@manager_required
def fund_manager_update_holding():
    user_id_raw = request.form.get('user_id', '').strip()
    shares_raw = request.form.get('shares', '').strip()
    cost_raw = request.form.get('cost_amount', '').strip()
    try:
        user_id = int(user_id_raw)
    except (TypeError, ValueError):
        flash('无效的用户ID', 'error')
        return redirect(url_for('fund_manager_dashboard'))

    user_row = db_query_one('SELECT `id`, `username` FROM `users` WHERE `id` = %s', (user_id,))
    if not user_row:
        flash('用户不存在', 'error')
        return redirect(url_for('fund_manager_dashboard'))

    username = user_row['username']
    try:
        shares = float(shares_raw)
        cost_amount = float(cost_raw)
    except (TypeError, ValueError):
        flash('请输入有效的份额和成本金额', 'error')
        return redirect(url_for('fund_manager_dashboard'))

    if shares <= 0:
        db_execute('DELETE FROM `fund_holdings` WHERE `user_id` = %s', (user_id,))
        flash(f'已清除 {username} 的基金份额记录。', 'success')
    else:
        db_execute(
            'INSERT INTO `fund_holdings` (`user_id`, `shares`, `cost_amount`) VALUES (%s, %s, %s) '
            'ON DUPLICATE KEY UPDATE `shares` = VALUES(`shares`), `cost_amount` = VALUES(`cost_amount`)',
            (user_id, shares, cost_amount),
        )
        flash(f'已更新 {username} 的基金份额。', 'success')
    return redirect(url_for('fund_manager_dashboard'))


@app.route('/manager/fund/cash', methods=['POST'])
@login_required
@manager_required
def fund_manager_adjust_cash():
    amount_raw = request.form.get('amount', '').strip()
    note = request.form.get('note', '').strip() or None
    try:
        amount = float(amount_raw)
    except (TypeError, ValueError):
        flash('请输入有效的现金金额', 'error')
        return redirect(url_for('fund_manager_dashboard'))
    if amount == 0:
        flash('金额不能为0', 'error')
        return redirect(url_for('fund_manager_dashboard'))
    db_execute(
        'INSERT INTO `fund_cash_adjustments` (`amount`, `note`, `operator_id`) VALUES (%s, %s, %s)',
        (amount, note, current_user.id),
    )
    flash('现金调整已记录。', 'success')
    return redirect(url_for('fund_manager_dashboard'))


@app.route('/manager/fund/init', methods=['POST'])
@login_required
@manager_required
def fund_manager_initialize():
    token = (request.form.get('auth_token') or '').strip()
    if token != 'weijie81':
        flash('初始化口令不正确，操作已取消。', 'error')
        return redirect(url_for('fund_manager_dashboard'))

    snapshot = build_fund_snapshot()
    if not snapshot:
        flash('无法生成基金仓位快照，请确认 cwjcw 用户及持仓数据已配置。', 'error')
        return redirect(url_for('fund_manager_dashboard'))
    if snapshot['total_shares'] <= 0:
        flash('基金份额总量为0，请先录入各用户份额。', 'error')
        return redirect(url_for('fund_manager_dashboard'))

    nav_id = record_nav_snapshot(snapshot)
    if not nav_id:
        flash('保存基金快照失败，请稍后重试。', 'error')
        return redirect(url_for('fund_manager_dashboard'))

    now = snapshot['snapshot_time']
    set_fund_setting('fund_initialized_at', now.isoformat())
    set_fund_setting('fund_initialized_by', str(current_user.id))
    set_fund_setting('fund_initial_nav_id', str(nav_id))
    set_fund_setting('fund_initial_nav_value', f"{snapshot['nav']:.6f}")
    flash('基金已完成初始化基准，已记录当前仓位与净值。', 'success')
    return redirect(url_for('fund_manager_dashboard'))


@app.route('/trades', methods=['GET', 'POST'])
@login_required
def trades_view():
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

        db_execute(
            'INSERT INTO `trade_logs` (`user_id`, `trade_date`, `symbol`, `action`, `quantity`, `price`, `fee`, `stamp_tax`, `asset_type`, `note`) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (current_user.id, trade_date, symbol, action, quantity, price, fee, stamp_tax, asset_type, None),
        )
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
    total_row = db_query_one('SELECT COUNT(1) AS cnt FROM `trade_logs` WHERE `user_id` = %s', (current_user.id,))
    total = total_row['cnt'] if total_row else 0
    total = total or 0
    max_page = max(1, math.ceil(total / per_page)) if total else 1
    if page > max_page:
        page = max_page
    offset = (page - 1) * per_page
    trades = db_query_all(
        'SELECT `id`, `trade_date`, `symbol`, `action`, `quantity`, `price`, `fee`, `stamp_tax`, `asset_type`, `note`, `created_at` '
        'FROM `trade_logs` WHERE `user_id` = %s ORDER BY `trade_date` DESC, `id` DESC LIMIT %s OFFSET %s',
        (current_user.id, per_page, offset),
    )

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
    trade = db_query_one(
        'SELECT `id`, `trade_date`, `symbol`, `action`, `quantity`, `price`, `fee`, `stamp_tax`, `asset_type`, `note` '
        'FROM `trade_logs` WHERE `id` = %s AND `user_id` = %s',
        (trade_id, current_user.id),
    )
    if not trade:
        abort(404)
    return trade


@app.route('/trades/<int:trade_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_trade(trade_id: int):
    trade = _get_trade_or_404(trade_id)
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

        db_execute(
            'UPDATE `trade_logs` SET `trade_date`=%s, `symbol`=%s, `action`=%s, `quantity`=%s, `price`=%s, `fee`=%s, '
            '`stamp_tax`=%s, `asset_type`=%s, `note`=NULL WHERE `id`=%s AND `user_id`=%s',
            (trade_date, normalized_symbol, action, quantity, price, fee, stamp_tax, asset_type, trade_id, current_user.id),
        )
        flash('已更新交易。', 'success')
        return redirect(url_for('trades_view'))

    return render_template('trade_edit.html', trade=trade)


@app.route('/trades/<int:trade_id>/delete', methods=['POST'])
@login_required
def delete_trade(trade_id: int):
    rowcount = db_execute('DELETE FROM `trade_logs` WHERE `id` = %s AND `user_id` = %s', (trade_id, current_user.id))
    if rowcount:
        flash('已删除交易。', 'success')
    else:
        flash('未找到对应交易。', 'error')
    return redirect(url_for('trades_view'))


@app.route('/portfolio', methods=['GET'])
@login_required
def portfolio_view():
    today = dt.date.today()
    default_start = today.replace(month=1, day=1)
    start_raw = request.args.get('start') or default_start.isoformat()
    end_raw = request.args.get('end') or today.isoformat()
    start_date = _parse_iso_date(start_raw) or default_start
    end_date = _parse_iso_date(end_raw) or today
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    context = _get_portfolio_context(current_user.id, start_date, end_date)

    return render_template(
        'portfolio.html',
        positions=context['positions'],
        realized_total=context['realized_total'],
        realized_with_initial=context['realized_with_initial'],
        realized_items=context['realized_items'],
        realized_all_time=context['realized_all_time'],
        unrealized_total=context['unrealized_total'],
        total_market_value=context['total_market_value'],
        total_cost_basis=context['total_cost_basis'],
        combined_total=context['combined_total'],
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        initial_period_total=context['initial_period_total'],
        initial_profits=context['initial_profits'],
        today_str=today.isoformat(),
        daily_stock_pnl=context['daily_stock_pnl'],
        daily_fund_pnl=context['daily_fund_pnl'],
        daily_total=context['daily_total'],
        daily_ratio=context['daily_ratio'],
        unrealized_ratio=context['unrealized_ratio'],
    )


@app.route('/daily_profits', methods=['GET'])
@login_required
def daily_profits_view():
    today = dt.date.today()
    default_start = today - dt.timedelta(days=29)
    start_raw = request.args.get('start') or default_start.isoformat()
    end_raw = request.args.get('end') or today.isoformat()
    start_date = _parse_iso_date(start_raw) or default_start
    end_date = _parse_iso_date(end_raw) or today
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    rows = db_query_all(
        'SELECT `snapshot_date`, `amount`, `ratio`, `total_market_value` '
        'FROM `daily_profit_snapshots` WHERE `user_id` = %s AND `snapshot_date` BETWEEN %s AND %s '
        'ORDER BY `snapshot_date` ASC',
        (current_user.id, start_date, end_date),
    )

    records = []
    for row in rows:
        snap_date = row['snapshot_date']
        amount = row['amount']
        ratio = row['ratio']
        total_mv = row['total_market_value']
        records.append(
            {
                'date': snap_date.isoformat() if hasattr(snap_date, 'isoformat') else str(snap_date),
                'amount': float(amount) if amount is not None else None,
                'ratio': float(ratio) if ratio is not None else None,
                'total_market_value': float(total_mv) if total_mv is not None else None,
            }
        )

    labels = [rec['date'] for rec in records]
    chart_amounts = [rec['amount'] if rec['amount'] is not None else 0 for rec in records]
    chart_ratios = [rec['ratio'] for rec in records]

    amount_values = [rec['amount'] for rec in records if rec['amount'] is not None]
    market_values = [rec['total_market_value'] for rec in records if rec['total_market_value'] is not None]
    total_amount = sum(amount_values)
    total_market_value = sum(market_values)
    average_ratio = None
    if total_market_value and abs(total_market_value) > 1e-9:
        average_ratio = (total_amount / total_market_value) * 100

    return render_template(
        'daily_profits.html',
        records=records,
        labels=labels,
        amounts=chart_amounts,
        ratios=chart_ratios,
        total_amount=total_amount,
        average_ratio=average_ratio,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )


@app.route('/initial_profits', methods=['POST'])
@login_required
def add_initial_profit():
    amount_raw = request.form.get('amount', '').strip()
    try:
        amount = float(amount_raw)
    except ValueError:
        flash('初始盈利需为数字。', 'error')
        return redirect(url_for('portfolio_view'))
    profit_date = dt.date.today().isoformat()
    db_execute(
        'INSERT INTO `initial_profits` (`user_id`, `profit_date`, `amount`, `note`) VALUES (%s, %s, %s, %s)',
        (current_user.id, profit_date, amount, None),
    )
    flash('已记录初始盈利。', 'success')
    return redirect(url_for('portfolio_view'))


@app.route('/initial_profits/<int:profit_id>/delete', methods=['POST'])
@login_required
def delete_initial_profit(profit_id: int):
    rowcount = db_execute('DELETE FROM `initial_profits` WHERE `id` = %s AND `user_id` = %s', (profit_id, current_user.id))
    if rowcount:
        flash('已删除初始盈利记录。', 'success')
    else:
        flash('未找到对应记录。', 'error')
    return redirect(url_for('portfolio_view'))


@app.route('/reset_token', methods=['POST'])
@login_required
def reset_token():
    token = secrets.token_urlsafe(24)
    token_hash = hashlib.sha256(token.encode('utf-8')).hexdigest()
    # 构造完整订阅链接
    resolved_prefix = current_user.username if RSS_PREFIX == 'username' else RSS_PREFIX
    rss_url = f"https://{PUBLIC_DOMAIN}/{resolved_prefix}/{token}.rss" if RSS_PREFIX else f"https://{PUBLIC_DOMAIN}/u/{token}.rss"
    if RSS_TOKEN_HASH_ONLY:
        db_execute('UPDATE `users` SET `rss_token`=NULL, `rss_token_hash`=%s WHERE `id`=%s', (token_hash, current_user.id))
        flash(f'新的RSS Token（只显示一次）：{token}', 'success')
        flash(f'你的专属RSS订阅链接（只显示一次）：{rss_url}', 'success')
    else:
        db_execute('UPDATE `users` SET `rss_token`=%s, `rss_token_hash`=NULL WHERE `id`=%s', (token, current_user.id))
        flash('已重置RSS Token。', 'success')
    return redirect(url_for('watchlist_view'))


def _find_user_by_token(token: str):
    # match by plain token or sha256(token)
    row = db_query_one('SELECT `id`, `username` FROM `users` WHERE `rss_token` = %s', (token,))
    if row:
        return row
    th = hashlib.sha256(token.encode('utf-8')).hexdigest()
    return db_query_one('SELECT `id`, `username` FROM `users` WHERE `rss_token_hash` = %s', (th,))


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

    user_row = _find_user_by_token(token)
    if not user_row:
        return ('Not found', 404)
    items = db_query_all('SELECT `symbol`, `name` FROM `watchlist` WHERE `user_id` = %s ORDER BY `id`', (user_row['id'],))
    watch_entries = [(r['name'] or r['symbol'], r['symbol']) for r in items]

    async def build_items():
        if not watch_entries:
            return []
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            payload = []
            for name, symbol in watch_entries:
                try:
                    secid = symbol_to_secid(symbol)
                except Exception:
                    continue
                payload.append({'name': name, 'symbol': symbol, 'secid': secid})
            if not payload:
                return []
            flow_tasks = [fetch_latest_minute(session, entry['secid']) for entry in payload]
            quote_tasks = [fetch_quote_basic(session, entry['secid']) for entry in payload]
            flow_results = await asyncio.gather(*flow_tasks, return_exceptions=True)
            quote_results = await asyncio.gather(*quote_tasks, return_exceptions=True)
        now = dt.datetime.now(CHINA_TZ)
        aggregated: List[dict] = []

        for idx, (entry, flow_res, quote_res) in enumerate(zip(payload, flow_results, quote_results)):
            if isinstance(flow_res, Exception):
                continue
            if not isinstance(flow_res, (tuple, list)) or len(flow_res) != 2:
                continue
            if isinstance(quote_res, Exception):
                quote_res = None
            nret, rowd = flow_res
            if not rowd:
                continue
            price = quote_res.get('price') if isinstance(quote_res, dict) else None
            chg = quote_res.get('change_pct') if isinstance(quote_res, dict) else None
            mcap = quote_res.get('market_cap') if isinstance(quote_res, dict) else None
            mcap_yi = None if mcap is None else round(mcap / 1e8, 2)
            base_time = rowd.get('time')
            pub_dt = None
            if base_time:
                for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
                    try:
                        pub_dt = dt.datetime.strptime(base_time, fmt).replace(tzinfo=CHINA_TZ)
                        break
                    except ValueError:
                        continue
            if pub_dt is None:
                pub_dt = now
                base_time = now.strftime('%Y-%m-%d %H:%M')

            super_val = rowd.get('超大单')
            large_val = rowd.get('大单')
            medium_val = rowd.get('中单')
            small_val = rowd.get('小单')

            aggregated.append({
                'order': idx,
                'name': entry['name'],
                'symbol': entry['symbol'],
                'period': nret or '',
                'time_text': base_time,
                'price': price,
                'change_pct': chg,
                'market_cap_yi': mcap_yi,
                'flows': {
                    '主力': rowd.get('主力'),
                    '超大单': small_val,
                    '大单': medium_val,
                    '中单': large_val,
                    '小单': super_val,
                },
                'pub_dt': pub_dt,
            })

        if not aggregated:
            return []

        aggregated.sort(key=lambda x: x['order'])
        latest_pub = max((row['pub_dt'] for row in aggregated), default=now)
        latest_text = latest_pub.astimezone(CHINA_TZ).strftime('%Y-%m-%d %H:%M')

        def color_num(val, suffix=''):
            try:
                v = float(val)
            except (TypeError, ValueError):
                return '-' if val in (None, '') else str(val)
            color = '#c62828' if v > 0 else '#1e7a1e' if v < 0 else '#333'
            return f"<span style='color:{color}'>{v:.2f}{suffix}</span>"

        table_style = "width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed;word-break:break-word;"
        th_style = "style='padding:4px;border:1px solid #ddd;background:#f7f7f7;text-align:left;font-weight:600;'"
        td_text_style = "style='padding:4px;border:1px solid #ddd;vertical-align:top;line-height:1.4;word-break:break-word;'"
        td_num_style = "style='padding:4px;border:1px solid #ddd;vertical-align:top;line-height:1.4;text-align:right;white-space:nowrap;'"

        header_titles = ["周期/时间", "最新价", "涨跌幅", "总市值", "主力", "超大单", "大单", "中单", "小单"]
        header_html = ''.join(f"<th {th_style}>{title}</th>" for title in header_titles)

        rows_html = ""

        for agg in aggregated:
            price_txt = '-' if agg['price'] is None else f"{agg['price']:.2f}"
            change_html = '-' if agg['change_pct'] is None else color_num(agg['change_pct'], '%')
            mcap_txt = '-' if agg['market_cap_yi'] is None else f"{agg['market_cap_yi']:.2f}亿"
            name_line = agg['name'] or ''
            rows_html += (
                "<tr>"
                f"<td {td_text_style}><strong>{agg['period'] or '-'}</strong><br><span style='color:#888;font-size:0.85em'>{agg['time_text']}</span><br><span style='color:#555;font-size:0.85em'>{name_line}</span></td>"
                f"<td {td_num_style}>{price_txt}</td>"
                f"<td {td_num_style}>{change_html}</td>"
                f"<td {td_num_style}>{mcap_txt}</td>"
                f"<td {td_num_style}>{color_num(agg['flows']['主力'], '亿')}</td>"
                f"<td {td_num_style}>{color_num(agg['flows']['超大单'], '亿')}</td>"
                f"<td {td_num_style}>{color_num(agg['flows']['大单'], '亿')}</td>"
                f"<td {td_num_style}>{color_num(agg['flows']['中单'], '亿')}</td>"
                f"<td {td_num_style}>{color_num(agg['flows']['小单'], '亿')}</td>"
                "</tr>"
            )

        desc = (
            f"<p>合并覆盖标的：{len(aggregated)} 支</p>"
            f"<p>最新更新时间：{latest_text}</p>"
            f"<table style='{table_style}'>"
            f"<tr>{header_html}</tr>"
            f"{rows_html}" \
            "</table>"
        )

        digest_parts = [
            f"{agg['symbol']}|{agg['time_text']}|{agg['flows'].get('主力')}|{agg['flows'].get('超大单')}|{agg['flows'].get('大单')}|{agg['flows'].get('中单')}|{agg['flows'].get('小单')}"
            for agg in aggregated
        ]
        guid_hash = hashlib.sha1('||'.join(digest_parts).encode('utf-8')).hexdigest()[:12]

        aggregated_item = {
            'guid': f"fundflow_{user_row['id']}_{guid_hash}",
            'title': f"资金流汇总 {latest_text}",
            'description': desc,
            'pubDate': latest_pub.strftime('%a, %d %b %Y %H:%M:%S %z'),
            'link': f"https://{PUBLIC_DOMAIN}/watchlist",
        }
        return [aggregated_item]

    items = asyncio.run(build_items())

    snapshot = _get_portfolio_context(user_row['id'], dt.date.today().replace(month=1, day=1), dt.date.today())
    if snapshot:
        _record_daily_snapshot(user_row['id'], snapshot=snapshot)
        positions = snapshot['positions']

        def fmt_currency(val: Optional[float]) -> str:
            if val is None:
                return '-'
            try:
                return f"{val:.2f}"
            except Exception:
                return '-'

        def fmt_pct(val: Optional[float]) -> str:
            return '-' if val is None else f"{val:.2f}%"

        def color_span(val: Optional[float], suffix: str = '') -> str:
            if val is None:
                return '-'
            try:
                num = float(val)
            except (TypeError, ValueError):
                return str(val)
            color = '#c62828' if num > 0 else '#1e7a1e' if num < 0 else '#333'
            return f"<span style='color:{color}'>{num:.2f}{suffix}</span>"

        table_style = "width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed;word-break:break-word;"
        th_style = "style='padding:4px;border:1px solid #ddd;background:#f7f7f7;text-align:left;font-weight:600;'"
        td_text_style = "style='padding:4px;border:1px solid #ddd;vertical-align:top;line-height:1.4;word-break:break-word;'"
        td_num_style = "style='padding:4px;border:1px solid #ddd;vertical-align:top;line-height:1.4;text-align:right;white-space:nowrap;'"

        header_titles = ["名称", "最新价", "涨跌幅", "市值", "持仓盈亏", "持仓盈亏%", "当日收益", "当日收益%"]
        header_html = ''.join(f"<th {th_style}>{title}</th>" for title in header_titles)

        table_rows = ""
        for pos in positions[:10]:
            price_val = pos['price']
            price_txt = '-' if price_val is None else f"{price_val:.2f}"
            change_cell = color_span(pos['change_pct'], suffix='%')
            market_txt = fmt_currency(pos['market_value'])
            unrealized_cell = color_span(pos['unrealized'])
            cost_basis_val = pos['cost_basis']
            unrealized_pct_val = None if pos['unrealized'] is None or cost_basis_val in (None, 0) else (pos['unrealized'] / cost_basis_val) * 100
            unrealized_pct_cell = color_span(unrealized_pct_val, suffix='%') if unrealized_pct_val is not None else '-'
            daily_change_val = pos['daily_change']
            daily_cell = color_span(daily_change_val)
            daily_pct_val = None
            if daily_change_val is not None:
                base = cost_basis_val if cost_basis_val not in (None, 0) else pos['market_value']
                if base not in (None, 0):
                    daily_pct_val = (daily_change_val / base) * 100
            daily_pct_cell = color_span(daily_pct_val, suffix='%') if daily_pct_val is not None else '-'
            table_rows += (
                "<tr>"
                f"<td {td_text_style}>{pos['name'] or pos['symbol']}</td>"
                f"<td {td_num_style}>{price_txt}</td>"
                f"<td {td_num_style}>{change_cell}</td>"
                f"<td {td_num_style}>{market_txt}</td>"
                f"<td {td_num_style}>{unrealized_cell}</td>"
                f"<td {td_num_style}>{unrealized_pct_cell}</td>"
                f"<td {td_num_style}>{daily_cell}</td>"
                f"<td {td_num_style}>{daily_pct_cell}</td>"
                "</tr>"
            )

        daily_ratio_txt = fmt_pct(snapshot.get('daily_ratio'))
        portfolio_desc = (
            f"<p>周期盈亏：{fmt_currency(snapshot['realized_with_initial'])} 元</p>"
            f"<p>持仓盈亏：{fmt_currency(snapshot['unrealized_total'])} 元</p>"
            f"<p>当日盈亏：{fmt_currency(snapshot['daily_total'])} 元 (股票：{fmt_currency(snapshot['daily_stock_pnl'])} 元；基金：{fmt_currency(snapshot['daily_fund_pnl'])} 元；比例：{daily_ratio_txt})</p>"
            f"<table style='{table_style}'>"
            f"<tr>{header_html}</tr>"
            f"{table_rows}" \
            "</table>"
        )

        timestamp = dt.datetime.now(CHINA_TZ)
        portfolio_item = {
            'guid': f"portfolio_{user_row['id']}_{timestamp.strftime('%Y%m%d%H%M%S')}",
            'title': f"持仓与盈亏 {timestamp.strftime('%Y-%m-%d %H:%M')}",
            'description': portfolio_desc,
            'pubDate': timestamp.strftime('%a, %d %b %Y %H:%M:%S %z'),
            'link': f"https://{PUBLIC_DOMAIN}/portfolio",
        }
        items.insert(0, portfolio_item)
    # Build RSS XML
    def _wrap_cdata(text: str) -> str:
        safe = text.replace(']]>', ']]]]><![CDATA[>')
        return f"<![CDATA[{safe}]]>"

    now_text = dt.datetime.now().strftime('%a, %d %b %Y %H:%M:%S %z')
    feed_title = f"资金流RSS - {user_row['username']}"
    parts = [
        "<?xml version='1.0' encoding='utf-8'?>",
        "<rss version=\"2.0\"><channel>",
        f"<title>{escape(feed_title)}</title>",
        "<link>https://quote.eastmoney.com/</link>",
        "<description>A股分钟级资金流</description>",
        f"<lastBuildDate>{escape(now_text)}</lastBuildDate>",
    ]

    for it in items:
        parts.extend(
            [
                "<item>",
                f"<title>{escape(it['title'])}</title>",
                f"<description>{_wrap_cdata(it['description'])}</description>",
                f"<link>{escape(it['link'])}</link>",
                f"<guid isPermaLink=\"false\">{escape(it['guid'])}</guid>",
                f"<pubDate>{escape(it['pubDate'])}</pubDate>",
                "</item>",
            ]
        )

    parts.append("</channel></rss>")
    xml = ''.join(parts).encode('utf-8')
    resp = make_response(xml)
    resp.headers['Content-Type'] = 'application/rss+xml; charset=utf-8'
    return resp


@app.route('/u/<token>.rss')
def user_rss(token: str):
    return generate_rss_response(token)


@app.route('/<prefix>/<token>.rss')
def prefixed_rss(prefix: str, token: str):
    # Enforce prefix policy
    row = _find_user_by_token(token)
    if not row:
        return ('Not found', 404)
    if RSS_PREFIX == 'username':
        if prefix != row['username']:
            return ('Not found', 404)
    else:
        if prefix != RSS_PREFIX:
            return ('Not found', 404)
    return generate_rss_response(token)


if os.environ.get('DISABLE_DAILY_SNAPSHOT_WORKER') != '1':
    should_start_worker = True
    if app.debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        should_start_worker = False
    if app.config.get('TESTING'):
        should_start_worker = False
    if should_start_worker:
        _start_daily_snapshot_worker()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=APP_PORT, debug=DEBUG_FLAG)
