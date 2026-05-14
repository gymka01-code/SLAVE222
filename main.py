import asyncio
import os
import random
import json
import base64
import uuid
import math
import time
import hmac as _hmac
import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import asyncpg
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Form, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, LabeledPrice, PreCheckoutQuery, FSInputFile
from pydantic import BaseModel, validator
from pathlib import Path

BOT_TOKEN    = os.getenv("BOT_TOKEN", "8759784117:AAEM280b-iM30y9hRJgYPZ76puRzV_B3Yu4")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379")
WEBAPP_URL   = os.getenv("WEBAPP_URL", "https://slave222-production.up.railway.app")
ADMIN_IDS    = list(map(int, filter(None, os.getenv("ADMIN_IDS", "7502434760").split(","))))
BOT_USERNAME = os.getenv("BOT_USERNAME", "RabstvoSlave_bot")
SEASON_PASS  = os.getenv("SEASON_PASS", "change_me_in_env")
SUPER_ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0
DEV_MODE     = os.getenv("DEV_MODE", "0") == "1"
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

_VOLUME     = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", ".")
SUPPORT_DIR = Path(_VOLUME) / "support"
SUPPORT_DIR.mkdir(parents=True, exist_ok=True)

bot: Bot = None
dp: Dispatcher = None
db: asyncpg.Pool = None
rdb: aioredis.Redis = None
story_cache: dict = {}

class EscapeGame:
    def __init__(self):
        self.round_id = 0
        self.status = "waiting"
        self.mult = 1.00
        self.start_time = 0.0
        self.crash_point = 1.00
        self.history = []
        self.bets: Dict[int, Dict[str, Any]] = {} 
        self.connections: List[WebSocket] = []

    async def broadcast(self, message: dict):
        dead = []
        msg_str = json.dumps(message)
        for ws in list(self.connections):
            try: await ws.send_text(msg_str)
            except: dead.append(ws)
        for ws in dead:
            if ws in self.connections: self.connections.remove(ws)

escape_game = EscapeGame()

SCHEMA = """
CREATE TABLE IF NOT EXISTS user_chats (
    id SERIAL PRIMARY KEY, sender_id BIGINT, receiver_id BIGINT, 
    text TEXT, is_read BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT NOW()
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS max_slaves_override INT DEFAULT NULL;

CREATE SEQUENCE IF NOT EXISTS user_uid_seq START 10000;
ALTER TABLE users ADD COLUMN IF NOT EXISTS uid INT UNIQUE;
ALTER TABLE users ALTER COLUMN uid SET DEFAULT nextval('user_uid_seq');
ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_prefs TEXT DEFAULT '{"all": true, "trade": true, "jobs": true, "messages": true, "support": true}';

CREATE TABLE IF NOT EXISTS syndicates (
    id SERIAL PRIMARY KEY, name TEXT UNIQUE, description TEXT, 
    owner_id BIGINT, treasury DECIMAL DEFAULT 0, level INT DEFAULT 1, 
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS syndicate_requests (
    id SERIAL PRIMARY KEY, user_id BIGINT, syndicate_id INT, created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS syndicate_wars (
    id SERIAL PRIMARY KEY, attacker_id INT, defender_id INT, 
    status TEXT DEFAULT 'pending', fund_attacker DECIMAL DEFAULT 0, 
    fund_defender DECIMAL DEFAULT 0, expires_at TIMESTAMP, winner_id INT
);

CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY, username TEXT, first_name TEXT, photo_url TEXT,
    balance DECIMAL DEFAULT 50, current_price DECIMAL DEFAULT 100, owner_id BIGINT,
    custom_name TEXT, job_id INT, job_assigned_at TIMESTAMP, shield_until TIMESTAMP,
    chains_until TIMESTAMP, booster_mult DECIMAL DEFAULT 1.0, booster_until TIMESTAMP, 
    stealth_until TIMESTAMP, name_color TEXT DEFAULT 'default', avatar_frame TEXT DEFAULT 'none', 
    emoji_status TEXT DEFAULT '', vip_level INT DEFAULT 0, vip_until TIMESTAMP, 
    is_banned BOOLEAN DEFAULT FALSE, is_admin_flag BOOLEAN DEFAULT FALSE, admin_hidden BOOLEAN DEFAULT FALSE,
    purchase_protection_until TIMESTAMP, created_at TIMESTAMP DEFAULT NOW(), slaves_count INT DEFAULT 0,
    ban_reason TEXT DEFAULT NULL, login_streak INT DEFAULT 0, last_login_date TEXT,
    referrer_id BIGINT,
    energy DECIMAL DEFAULT 300, max_energy INT DEFAULT 300, click_power DECIMAL DEFAULT 1.0, last_energy_update TIMESTAMP DEFAULT NOW(),
    robberies_count INT DEFAULT 0, robbery_reset_at TIMESTAMP DEFAULT NOW(),
    riot_expires_at TIMESTAMP DEFAULT NULL, is_injured_until TIMESTAMP DEFAULT NULL,
    riots_today INT DEFAULT 0, last_riot_date TEXT, last_riot_time TIMESTAMP,
    last_tax_date TEXT DEFAULT '', syndicate_id INT DEFAULT NULL, syndicate_role TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS escape_rounds (id SERIAL PRIMARY KEY, crash_mult DECIMAL, created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS escape_bets (id SERIAL PRIMARY KEY, round_id INT, user_id BIGINT, amount DECIMAL, cashout_mult DECIMAL DEFAULT NULL, win_amount DECIMAL DEFAULT 0, created_at TIMESTAMP DEFAULT NOW());

CREATE TABLE IF NOT EXISTS jobs (id SERIAL PRIMARY KEY, title TEXT UNIQUE, min_yield DECIMAL, max_yield DECIMAL, drop_chance INT, emoji TEXT);
CREATE TABLE IF NOT EXISTS transactions (id SERIAL PRIMARY KEY, buyer_id BIGINT, slave_id BIGINT, seller_id BIGINT, amount DECIMAL, fee DECIMAL, created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS tickets (id SERIAL PRIMARY KEY, user_id BIGINT, status TEXT DEFAULT 'open', claimed_by BIGINT, created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS support_messages (id SERIAL PRIMARY KEY, ticket_id INT, user_id BIGINT, text TEXT, photo_b64 TEXT, direction TEXT, reply_to_id INT, is_from_admin BOOLEAN DEFAULT FALSE, is_read BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS stories_claims (id SERIAL PRIMARY KEY, user_id BIGINT, status TEXT DEFAULT 'pending', created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS cosmetics (id SERIAL PRIMARY KEY, type TEXT, name TEXT UNIQUE, value TEXT, price_stars INT, css_class TEXT, price_rc INT DEFAULT 0);
CREATE TABLE IF NOT EXISTS user_cosmetics (user_id BIGINT, cosmetic_id INT, bought_at TIMESTAMP DEFAULT NOW(), PRIMARY KEY (user_id, cosmetic_id));
CREATE TABLE IF NOT EXISTS promo_codes (id SERIAL PRIMARY KEY, code TEXT UNIQUE, reward_rc DECIMAL, max_uses INT DEFAULT 1, used_count INT DEFAULT 0, expires_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS promo_uses (user_id BIGINT, promo_id INT, used_at TIMESTAMP DEFAULT NOW(), PRIMARY KEY (user_id, promo_id));
CREATE TABLE IF NOT EXISTS pending_purchases (id SERIAL PRIMARY KEY, user_id BIGINT, item_type TEXT, cosmetic_id INT, slave_id BIGINT, stars INT, status TEXT DEFAULT 'pending', created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS admin_users (user_id BIGINT PRIMARY KEY, added_by BIGINT, added_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS sponsors (id SERIAL PRIMARY KEY, channel_id TEXT UNIQUE NOT NULL, channel_title TEXT, channel_url TEXT, reward_rc DECIMAL DEFAULT 0, is_main BOOLEAN DEFAULT FALSE, is_active BOOLEAN DEFAULT TRUE);
CREATE TABLE IF NOT EXISTS user_sponsors (user_id BIGINT, sponsor_id INT, claimed_at TIMESTAMP DEFAULT NOW(), PRIMARY KEY (user_id, sponsor_id));
CREATE TABLE IF NOT EXISTS global_settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS daily_progress (
    user_id BIGINT, task_id TEXT, date_str TEXT, progress INT DEFAULT 0, claimed BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (user_id, task_id, date_str)
);
CREATE TABLE IF NOT EXISTS inventory (user_id BIGINT, item_id TEXT, quantity INT DEFAULT 0, PRIMARY KEY (user_id, item_id));

ALTER TABLE users ADD COLUMN IF NOT EXISTS energy DECIMAL DEFAULT 300;
ALTER TABLE users ADD COLUMN IF NOT EXISTS max_energy INT DEFAULT 300;
ALTER TABLE users ADD COLUMN IF NOT EXISTS click_power DECIMAL DEFAULT 1.0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_energy_update TIMESTAMP DEFAULT NOW();
ALTER TABLE users ADD COLUMN IF NOT EXISTS robberies_count INT DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS robbery_reset_at TIMESTAMP DEFAULT NOW();
ALTER TABLE users ADD COLUMN IF NOT EXISTS riot_expires_at TIMESTAMP DEFAULT NULL;
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_injured_until TIMESTAMP DEFAULT NULL;
ALTER TABLE users ADD COLUMN IF NOT EXISTS riots_today INT DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_riot_date TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_riot_time TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_vip_boost_claim TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS admin_god_mode BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_tax_date TEXT DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS syndicate_id INT DEFAULT NULL;
ALTER TABLE users ADD COLUMN IF NOT EXISTS syndicate_role TEXT DEFAULT NULL;

ALTER TABLE users ALTER COLUMN energy TYPE DECIMAL USING energy::DECIMAL;
ALTER TABLE users ALTER COLUMN click_power TYPE DECIMAL USING click_power::DECIMAL;

INSERT INTO global_settings (key, value) VALUES ('maintenance', '0') ON CONFLICT DO NOTHING;

DROP TABLE IF EXISTS jobs CASCADE;
CREATE TABLE jobs (id SERIAL PRIMARY KEY, title TEXT UNIQUE, min_yield DECIMAL, max_yield DECIMAL, drop_chance INT, emoji TEXT);
INSERT INTO jobs (title, min_yield, max_yield, drop_chance, emoji) VALUES
  ('Подметать полы', 0.15, 0.20, 70, '🧹'), 
  ('Раздавать листовки', 0.175, 0.225, 70, '📄'), 
  ('Майнить крипту', 0.225, 0.275, 25, '⛏'), 
  ('Петь на улице', 0.20, 0.25, 25, '🎤'), 
  ('Тапать хомяка', 0.30, 0.35, 5, '🐹'), 
  ('Просить милостыню', 0.275, 0.325, 5, '🙏');

DELETE FROM cosmetics WHERE id NOT IN (SELECT MIN(id) FROM cosmetics GROUP BY name);
CREATE UNIQUE INDEX IF NOT EXISTS cosmetics_name_idx ON cosmetics(name);

INSERT INTO cosmetics (type, name, value, price_stars, css_class, price_rc) VALUES
  ('color', 'Рубиновый', 'ruby', 10, 'clr-ruby', 0), ('color', 'Золотой', 'gold', 15, 'clr-gold', 0),
  ('color', 'Неоновый', 'neon', 20, 'clr-neon', 0), ('frame', 'Пламя', 'fire', 25, 'frame-fire', 0),
  ('frame', 'Алмаз', 'diamond', 30, 'frame-diamond', 0), ('frame', 'Радуга 🌈', 'rainbow', 40, 'frame-rainbow', 0),
  ('emoji', 'Корона', '👑', 40, '', 0), ('emoji', 'Бриллиант', '💎', 25, '', 0),
  ('emoji', 'Молния', '⚡', 15, '', 0)
ON CONFLICT (name) DO UPDATE SET price_stars=EXCLUDED.price_stars, price_rc=0;
"""

SHOP_ITEMS = [
    {"id":"shield_6", "name":"🛡 Щит 6ч", "price_stars":25, "price_rc":0, "desc":"Защита от покупки/ограблений на 6ч"},
    {"id":"shield_12", "name":"🛡 Щит 12ч", "price_stars":40, "price_rc":0, "desc":"Защита от покупки/ограблений на 12ч"},
    {"id":"shield_24", "name":"🛡 Щит 24ч", "price_stars":70, "price_rc":0, "desc":"Защита от покупки/ограблений на 24ч"},
    {"id":"chains", "name":"⛓ Оковы", "price_stars":20, "price_rc":0, "desc":"Выкуп раба дорожает на 50% на 7 дней"},
    {"id":"boost_15", "name":"🚀 Буст ×1.5", "price_stars":20, "price_rc":0, "desc":"Доход +50% на 4 часа"},
    {"id":"boost_20", "name":"🚀 Буст ×2.0", "price_stars":35, "price_rc":0, "desc":"Двойной доход на 4 часа"},
    {"id":"stealth_3", "name":"👻 Стелс 3д", "price_stars":20, "price_rc":0, "desc":"Скрывает вас из рейтингов на 3 дня"},
    {"id":"stealth_7", "name":"👻 Стелс 7д", "price_stars":40, "price_rc":0, "desc":"Скрывает вас из рейтингов на 7 дней"},
    {"id":"vip_1", "name":"🥉 VIP Бронза", "price_stars":100, "price_rc":100000, "desc":"Лимит +5 рабов. Ежедневный буст x1.5. На 30 дн."},
    {"id":"vip_2", "name":"🥈 VIP Серебро", "price_stars":200, "price_rc":200000, "desc":"Лимит +15 рабов. Ежедневный буст x2. На 30 дн."},
    {"id":"vip_3", "name":"👑 VIP Золото", "price_stars":350, "price_rc":300000, "desc":"Лимит +35 рабов. Доход +25% навсегда, Стелс. На 30 дн."},
]

DAILY_TASKS_POOL = [
    *[{"id": f"buy_{i}", "title": f"Купить {i} рабов", "action": "buy_slave", "target": i, "reward": i * 50} for i in [1, 3, 5, 10]],
    *[{"id": f"work_{i}", "title": f"Отправить на работу {i} раз", "action": "send_work", "target": i, "reward": i * 20} for i in [1, 3, 5, 10]],
    *[{"id": f"collect_{i}", "title": f"Собрать налог {i} раз", "action": "collect", "target": i, "reward": i * 30} for i in [1, 3, 5, 10]],
    *[{"id": f"arena_{i}", "title": f"Сыграть на Арене {i} раз", "action": "arena_fight", "target": i, "reward": i * 40} for i in [1, 3, 5]],
    *[{"id": f"mine_{i}", "title": f"Добыть {i}00 руды", "action": "mine_click", "target": i*100, "reward": i * 50} for i in [1, 2, 5]],
    *[{"id": f"rob_{i}", "title": f"Ограбить {i} раз", "action": "robbery", "target": i, "reward": i * 100} for i in [1, 2, 3]],
    *[{"id": f"escape_{i}", "title": f"Сыграть в Побег {i} раз", "action": "escape_bet", "target": i, "reward": i * 50} for i in [3, 5, 10]]
]
LOGIN_REWARDS = [50, 100, 150, 200, 300, 500, 1000]

def get_daily_tasks():
    msk_time = datetime.utcnow() + timedelta(hours=3)
    date_str = msk_time.strftime("%Y-%m-%d")
    random.seed(date_str)
    selected = random.sample(DAILY_TASKS_POOL, 5)
    random.seed()
    return date_str, selected

async def add_task_progress(user_id: int, action: str, amount: int = 1):
    date_str, daily_tasks = get_daily_tasks()
    for t in daily_tasks:
        if t['action'] == action:
            await db.execute("INSERT INTO daily_progress (user_id, task_id, date_str, progress) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id, task_id, date_str) DO UPDATE SET progress = daily_progress.progress + $4", user_id, t['id'], date_str, amount)

def verify_webapp(init_data: str) -> Optional[dict]:
    from urllib.parse import unquote
    try:
        if DEV_MODE and init_data.startswith("dev:"):
            uid = int(init_data.split(":")[1])
            return {"id": uid, "first_name": "DevUser", "username": f"dev_{uid}"}
        parsed = {}
        for item in init_data.split("&"):
            if "=" in item: k, v = item.split("=", 1); parsed[k] = unquote(v)
        received_hash = parsed.pop("hash", None)
        if not received_hash: return None
        check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = _hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        computed_hash = _hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()
        if not _hmac.compare_digest(computed_hash, received_hash): return None
        return json.loads(parsed.get("user", "{}"))
    except: return None

async def rate_limit(key: str, limit: int, window: int):
    try:
        pipe = rdb.pipeline()
        await pipe.incr(key)
        await pipe.expire(key, window)
        results = await pipe.execute()
        if results[0] > limit: raise HTTPException(429, "Слишком много запросов.")
    except HTTPException: raise
    except: pass

async def get_jobs_list() -> list: return [dict(r) for r in await db.fetch("SELECT * FROM jobs ORDER BY id")]
def pick_job(vip_level: int, jobs: list) -> dict:
    roll = random.random() * 100
    if roll < 70: pool = [j for j in jobs if j['drop_chance'] == 70]
    elif roll < 95: pool = [j for j in jobs if j['drop_chance'] == 25]
    else: pool = [j for j in jobs if j['drop_chance'] == 5] if vip_level >= 3 else [j for j in jobs if j['drop_chance'] == 25]
    return random.choice(pool) if pool else jobs[0]

async def push(uid: int, text: str, notif_type: str = "all", parse_mode: str = "HTML", photo_path: str = None):
    if bot:
        try:
            prefs_str = await db.fetchval("SELECT notify_prefs FROM users WHERE id=$1", uid)
            prefs = json.loads(prefs_str) if prefs_str else {"all": True, "trade": True, "jobs": True, "messages": True, "support": True}
            if not prefs.get("all", True): return
            if notif_type != "all" and not prefs.get(notif_type, True): return
            if photo_path: await bot.send_photo(uid, photo=FSInputFile(photo_path), caption=text, parse_mode=parse_mode)
            else: await bot.send_message(uid, text, parse_mode=parse_mode)
        except: pass

async def get_admin_ids() -> set:
    ids = set(ADMIN_IDS)
    try: ids.update([r['user_id'] for r in await db.fetch("SELECT user_id FROM admin_users")])
    except: pass
    return ids

async def notify_admins(text: str, photo_path: str = None):
    for uid in await get_admin_ids(): asyncio.create_task(push(uid, text, photo_path=photo_path))

async def is_admin_user(uid: int) -> bool:
    if uid in ADMIN_IDS: return True
    try: return bool(await db.fetchrow("SELECT 1 FROM admin_users WHERE user_id=$1", uid))
    except: return False

async def collect_income(owner_id: int) -> float:
    now = datetime.utcnow()
    owner_info = await db.fetchrow("SELECT vip_level, syndicate_id FROM users WHERE id=$1", owner_id)
    if not owner_info: return 0.0
    
    slaves = await db.fetch("SELECT u.id, u.current_price, u.booster_mult, u.booster_until, j.min_yield, j.max_yield FROM users u JOIN jobs j ON u.job_id = j.id WHERE u.owner_id=$1 AND u.job_id IS NOT NULL AND u.job_assigned_at <= $2", owner_id, now - timedelta(hours=2))
    if not slaves: return 0.0
    
    ev_data = await db.fetchval("SELECT value FROM global_settings WHERE key='event_data'")
    event = json.loads(ev_data) if ev_data else {"type": "normal"}
    
    clan_level = 1
    if owner_info['syndicate_id']:
        clan_level = await db.fetchval("SELECT level FROM syndicates WHERE id=$1", owner_info['syndicate_id']) or 1
    
    total = 0.0
    for s in slaves:
        base_reward = float(s['current_price']) * random.uniform(float(s['min_yield']), float(s['max_yield']))
        if event.get("type") == "crisis": base_reward *= 0.8
        
        if clan_level >= 3: base_reward *= 1.10
        elif clan_level >= 2: base_reward *= 1.05
        
        mult = float(s['booster_mult']) if s['booster_until'] and s['booster_until'] > now else 1.0
        total += base_reward * mult
        await db.execute("UPDATE users SET job_id=NULL, job_assigned_at=NULL WHERE id=$1", s['id'])
        await add_task_progress(owner_id, 'collect')
        
    if owner_info['vip_level'] >= 3: total *= 1.25

    if total > 0: 
        await db.execute("UPDATE users SET balance=balance+$1::numeric WHERE id=$2", round(total, 2), owner_id)
        owner_ref = await db.fetchval("SELECT referrer_id FROM users WHERE id=$1", owner_id)
        if owner_ref: await db.execute("UPDATE users SET balance=balance+$1::numeric WHERE id=$2", round(total * 0.05, 2), owner_ref)
    return total

async def _grant_shop_item(uid: int, item_type: str, cosmetic_id: Optional[int], slave_id: Optional[int] = None):
    now = datetime.utcnow()
    if item_type in ("shield_6", "shield_12", "shield_24", "chains", "boost_15", "boost_20", "stealth_3", "stealth_7"):
        await db.execute("INSERT INTO inventory (user_id, item_id, quantity) VALUES ($1, $2, 1) ON CONFLICT (user_id, item_id) DO UPDATE SET quantity = inventory.quantity + 1", uid, item_type)
    elif item_type in ("vip_1","vip_2","vip_3"):
        await db.execute("UPDATE users SET vip_level=$1,vip_until=$2 WHERE id=$3", int(item_type[-1]), now+timedelta(days=30), uid)
    elif item_type == "cosmetic" and cosmetic_id:
        c = await db.fetchrow("SELECT * FROM cosmetics WHERE id=$1", cosmetic_id)
        if c:
            await db.execute("INSERT INTO user_cosmetics(user_id,cosmetic_id) VALUES($1,$2) ON CONFLICT DO NOTHING", uid, cosmetic_id)
            if c['type'] == 'color': await db.execute("UPDATE users SET name_color=$1 WHERE id=$2", c['value'], uid)
            elif c['type'] == 'frame': await db.execute("UPDATE users SET avatar_frame=$1 WHERE id=$2", c['value'], uid)
            elif c['type'] == 'emoji': await db.execute("UPDATE users SET emoji_status=$1 WHERE id=$2", c['value'], uid)

async def _resolve_uid(x_init_data: str) -> int:
    if DEV_MODE and x_init_data.startswith("dev:"): return int(x_init_data.split(":")[1])
    data = verify_webapp(x_init_data)
    if not data: raise HTTPException(401, "Invalid Telegram auth")
    return int(data['id'])

async def get_current_user(x_init_data: str = Header(...)) -> dict:
    uid = await _resolve_uid(x_init_data)
    user = await db.fetchrow("SELECT * FROM users WHERE id=$1", uid)
    if not user: raise HTTPException(404, "User not found")
    if user['is_banned']: raise HTTPException(403, "Banned")
    return dict(user)

async def get_admin_user(x_init_data: str = Header(...)) -> dict:
    user = await get_current_user(x_init_data)
    if not await is_admin_user(user['id']): raise HTTPException(403, "Admins only")
    return user

async def get_super_admin(x_init_data: str = Header(...)) -> dict:
    user = await get_current_user(x_init_data)
    if user['id'] != SUPER_ADMIN_ID: raise HTTPException(403, "Super admin only")
    return user

def _calc_energy(u: dict) -> float:
    now = datetime.utcnow()
    last_update = u.get('last_energy_update')
    if not last_update: last_update = now
    elif isinstance(last_update, str): last_update = datetime.fromisoformat(last_update.replace("Z", "+00:00")).replace(tzinfo=None)
    seconds_passed = (now - last_update).total_seconds()
    energy = float(u.get('energy') if u.get('energy') is not None else 300)
    max_energy = float(u.get('max_energy') if u.get('max_energy') is not None else 300)
    regen_per_sec = max_energy / 7200.0
    return min(max_energy, energy + seconds_passed * regen_per_sec)

def get_crash_point() -> float:
    if random.random() < 0.05: return 1.00
    return round(max(1.00, 0.95 / (1.0 - random.random())), 2)

async def _escape_game_loop():
    try: escape_game.history = [float(r['crash_mult']) for r in await db.fetch("SELECT crash_mult FROM escape_rounds WHERE status='crashed' ORDER BY id DESC LIMIT 10")]
    except: pass
    while True:
        try:
            escape_game.status = "waiting"
            escape_game.mult = 1.00
            escape_game.bets = {}
            escape_game.round_id = await db.fetchval("INSERT INTO escape_rounds DEFAULT VALUES RETURNING id")
            for i in range(100, 0, -1):
                await escape_game.broadcast({"type": "state", "status": escape_game.status, "time_left": i / 10.0, "history": escape_game.history, "bets": escape_game.bets})
                await asyncio.sleep(0.1)
            escape_game.status = "running"
            escape_game.crash_point = get_crash_point()
            escape_game.start_time = time.time()
            await escape_game.broadcast({"type": "start", "start_time": escape_game.start_time})
            while True:
                elapsed = time.time() - escape_game.start_time
                current_mult = math.exp(0.08 * elapsed) 
                
                is_crash = current_mult >= escape_game.crash_point
                if is_crash:
                    current_mult = escape_game.crash_point
                    
                escape_game.mult = current_mult
                
                for uid, bet in list(escape_game.bets.items()):
                    if not bet['cashed_out'] and bet.get('auto_cashout') and bet['auto_cashout'] <= current_mult:
                        bet['cashed_out'] = True
                        win = float(bet['amount']) * bet['auto_cashout']
                        bet['win'] = win
                        bet['mult'] = bet['auto_cashout']
                        try:
                            await db.execute("UPDATE escape_bets SET cashout_mult=$1, win_amount=$2 WHERE round_id=$3 AND user_id=$4", bet['auto_cashout'], win, escape_game.round_id, uid)
                            await db.execute("UPDATE users SET balance=balance+$1::numeric WHERE id=$2", win, uid)
                        except Exception as e:
                            print(f"Escape auto cashout error: {e}")
                
                if is_crash:
                    break
                    
                await escape_game.broadcast({"type": "tick", "mult": round(current_mult, 2), "bets": escape_game.bets})
                await asyncio.sleep(0.1)
                
            escape_game.status = "crashed"
            escape_game.history = ([round(escape_game.crash_point, 2)] + escape_game.history)[:10]
            await db.execute("UPDATE escape_rounds SET crash_mult=$1, status='crashed' WHERE id=$2", escape_game.crash_point, escape_game.round_id)
            await escape_game.broadcast({"type": "crash", "mult": round(escape_game.crash_point, 2), "history": escape_game.history, "bets": escape_game.bets})
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Escape Game Error: {e}")
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot, dp, db, rdb
    import urllib.parse as _up
    _parsed = _up.urlparse(DATABASE_URL)
    _ssl = "require" if _parsed.hostname and "railway" in _parsed.hostname else False
    for _ in range(13):
        try: db = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=15, **({} if not _ssl else {"ssl": _ssl})); break
        except: await asyncio.sleep(5)
        
    rdb = aioredis.from_url(REDIS_URL, decode_responses=True)
    
    async with db.acquire() as c:
        await c.execute(SCHEMA)
        await c.execute("UPDATE users SET uid = nextval('user_uid_seq') WHERE uid IS NULL")
        try: await c.execute("ALTER TABLE escape_rounds ADD COLUMN status TEXT DEFAULT 'waiting'")
        except: pass
        await c.execute("UPDATE users SET max_energy = 300, energy = LEAST(energy, 300) WHERE max_energy > 300")
        
    for aid in ADMIN_IDS:
        try: await db.execute("UPDATE users SET is_admin_flag=TRUE WHERE id=$1", aid)
        except: pass
        
    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher()
    
    @dp.message(CommandStart())
    async def cmd_start(msg: types.Message):
        uid = msg.from_user.id
        
        ref_id = None
        if msg.text and "ref_" in msg.text:
            try: 
                raw_ref = int(msg.text.split("ref_")[1])
                if raw_ref > 100000000: ref_id = raw_ref
                else:
                    ref_row = await db.fetchrow("SELECT id FROM users WHERE uid=$1", raw_ref)
                    if ref_row: ref_id = ref_row['id']
            except: pass
            
        if not await db.fetchrow("SELECT id FROM users WHERE id=$1", uid):
            owner_id = ref_id if ref_id and ref_id != uid else None
            await db.execute(
                "INSERT INTO users(id,username,first_name,balance,current_price,owner_id,referrer_id) VALUES($1,$2,$3,50,100,$4,$5) ON CONFLICT DO NOTHING", 
                uid, msg.from_user.username, msg.from_user.first_name, owner_id, ref_id
            )
            if owner_id: 
                asyncio.create_task(push(owner_id, f"🎣 По вашей ссылке перешёл @{msg.from_user.username or uid}! Вы будете получать 5% с его доходов.", notif_type="trade"))

        main_sponsors = await db.fetch("SELECT * FROM sponsors WHERE is_main=TRUE AND is_active=TRUE")
        missing_subs = []
        for s in main_sponsors:
            try:
                member = await bot.get_chat_member(chat_id=s['channel_id'], user_id=uid)
                if member.status not in ['member', 'administrator', 'creator']: missing_subs.append(s)
            except: missing_subs.append(s)
            
        if missing_subs:
            buttons = [[InlineKeyboardButton(text=s['channel_title'], url=s['channel_url'])] for s in missing_subs]
            buttons.append([InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="check_main_subs")])
            await msg.answer("⛔️ <b>Для использования игры необходимо подписаться на наши генеральные каналы:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
            return

        await msg.answer("⛓ <b>РАБСТВО</b>\n\nСоциальная экономическая стратегия внутри Telegram.\nПокупай людей → назначай работу → собирай доход.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⛓ Открыть Рабство", web_app=WebAppInfo(url=WEBAPP_URL))]]))

    @dp.callback_query(F.data == "check_main_subs")
    async def cb_check_subs(cq: types.CallbackQuery):
        uid = cq.from_user.id
        main_sponsors = await db.fetch("SELECT * FROM sponsors WHERE is_main=TRUE AND is_active=TRUE")
        missing_subs = []
        for s in main_sponsors:
            try:
                member = await bot.get_chat_member(chat_id=s['channel_id'], user_id=uid)
                if member.status not in ['member', 'administrator', 'creator']: missing_subs.append(s)
            except: missing_subs.append(s)
            
        if missing_subs: 
            await cq.answer("❌ Вы не подписаны на все каналы!", show_alert=True)
        else:
            await cq.answer("✅ Доступ разрешен!", show_alert=True)
            await cq.message.delete()
            await bot.send_message(
                uid,
                "⛓ <b>РАБСТВО</b>\n\nСоциальная экономическая стратегия внутри Telegram.\nПокупай людей → назначай работу → собирай доход.", 
                parse_mode="HTML", 
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⛓ Открыть Рабство", web_app=WebAppInfo(url=WEBAPP_URL))]])
            )

    @dp.pre_checkout_query()
    async def pre_checkout(query: PreCheckoutQuery): await query.answer(ok=True)

    @dp.message(F.successful_payment)
    async def successful_payment(msg: types.Message):
        try:
            parts = msg.successful_payment.invoice_payload.split(":")
            purchase_id, uid, item_type = int(parts[1]), int(parts[2]), parts[3]
            purchase = await db.fetchrow("SELECT * FROM pending_purchases WHERE id=$1 AND user_id=$2 AND status='pending'", purchase_id, uid)
            if not purchase: return await msg.answer("⚠️ Покупка уже обработана.")
            await db.execute("UPDATE pending_purchases SET status='completed' WHERE id=$1", purchase_id)
            await _grant_shop_item(uid, item_type, purchase['cosmetic_id'], purchase['slave_id'])
            await msg.answer("✅ Покупка успешна! Предмет добавлен на Склад (или активирован).")
            asyncio.create_task(push(SUPER_ADMIN_ID, f"💰 <b>Покупка за Звезды!</b>\nПользователь ID: {uid} купил {item_type}.", notif_type="all"))
        except: pass

    try: await bot.delete_webhook(drop_pending_updates=True)
    except: pass
    asyncio.create_task(dp.start_polling(bot, skip_updates=True))
    
    async def _system_cron():
        while True:
            await asyncio.sleep(60)
            try:
                now = datetime.utcnow()
                msk_time = now + timedelta(hours=3)
                date_str = msk_time.strftime("%Y-%m-%d")

                ev_data = await db.fetchval("SELECT value FROM global_settings WHERE key='event_data'")
                ev = json.loads(ev_data) if ev_data else None
                if not ev or ev.get('expires_at', 0) < now.timestamp():
                    new_ev = random.choice([
                        {"type": "normal", "name": "Штиль", "desc": "Экономика стабильна. Никаких эффектов."},
                        {"type": "crisis", "name": "📉 Кризис", "desc": "Доходы от работ снижены на 20%!"},
                        {"type": "boom", "name": "📈 Экономический Бум", "desc": "Инфляция замедлена! Цены на покупку растут на 15%."}
                    ])
                    new_ev['expires_at'] = (now + timedelta(hours=6)).timestamp()
                    await db.execute("INSERT INTO global_settings(key,value) VALUES('event_data', $1) ON CONFLICT(key) DO UPDATE SET value=$1", json.dumps(new_ev))

                if now.minute == 0:
                    await db.execute("UPDATE users SET current_price = GREATEST(current_price * 0.95, 100) WHERE (job_assigned_at IS NULL OR job_assigned_at < NOW() - INTERVAL '24 hours') AND created_at < NOW() - INTERVAL '24 hours'")
                
                owners_to_collect = await db.fetch("SELECT DISTINCT owner_id FROM users WHERE job_assigned_at <= NOW() - INTERVAL '2 hours'")
                for r in owners_to_collect:
                    if r['owner_id']: await collect_income(r['owner_id'])
                
                # --- ТАЙМЕР НАЛОГОВ ---
                last_tax_cron = await db.fetchval("SELECT value FROM global_settings WHERE key='last_tax_cron'")
                if last_tax_cron != date_str and msk_time.hour >= 0:
                    await db.execute("INSERT INTO global_settings(key,value) VALUES('last_tax_cron', $1) ON CONFLICT(key) DO UPDATE SET value=$1", date_str)
                    
                    tax_users = await db.fetch("SELECT id, balance, slaves_count FROM users WHERE slaves_count > 3 AND COALESCE(last_tax_date, '') != $1", date_str)
                    for tu in tax_users:
                        slaves_count = tu['slaves_count']
                        tax = 0
                        if slaves_count <= 10:
                            tax = (slaves_count - 3) * 100
                        else:
                            tax = (7 * 100) + (slaves_count - 10) * 200
                        
                        current_bal = float(tu['balance'])
                        if current_bal >= tax:
                            await db.execute("UPDATE users SET balance = balance - $1::numeric, last_tax_date = $2 WHERE id = $3", tax, date_str, tu['id'])
                        else:
                            await db.execute("UPDATE users SET balance = 0, last_tax_date = $1 WHERE id = $2", date_str, tu['id'])
                            cheapest_slave = await db.fetchrow("SELECT id, current_price FROM users WHERE owner_id = $1 ORDER BY current_price ASC LIMIT 1", tu['id'])
                            if cheapest_slave:
                                await db.execute("UPDATE users SET owner_id = NULL, custom_name = NULL, job_id = NULL, job_assigned_at = NULL WHERE id = $1", cheapest_slave['id'])
                                asyncio.create_task(push(tu['id'], f"💸 <b>Налоговая инспекция!</b>\nУ вас не хватило денег на содержание рабов (Нужно {tax} RC). Ваш баланс обнулен, а самый дешевый раб сбежал!", notif_type="trade"))
                                asyncio.create_task(push(cheapest_slave['id'], "🕊️ <b>Вы свободны!</b>\nВаш хозяин обанкротился и не смог платить налог за ваше содержание.", notif_type="trade"))
                            else:
                                asyncio.create_task(push(tu['id'], f"💸 <b>Налог уплачен (Банкрот)!</b>\nСписано всё до 0.", notif_type="trade"))

                # --- ТАЙМЕР ЖЕСТКИХ БУНТОВ 2.0 ---
                if now.minute == 0:
                    riot_candidates = await db.fetch("""
                        SELECT u.id, u.slaves_count, u.syndicate_id,
                        (SELECT COALESCE(SUM(current_price), 0) FROM users u2 WHERE u2.owner_id = u.id) as total_value 
                        FROM users u WHERE u.slaves_count > 0 AND u.riot_expires_at IS NULL
                    """)
                    for rc in riot_candidates:
                        if rc['syndicate_id']:
                            clan = await db.fetchrow("SELECT level FROM syndicates WHERE id=$1", rc['syndicate_id'])
                            if clan and clan['level'] >= 4:
                                continue 
                        slaves = rc['slaves_count']
                        val = float(rc['total_value'])
                        chance = 1.0 + (slaves // 10) * 1.0 + (val // 50000) * 2.0
                        chance = min(chance, 50.0) 
                        if random.random() * 100 < chance:
                            await db.execute("UPDATE users SET riot_expires_at = $1 WHERE id = $2", now + timedelta(minutes=15), rc['id'])
                            asyncio.create_task(push(rc['id'], "🚨 <b>ВНИМАНИЕ! БУНТ!</b>\nВаши рабы взбунтовались! У вас есть 15 минут, чтобы подавить бунт, иначе вы потеряете 10% казны и одного дорогого раба!", notif_type="trade"))
                        
                expired_riots = await db.fetch("SELECT id, balance FROM users WHERE riot_expires_at IS NOT NULL AND riot_expires_at < $1", now)
                for er in expired_riots:
                    penalty = float(er['balance']) * 0.10
                    await db.execute("UPDATE users SET balance = balance - $1::numeric, riot_expires_at = NULL WHERE id=$2", penalty, er['id'])
                    
                    top_slaves = await db.fetch("SELECT id, username, first_name FROM users WHERE owner_id = $1 ORDER BY current_price DESC LIMIT 3", er['id'])
                    escaped_slave_name = "Никто"
                    if top_slaves:
                        escapee = random.choice(top_slaves)
                        await db.execute("UPDATE users SET owner_id = NULL, custom_name = NULL, job_id = NULL, job_assigned_at = NULL WHERE id = $1", escapee['id'])
                        escaped_slave_name = escapee['first_name'] or escapee['username'] or f"ID:{escapee['id']}"
                        asyncio.create_task(push(escapee['id'], "🕊️ <b>Свобода!</b>\nВы воспользовались бунтом и сбежали от хозяина!", notif_type="trade"))

                    asyncio.create_task(push(er['id'], f"🚨 <b>Бунт завершился провалом!</b>\nВы не подавили бунт. Разграблено {penalty:.0f} RC. К тому же, раб {escaped_slave_name} сбежал на свободу!", notif_type="trade"))

                # --- КЛАНОВЫЕ ВОЙНЫ ---
                await db.execute("UPDATE syndicate_wars SET status='finished' WHERE status='pending' AND expires_at <= $1", now)
                
                active_wars = await db.fetch("SELECT * FROM syndicate_wars WHERE status='active' AND expires_at <= $1", now)
                for w in active_wars:
                    if float(w['fund_attacker']) > float(w['fund_defender']):
                        winner_id, loser_id = w['attacker_id'], w['defender_id']
                        win_fund, lose_fund = float(w['fund_attacker']), float(w['fund_defender'])
                    else:
                        winner_id, loser_id = w['defender_id'], w['attacker_id']
                        win_fund, lose_fund = float(w['fund_defender']), float(w['fund_attacker'])
                    
                    reward = win_fund + (lose_fund * 0.5)
                    await db.execute("UPDATE syndicates SET treasury = treasury + $1::numeric WHERE id=$2", reward, winner_id)
                    await db.execute("UPDATE syndicate_wars SET status='finished', winner_id=$1 WHERE id=$2", winner_id, w['id'])
                    
                    members = await db.fetch("SELECT id FROM users WHERE syndicate_id=$1", winner_id)
                    for m in members:
                        await db.execute("INSERT INTO inventory (user_id, item_id, quantity) VALUES ($1, 'boost_20', 1) ON CONFLICT (user_id, item_id) DO UPDATE SET quantity = inventory.quantity + 1", m['id'])
                        asyncio.create_task(push(m['id'], f"🏆 <b>Победа в Клановой Войне!</b>\nВаш клан одержал победу! В казну добавлено {reward:.0f} RC. В ваш инвентарь добавлен Буст x2 (4ч).", notif_type="trade"))
                        
                    l_members = await db.fetch("SELECT id FROM users WHERE syndicate_id=$1", loser_id)
                    for m in l_members:
                        asyncio.create_task(push(m['id'], f"💀 <b>Поражение в Клановой Войне...</b>\nВаш клан проиграл гонку кошельков. Все вложенные в войну средства потеряны.", notif_type="trade"))

            except Exception as e:
                print(f"Cron Error: {e}")
            
    asyncio.create_task(_system_cron())
    asyncio.create_task(_escape_game_loop())
    yield
    await db.close()
    await rdb.aclose()

app = FastAPI(lifespan=lifespan, title="Рабство API")
allow_credentials = True if "*" not in ALLOWED_ORIGINS else False
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_credentials=allow_credentials, allow_methods=["*"], allow_headers=["*"])

class InitReq(BaseModel): init_data: str; ref_id: Optional[int] = None; photo_url: Optional[str] = None
class BuyReq(BaseModel): target_id: int
class RenameReq(BaseModel): slave_id: int; new_name: str
class SendWorkReq(BaseModel): slave_id: int
class HealReq(BaseModel): slave_id: int
class ShopInvoiceReq(BaseModel): item_type: str; cosmetic_id: Optional[int] = None; target_slave_id: Optional[int] = None
class ShopRcBuyReq(BaseModel): item_type: str; cosmetic_id: Optional[int] = None; target_slave_id: Optional[int] = None
class PromoUseReq(BaseModel): code: str
class AdminEditReq(BaseModel): user_id: int; balance: Optional[float]=None; price: Optional[float]=None; custom_name: Optional[str]=None; is_banned: Optional[bool]=None; free_slave: Optional[bool]=None; ban_reason: Optional[str]=None; vip_level: Optional[int]=None; max_slaves_override: Optional[int]=None
class SeasonReq(BaseModel): password: str
class BroadcastReq(BaseModel): text: str
class AdminChatSendReq(BaseModel): target_uid: int; text: str = ""; reply_to_id: Optional[int] = None
class EditMsgReq(BaseModel): text: str
class PromoCreateReq(BaseModel): code: str; reward_rc: float; max_uses: int = 1
class AdminManageReq(BaseModel): user_id: int
class StoryUploadReq(BaseModel): b64_data: str
class AdminGrantCosmeticReq(BaseModel): identifier: str; field: str; value: str
class SponsorAddReq(BaseModel): channel_id: str; channel_title: str; channel_url: str; reward_rc: float; is_main: bool = False
class SponsorDelReq(BaseModel): sponsor_id: int
class SponsorToggleReq(BaseModel): sponsor_id: int
class CheckSponsorReq(BaseModel): sponsor_id: int
class ClaimTaskReq(BaseModel): task_id: str
class AdminHiddenReq(BaseModel): hidden: bool
class MaintToggleReq(BaseModel): state: bool
class SupportMessageReq(BaseModel): message: str = ""; photo_b64: Optional[str] = None; reply_to_id: Optional[int] = None
class MineSyncReq(BaseModel): clicks: int
class MineUpgradeReq(BaseModel): type: str
class RobberyReq(BaseModel): target_id: int; status: str; amount: float
class ArenaFightReq(BaseModel): slave_id: int; bet: float
class SendChatReq(BaseModel): target_uid: int; text: str
class EscapeBetReq(BaseModel): amount: float; auto_cashout: Optional[float] = None
class UpdatePrefsReq(BaseModel): prefs: dict
class InventoryUseReq(BaseModel): item_id: str; target_slave_id: Optional[int] = None
class EquipCosmeticReq(BaseModel): field: str; value: str

class SyndicateCreateReq(BaseModel): name: str; description: str = ""
class SyndicateJoinReq(BaseModel): syndicate_id: int
class SyndicateResolveReq(BaseModel): request_id: int; action: str
class SyndicateKickReq(BaseModel): user_id: int
class SyndicatePromoteReq(BaseModel): user_id: int
class SyndicateDonateReq(BaseModel): amount: float
class SyndicateWarDeclareReq(BaseModel): target_id: int
class SyndicateWarResolveReq(BaseModel): war_id: int; action: str
class SyndicateWarDonateReq(BaseModel): war_id: int; amount: float

async def _full_profile(uid: int) -> dict:
    u = await db.fetchrow("SELECT * FROM users WHERE id=$1", uid)
    if not u: raise HTTPException(404)
    u = dict(u)
    now = datetime.utcnow()
    owner = dict(await db.fetchrow("SELECT id,username,first_name,photo_url FROM users WHERE id=$1", u['owner_id'])) if u['owner_id'] else None
    slaves_raw = await db.fetch("SELECT u.id,u.username,u.first_name,u.custom_name,u.current_price,u.photo_url,u.job_id, u.job_assigned_at, j.title as job_title, j.emoji as job_emoji, j.min_yield, j.max_yield, u.purchase_protection_until, u.is_injured_until FROM users u LEFT JOIN jobs j ON u.job_id=j.id WHERE u.owner_id=$1", uid)
    slaves = []
    for s in slaves_raw:
        job_finishes_at = (s['job_assigned_at'] + timedelta(hours=2)).isoformat() + "Z" if s['job_assigned_at'] else None
        slaves.append({
            "id": s['id'], "username": s['username'], "first_name": s['first_name'], 
            "photo_url": s['photo_url'], "custom_name": s['custom_name'], 
            "price": float(s['current_price']), "job_id": s['job_id'], 
            "job_title": s['job_title'], "job_emoji": s['job_emoji'], 
            "min_yield_perc": float(s['min_yield'] or 0) * 100, 
            "max_yield_perc": float(s['max_yield'] or 0) * 100, 
            "job_finishes_at": job_finishes_at,
            "purchase_protection_until": s['purchase_protection_until'].isoformat() + "Z" if s['purchase_protection_until'] else None,
            "is_injured": bool(s['is_injured_until'] and s['is_injured_until'] > now)
        })
    await db.execute("UPDATE users SET slaves_count=$1 WHERE id=$2", len(slaves), uid)
    if u.get('max_slaves_override') is not None: max_slaves = u['max_slaves_override']
    else:
        max_slaves = 15
        if u['vip_level'] == 1: max_slaves = 20
        elif u['vip_level'] == 2: max_slaves = 30
        elif u['vip_level'] >= 3: max_slaves = 50
    last_story = await db.fetchrow("SELECT created_at FROM stories_claims WHERE user_id=$1 AND status='approved' ORDER BY created_at DESC LIMIT 1", uid)
    story_cooldown_until = None
    if last_story and (last_story['created_at'] + timedelta(hours=24)) > now: story_cooldown_until = (last_story['created_at'] + timedelta(hours=24)).isoformat() + "Z"
    ev_data = await db.fetchval("SELECT value FROM global_settings WHERE key='event_data'")
    event = json.loads(ev_data) if ev_data else {"type": "normal", "name": "Штиль"}
    reset_at = u.get('robbery_reset_at') or now
    if isinstance(reset_at, str): reset_at = datetime.fromisoformat(reset_at.replace("Z", "+00:00")).replace(tzinfo=None)
    robberies_left = 3 if now >= reset_at else max(0, 3 - u.get('robberies_count', 0))

    syndicate_name = None
    if u['syndicate_id']:
        syndicate_name = await db.fetchval("SELECT name FROM syndicates WHERE id=$1", u['syndicate_id'])

    msk_time = datetime.utcnow() + timedelta(hours=3)
    current_date_str = msk_time.strftime("%Y-%m-%d")

    return {
        "id": u['id'], "uid": u.get('uid'), "username": u['username'], "first_name": u['first_name'], "photo_url": u['photo_url'], 
        "balance": round(float(u['balance']), 2), "price": round(float(u['current_price']), 2), 
        "owner": owner, "custom_name": u['custom_name'], "slaves": slaves, "slaves_count": len(slaves), 
        "max_slaves": max_slaves, "shield_until": u['shield_until'].isoformat() + "Z" if u['shield_until'] else None,
        "purchase_protection_until": u['purchase_protection_until'].isoformat() + "Z" if u.get('purchase_protection_until') else None,
        "shield_active": bool(u['shield_until'] and u['shield_until'] > now),
        "stealth_active": bool(u['stealth_until'] and u['stealth_until'] > now), 
        "booster_active": bool(u['booster_until'] and u['booster_until'] > now), "booster_mult": float(u['booster_mult']), 
        "chains_active": bool(u['chains_until'] and u['chains_until'] > now), "vip_level": u['vip_level'], 
        "last_vip_boost_claim": u['last_vip_boost_claim'].isoformat() + "Z" if u.get('last_vip_boost_claim') else None,
        "name_color": u['name_color'], "avatar_frame": u['avatar_frame'], "emoji_status": u['emoji_status'], 
        "is_admin": await is_admin_user(u['id']), "is_admin_flag": bool(u.get('is_admin_flag')), 
        "is_super_admin": u['id'] == SUPER_ADMIN_ID, "admin_hidden": bool(u.get('admin_hidden')), 
        "admin_god_mode": bool(u.get('admin_god_mode')),
        "story_cooldown_until": story_cooldown_until, "bot_username": BOT_USERNAME, 
        "maintenance": (await db.fetchval("SELECT value FROM global_settings WHERE key='maintenance'") == '1'), 
        "is_banned": bool(u['is_banned']), "ban_reason": u.get('ban_reason'),
        "current_event": event,
        "energy": _calc_energy(u), "max_energy": u['max_energy'], "click_power": float(u['click_power']),
        "robberies_left": robberies_left, "riot_active": bool(u['riot_expires_at'] and u['riot_expires_at'] > now),
        "notify_prefs": json.loads(u.get('notify_prefs') or '{"all":true,"trade":true,"jobs":true,"messages":true,"support":true}'),
        "last_tax_date": u.get('last_tax_date', ''), "current_date_str": current_date_str,
        "syndicate_id": u['syndicate_id'], "syndicate_role": u['syndicate_role'], "syndicate_name": syndicate_name
    }

@app.get("/api/config")
async def get_config(): return {"bot_username": BOT_USERNAME, "webapp_url": WEBAPP_URL}

@app.post("/api/init")
async def init_user(req: InitReq):
    tg = verify_webapp(req.init_data)
    if not tg: raise HTTPException(401, "Bad initData")
    uid, photo_url = int(tg['id']), req.photo_url or tg.get('photo_url')
    if not photo_url and bot:
        try:
            photos = await bot.get_user_profile_photos(uid, limit=1)
            if photos.total_count > 0: photo_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{(await bot.get_file(photos.photos[0][0].file_id)).file_path}"
        except: pass
    if not await db.fetchrow("SELECT id FROM users WHERE id=$1", uid):
        owner_id = req.ref_id if (req.ref_id and req.ref_id != uid) else None
       await db.execute("INSERT INTO users(id,username,first_name,photo_url,balance,current_price,owner_id,referrer_id) VALUES($1,$2,$3,$4,50,100,$5,$6) ON CONFLICT DO NOTHING", uid, tg.get('username'), tg.get('first_name'), photo_url, owner_id, req.ref_id)
        if owner_id: asyncio.create_task(push(owner_id, f"🎣 По вашей ссылке перешёл @{tg.get('username') or uid}! Вы будете получать 5% с его доходов.", notif_type="trade"))
    else: await db.execute("UPDATE users SET username=$1,first_name=$2,photo_url=$3 WHERE id=$4", tg.get('username'), tg.get('first_name'), photo_url, uid)
    return await _full_profile(uid)

@app.get("/api/profile")
async def get_profile(x_init_data: str = Header(...)):
    uid = await _resolve_uid(x_init_data)
    await collect_income(uid)
    return await _full_profile(uid)

@app.get("/api/profile/{uid}")
async def get_user_profile(uid: int, x_init_data: str = Header(...)):
    await _resolve_uid(x_init_data)
    return await _full_profile(uid)

@app.post("/api/tax/pay")
async def pay_tax(user: dict = Depends(get_current_user)):
    uid = user['id']
    count = user['slaves_count']
    if count <= 3: return {"ok": True, "msg": "Налог не требуется."}
    
    tax = 0
    if count <= 10: tax = (count - 3) * 100
    else: tax = (7 * 100) + (count - 10) * 200
    
    msk_time = datetime.utcnow() + timedelta(hours=3)
    current_date_str = msk_time.strftime("%Y-%m-%d")
    
    if user['last_tax_date'] == current_date_str:
        return {"ok": True, "msg": "Налог за сегодня уже уплачен."}
        
    if float(user['balance']) < tax:
        raise HTTPException(400, f"Недостаточно RC. Требуется {tax} RC.")
        
    await db.execute("UPDATE users SET balance = balance - $1::numeric, last_tax_date = $2 WHERE id = $3", tax, current_date_str, uid)
    return {"ok": True, "msg": f"Успешно оплачено {tax} RC!"}

@app.post("/api/buy")
async def buy_player(req: BuyReq, user: dict = Depends(get_current_user)):
    buyer_id, target_id = user['id'], req.target_id
    await rate_limit(f"rl:buy:{buyer_id}", 15, 60)
    if buyer_id == target_id: raise HTTPException(400, "Нельзя купить самого себя")
    await collect_income(buyer_id)
    max_slaves = 15
    if user.get('vip_level', 0) == 1: max_slaves = 20
    elif user.get('vip_level', 0) == 2: max_slaves = 30
    elif user.get('vip_level', 0) >= 3: max_slaves = 50
    if user.get('slaves_count', 0) >= max_slaves: raise HTTPException(400, f"Лимит ({max_slaves})")
    now = datetime.utcnow()
    
    clan_level = 1
    if user['syndicate_id']:
        clan_level = await db.fetchval("SELECT level FROM syndicates WHERE id=$1", user['syndicate_id']) or 1

    async with db.acquire() as conn:
        async with conn.transaction():
            target_pre = await conn.fetchrow("SELECT owner_id FROM users WHERE id=$1", target_id)
            if not target_pre: raise HTTPException(404, "Игрок не найден")
            seller_id = target_pre['owner_id'] if target_pre['owner_id'] else target_id
            for locked_id in sorted(list(set([buyer_id, target_id, seller_id]))): await conn.execute("SELECT 1 FROM users WHERE id=$1 FOR UPDATE", locked_id)
            buyer = dict(await conn.fetchrow("SELECT * FROM users WHERE id=$1", buyer_id))
            target = dict(await conn.fetchrow("SELECT * FROM users WHERE id=$1", target_id))
            if target.get('admin_god_mode'): raise HTTPException(400, "Этот игрок неприкасаем (God Mode)")
            if target['owner_id'] == buyer_id: raise HTTPException(400, "Этот игрок уже ваш раб")
            if target['shield_until'] and target['shield_until'] > now: raise HTTPException(400, "Цель защищена Щитом 🛡")
            if target.get('purchase_protection_until') and target.get('purchase_protection_until') > now: raise HTTPException(400, "Защита от покупки активна")
            
            raw_price = float(target['current_price']) * (1.5 if target['chains_until'] and target['chains_until'] > now else 1.0)
            
            final_price = raw_price
            if clan_level >= 3: final_price = raw_price * 0.90 

            if float(buyer['balance']) < final_price: raise HTTPException(400, f"Недостаточно RC. Нужно {final_price:.0f}")
            fee, payout, new_price = round(raw_price * 0.10, 2), round(raw_price * 0.90, 2), round(float(target['current_price']) * 1.50, 2)
            await conn.execute("UPDATE users SET balance=balance-$1::numeric WHERE id=$2", final_price, buyer_id)
            await conn.execute("UPDATE users SET current_price=$1::numeric,owner_id=$2,custom_name=NULL,job_id=NULL,job_assigned_at=NULL,purchase_protection_until=$3 WHERE id=$4", new_price, buyer_id, now + timedelta(hours=1), target_id)
            await conn.execute("UPDATE users SET balance=balance+$1::numeric WHERE id=$2", payout, seller_id)
            await conn.execute("INSERT INTO transactions(buyer_id,slave_id,seller_id,amount,fee) VALUES($1,$2,$3,$4,$5)", buyer_id, target_id, target['owner_id'], raw_price, fee)
            
    asyncio.create_task(push(target_id, f"⛓ Вас купил @{buyer['username'] or buyer_id}!", notif_type="trade"))
    if target['owner_id']: asyncio.create_task(push(target['owner_id'], f"💰 Вашего раба @{target['username'] or target_id} перекупили! Прибыль: <b>{payout:.0f} RC</b>.", notif_type="trade"))
    await add_task_progress(buyer_id, 'buy_slave')
    return {"ok": True, "paid": final_price, "new_price": new_price}

@app.post("/api/slaves/send_to_work")
async def send_to_work(req: SendWorkReq, user: dict = Depends(get_current_user)):
    s = await db.fetchrow("SELECT * FROM users WHERE id=$1", req.slave_id)
    if not s or s['owner_id'] != user['id']: raise HTTPException(403)
    if s['job_assigned_at'] and (datetime.utcnow() - s['job_assigned_at']).total_seconds() < 7200: raise HTTPException(400, "Уже работает")
    if s['is_injured_until'] and s['is_injured_until'] > datetime.utcnow(): raise HTTPException(400, "Раб травмирован")
    new_job = pick_job(user['vip_level'], await get_jobs_list())
    await db.execute("UPDATE users SET job_id=$1, job_assigned_at=$2 WHERE id=$3", new_job['id'], datetime.utcnow(), req.slave_id)
    asyncio.create_task(push(req.slave_id, f"💼 Вам назначена работа: {new_job['emoji']} <b>{new_job['title']}</b> на 2 часа!", notif_type="jobs"))
    await add_task_progress(user['id'], 'send_work')
    return {"ok": True, "job": dict(new_job)}

@app.post("/api/slaves/release")
async def release_slave(req: SendWorkReq, user: dict = Depends(get_current_user)):
    slave = await db.fetchrow("SELECT * FROM users WHERE id=$1 AND owner_id=$2", req.slave_id, user['id'])
    if not slave: raise HTTPException(403, "Раб не найден")
    await db.execute("UPDATE users SET owner_id=NULL, purchase_protection_until=NULL WHERE id=$1", req.slave_id)
    asyncio.create_task(push(req.slave_id, "🕊️ Ваш хозяин отпустил вас на свободу!", notif_type="trade"))
    return {"ok": True}

@app.post("/api/slaves/heal")
async def heal_slave(req: HealReq, user: dict = Depends(get_current_user)):
    slave = await db.fetchrow("SELECT * FROM users WHERE id=$1 AND owner_id=$2", req.slave_id, user['id'])
    if not slave: raise HTTPException(404, "Раб не найден")
    if not slave['is_injured_until'] or slave['is_injured_until'] < datetime.utcnow(): raise HTTPException(400, "Раб здоров")
    cost = 500
    if float(user['balance']) < cost: raise HTTPException(400, f"Нужно {cost} RC")
    await db.execute("UPDATE users SET balance=balance-$1::numeric WHERE id=$2", cost, user['id'])
    await db.execute("UPDATE users SET is_injured_until=NULL WHERE id=$1", req.slave_id)
    return {"ok": True, "cost": cost}

@app.post("/api/vip/claim_boost")
async def claim_vip_boost(user: dict = Depends(get_current_user)):
    if user['vip_level'] < 1: raise HTTPException(400, "Нужен VIP статус")
    now = datetime.utcnow()
    last_claim = user.get('last_vip_boost_claim')
    if isinstance(last_claim, str): last_claim = datetime.fromisoformat(last_claim.replace("Z", "+00:00")).replace(tzinfo=None)
    if last_claim and last_claim.date() == now.date(): raise HTTPException(400, "Буст на сегодня уже получен")
    
    item_type = "boost_20" if user['vip_level'] >= 2 else "boost_15"
    await db.execute("INSERT INTO inventory (user_id, item_id, quantity) VALUES ($1, $2, 1) ON CONFLICT (user_id, item_id) DO UPDATE SET quantity = inventory.quantity + 1", user['id'], item_type)
    await db.execute("UPDATE users SET last_vip_boost_claim=$1 WHERE id=$2", now, user['id'])
    return {"ok": True, "msg": "Карточка Буста добавлена на Склад!"}

@app.get("/api/jobs")
async def get_jobs(_: dict = Depends(get_current_user)): 
    jobs = await get_jobs_list()
    for j in jobs:
        j['min_yield'] = round(float(j['min_yield']) * 100, 1)
        j['max_yield'] = round(float(j['max_yield']) * 100, 1)
    return jobs

@app.post("/api/settings/notifications")
async def update_notifications(req: UpdatePrefsReq, user: dict = Depends(get_current_user)):
    await db.execute("UPDATE users SET notify_prefs=$1 WHERE id=$2", json.dumps(req.prefs), user['id'])
    return {"ok": True}

@app.post("/api/selfbuy")
async def self_buy(user: dict = Depends(get_current_user)):
    uid = user['id']
    await collect_income(uid)
    async with db.acquire() as conn:
        async with conn.transaction():
            u_pre = await conn.fetchrow("SELECT owner_id FROM users WHERE id=$1", uid)
            if not u_pre or not u_pre['owner_id']: raise HTTPException(400, "Вы уже свободны")
            owner_id = u_pre['owner_id']
            for locked_id in sorted([uid, owner_id]): await conn.execute("SELECT 1 FROM users WHERE id=$1 FOR UPDATE", locked_id)
            u = dict(await conn.fetchrow("SELECT * FROM users WHERE id=$1", uid))
            price = float(u['current_price']) * 1.5
            if float(u['balance']) < price: raise HTTPException(400, f"Нужно {price:.0f} RC")
            payout = round(price * 0.50, 2)
            await conn.execute("UPDATE users SET balance=balance-$1::numeric WHERE id=$2", price, uid)
            await conn.execute("UPDATE users SET owner_id=NULL,custom_name=NULL,job_id=NULL,job_assigned_at=NULL WHERE id=$1", uid)
            await conn.execute("UPDATE users SET balance=balance+$1::numeric WHERE id=$2", payout, owner_id)
    asyncio.create_task(push(owner_id, f"💸 @{u['username'] or uid} выкупил себя! Вы получили <b>{payout:.0f} RC</b>.", notif_type="trade"))
    return {"ok": True}

@app.get("/api/search")
async def search_players(q: Optional[str] = None, min_price: Optional[float] = None, max_price: Optional[float] = None, random_pick: bool = False, newcomers: bool = False, user: dict = Depends(get_current_user)):
    now, base, stealth = datetime.utcnow(), "SELECT id,uid,username,first_name,current_price,custom_name,vip_level,avatar_frame,name_color,emoji_status,photo_url,owner_id,is_admin_flag FROM users WHERE is_banned=FALSE AND (admin_hidden=FALSE OR admin_hidden IS NULL)", " AND (stealth_until IS NULL OR stealth_until<$1)"
    if newcomers: rows = await db.fetch(base + stealth + " ORDER BY created_at DESC LIMIT 50", now)
    elif random_pick: rows = await db.fetch(base + stealth + " AND id != $2 AND (owner_id IS NULL OR owner_id != $2) ORDER BY RANDOM() LIMIT 10", now, user['id'])
    elif q: rows = await db.fetch(base + stealth + " AND (username ILIKE $2 OR first_name ILIKE $2 OR uid::text ILIKE $2) LIMIT 20", now, f"%{q}%")
    else:
        params, where = [now], base + stealth
        if min_price is not None: params.append(min_price); where += f" AND current_price>=${len(params)}"
        if max_price is not None: params.append(max_price); where += f" AND current_price<=${len(params)}"
        rows = await db.fetch(where + " ORDER BY created_at DESC LIMIT 50", *params)
    return [dict(r) for r in rows]

@app.get("/api/top")
async def get_top(cat: str = "forbes"):
    ck = f"top:{cat}"
    if cached := await rdb.get(ck): return json.loads(cached)
    now = datetime.utcnow()
    hf = "AND (u.admin_hidden=FALSE OR u.admin_hidden IS NULL)" 
    if cat == "forbes": rows = await db.fetch(f"SELECT u.id, u.uid, u.username, u.first_name, u.balance as value, u.vip_level, u.name_color, u.avatar_frame, u.emoji_status, u.photo_url, u.is_admin_flag FROM users u WHERE (u.stealth_until IS NULL OR u.stealth_until<$1) AND u.is_banned=FALSE {hf} ORDER BY u.balance DESC LIMIT 100", now)
    elif cat == "owners": rows = await db.fetch(f"SELECT u.id, u.uid, u.username, u.first_name, u.vip_level, u.name_color, u.avatar_frame, u.emoji_status, u.photo_url, u.is_admin_flag, COUNT(s.id) as value FROM users u INNER JOIN users s ON s.owner_id = u.id WHERE (u.stealth_until IS NULL OR u.stealth_until < $1) AND u.is_banned = FALSE {hf} GROUP BY u.id ORDER BY value DESC LIMIT 100", now)
    else: rows = await db.fetch(f"SELECT u.id, u.uid, u.username, u.first_name, u.current_price as value, u.vip_level, u.name_color, u.avatar_frame, u.emoji_status, u.photo_url, u.is_admin_flag FROM users u WHERE (u.stealth_until IS NULL OR u.stealth_until<$1) AND u.is_banned=FALSE {hf} ORDER BY u.current_price DESC LIMIT 100", now)
    result = [dict(r) for r in rows]
    await rdb.setex(ck, 300, json.dumps(result, default=str))
    return result

@app.post("/api/rename")
async def rename_slave(req: RenameReq, user: dict = Depends(get_current_user)):
    s = await db.fetchrow("SELECT owner_id FROM users WHERE id=$1", req.slave_id)
    if not s or s['owner_id'] != user['id']: raise HTTPException(403)
    await db.execute("UPDATE users SET custom_name=$1 WHERE id=$2", req.new_name.strip()[:32], req.slave_id)
    return {"ok": True}

@app.post("/api/stories/upload")
async def stories_upload(req: StoryUploadReq, _: dict = Depends(get_current_user)):
    img_id, img_data = str(uuid.uuid4()), base64.b64decode(req.b64_data.split(',')[1])
    story_cache[img_id] = img_data
    asyncio.get_running_loop().call_later(300, story_cache.pop, img_id, None)
    return {"url": f"{WEBAPP_URL}/api/stories/image/{img_id}"}

@app.get("/api/stories/image/{img_id}")
async def get_story_img(img_id: str):
    if img_id not in story_cache: raise HTTPException(404)
    return Response(content=story_cache[img_id], media_type="image/png")

@app.post("/api/stories/claim")
async def stories_claim(u: dict = Depends(get_current_user)):
    last = await db.fetchrow("SELECT created_at FROM stories_claims WHERE user_id=$1 AND status='approved' ORDER BY created_at DESC LIMIT 1", u['id'])
    if last and (datetime.utcnow() - last['created_at']).total_seconds() < 86400: raise HTTPException(400, "Доступно раз в 24 часа")
    await db.execute("INSERT INTO stories_claims(user_id, status) VALUES($1, 'pending')", u['id'])
    asyncio.create_task(notify_admins(f"📸 <b>Новая заявка на Story!</b>\nПользователь ID {u['id']} ({u.get('first_name','')}) ожидает проверки."))
    return {"ok": True, "msg": "Заявка отправлена модераторам!"}

@app.get("/api/tasks/status")
async def get_tasks_status(x_init_data: str = Header(...)):
    uid = await _resolve_uid(x_init_data)
    sponsors = await db.fetch("SELECT * FROM sponsors WHERE is_active=TRUE ORDER BY is_main DESC, id ASC")
    missing_main_sponsors, tasks_sponsors = [], []
    for s in sponsors:
        is_subbed = False
        try:
            member = await bot.get_chat_member(chat_id=s['channel_id'], user_id=uid)
            is_subbed = member.status in ['member', 'administrator', 'creator']
        except: pass
        if s['is_main'] and not is_subbed: missing_main_sponsors.append(dict(s))
        if not s['is_main']:
            claimed = bool(await db.fetchrow("SELECT 1 FROM user_sponsors WHERE user_id=$1 AND sponsor_id=$2", uid, s['id']))
            tasks_sponsors.append({**dict(s), 'claimed': claimed, 'is_subbed': is_subbed})

    date_str, daily_tasks_data = get_daily_tasks()
    user_progress = await db.fetch("SELECT task_id, progress, claimed FROM daily_progress WHERE user_id=$1 AND date_str=$2", uid, date_str)
    prog_map = {r['task_id']: dict(r) for r in user_progress}

    daily_tasks = []
    for t in daily_tasks_data:
        p = prog_map.get(t['id'], {'progress': 0, 'claimed': False})
        daily_tasks.append({**t, "current_progress": min(p['progress'], t['target']), "is_completed": p['progress'] >= t['target'], "is_claimed": p['claimed']})
        
    u = await db.fetchrow("SELECT login_streak, last_login_date FROM users WHERE id=$1", uid)
    today = (datetime.utcnow() + timedelta(hours=3)).strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() + timedelta(hours=3) - timedelta(days=1)).strftime("%Y-%m-%d")
    streak, last_date = u['login_streak'] or 0, u['last_login_date']
    can_claim_login = False
    display_streak = streak
    if last_date == today: can_claim_login = False
    elif last_date == yesterday: can_claim_login = True; display_streak = (streak % 7) + 1
    else: can_claim_login = True; display_streak = 1
    if display_streak == 0: display_streak = 1
    return {"missing_main_sponsors": missing_main_sponsors, "sponsors": tasks_sponsors, "daily": daily_tasks, "login_bonus": {"current_day": display_streak, "can_claim": can_claim_login, "rewards": LOGIN_REWARDS}}

@app.post("/api/tasks/claim_login")
async def claim_login_bonus(u: dict = Depends(get_current_user)):
    today = (datetime.utcnow() + timedelta(hours=3)).strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() + timedelta(hours=3) - timedelta(days=1)).strftime("%Y-%m-%d")
    uid = u['id']
    user_db = await db.fetchrow("SELECT login_streak, last_login_date FROM users WHERE id=$1", uid)
    streak, last_date = user_db['login_streak'] or 0, user_db['last_login_date']
    if last_date == today: raise HTTPException(400, "Награда за сегодня уже получена")
    if last_date == yesterday: new_streak = (streak % 7) + 1
    else: new_streak = 1
    reward = LOGIN_REWARDS[new_streak - 1]
    await db.execute("UPDATE users SET balance=balance+$1::numeric, login_streak=$2, last_login_date=$3 WHERE id=$4", reward, new_streak, today, uid)
    return {"ok": True, "reward": reward, "day": new_streak}

@app.post("/api/tasks/claim_daily")
async def claim_daily_task(req: ClaimTaskReq, u: dict = Depends(get_current_user)):
    date_str, daily_tasks_data = get_daily_tasks()
    task = next((t for t in daily_tasks_data if t['id'] == req.task_id), None)
    if not task: raise HTTPException(404, "Задание не найдено")
    prog = await db.fetchrow("SELECT progress, claimed FROM daily_progress WHERE user_id=$1 AND task_id=$2 AND date_str=$3", u['id'], req.task_id, date_str)
    if not prog or prog['progress'] < task['target']: raise HTTPException(400, "Еще не выполнено")
    if prog.get('claimed'): raise HTTPException(400, "Награда уже получена")
    await db.execute("UPDATE daily_progress SET claimed=TRUE WHERE user_id=$1 AND task_id=$2 AND date_str=$3", u['id'], req.task_id, date_str)
    await db.execute("UPDATE users SET balance=balance+$1::numeric WHERE id=$2", task['reward'], u['id'])
    return {"ok": True, "reward": task['reward']}

@app.post("/api/sponsors/check")
async def check_sponsor(req: CheckSponsorReq, u: dict = Depends(get_current_user)):
    s = await db.fetchrow("SELECT * FROM sponsors WHERE id=$1 AND is_active=TRUE", req.sponsor_id)
    if not s: raise HTTPException(404, "Спонсор не найден")
    if await db.fetchrow("SELECT 1 FROM user_sponsors WHERE user_id=$1 AND sponsor_id=$2", u['id'], s['id']): raise HTTPException(400, "Награда уже получена")
    try:
        member = await bot.get_chat_member(chat_id=s['channel_id'], user_id=u['id'])
        if member.status not in ['member', 'administrator', 'creator']: raise HTTPException(400, "Вы не подписаны на канал.")
    except HTTPException: raise
    except Exception: raise HTTPException(400, "Не удалось проверить подписку.")
    await db.execute("INSERT INTO user_sponsors(user_id, sponsor_id) VALUES($1,$2)", u['id'], s['id'])
    await db.execute("UPDATE users SET balance=balance+$1::numeric WHERE id=$2", s['reward_rc'], u['id'])
    return {"ok": True, "reward": float(s['reward_rc'])}

@app.post("/api/support")
async def send_support(req: SupportMessageReq, x_init_data: str = Header(...)):
    uid = await _resolve_uid(x_init_data)
    message = (req.message or "").strip()[:2000]
    tkt = await db.fetchrow("SELECT id, status FROM tickets WHERE user_id=$1 ORDER BY id DESC LIMIT 1", uid)
    if not tkt: tkt_id = await db.fetchval("INSERT INTO tickets (user_id) VALUES ($1) RETURNING id", uid)
    else:
        tkt_id = tkt['id']
        if tkt['status'] == 'closed': await db.execute("UPDATE tickets SET status='open' WHERE id=$1", tkt_id)
    await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, photo_b64, reply_to_id, is_from_admin) VALUES ($1,$2,$3,$4,$5,FALSE)", tkt_id, uid, message, req.photo_b64, req.reply_to_id)
    asyncio.create_task(notify_admins(f"💬 <b>Новое сообщение в ТП!</b>\nОт ID: {uid}\nТекст: {message[:100]}"))
    return {"ok": True}

@app.get("/api/support/history")
async def support_history(x_init_data: str = Header(...)):
    uid = await _resolve_uid(x_init_data)
    rows = await db.fetch("SELECT st.id, st.text as message, st.photo_b64, st.is_from_admin, st.created_at, rt.text as reply_text FROM support_messages st LEFT JOIN support_messages rt ON st.reply_to_id = rt.id WHERE st.user_id=$1 ORDER BY st.created_at ASC", uid)
    await db.execute("UPDATE support_messages SET is_read=TRUE WHERE user_id=$1 AND is_from_admin=TRUE", uid)
    return [dict(r) for r in rows]

# --- SYNDICATES ENDPOINTS ---

@app.get("/api/syndicates")
async def get_syndicates(user: dict = Depends(get_current_user)):
    rows = await db.fetch("""
        SELECT s.*, u.username as owner_username, u.first_name as owner_name,
        (SELECT COUNT(*) FROM users WHERE syndicate_id=s.id) as members_count
        FROM syndicates s
        LEFT JOIN users u ON u.id = s.owner_id
        ORDER BY s.treasury DESC LIMIT 50
    """)
    return [dict(r) for r in rows]

@app.get("/api/syndicates/my")
async def get_my_syndicate(user: dict = Depends(get_current_user)):
    if not user['syndicate_id']: return None
    s = await db.fetchrow("SELECT * FROM syndicates WHERE id=$1", user['syndicate_id'])
    if not s: return None
    
    members = await db.fetch("SELECT id, username, first_name, photo_url, syndicate_role, balance, current_price FROM users WHERE syndicate_id=$1 ORDER BY (CASE WHEN syndicate_role='don' THEN 1 WHEN syndicate_role='deputy' THEN 2 ELSE 3 END), balance DESC", s['id'])
    requests = []
    if user['syndicate_role'] in ('don', 'deputy'):
        requests = await db.fetch("SELECT sr.id as req_id, u.id, u.username, u.first_name, u.photo_url, u.balance FROM syndicate_requests sr JOIN users u ON u.id=sr.user_id WHERE sr.syndicate_id=$1 ORDER BY sr.created_at ASC", s['id'])
        
    now = datetime.utcnow()
    active_wars = await db.fetch("SELECT w.*, sa.name as attacker_name, sd.name as defender_name FROM syndicate_wars w JOIN syndicates sa ON sa.id=w.attacker_id JOIN syndicates sd ON sd.id=w.defender_id WHERE (w.attacker_id=$1 OR w.defender_id=$1) AND w.status='active'", s['id'])
    pending_wars = await db.fetch("SELECT w.*, sa.name as attacker_name, sd.name as defender_name FROM syndicate_wars w JOIN syndicates sa ON sa.id=w.attacker_id JOIN syndicates sd ON sd.id=w.defender_id WHERE (w.attacker_id=$1 OR w.defender_id=$1) AND w.status='pending'", s['id'])
    
    return {
        "clan": dict(s), 
        "members": [dict(m) for m in members], 
        "requests": [dict(r) for r in requests],
        "wars": {
            "active": [{**dict(w), "expires_at": w['expires_at'].isoformat() + "Z"} for w in active_wars],
            "pending": [{**dict(w), "expires_at": w['expires_at'].isoformat() + "Z"} for w in pending_wars]
        }
    }

@app.post("/api/syndicates/create")
async def create_syndicate(req: SyndicateCreateReq, user: dict = Depends(get_current_user)):
    if user['syndicate_id']: raise HTTPException(400, "Вы уже состоите в синдикате.")
    if float(user['balance']) < 20000: raise HTTPException(400, "Нужно 20,000 RC для создания.")
    name = req.name.strip()[:32]
    if len(name) < 3: raise HTTPException(400, "Слишком короткое название.")
    
    async with db.acquire() as conn:
        async with conn.transaction():
            exists = await conn.fetchrow("SELECT id FROM syndicates WHERE name ILIKE $1", name)
            if exists: raise HTTPException(400, "Синдикат с таким названием уже существует.")
            
            await conn.execute("UPDATE users SET balance = balance - 20000::numeric WHERE id=$1", user['id'])
            syn_id = await conn.fetchval("INSERT INTO syndicates (name, description, owner_id) VALUES ($1, $2, $3) RETURNING id", name, req.description.strip()[:200], user['id'])
            await conn.execute("UPDATE users SET syndicate_id=$1, syndicate_role='don' WHERE id=$2", syn_id, user['id'])
            
    return {"ok": True, "id": syn_id}

@app.post("/api/syndicates/join")
async def join_syndicate(req: SyndicateJoinReq, user: dict = Depends(get_current_user)):
    if user['syndicate_id']: raise HTTPException(400, "Вы уже состоите в клане.")
    exists = await db.fetchrow("SELECT 1 FROM syndicate_requests WHERE user_id=$1 AND syndicate_id=$2", user['id'], req.syndicate_id)
    if exists: raise HTTPException(400, "Заявка уже подана.")
    
    clan = await db.fetchrow("SELECT level, (SELECT COUNT(*) FROM users WHERE syndicate_id=$1) as cnt FROM syndicates WHERE id=$1", req.syndicate_id)
    if not clan: raise HTTPException(404, "Клан не найден.")
    limits = {1:5, 2:10, 3:20, 4:30}
    if clan['cnt'] >= limits.get(clan['level'], 5): raise HTTPException(400, "Клан переполнен.")
    
    await db.execute("INSERT INTO syndicate_requests (user_id, syndicate_id) VALUES ($1, $2)", user['id'], req.syndicate_id)
    return {"ok": True, "msg": "Заявка подана!"}

@app.post("/api/syndicates/leave")
async def leave_syndicate(user: dict = Depends(get_current_user)):
    if not user['syndicate_id']: raise HTTPException(400, "Вы не в клане.")
    active_war = await db.fetchrow("SELECT 1 FROM syndicate_wars WHERE (attacker_id=$1 OR defender_id=$1) AND status='active'", user['syndicate_id'])
    if active_war: raise HTTPException(400, "Нельзя покинуть клан во время активной войны!")
    
    if user['syndicate_role'] == 'don':
        await db.execute("DELETE FROM syndicates WHERE id=$1", user['syndicate_id'])
        await db.execute("UPDATE users SET syndicate_id=NULL, syndicate_role=NULL WHERE syndicate_id=$1", user['syndicate_id'])
    else:
        await db.execute("UPDATE users SET syndicate_id=NULL, syndicate_role=NULL WHERE id=$1", user['id'])
    return {"ok": True}

@app.post("/api/syndicates/requests/resolve")
async def resolve_request(req: SyndicateResolveReq, user: dict = Depends(get_current_user)):
    if user['syndicate_role'] not in ('don', 'deputy'): raise HTTPException(403)
    r = await db.fetchrow("SELECT * FROM syndicate_requests WHERE id=$1 AND syndicate_id=$2", req.request_id, user['syndicate_id'])
    if not r: raise HTTPException(404)
    
    if req.action == 'accept':
        clan = await db.fetchrow("SELECT level, (SELECT COUNT(*) FROM users WHERE syndicate_id=$1) as cnt FROM syndicates WHERE id=$1", user['syndicate_id'])
        limits = {1:5, 2:10, 3:20, 4:30}
        if clan['cnt'] >= limits.get(clan['level'], 5): raise HTTPException(400, "Клан переполнен.")
        
        target = await db.fetchrow("SELECT syndicate_id FROM users WHERE id=$1", r['user_id'])
        if target and not target['syndicate_id']:
            await db.execute("UPDATE users SET syndicate_id=$1, syndicate_role='member' WHERE id=$2", user['syndicate_id'], r['user_id'])
            
    await db.execute("DELETE FROM syndicate_requests WHERE id=$1", req.request_id)
    return {"ok": True}

@app.post("/api/syndicates/members/kick")
async def kick_member(req: SyndicateKickReq, user: dict = Depends(get_current_user)):
    if user['syndicate_role'] not in ('don', 'deputy'): raise HTTPException(403)
    if user['id'] == req.user_id: raise HTTPException(400)
    
    active_war = await db.fetchrow("SELECT 1 FROM syndicate_wars WHERE (attacker_id=$1 OR defender_id=$1) AND status='active'", user['syndicate_id'])
    if active_war: raise HTTPException(400, "Нельзя кикать во время войны!")
    
    target = await db.fetchrow("SELECT syndicate_id, syndicate_role FROM users WHERE id=$1", req.user_id)
    if not target or target['syndicate_id'] != user['syndicate_id']: raise HTTPException(404)
    if target['syndicate_role'] == 'don': raise HTTPException(403)
    if user['syndicate_role'] == 'deputy' and target['syndicate_role'] == 'deputy': raise HTTPException(403)
    
    await db.execute("UPDATE users SET syndicate_id=NULL, syndicate_role=NULL WHERE id=$1", req.user_id)
    return {"ok": True}

@app.post("/api/syndicates/members/promote")
async def promote_member(req: SyndicatePromoteReq, user: dict = Depends(get_current_user)):
    if user['syndicate_role'] != 'don': raise HTTPException(403)
    target = await db.fetchrow("SELECT syndicate_id, syndicate_role FROM users WHERE id=$1", req.user_id)
    if not target or target['syndicate_id'] != user['syndicate_id']: raise HTTPException(404)
    if target['syndicate_role'] == 'don': raise HTTPException(400)
    
    new_role = 'member' if target['syndicate_role'] == 'deputy' else 'deputy'
    await db.execute("UPDATE users SET syndicate_role=$1 WHERE id=$2", new_role, req.user_id)
    return {"ok": True, "new_role": new_role}

@app.post("/api/syndicates/donate")
async def donate_syndicate(req: SyndicateDonateReq, user: dict = Depends(get_current_user)):
    if not user['syndicate_id']: raise HTTPException(400)
    if req.amount <= 0: raise HTTPException(400, "Неверная сумма.")
    
    # Атомарное списание (не даст уйти в минус)
    res = await db.execute("UPDATE users SET balance=balance-$1::numeric WHERE id=$2 AND balance >= $1::numeric", req.amount, user['id'])
    if res == "UPDATE 0":
        raise HTTPException(400, "Недостаточно средств.")
        
    await db.execute("UPDATE syndicates SET treasury=treasury+$1::numeric WHERE id=$2", req.amount, user['syndicate_id'])
    return {"ok": True}

@app.post("/api/syndicates/upgrade")
async def upgrade_syndicate(user: dict = Depends(get_current_user)):
    if user['syndicate_role'] != 'don': raise HTTPException(403)
    clan = await db.fetchrow("SELECT level, treasury FROM syndicates WHERE id=$1", user['syndicate_id'])
    
    costs = {1: 100000, 2: 500000, 3: 1000000}
    if clan['level'] >= 4: raise HTTPException(400, "Максимальный уровень.")
    cost = costs[clan['level']]
    
    if float(clan['treasury']) < cost: raise HTTPException(400, f"Нужно {cost} RC в общаке.")
    
    await db.execute("UPDATE syndicates SET treasury=treasury-$1::numeric, level=level+1 WHERE id=$2", cost, user['syndicate_id'])
    return {"ok": True}

# --- SYNDICATES WARS ---

@app.get("/api/syndicates/wars/targets")
async def get_war_targets(user: dict = Depends(get_current_user)):
    if not user['syndicate_id']: return []
    rows = await db.fetch("SELECT id, name, level, treasury FROM syndicates WHERE id != $1 ORDER BY treasury DESC LIMIT 30", user['syndicate_id'])
    return [dict(r) for r in rows]

@app.post("/api/syndicates/wars/declare")
async def declare_war(req: SyndicateWarDeclareReq, user: dict = Depends(get_current_user)):
    if user['syndicate_role'] not in ('don', 'deputy'): raise HTTPException(403)
    if req.target_id == user['syndicate_id']: raise HTTPException(400)
    
    active_war = await db.fetchrow("SELECT 1 FROM syndicate_wars WHERE (attacker_id=$1 OR defender_id=$1) AND status IN ('pending','active')", user['syndicate_id'])
    if active_war: raise HTTPException(400, "Ваш клан уже участвует в войне или ожидает ответа.")
    
    target_war = await db.fetchrow("SELECT 1 FROM syndicate_wars WHERE (attacker_id=$1 OR defender_id=$1) AND status IN ('pending','active')", req.target_id)
    if target_war: raise HTTPException(400, "Клан противника сейчас занят.")
    
    expires = datetime.utcnow() + timedelta(hours=12)
    await db.execute("INSERT INTO syndicate_wars (attacker_id, defender_id, status, expires_at) VALUES ($1, $2, 'pending', $3)", user['syndicate_id'], req.target_id, expires)
    return {"ok": True}

@app.post("/api/syndicates/wars/resolve")
async def resolve_war(req: SyndicateWarResolveReq, user: dict = Depends(get_current_user)):
    if user['syndicate_role'] not in ('don', 'deputy'): raise HTTPException(403)
    w = await db.fetchrow("SELECT * FROM syndicate_wars WHERE id=$1 AND defender_id=$2 AND status='pending'", req.war_id, user['syndicate_id'])
    if not w: raise HTTPException(404)
    
    if req.action == 'accept':
        expires = datetime.utcnow() + timedelta(hours=24)
        await db.execute("UPDATE syndicate_wars SET status='active', expires_at=$1 WHERE id=$2", expires, req.war_id)
        return {"ok": True, "msg": "Война началась! Длительность 24 часа."}
    else:
        await db.execute("UPDATE syndicate_wars SET status='finished' WHERE id=$1", req.war_id)
        return {"ok": True, "msg": "Вызов отклонен."}

@app.post("/api/syndicates/wars/donate")
async def donate_war(req: SyndicateWarDonateReq, user: dict = Depends(get_current_user)):
    if not user['syndicate_id']: raise HTTPException(400)
    w = await db.fetchrow("SELECT * FROM syndicate_wars WHERE id=$1 AND status='active'", req.war_id)
    if not w: raise HTTPException(404)
    
    if user['syndicate_id'] not in (w['attacker_id'], w['defender_id']): raise HTTPException(403)
    if req.amount <= 0 or req.amount > float(user['balance']): raise HTTPException(400, "Неверная сумма")
    
    is_attacker = user['syndicate_id'] == w['attacker_id']
    col = 'fund_attacker' if is_attacker else 'fund_defender'
    
    res = await db.execute("UPDATE users SET balance=balance-$1::numeric WHERE id=$2 AND balance >= $1::numeric", req.amount, user['id'])
    if res == "UPDATE 0":
        raise HTTPException(400, "Недостаточно средств.")
    await db.execute(f"UPDATE syndicate_wars SET {col}={col}+$1::numeric WHERE id=$2", req.amount, req.war_id)
    return {"ok": True}


@app.get("/api/shop")
async def get_shop(user: dict = Depends(get_current_user)):
    cosmetics = await db.fetch("SELECT * FROM cosmetics ORDER BY type,id")
    owned = {r['cosmetic_id'] for r in await db.fetch("SELECT cosmetic_id FROM user_cosmetics WHERE user_id=$1", user['id'])}
    return {"items": SHOP_ITEMS, "cosmetics": [{**dict(c), "owned": c['id'] in owned} for c in cosmetics]}

@app.post("/api/shop/invoice")
async def create_invoice(req: ShopInvoiceReq, user: dict = Depends(get_current_user)):
    uid = user['id']
    if req.item_type == "cosmetic":
        c = await db.fetchrow("SELECT * FROM cosmetics WHERE id=$1", req.cosmetic_id)
        if not c: raise HTTPException(404)
        item = {"name": c['name'], "price_stars": c['price_stars'], "desc": "Косметика"}
    else:
        item = next((i for i in SHOP_ITEMS if i['id'] == req.item_type), None)
        if not item: raise HTTPException(400, "Неизвестный товар")
    if req.item_type == "chains":
        if not req.target_slave_id: raise HTTPException(400, "Укажите раба")
        s = await db.fetchrow("SELECT owner_id FROM users WHERE id=$1", req.target_slave_id)
        if not s or s['owner_id'] != uid: raise HTTPException(403)
    purchase_id = await db.fetchval("INSERT INTO pending_purchases(user_id,item_type,cosmetic_id,slave_id,stars) VALUES($1,$2,$3,$4,$5) RETURNING id", uid, req.item_type, req.cosmetic_id, req.target_slave_id, item['price_stars'])
    try: return {"ok": True, "invoice_link": await bot.create_invoice_link(title=item['name'], description=item['desc'], payload=f"shop:{purchase_id}:{uid}:{req.item_type}", provider_token="", currency="XTR", prices=[LabeledPrice(label=item['name'], amount=item['price_stars'])])}
    except Exception as e: raise HTTPException(500, f"Ошибка инвойса: {e}")

@app.post("/api/shop/buy_rc")
async def buy_shop_rc(req: ShopRcBuyReq, user: dict = Depends(get_current_user)):
    uid = user['id']
    if req.item_type == "cosmetic": raise HTTPException(400, "Только за Звёзды")
    item = next((i for i in SHOP_ITEMS if i['id'] == req.item_type), None)
    if not item or not item.get('price_rc'): raise HTTPException(400, "Нельзя купить за RC")
    if req.item_type == "chains":
        if not req.target_slave_id: raise HTTPException(400)
        s = await db.fetchrow("SELECT owner_id FROM users WHERE id=$1", req.target_slave_id)
        if not s or s['owner_id'] != uid: raise HTTPException(403)
    bal = float((await db.fetchrow("SELECT balance FROM users WHERE id=$1", uid))['balance'])
    if bal < item['price_rc']: raise HTTPException(400, "Недостаточно RC")
    await db.execute("UPDATE users SET balance=balance-$1::numeric WHERE id=$2", item['price_rc'], uid)
    await _grant_shop_item(uid, req.item_type, None, req.target_slave_id)
    return {"ok": True, "spent_rc": item['price_rc']}

@app.get("/api/inventory")
async def get_inventory(user: dict = Depends(get_current_user)):
    items = await db.fetch("SELECT item_id, quantity FROM inventory WHERE user_id=$1 AND quantity > 0", user['id'])
    cosmetics = await db.fetch("SELECT c.* FROM cosmetics c JOIN user_cosmetics uc ON c.id = uc.cosmetic_id WHERE uc.user_id=$1", user['id'])
    return {
        "items": [dict(r) for r in items],
        "cosmetics": [dict(c) for c in cosmetics]
    }

@app.post("/api/inventory/use")
async def use_inventory_item(req: InventoryUseReq, user: dict = Depends(get_current_user)):
    uid = user['id']
    now = datetime.utcnow()
    async with db.acquire() as conn:
        async with conn.transaction():
            item = await conn.fetchrow("SELECT quantity FROM inventory WHERE user_id=$1 AND item_id=$2 FOR UPDATE", uid, req.item_id)
            if not item or item['quantity'] <= 0:
                raise HTTPException(400, "Предмет не найден или закончился")
            
            if req.item_id == "shield_6":
                await conn.execute("UPDATE users SET shield_until=GREATEST(shield_until, $1) + INTERVAL '6 hours' WHERE id=$2", now, uid)
            elif req.item_id == "shield_12":
                await conn.execute("UPDATE users SET shield_until=GREATEST(shield_until, $1) + INTERVAL '12 hours' WHERE id=$2", now, uid)
            elif req.item_id == "shield_24":
                await conn.execute("UPDATE users SET shield_until=GREATEST(shield_until, $1) + INTERVAL '24 hours' WHERE id=$2", now, uid)
            elif req.item_id == "boost_15":
                await conn.execute("UPDATE users SET booster_mult=GREATEST(booster_mult, 1.5::numeric), booster_until=GREATEST(booster_until, $1) + INTERVAL '4 hours' WHERE id=$2", now, uid)
            elif req.item_id == "boost_20":
                await conn.execute("UPDATE users SET booster_mult=GREATEST(booster_mult, 2.0::numeric), booster_until=GREATEST(booster_until, $1) + INTERVAL '4 hours' WHERE id=$2", now, uid)
            elif req.item_id == "stealth_3":
                await conn.execute("UPDATE users SET stealth_until=GREATEST(stealth_until, $1) + INTERVAL '3 days' WHERE id=$2", now, uid)
            elif req.item_id == "stealth_7":
                await conn.execute("UPDATE users SET stealth_until=GREATEST(stealth_until, $1) + INTERVAL '7 days' WHERE id=$2", now, uid)
            elif req.item_id == "chains":
                if not req.target_slave_id: raise HTTPException(400, "Укажите раба")
                s = await conn.fetchrow("SELECT owner_id FROM users WHERE id=$1", req.target_slave_id)
                if not s or s['owner_id'] != uid: raise HTTPException(403, "Это не ваш раб")
                await conn.execute("UPDATE users SET chains_until=GREATEST(chains_until, $1) + INTERVAL '7 days' WHERE id=$2", now, req.target_slave_id)
                asyncio.create_task(push(req.target_slave_id, "🔒 На вас повесили Оковы! Ваш выкуп стал на 50% дороже на 7 дней.", notif_type="trade"))
            else:
                raise HTTPException(400, "Неизвестный предмет")

            await conn.execute("UPDATE inventory SET quantity = quantity - 1 WHERE user_id=$1 AND item_id=$2", uid, req.item_id)
    return {"ok": True}

@app.post("/api/inventory/equip_cosmetic")
async def equip_cosmetic(req: EquipCosmeticReq, user: dict = Depends(get_current_user)):
    uid = user['id']
    if req.field not in ['name_color', 'avatar_frame', 'emoji_status']: raise HTTPException(400, "Неверное поле")
    
    if req.value in ['none', 'default', '']:
        val = 'default' if req.field == 'name_color' else ('none' if req.field == 'avatar_frame' else '')
        await db.execute(f"UPDATE users SET {req.field}=$1 WHERE id=$2", val, uid)
        return {"ok": True}
    
    ctype = req.field.replace('name_', '').replace('avatar_', '').replace('_status', '')
    c = await db.fetchrow("SELECT c.id FROM cosmetics c JOIN user_cosmetics uc ON c.id = uc.cosmetic_id WHERE uc.user_id=$1 AND c.value=$2 AND c.type=$3", uid, req.value, ctype)
    if not c: raise HTTPException(403, "Косметика не приобретена")
    
    await db.execute(f"UPDATE users SET {req.field}=$1 WHERE id=$2", req.value, uid)
    return {"ok": True}

@app.post("/api/promo")
async def use_promo(req: PromoUseReq, user: dict = Depends(get_current_user)):
    uid = user['id']
    await rate_limit(f"rl:promo:{uid}", 5, 60)
    async with db.acquire() as conn:
        async with conn.transaction():
            promo = await conn.fetchrow("SELECT * FROM promo_codes WHERE code=$1 FOR UPDATE", req.code.upper())
            if not promo: raise HTTPException(404, "Не найден")
            if promo['expires_at'] and promo['expires_at'] < datetime.utcnow(): raise HTTPException(400, "Истёк")
            if promo['used_count'] >= promo['max_uses']: raise HTTPException(400, "Исчерпан")
            if await conn.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND promo_id=$2", uid, promo['id']): raise HTTPException(400, "Уже использован")
            await conn.execute("UPDATE users SET balance=balance+$1::numeric WHERE id=$2", promo['reward_rc'], uid)
            await conn.execute("UPDATE promo_codes SET used_count=used_count+1 WHERE id=$1", promo['id'])
            await conn.execute("INSERT INTO promo_uses(user_id,promo_id) VALUES($1,$2)", uid, promo['id'])
    return {"ok": True, "reward": float(promo['reward_rc'])}

@app.post("/api/admin/maintenance")
async def admin_maintenance(req: MaintToggleReq, _: dict = Depends(get_admin_user)):
    await db.execute("UPDATE global_settings SET value=$1 WHERE key='maintenance'", '1' if req.state else '0')
    return {"ok": True}

@app.get("/api/admin/dashboard")
async def admin_dashboard(_: dict = Depends(get_admin_user)):
    stats = await db.fetchrow("SELECT COUNT(*) AS total_users, COUNT(CASE WHEN created_at>NOW()-INTERVAL '1 day' THEN 1 END) AS new_today, COALESCE(SUM(balance),0) AS total_rc, (SELECT COUNT(*) FROM tickets WHERE status='open' OR status='claimed')::INT AS tickets, (SELECT COUNT(*) FROM stories_claims WHERE status='pending')::INT AS stories_pending FROM users WHERE is_banned=FALSE")
    return dict(stats)

@app.get("/api/admin/users")
async def admin_users(q: Optional[str] = None, _: dict = Depends(get_admin_user)):
    if q: return [dict(r) for r in await db.fetch("SELECT * FROM users WHERE username ILIKE $1 OR first_name ILIKE $1 OR id::text=$2 OR uid::text=$2 LIMIT 100", f"%{q}%", q)]
    return [dict(r) for r in await db.fetch("SELECT * FROM users ORDER BY created_at DESC LIMIT 100")]

@app.post("/api/admin/users/edit")
async def admin_edit(req: AdminEditReq, _: dict = Depends(get_admin_user)):
    parts, vals = [], []
    def add(col, v): vals.append(v); parts.append(f"{col}=${len(vals)}")
    if req.balance is not None: add("balance", req.balance)
    if req.price is not None: add("current_price", req.price)
    if req.is_banned is not None: add("is_banned", req.is_banned)
    if req.ban_reason is not None: add("ban_reason", req.ban_reason if req.ban_reason else None)
    if req.vip_level is not None: add("vip_level", req.vip_level)
    if req.max_slaves_override is not None: add("max_slaves_override", req.max_slaves_override if req.max_slaves_override >= 0 else None)
    if req.free_slave: parts += ["owner_id=NULL","custom_name=NULL","job_id=NULL","job_assigned_at=NULL"]
    if req.custom_name is not None: add("custom_name", req.custom_name or None)
    if parts: 
        vals.append(req.user_id)
        await db.execute(f"UPDATE users SET {','.join(parts)} WHERE id=${len(vals)}", *vals)
    return {"ok": True}

@app.post("/api/admin/users/cosmetic")
async def admin_grant_cosmetic(req: AdminGrantCosmeticReq, _: dict = Depends(get_admin_user)):
    if req.field not in {"name_color", "avatar_frame", "emoji_status"}: raise HTTPException(400)
    try: uid = int(req.identifier)
    except: 
        row = await db.fetchrow("SELECT id FROM users WHERE username ILIKE $1 OR first_name ILIKE $1 LIMIT 1", req.identifier.lstrip('@'))
        if not row: raise HTTPException(404)
        uid = row['id']
    c = await db.fetchrow("SELECT id FROM cosmetics WHERE type=$1 AND value=$2", {"name_color": "color", "avatar_frame": "frame", "emoji_status": "emoji"}.get(req.field), req.value)
    if c: await db.execute("INSERT INTO user_cosmetics(user_id, cosmetic_id) VALUES($1,$2) ON CONFLICT DO NOTHING", uid, c['id'])
    await db.execute(f"UPDATE users SET {req.field}=$1 WHERE id=$2", req.value, uid)
    return {"ok": True}

@app.post("/api/admin/toggle_hidden")
async def admin_toggle_hidden(req: AdminHiddenReq, admin: dict = Depends(get_admin_user)):
    await db.execute("UPDATE users SET admin_hidden=$1 WHERE id=$2", req.hidden, admin['id'])
    return {"ok": True}

@app.post("/api/admin/god_mode")
async def admin_toggle_god_mode(req: AdminHiddenReq, admin: dict = Depends(get_admin_user)):
    await db.execute("UPDATE users SET admin_god_mode=$1 WHERE id=$2", req.hidden, admin['id'])
    return {"ok": True}

@app.get("/api/admin/tickets")
async def admin_tickets(_: dict = Depends(get_admin_user)):
    rows = await db.fetch("SELECT t.user_id, u.username, u.first_name, u.photo_url, COUNT(CASE WHEN sm.is_from_admin=FALSE AND sm.is_read=FALSE THEN 1 END) AS unread, MAX(sm.created_at) AS last_at, (array_agg(sm.text ORDER BY sm.created_at DESC))[1] AS last_msg FROM tickets t JOIN users u ON u.id=t.user_id LEFT JOIN support_messages sm ON sm.ticket_id=t.id WHERE t.status != 'closed' GROUP BY t.user_id, u.username, u.first_name, u.photo_url ORDER BY MAX(sm.created_at) DESC")
    return [dict(r) for r in rows]

@app.get("/api/admin/chat/{user_id}")
async def admin_ticket_chat(user_id: int, _: dict = Depends(get_admin_user)):
    await db.execute("UPDATE support_messages SET is_read=TRUE WHERE user_id=$1 AND is_from_admin=FALSE", user_id)
    return {"messages": [dict(r) for r in await db.fetch("SELECT st.id, st.text as message, st.photo_b64, st.is_from_admin, st.created_at, rt.text as reply_text FROM support_messages st LEFT JOIN support_messages rt ON st.reply_to_id = rt.id WHERE st.user_id=$1 ORDER BY st.created_at ASC", user_id)]}

@app.post("/api/admin/chat/send")
async def admin_chat_send(req: AdminChatSendReq, admin: dict = Depends(get_admin_user)):
    text = req.text.strip()[:2000]
    tkt = await db.fetchrow("SELECT id FROM tickets WHERE user_id=$1 ORDER BY id DESC LIMIT 1", req.target_uid)
    if tkt:
        await db.execute("UPDATE tickets SET status='claimed', claimed_by=$1 WHERE id=$2", tkt['id'])
        tkt_id = tkt['id']
    else: tkt_id = await db.fetchval("INSERT INTO tickets (user_id, status, claimed_by) VALUES ($1, 'claimed', $2) RETURNING id", req.target_uid, admin['id'])
    await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction, reply_to_id, is_from_admin, is_read) VALUES ($1,$2,$3,'out',$4,TRUE,FALSE)", tkt_id, req.target_uid, text, req.reply_to_id)
    asyncio.create_task(push(req.target_uid, f"👨‍💻 <b>Поддержка ответила:</b>\n{text[:100]}", notif_type="support"))
    return {"ok": True}

@app.post("/api/admin/chat/msg/{msg_id}/edit")
async def admin_edit_msg(msg_id: int, req: EditMsgReq, _: dict = Depends(get_admin_user)):
    await db.execute("UPDATE support_messages SET text=$1 WHERE id=$2", req.text.strip(), msg_id)
    return {"ok": True}

@app.post("/api/admin/chat/msg/{msg_id}/delete")
async def admin_del_msg(msg_id: int, _: dict = Depends(get_admin_user)):
    await db.execute("DELETE FROM support_messages WHERE id=$1", msg_id)
    return {"ok": True}

@app.post("/api/admin/chat/ticket/{user_id}/close")
async def admin_close_ticket(user_id: int, _: dict = Depends(get_admin_user)):
    tkt = await db.fetchrow("SELECT id FROM tickets WHERE user_id=$1 ORDER BY id DESC LIMIT 1", user_id)
    if tkt:
        await db.execute("UPDATE tickets SET status='closed' WHERE id=$1", tkt['id'])
        await db.execute("INSERT INTO support_messages (ticket_id, user_id, text, direction, is_from_admin) VALUES ($1,$2,$3,'system',TRUE)", tkt['id'], user_id, "✅ Чат закрыт.")
    return {"ok": True}

@app.post("/api/admin/chat/ticket/{user_id}/delete")
async def admin_delete_ticket(user_id: int, _: dict = Depends(get_admin_user)):
    await db.execute("DELETE FROM support_messages WHERE user_id=$1", user_id)
    await db.execute("DELETE FROM tickets WHERE user_id=$1", user_id)
    return {"ok": True}

@app.get("/api/admin/stories")
async def admin_stories(_: dict = Depends(get_admin_user)):
    return [dict(r) for r in await db.fetch("SELECT sc.*,u.username,u.first_name FROM stories_claims sc JOIN users u ON u.id=sc.user_id WHERE sc.status='pending' ORDER BY sc.created_at")]

@app.post("/api/admin/stories/approve")
async def admin_approve_story(claim_id: int, _: dict = Depends(get_admin_user)):
    c = await db.fetchrow("SELECT * FROM stories_claims WHERE id=$1", claim_id)
    if not c or c['status'] != 'pending': raise HTTPException(400)
    await db.execute("UPDATE stories_claims SET status='approved' WHERE id=$1", claim_id)
    await db.execute("UPDATE users SET balance=balance+250::numeric WHERE id=$1", c['user_id'])
    return {"ok": True}

@app.post("/api/admin/stories/reject")
async def admin_reject_story(claim_id: int, _: dict = Depends(get_admin_user)):
    c = await db.fetchrow("SELECT * FROM stories_claims WHERE id=$1", claim_id)
    if not c or c['status'] != 'pending': raise HTTPException(400)
    await db.execute("UPDATE stories_claims SET status='rejected' WHERE id=$1", claim_id)
    asyncio.create_task(push(c['user_id'], "❌ Ваша заявка на Story была отклонена модератором."))
    return {"ok": True}

@app.post("/api/admin/broadcast")
async def admin_broadcast(req: BroadcastReq, _: dict = Depends(get_admin_user)):
    ids = [u['id'] for u in await db.fetch("SELECT id FROM users WHERE is_banned=FALSE")]
    async def _br():
        for i in range(0, len(ids), 25):
            for uid in ids[i:i+25]: await push(uid, req.text)
            await asyncio.sleep(1)
    asyncio.create_task(_br())
    return {"ok": True, "count": len(ids)}

@app.post("/api/admin/season/start")
async def season_start(req: SeasonReq, _: dict = Depends(get_admin_user)):
    if req.password != SEASON_PASS: raise HTTPException(403)
    await db.execute("UPDATE users SET balance=50,current_price=100,owner_id=NULL,custom_name=NULL,job_id=NULL,job_assigned_at=NULL,shield_until=NULL,chains_until=NULL,booster_mult=1.0,booster_until=NULL,stealth_until=NULL,energy=300,max_energy=300,click_power=1.0,robberies_count=0,riot_expires_at=NULL,is_injured_until=NULL,last_tax_date='',syndicate_id=NULL,syndicate_role=NULL")
    await db.execute("DELETE FROM syndicates")
    await db.execute("DELETE FROM syndicate_wars")
    await db.execute("DELETE FROM syndicate_requests")
    await rdb.delete("top:forbes", "top:owners", "top:legends")
    return {"ok": True}

@app.post("/api/admin/db/clear")
async def clear_database(_: dict = Depends(get_admin_user)):
    async with db.acquire() as conn:
        async with conn.transaction():
            for t in ["pending_purchases", "promo_uses", "user_cosmetics", "user_sponsors", "stories_claims", "support_messages", "tickets", "transactions", "users", "escape_rounds", "escape_bets", "inventory", "syndicates", "syndicate_wars", "syndicate_requests"]:
                await conn.execute(f"DELETE FROM {t}")
    await rdb.delete("top:forbes", "top:owners", "top:legends")
    return {"ok": True}

@app.post("/api/admin/promo/create")
async def create_promo_admin(req: PromoCreateReq, _: dict = Depends(get_admin_user)):
    await db.execute("INSERT INTO promo_codes(code,reward_rc,max_uses) VALUES($1,$2,$3)", req.code.upper(), req.reward_rc, req.max_uses)
    return {"ok": True}

@app.get("/api/admin/sponsors")
async def admin_get_sponsors(_: dict = Depends(get_admin_user)):
    return [dict(r) for r in await db.fetch("SELECT * FROM sponsors ORDER BY is_main DESC, id ASC")]

@app.post("/api/admin/sponsors/add")
async def admin_add_sponsor(req: SponsorAddReq, _: dict = Depends(get_admin_user)):
    await db.execute("INSERT INTO sponsors(channel_id, channel_title, channel_url, reward_rc, is_main, is_active) VALUES($1::text, $2, $3, $4, $5, TRUE) ON CONFLICT(channel_id) DO UPDATE SET channel_title=EXCLUDED.channel_title, channel_url=EXCLUDED.channel_url, reward_rc=EXCLUDED.reward_rc, is_main=EXCLUDED.is_main", str(req.channel_id), req.channel_title, req.channel_url, req.reward_rc, req.is_main)
    return {"ok": True}

@app.post("/api/admin/sponsors/toggle_main")
async def admin_toggle_sponsor(req: SponsorToggleReq, _: dict = Depends(get_admin_user)):
    await db.execute("UPDATE sponsors SET is_main = NOT is_main WHERE id=$1", req.sponsor_id)
    return {"ok": True}

@app.post("/api/admin/sponsors/delete")
async def admin_del_sponsor(req: SponsorDelReq, _: dict = Depends(get_admin_user)):
    await db.execute("DELETE FROM sponsors WHERE id=$1", req.sponsor_id)
    return {"ok": True}

@app.get("/api/admin/admins_list")
async def get_admins_list(_: dict = Depends(get_super_admin)):
    return {"db_admins": [dict(r) for r in await db.fetch("SELECT au.user_id, u.username, u.first_name FROM admin_users au LEFT JOIN users u ON au.user_id = u.id")]}

@app.post("/api/admin/admins_list/add")
async def add_admin(req: AdminManageReq, u: dict = Depends(get_super_admin)):
    await db.execute("INSERT INTO admin_users(user_id, added_by) VALUES($1,$2) ON CONFLICT DO NOTHING", req.user_id, u['id'])
    await db.execute("UPDATE users SET is_admin_flag=TRUE WHERE id=$1", req.user_id)
    return {"ok": True}

@app.post("/api/admin/admins_list/remove")
async def remove_admin(req: AdminManageReq, _: dict = Depends(get_super_admin)):
    if req.user_id in ADMIN_IDS: raise HTTPException(400)
    await db.execute("DELETE FROM admin_users WHERE user_id=$1", req.user_id)
    await db.execute("UPDATE users SET is_admin_flag=FALSE WHERE id=$1", req.user_id)
    return {"ok": True}

@app.get("/api/chat/list")
async def get_chat_list(user: dict = Depends(get_current_user)):
    uid = user['id']
    rows = await db.fetch("SELECT CASE WHEN sender_id = $1 THEN receiver_id ELSE sender_id END as other_user_id, MAX(created_at) as last_msg_time, COUNT(CASE WHEN receiver_id = $1 AND is_read = FALSE THEN 1 END) as unread_count FROM user_chats WHERE sender_id = $1 OR receiver_id = $1 GROUP BY other_user_id ORDER BY last_msg_time DESC", uid)
    result = []
    for r in rows:
        u = await db.fetchrow("SELECT id, first_name, photo_url, is_admin_flag FROM users WHERE id=$1", r['other_user_id'])
        if u: result.append({"user_id": u['id'], "first_name": u['first_name'], "photo_url": u['photo_url'], "is_admin_flag": u['is_admin_flag'], "unread": r['unread_count']})
    return result

@app.get("/api/chat/messages/{target_uid}")
async def get_chat_messages(target_uid: int, user: dict = Depends(get_current_user)):
    uid = user['id']
    await db.execute("UPDATE user_chats SET is_read=TRUE WHERE sender_id=$1 AND receiver_id=$2", target_uid, uid)
    return [dict(r) for r in await db.fetch("SELECT * FROM user_chats WHERE (sender_id=$1 AND receiver_id=$2) OR (sender_id=$2 AND receiver_id=$1) ORDER BY created_at ASC", uid, target_uid)]

@app.post("/api/chat/send")
async def send_chat_message(req: SendChatReq, user: dict = Depends(get_current_user)):
    text = req.text.strip()[:1000]
    if not text: return {"ok": False}
    await db.execute("INSERT INTO user_chats (sender_id, receiver_id, text) VALUES ($1,$2,$3)", user['id'], req.target_uid, text)
    asyncio.create_task(push(req.target_uid, f"✉️ Новое сообщение от {user['first_name'] or 'Игрока'}!", notif_type="messages"))
    return {"ok": True}

@app.get("/")
async def serve(): return FileResponse("index.html")

if __name__ == "__main__": uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), workers=1)
