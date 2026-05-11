import asyncio, os, random, json, base64, uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import asyncpg
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo,
    LabeledPrice, PreCheckoutQuery
)
from pydantic import BaseModel

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BOT_TOKEN    = os.getenv("BOT_TOKEN",    "YOUR_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://rabstvo:rabstvo@localhost/rabstvo")
REDIS_URL    = os.getenv("REDIS_URL",    "redis://localhost:6379")
WEBAPP_URL   = os.getenv("WEBAPP_URL",   "https://yourdomain.com")
ADMIN_IDS    = list(map(int, os.getenv("ADMIN_IDS", "000000000").split(",")))
BOT_USERNAME = os.getenv("BOT_USERNAME", "Rabstvo_Slave_bot")
SEASON_PASS  = "Niva01102007"
SUPER_ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0

# ─── GLOBALS ──────────────────────────────────────────────────────────────────

bot:  Bot            = None
dp:   Dispatcher     = None
db:   asyncpg.Pool   = None
rdb:  aioredis.Redis = None
story_cache: dict    = {}  # In-memory temporary cache for Story base64 -> Image

# ─── DB SCHEMA ────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id                    BIGINT PRIMARY KEY,
    username              TEXT,
    first_name            TEXT,
    photo_url             TEXT,
    balance               DECIMAL  DEFAULT 50,
    current_price         DECIMAL  DEFAULT 100,
    owner_id              BIGINT   REFERENCES users(id) ON DELETE SET NULL,
    custom_name           TEXT,
    job_id                INT,
    job_assigned_at       TIMESTAMP,
    shield_until          TIMESTAMP,
    chains_until          TIMESTAMP,
    booster_mult          DECIMAL  DEFAULT 1.0,
    booster_until         TIMESTAMP,
    stealth_until         TIMESTAMP,
    name_color            TEXT     DEFAULT 'default',
    avatar_frame          TEXT     DEFAULT 'none',
    emoji_status          TEXT     DEFAULT '',
    vip_level             INT      DEFAULT 0,
    vip_until             TIMESTAMP,
    is_banned             BOOLEAN  DEFAULT FALSE,
    is_admin_flag         BOOLEAN  DEFAULT FALSE,
    admin_hidden          BOOLEAN  DEFAULT FALSE,
    purchase_protection_until TIMESTAMP,
    created_at            TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jobs (
    id              SERIAL PRIMARY KEY,
    title           TEXT    NOT NULL,
    income_per_hour DECIMAL NOT NULL,
    drop_chance     INT     NOT NULL,
    emoji           TEXT    DEFAULT '💼'
);

CREATE TABLE IF NOT EXISTS transactions (
    id         SERIAL PRIMARY KEY,
    buyer_id   BIGINT,
    slave_id   BIGINT,
    seller_id  BIGINT,
    amount     DECIMAL,
    fee        DECIMAL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS support_tickets (
    id            SERIAL PRIMARY KEY,
    user_id       BIGINT,
    message       TEXT,
    photo_file_id TEXT,
    is_from_admin BOOLEAN   DEFAULT FALSE,
    is_read       BOOLEAN   DEFAULT FALSE,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stories_claims (
    id         SERIAL PRIMARY KEY,
    user_id    BIGINT,
    status     TEXT      DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS season_snapshots (
    id            SERIAL PRIMARY KEY,
    snapshot_data JSONB,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cosmetics (
    id          SERIAL PRIMARY KEY,
    type        TEXT,
    name        TEXT UNIQUE,
    value       TEXT,
    price_stars INT,
    css_class   TEXT,
    price_rc    INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_cosmetics (
    user_id     BIGINT,
    cosmetic_id INT,
    bought_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, cosmetic_id)
);

CREATE TABLE IF NOT EXISTS promo_codes (
    id         SERIAL PRIMARY KEY,
    code       TEXT    UNIQUE,
    reward_rc  DECIMAL,
    max_uses   INT     DEFAULT 1,
    used_count INT     DEFAULT 0,
    expires_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS promo_uses (
    user_id  BIGINT,
    promo_id INT,
    used_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, promo_id)
);

CREATE TABLE IF NOT EXISTS pending_purchases (
    id          SERIAL PRIMARY KEY,
    user_id     BIGINT,
    item_type   TEXT,
    cosmetic_id INT,
    slave_id    BIGINT,
    stars       INT,
    status      TEXT      DEFAULT 'pending',
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admin_users (
    user_id    BIGINT PRIMARY KEY,
    added_by   BIGINT,
    added_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sponsors (
    id            SERIAL PRIMARY KEY,
    channel_id    TEXT    UNIQUE NOT NULL,
    channel_title TEXT,
    channel_url   TEXT,
    reward_rc     DECIMAL DEFAULT 0,
    is_main       BOOLEAN DEFAULT FALSE,
    is_active     BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS user_sponsors (
    user_id    BIGINT,
    sponsor_id INT,
    claimed_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, sponsor_id)
);

DELETE FROM jobs WHERE id NOT IN (SELECT MIN(id) FROM jobs GROUP BY title);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_title_idx ON jobs(title);

INSERT INTO jobs (title, income_per_hour, drop_chance, emoji) VALUES
  ('Подметать полы',     15.0,  70, '🧹'),
  ('Раздавать листовки', 20.0,  70, '📄'),
  ('Майнить крипту',     85.0,  25, '⛏'),
  ('Петь на улице',      70.0,  25, '🎤'),
  ('Тапать хомяка',     250.0,   5, '🐹'),
  ('Просить милостыню', 220.0,   5, '🙏')
ON CONFLICT (title) DO UPDATE SET income_per_hour=EXCLUDED.income_per_hour;

DELETE FROM cosmetics WHERE id NOT IN (SELECT MIN(id) FROM cosmetics GROUP BY name);
CREATE UNIQUE INDEX IF NOT EXISTS cosmetics_name_idx ON cosmetics(name);

-- Косметика теперь только за звезды. RC цены = 0.
INSERT INTO cosmetics (type, name, value, price_stars, css_class, price_rc) VALUES
  ('color', 'Рубиновый',  'ruby',    10, 'clr-ruby',    0),
  ('color', 'Золотой',    'gold',    15, 'clr-gold',    0),
  ('color', 'Неоновый',   'neon',    20, 'clr-neon',    0),
  ('frame', 'Пламя',      'fire',    25, 'frame-fire',  0),
  ('frame', 'Алмаз',      'diamond', 30, 'frame-diamond',0),
  ('emoji', 'Корона',     '👑',     40, '',            0),
  ('emoji', 'Бриллиант',  '💎',      25, '',            0),
  ('emoji', 'Молния',     '⚡',      15, '',            0)
ON CONFLICT (name) DO UPDATE SET price_stars=EXCLUDED.price_stars, price_rc=0;
"""

SHOP_ITEMS = [
    {"id":"shield_24", "name":"🛡 Щит 24ч",   "price_stars":25,  "price_rc":1200,  "desc":"Защищает вас от покупки другими игроками на 24 часа"},
    {"id":"shield_48", "name":"🛡 Щит 48ч",   "price_stars":45,  "price_rc":2200,  "desc":"Защищает вас от покупки другими игроками на 48 часов"},
    {"id":"chains",    "name":"⛓ Оковы",      "price_stars":20,  "price_rc":800,   "desc":"Вешаются на вашего раба — его выкуп для других дорожает на 50%"},
    {"id":"boost_15",  "name":"🚀 Буст ×1.5", "price_stars":20,  "price_rc":1000,  "desc":"Увеличивает пассивный доход от рабов на +50% на 24 часа"},
    {"id":"boost_20",  "name":"🚀 Буст ×2.0", "price_stars":35,  "price_rc":1800,  "desc":"Удваивает пассивный доход от рабов на 24 часа"},
    {"id":"stealth",   "name":"👻 Стелс",      "price_stars":20,  "price_rc":900,   "desc":"Скрывает вас из рейтингов и поиска на 24 часа"},
    {"id":"vip_1",     "name":"🥉 Богач",      "price_stars":100, "price_rc":4000,  "desc":"Бронзовый статус: рамка + +2% к доходу. Действует 30 дней"},
    {"id":"vip_2",     "name":"🥈 Элита",      "price_stars":200, "price_rc":8000,  "desc":"Серебряный статус: рамка + +5% к доходу. Действует 30 дней"},
    {"id":"vip_3",     "name":"👑 Король",     "price_stars":350, "price_rc":14000, "desc":"Золотая корона: налог 8%, доступ к редким работам. 30 дней"},
]

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def verify_webapp(init_data: str) -> Optional[dict]:
    from urllib.parse import unquote
    try:
        if init_data.startswith("dev:"):
            return {"id": int(init_data.split(":")[1]), "first_name": "DevUser"}
        parsed = dict(x.split("=", 1) for x in init_data.split("&") if "=" in x)
        parsed.pop("hash", "")
        user_raw = parsed.get("user", "{}")
        return json.loads(unquote(user_raw))
    except Exception as e:
        print(f"[auth] parse error: {e}")
    return None

async def get_jobs_list() -> list:
    rows = await db.fetch("SELECT * FROM jobs ORDER BY id")
    return [dict(r) for r in rows]

def pick_job(vip_level: int, jobs: list) -> dict:
    roll = random.random() * 100
    if roll < 70:
        pool = [j for j in jobs if j['drop_chance'] == 70]
    elif roll < 95:
        pool = [j for j in jobs if j['drop_chance'] == 25]
    else:
        pool = [j for j in jobs if j['drop_chance'] == 5] if vip_level >= 3 else [j for j in jobs if j['drop_chance'] == 25]
    return random.choice(pool) if pool else jobs[0]

async def push(uid: int, text: str):
    if bot:
        try: await bot.send_message(uid, text, parse_mode="HTML")
        except Exception: pass

async def is_admin_user(uid: int) -> bool:
    if uid in ADMIN_IDS: return True
    try:
        row = await db.fetchrow("SELECT 1 FROM admin_users WHERE user_id=$1", uid)
        return row is not None
    except Exception: return False

async def notify_admins(text: str):
    notified: set = set(ADMIN_IDS)
    try:
        for r in await db.fetch("SELECT user_id FROM admin_users"): notified.add(r['user_id'])
    except: pass
    for uid in notified: asyncio.create_task(push(uid, text))

async def collect_income(owner_id: int) -> float:
    now = datetime.utcnow()
    slaves = await db.fetch(
        """SELECT u.id, u.job_assigned_at, u.booster_mult, u.booster_until, j.income_per_hour
           FROM users u LEFT JOIN jobs j ON u.job_id = j.id
           WHERE u.owner_id=$1 AND u.job_id IS NOT NULL AND u.job_assigned_at IS NOT NULL""",
        owner_id
    )
    total = 0.0
    for s in slaves:
        hrs = (now - s['job_assigned_at']).total_seconds() / 3600
        mult = float(s['booster_mult']) if s['booster_until'] and s['booster_until'] > now else 1.0
        inc  = hrs * float(s['income_per_hour']) * mult
        total += inc
        await db.execute("UPDATE users SET job_assigned_at=$1 WHERE id=$2", now, s['id'])
    if total > 0:
        await db.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", round(total, 2), owner_id)
    return total

async def _grant_shop_item(uid: int, item_type: str, cosmetic_id: Optional[int], slave_id: Optional[int] = None):
    now = datetime.utcnow()
    if item_type == "shield_24": await db.execute("UPDATE users SET shield_until=$1 WHERE id=$2", now+timedelta(hours=24), uid)
    elif item_type == "shield_48": await db.execute("UPDATE users SET shield_until=$1 WHERE id=$2", now+timedelta(hours=48), uid)
    elif item_type == "boost_15": await db.execute("UPDATE users SET booster_mult=1.5,booster_until=$1 WHERE id=$2", now+timedelta(hours=24), uid)
    elif item_type == "boost_20": await db.execute("UPDATE users SET booster_mult=2.0,booster_until=$1 WHERE id=$2", now+timedelta(hours=24), uid)
    elif item_type == "stealth": await db.execute("UPDATE users SET stealth_until=$1 WHERE id=$2", now+timedelta(hours=24), uid)
    elif item_type == "chains":
        if slave_id:
            s = await db.fetchrow("SELECT owner_id FROM users WHERE id=$1", slave_id)
            if s and s['owner_id'] == uid:
                await db.execute("UPDATE users SET chains_until=$1 WHERE id=$2", now+timedelta(days=7), slave_id)
                asyncio.create_task(push(slave_id, "🔒 На вас повесили Оковы! Ваш выкуп стал на 50% дороже."))
    elif item_type in ("vip_1","vip_2","vip_3"):
        lvl = int(item_type[-1])
        await db.execute("UPDATE users SET vip_level=$1,vip_until=$2 WHERE id=$3", lvl, now+timedelta(days=30), uid)
    elif item_type == "cosmetic" and cosmetic_id:
        c = await db.fetchrow("SELECT * FROM cosmetics WHERE id=$1", cosmetic_id)
        if c:
            await db.execute("INSERT INTO user_cosmetics(user_id,cosmetic_id) VALUES($1,$2) ON CONFLICT DO NOTHING", uid, cosmetic_id)
            if c['type'] == 'color': await db.execute("UPDATE users SET name_color=$1 WHERE id=$2", c['value'], uid)
            elif c['type'] == 'frame': await db.execute("UPDATE users SET avatar_frame=$1 WHERE id=$2", c['value'], uid)
            elif c['type'] == 'emoji': await db.execute("UPDATE users SET emoji_status=$1 WHERE id=$2", c['value'], uid)

# ─── AUTH ─────────────────────────────────────────────────────────────────────

async def _resolve_uid(x_init_data: str) -> int:
    if x_init_data.startswith("dev:"): return int(x_init_data.split(":")[1])
    data = verify_webapp(x_init_data)
    if not data: raise HTTPException(401, "Invalid Telegram auth")
    return int(data['id'])

async def get_current_user(x_init_data: str = Header(...)) -> dict:
    uid = await _resolve_uid(x_init_data)
    user = await db.fetchrow("SELECT * FROM users WHERE id=$1", uid)
    if not user: raise HTTPException(404, "User not found — call /api/init first")
    if user['is_banned']: raise HTTPException(403, "Banned")
    return dict(user)

async def get_admin_user(x_init_data: str = Header(...)) -> dict:
    user = await get_current_user(x_init_data)
    if not await is_admin_user(user['id']): raise HTTPException(403, "Admins only")
    return user

async def get_super_admin(x_init_data: str = Header(...)) -> dict:
    user = await get_current_user(x_init_data)
    if user['id'] != SUPER_ADMIN_ID: raise HTTPException(403, "Только главный администратор")
    return user

# ─── LIFESPAN ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot, dp, db, rdb
    import urllib.parse as _up
    _parsed = _up.urlparse(DATABASE_URL)
    _ssl = "require" if _parsed.hostname and "railway" in _parsed.hostname else False

    for attempt in range(1, 13):
        try:
            _pool_kwargs = {"ssl": _ssl} if _ssl else {}
            db = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=15, **_pool_kwargs)
            break
        except Exception as e:
            if attempt == 12: raise
            await asyncio.sleep(5)

    rdb = aioredis.from_url(REDIS_URL, decode_responses=True)
    async with db.acquire() as c:
        await c.execute(SCHEMA)
    for aid in ADMIN_IDS:
        try: await db.execute("UPDATE users SET is_admin_flag=TRUE WHERE id=$1", aid)
        except Exception: pass
    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher()
    _register_bot_handlers()
    asyncio.create_task(_bot_polling())
    asyncio.create_task(_income_cron())
    yield
    await db.close()
    await rdb.aclose()

app = FastAPI(lifespan=lifespan, title="Рабство API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ─── SCHEMAS ──────────────────────────────────────────────────────────────────

class InitReq(BaseModel): init_data: str; ref_id: Optional[int] = None; photo_url: Optional[str] = None
class BuyReq(BaseModel): target_id: int
class RenameReq(BaseModel): slave_id: int; new_name: str
class SendWorkReq(BaseModel): slave_id: int
class SupportReq(BaseModel): message: str; photo_file_id: Optional[str] = None
class ShopInvoiceReq(BaseModel): item_type: str; cosmetic_id: Optional[int] = None; target_slave_id: Optional[int] = None
class ShopRcBuyReq(BaseModel): item_type: str; cosmetic_id: Optional[int] = None; target_slave_id: Optional[int] = None
class PromoUseReq(BaseModel): code: str
class AdminEditReq(BaseModel): user_id: int; balance: Optional[float] = None; price: Optional[float] = None; custom_name: Optional[str] = None; is_banned: Optional[bool] = None; free_slave: Optional[bool] = None
class SeasonReq(BaseModel): password: str
class BroadcastReq(BaseModel): text: str
class TicketReplyReq(BaseModel): ticket_user_id: int; message: str
class PromoCreateReq(BaseModel): code: str; reward_rc: float; max_uses: int = 1
class AdminManageReq(BaseModel): user_id: int
class StoryUploadReq(BaseModel): b64_data: str
class AdminGrantCosmeticReq(BaseModel): identifier: str; field: str; value: str
class SponsorAddReq(BaseModel): channel_id: str; channel_title: str; channel_url: str; reward_rc: float; is_main: bool = False
class SponsorDelReq(BaseModel): sponsor_id: int
class CheckSponsorReq(BaseModel): sponsor_id: int

# ─── PROFILE HELPER ───────────────────────────────────────────────────────────

async def _full_profile(uid: int) -> dict:
    u = await db.fetchrow("SELECT * FROM users WHERE id=$1", uid)
    if not u: raise HTTPException(404)
    u = dict(u)
    now = datetime.utcnow()

    owner = None
    if u['owner_id']:
        o = await db.fetchrow("SELECT id,username,first_name,photo_url FROM users WHERE id=$1", u['owner_id'])
        if o: owner = dict(o)

    slaves_raw = await db.fetch(
        """SELECT u.id,u.username,u.first_name,u.custom_name,u.current_price,u.photo_url,
                  u.job_id, u.job_assigned_at, j.title as job_title, j.emoji as job_emoji, j.income_per_hour, u.purchase_protection_until
           FROM users u LEFT JOIN jobs j ON u.job_id=j.id WHERE u.owner_id=$1""", uid
    )
    owner_mult = float(u['booster_mult']) if u['booster_until'] and u['booster_until'] > now else 1.0
    slaves = [{
        "id":            s['id'],
        "username":      s['username'],
        "first_name":    s['first_name'],
        "photo_url":     s['photo_url'],
        "custom_name":   s['custom_name'],
        "price":         float(s['current_price']),
        "job_id":        s['job_id'],
        "job_title":     s['job_title'],
        "job_emoji":     s['job_emoji'],
        "income_per_hour": float(s['income_per_hour']) if s['income_per_hour'] else 0,
        "income_per_hour_effective": round(float(s['income_per_hour'] or 0) * owner_mult, 2),
        "working":       bool(s['job_id'] and s['job_assigned_at']),
        "purchase_protection_until": s['purchase_protection_until'].isoformat() if s['purchase_protection_until'] else None
    } for s in slaves_raw]
    total_income_ph = round(sum(s['income_per_hour_effective'] for s in slaves if s['working']), 2)

    last_story = await db.fetchrow("SELECT created_at FROM stories_claims WHERE user_id=$1 AND status='approved' ORDER BY created_at DESC LIMIT 1", uid)
    story_cooldown_until = None
    if last_story:
        next_avail = last_story['created_at'] + timedelta(hours=24)
        if next_avail > now: story_cooldown_until = next_avail.isoformat()

    return {
        "id":           u['id'],
        "username":     u['username'],
        "first_name":   u['first_name'],
        "photo_url":    u['photo_url'],
        "balance":      round(float(u['balance']), 2),
        "price":        round(float(u['current_price']), 2),
        "owner":        owner,
        "custom_name":  u['custom_name'],
        "slaves":       slaves,
        "slaves_count": len(slaves),
        "total_income_per_hour": total_income_ph,
        "shield_active": bool(u['shield_until'] and u['shield_until'] > now),
        "stealth_active": bool(u['stealth_until'] and u['stealth_until'] > now),
        "booster_active": bool(u['booster_until'] and u['booster_until'] > now),
        "booster_mult":   float(u['booster_mult']),
        "chains_active":  bool(u['chains_until'] and u['chains_until'] > now),
        "vip_level":  u['vip_level'],
        "name_color": u['name_color'],
        "avatar_frame": u['avatar_frame'],
        "emoji_status": u['emoji_status'],
        "is_admin":   await is_admin_user(u['id']),
        "is_admin_flag": bool(u.get('is_admin_flag')),
        "is_super_admin": u['id'] == SUPER_ADMIN_ID,
        "admin_hidden": bool(u.get('admin_hidden')),
        "story_cooldown_until": story_cooldown_until,
        "bot_username": BOT_USERNAME,
    }

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.get("/api/config")
async def get_config(): return {"bot_username": BOT_USERNAME, "webapp_url": WEBAPP_URL}

@app.post("/api/init")
async def init_user(req: InitReq):
    if req.init_data.startswith("dev:"):
        uid = int(req.init_data.split(":")[1])
        tg  = {"id": uid, "first_name": f"User{uid}", "username": f"user{uid}"}
    else:
        tg = verify_webapp(req.init_data)
        if not tg: raise HTTPException(401, "Bad initData")
        uid = int(tg['id'])

    photo_url = req.photo_url or tg.get('photo_url')
    existing = await db.fetchrow("SELECT id FROM users WHERE id=$1", uid)
    if not existing:
        owner_id = req.ref_id if (req.ref_id and req.ref_id != uid) else None
        await db.execute(
            """INSERT INTO users(id,username,first_name,photo_url,balance,current_price,owner_id)
               VALUES($1,$2,$3,$4,50,100,$5) ON CONFLICT DO NOTHING""",
            uid, tg.get('username'), tg.get('first_name'), photo_url, owner_id
        )
        if owner_id: asyncio.create_task(push(owner_id, f"🎣 По вашей ссылке перешёл @{tg.get('username', uid)}! Новый раб."))
    else:
        await db.execute("UPDATE users SET username=$1,first_name=$2,photo_url=$3 WHERE id=$4", tg.get('username'), tg.get('first_name'), photo_url, uid)
    return await _full_profile(uid)

@app.get("/api/profile")
async def get_profile(user: dict = Depends(get_current_user)):
    await collect_income(user['id'])
    return await _full_profile(user['id'])

@app.get("/api/profile/{uid}")
async def get_user_profile(uid: int, _: dict = Depends(get_current_user)):
    return await _full_profile(uid)

@app.post("/api/buy")
async def buy_player(req: BuyReq, bg: BackgroundTasks, user: dict = Depends(get_current_user)):
    buyer_id, target_id = user['id'], req.target_id
    if buyer_id == target_id: raise HTTPException(400, "Нельзя купить самого себя")

    await collect_income(buyer_id)
    buyer  = dict(await db.fetchrow("SELECT * FROM users WHERE id=$1", buyer_id))
    target = await db.fetchrow("SELECT * FROM users WHERE id=$1", target_id)
    if not target: raise HTTPException(404, "Игрок не найден")
    target = dict(target)

    if target['owner_id'] == buyer_id: raise HTTPException(400, "Этот игрок уже ваш раб")

    now = datetime.utcnow()
    if target['shield_until'] and target['shield_until'] > now: raise HTTPException(400, "Цель защищена Щитом 🛡")
    if target.get('purchase_protection_until') and target['purchase_protection_until'] > now:
        raise HTTPException(400, f"⛓ Недавно куплен. Защита истекает через {int((target['purchase_protection_until'] - now).total_seconds() / 60)} мин.")

    price = float(target['current_price'])
    if target['chains_until'] and target['chains_until'] > now: price = round(price * 1.5, 2)
    if float(buyer['balance']) < price: raise HTTPException(400, f"Недостаточно RC. Нужно {price:.0f}")

    fee, payout = round(price * 0.10, 2), round(price - round(price * 0.10, 2), 2)
    seller_id = target['owner_id']
    new_price = round(float(target['current_price']) * (1 + random.uniform(0.10, 0.20)), 2)
    protection_until = now + timedelta(hours=2)

    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE users SET balance=balance-$1 WHERE id=$2", price, buyer_id)
            await conn.execute(
                """UPDATE users SET current_price=$1,owner_id=$2,custom_name=NULL,job_id=NULL,job_assigned_at=NULL,purchase_protection_until=$3 WHERE id=$4""",
                new_price, buyer_id, protection_until, target_id
            )
            if seller_id: await conn.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", payout, seller_id)
            else: await conn.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", payout, target_id)
            await conn.execute("INSERT INTO transactions(buyer_id,slave_id,seller_id,amount,fee) VALUES($1,$2,$3,$4,$5)", buyer_id, target_id, seller_id, price, fee)

    bg.add_task(push, target_id, f"⛓ Вас купил @{buyer['username'] or buyer_id}! Ожидайте работу.")
    if seller_id: bg.add_task(push, seller_id, f"💰 Вашего раба @{target['username'] or target_id} перекупили! Прибыль: <b>{payout:.0f} RC</b>.")
    return {"ok": True, "paid": price, "new_price": new_price}

@app.post("/api/slaves/send_to_work")
async def send_to_work(req: SendWorkReq, bg: BackgroundTasks, user: dict = Depends(get_current_user)):
    s = await db.fetchrow("SELECT * FROM users WHERE id=$1", req.slave_id)
    if not s or s['owner_id'] != user['id']: raise HTTPException(403, "Это не ваш раб")
    jobs = await get_jobs_list()
    new_job = pick_job(user['vip_level'], jobs)
    await db.execute("UPDATE users SET job_id=$1, job_assigned_at=$2 WHERE id=$3", new_job['id'], datetime.utcnow(), req.slave_id)
    bg.add_task(push, req.slave_id, f"💼 Вам назначена работа: {new_job['emoji']} <b>{new_job['title']}</b>!")
    return {"ok": True, "job": dict(new_job)}

@app.get("/api/jobs")
async def get_jobs(_: dict = Depends(get_current_user)): return await get_jobs_list()

@app.post("/api/selfbuy")
async def self_buy(bg: BackgroundTasks, user: dict = Depends(get_current_user)):
    uid = user['id']
    await collect_income(uid)
    u = dict(await db.fetchrow("SELECT * FROM users WHERE id=$1", uid))
    if not u['owner_id']: raise HTTPException(400, "Вы уже свободны")
    price = float(u['current_price'])
    if float(u['balance']) < price: raise HTTPException(400, f"Нужно {price:.0f} RC")

    fee, payout = round(price * 0.10, 2), round(price - round(price * 0.10, 2), 2)
    owner_id, new_price = u['owner_id'], round(price * (1 + random.uniform(0.10, 0.20)), 2)

    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE users SET balance=balance-$1 WHERE id=$2", price, uid)
            await conn.execute("UPDATE users SET current_price=$1,owner_id=NULL,custom_name=NULL,job_id=NULL,job_assigned_at=NULL WHERE id=$2", new_price, uid)
            await conn.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", payout, owner_id)
    bg.add_task(push, owner_id, f"💸 @{u['username'] or uid} выкупил себя! Вы получили <b>{payout:.0f} RC</b>.")
    return {"ok": True}

@app.get("/api/search")
async def search_players(q: Optional[str] = None, min_price: Optional[float] = None, max_price: Optional[float] = None, random_pick: bool = False, newcomers: bool = False, user: dict = Depends(get_current_user)):
    now = datetime.utcnow()
    base = "SELECT id,username,first_name,current_price,custom_name,vip_level,avatar_frame,name_color,emoji_status,photo_url,owner_id,is_admin_flag FROM users WHERE is_banned=FALSE AND (admin_hidden=FALSE OR admin_hidden IS NULL)"
    stealth = " AND (stealth_until IS NULL OR stealth_until<$1)"

    if newcomers: rows = await db.fetch(base + stealth + " ORDER BY created_at DESC LIMIT 50", now)
    elif random_pick:
        bal = float((await db.fetchrow("SELECT balance FROM users WHERE id=$1", user['id']))['balance'])
        rows = await db.fetch(base + stealth + " AND id!=$2 AND current_price<=$3 ORDER BY RANDOM() LIMIT 1", now, user['id'], bal)
    elif q: rows = await db.fetch(base + stealth + " AND (username ILIKE $2 OR first_name ILIKE $2) LIMIT 20", now, f"%{q}%")
    else:
        params, where = [now], base + stealth
        if min_price is not None: params.append(min_price); where += f" AND current_price>=${len(params)}"
        if max_price is not None: params.append(max_price); where += f" AND current_price<=${len(params)}"
        where += " ORDER BY created_at DESC LIMIT 50"
        rows = await db.fetch(where, *params)
    return [dict(r) for r in rows]

# FIX #6: Top caching to 5 mins (300s)
@app.get("/api/top")
async def get_top(cat: str = "forbes"):
    ck = f"top:{cat}"
    if cached := await rdb.get(ck): return json.loads(cached)
    now = datetime.utcnow()
    hf = "AND (admin_hidden=FALSE OR admin_hidden IS NULL)"
    if cat == "forbes": rows = await db.fetch(f"SELECT id,username,first_name,balance as value,vip_level,name_color,avatar_frame,emoji_status,photo_url,is_admin_flag FROM users WHERE (stealth_until IS NULL OR stealth_until<$1) AND is_banned=FALSE {hf} ORDER BY balance DESC LIMIT 100", now)
    elif cat == "owners": rows = await db.fetch(f"SELECT u.id,u.username,u.first_name,u.vip_level,u.name_color,u.avatar_frame,u.emoji_status,u.photo_url,u.is_admin_flag,COUNT(s.id) as value FROM users u LEFT JOIN users s ON s.owner_id=u.id WHERE (u.stealth_until IS NULL OR u.stealth_until<$1) AND u.is_banned=FALSE {hf} GROUP BY u.id ORDER BY value DESC LIMIT 100", now)
    else: rows = await db.fetch(f"SELECT id,username,first_name,current_price as value,vip_level,name_color,avatar_frame,emoji_status,photo_url,is_admin_flag FROM users WHERE (stealth_until IS NULL OR stealth_until<$1) AND is_banned=FALSE {hf} ORDER BY current_price DESC LIMIT 100", now)
    result = [dict(r) for r in rows]
    await rdb.setex(ck, 300, json.dumps(result, default=str)) # 5 mins
    return result

@app.post("/api/rename")
async def rename_slave(req: RenameReq, user: dict = Depends(get_current_user)):
    s = await db.fetchrow("SELECT owner_id FROM users WHERE id=$1", req.slave_id)
    if not s or s['owner_id'] != user['id']: raise HTTPException(403)
    await db.execute("UPDATE users SET custom_name=$1 WHERE id=$2", req.new_name.strip()[:32], req.slave_id)
    return {"ok": True}

# ─── NATIVE STORIES API ───
@app.post("/api/stories/upload")
async def stories_upload(req: StoryUploadReq):
    img_id = str(uuid.uuid4())
    img_data = base64.b64decode(req.b64_data.split(',')[1])
    story_cache[img_id] = img_data
    # Храним 5 минут
    asyncio.get_event_loop().call_later(300, story_cache.pop, img_id, None)
    return {"url": f"{WEBAPP_URL}/api/stories/image/{img_id}"}

@app.get("/api/stories/image/{img_id}")
async def get_story_img(img_id: str):
    if img_id not in story_cache: raise HTTPException(404, "Image expired or not found")
    return Response(content=story_cache[img_id], media_type="image/png")

@app.post("/api/stories/claim")
async def stories_claim(bg: BackgroundTasks, user: dict = Depends(get_current_user)):
    uid = user['id']
    now = datetime.utcnow()
    last = await db.fetchrow("SELECT created_at FROM stories_claims WHERE user_id=$1 AND status='approved' ORDER BY created_at DESC LIMIT 1", uid)
    if last and (last['created_at'] + timedelta(hours=24)) > now: raise HTTPException(400, "Только раз в 24 часа")
    
    # Auto-approve for native TG stories 
    await db.execute("INSERT INTO stories_claims(user_id, status) VALUES($1, 'approved')", uid)
    await db.execute("UPDATE users SET balance=balance+250 WHERE id=$1", uid)
    return {"ok": True, "msg": "+250 RC успешно начислены!"}

# ─── SPONSORS API ───
@app.get("/api/sponsors/status")
async def get_sponsors_status(u: dict = Depends(get_current_user)):
    sponsors = await db.fetch("SELECT * FROM sponsors")
    main_subbed, main_sponsor, tasks = True, None, []
    for s in sponsors:
        try:
            member = await bot.get_chat_member(chat_id=s['channel_id'], user_id=u['id'])
            is_subbed = member.status in ['member', 'administrator', 'creator']
        except Exception:
            is_subbed = False
            
        if s['is_main']:
            main_sponsor = dict(s)
            main_subbed = is_subbed
        else:
            claimed = await db.fetchrow("SELECT 1 FROM user_sponsors WHERE user_id=$1 AND sponsor_id=$2", u['id'], s['id'])
            task = dict(s)
            task['claimed'] = bool(claimed)
            tasks.append(task)
    return {"main_subscribed": main_subbed, "main_sponsor": main_sponsor, "tasks": tasks}

@app.post("/api/sponsors/check")
async def check_sponsor(req: CheckSponsorReq, u: dict = Depends(get_current_user)):
    s = await db.fetchrow("SELECT * FROM sponsors WHERE id=$1", req.sponsor_id)
    if not s: raise HTTPException(404)
    claimed = await db.fetchrow("SELECT 1 FROM user_sponsors WHERE user_id=$1 AND sponsor_id=$2", u['id'], s['id'])
    if claimed: raise HTTPException(400, "Уже получено")
    try:
        member = await bot.get_chat_member(chat_id=s['channel_id'], user_id=u['id'])
        if member.status not in ['member', 'administrator', 'creator']: raise HTTPException(400, "Вы не подписаны!")
    except Exception: raise HTTPException(400, "Ошибка проверки. Убедитесь, что бот - админ канала.")
    await db.execute("INSERT INTO user_sponsors(user_id, sponsor_id) VALUES($1,$2)", u['id'], s['id'])
    await db.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", s['reward_rc'], u['id'])
    return {"ok": True, "reward": float(s['reward_rc'])}

# ─── SUPPORT & PROMO ───
@app.post("/api/support")
async def send_support(req: SupportReq, user: dict = Depends(get_current_user)):
    await db.execute("INSERT INTO support_tickets(user_id,message,photo_file_id,is_read) VALUES($1,$2,$3,FALSE)", user['id'], req.message, req.photo_file_id)
    return {"ok": True}

@app.get("/api/support/history")
async def support_history(user: dict = Depends(get_current_user)):
    rows = await db.fetch("SELECT id,message,is_from_admin,is_read,created_at FROM support_tickets WHERE user_id=$1 ORDER BY created_at ASC", user['id'])
    await db.execute("UPDATE support_tickets SET is_read=TRUE WHERE user_id=$1 AND is_from_admin=TRUE AND is_read=FALSE", user['id'])
    return [dict(r) for r in rows]

@app.post("/api/promo")
async def use_promo(req: PromoUseReq, user: dict = Depends(get_current_user)):
    uid = user['id']
    promo = await db.fetchrow("SELECT * FROM promo_codes WHERE code=$1", req.code.upper())
    if not promo: raise HTTPException(404, "Промокод не найден")
    if promo['expires_at'] and promo['expires_at'] < datetime.utcnow(): raise HTTPException(400, "Истёк")
    if promo['used_count'] >= promo['max_uses']: raise HTTPException(400, "Исчерпан")
    if await db.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND promo_id=$2", uid, promo['id']): raise HTTPException(400, "Уже использован")
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", promo['reward_rc'], uid)
            await conn.execute("UPDATE promo_codes SET used_count=used_count+1 WHERE id=$1", promo['id'])
            await conn.execute("INSERT INTO promo_uses(user_id,promo_id) VALUES($1,$2)", uid, promo['id'])
    return {"ok": True, "reward": float(promo['reward_rc'])}

# ─── SHOP ───
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

    purchase_id = await db.fetchval(
        "INSERT INTO pending_purchases(user_id,item_type,cosmetic_id,slave_id,stars) VALUES($1,$2,$3,$4,$5) RETURNING id",
        uid, req.item_type, req.cosmetic_id, req.target_slave_id, item['price_stars']
    )
    try:
        invoice_link = await bot.create_invoice_link(
            title=item['name'], description=item['desc'],
            payload=f"shop:{purchase_id}:{uid}:{req.item_type}",
            provider_token="", currency="XTR", prices=[LabeledPrice(label=item['name'], amount=item['price_stars'])]
        )
        return {"ok": True, "invoice_link": invoice_link}
    except Exception as e: raise HTTPException(500, f"Ошибка инвойса: {e}")

@app.post("/api/shop/buy_rc")
async def buy_shop_rc(req: ShopRcBuyReq, user: dict = Depends(get_current_user)):
    uid = user['id']
    if req.item_type == "cosmetic": raise HTTPException(400, "Косметика только за Звёзды")
    item = next((i for i in SHOP_ITEMS if i['id'] == req.item_type), None)
    if not item or not item.get('price_rc'): raise HTTPException(400, "Товар нельзя купить за RC")
    if req.item_type == "chains":
        if not req.target_slave_id: raise HTTPException(400)
        s = await db.fetchrow("SELECT owner_id FROM users WHERE id=$1", req.target_slave_id)
        if not s or s['owner_id'] != uid: raise HTTPException(403)
        
    await collect_income(uid)
    bal = float((await db.fetchrow("SELECT balance FROM users WHERE id=$1", uid))['balance'])
    if bal < item['price_rc']: raise HTTPException(400, "Недостаточно RC")
    await db.execute("UPDATE users SET balance=balance-$1 WHERE id=$2", item['price_rc'], uid)
    await _grant_shop_item(uid, req.item_type, None, req.target_slave_id)
    return {"ok": True, "spent_rc": item['price_rc']}

# ─── ADMIN ENDPOINTS ───
@app.get("/api/admin/dashboard")
async def admin_dashboard(_: dict = Depends(get_admin_user)):
    stats = await db.fetchrow(
        """SELECT COUNT(*) AS total_users, COUNT(CASE WHEN created_at>NOW()-INTERVAL '1 day' THEN 1 END) AS new_today,
                  COALESCE(SUM(balance),0) AS total_rc, (SELECT COUNT(*) FROM support_tickets WHERE is_from_admin=FALSE AND is_read=FALSE)::INT AS tickets,
                  (SELECT COUNT(*) FROM stories_claims WHERE status='pending')::INT AS stories_pending FROM users WHERE is_banned=FALSE"""
    )
    return dict(stats)

@app.get("/api/admin/users")
async def admin_users(q: Optional[str] = None, _: dict = Depends(get_admin_user)):
    if q: return [dict(r) for r in await db.fetch("SELECT * FROM users WHERE username ILIKE $1 OR first_name ILIKE $1 OR id::text=$2 LIMIT 100", f"%{q}%", q)]
    return [dict(r) for r in await db.fetch("SELECT * FROM users ORDER BY created_at DESC LIMIT 100")]

@app.post("/api/admin/users/edit")
async def admin_edit(req: AdminEditReq, _: dict = Depends(get_admin_user)):
    parts, vals = [], []
    def add(col, v): vals.append(v); parts.append(f"{col}=${len(vals)}")
    if req.balance is not None: add("balance", req.balance)
    if req.price is not None: add("current_price", req.price)
    if req.is_banned is not None: add("is_banned", req.is_banned)
    if req.free_slave: parts += ["owner_id=NULL","custom_name=NULL","job_id=NULL","job_assigned_at=NULL"]
    if parts: vals.append(req.user_id); await db.execute(f"UPDATE users SET {','.join(parts)} WHERE id=${len(vals)}", *vals)
    return {"ok": True}

@app.post("/api/admin/users/cosmetic")
async def admin_grant_cosmetic(req: AdminGrantCosmeticReq, _: dict = Depends(get_admin_user)):
    try: uid = int(req.identifier)
    except: uid = (await db.fetchrow("SELECT id FROM users WHERE username ILIKE $1 OR first_name ILIKE $1 LIMIT 1", req.identifier.lstrip('@')))['id']
    if not uid: raise HTTPException(404)
    
    # Fix: Actually bind the cosmetic ID so it shows as "owned" in the shop
    mapping = {"name_color": "color", "avatar_frame": "frame", "emoji_status": "emoji"}
    c = await db.fetchrow("SELECT id FROM cosmetics WHERE type=$1 AND value=$2", mapping.get(req.field), req.value)
    if c: await db.execute("INSERT INTO user_cosmetics(user_id, cosmetic_id) VALUES($1,$2) ON CONFLICT DO NOTHING", uid, c['id'])
    
    await db.execute(f"UPDATE users SET {req.field}=$1 WHERE id=$2", req.value, uid)
    return {"ok": True}

@app.post("/api/admin/toggle_hidden")
async def admin_toggle_hidden(admin: dict = Depends(get_admin_user)):
    new_val = not bool(await db.fetchval("SELECT admin_hidden FROM users WHERE id=$1", admin['id']))
    await db.execute("UPDATE users SET admin_hidden=$1 WHERE id=$2", new_val, admin['id'])
    return {"ok": True}

@app.get("/api/admin/tickets")
async def admin_tickets(_: dict = Depends(get_admin_user)):
    rows = await db.fetch("""SELECT st.user_id, u.username, u.first_name, u.photo_url, COUNT(*) AS total_msgs, COUNT(CASE WHEN st.is_from_admin=FALSE AND st.is_read=FALSE THEN 1 END) AS unread, MAX(st.created_at) AS last_at, (array_agg(st.message ORDER BY st.created_at DESC))[1] AS last_msg, (array_agg(st.is_from_admin ORDER BY st.created_at DESC))[1] AS last_is_admin FROM support_tickets st JOIN users u ON u.id=st.user_id GROUP BY st.user_id, u.username, u.first_name, u.photo_url ORDER BY MAX(st.created_at) DESC LIMIT 200""")
    return [dict(r) for r in rows]

@app.get("/api/admin/tickets/{user_id}")
async def admin_ticket_chat(user_id: int, _: dict = Depends(get_admin_user)):
    await db.execute("UPDATE support_tickets SET is_read=TRUE WHERE user_id=$1 AND is_from_admin=FALSE", user_id)
    return [dict(r) for r in await db.fetch("SELECT * FROM support_tickets WHERE user_id=$1 ORDER BY created_at ASC", user_id)]

@app.post("/api/admin/tickets/reply")
async def admin_reply(req: TicketReplyReq, bg: BackgroundTasks, _: dict = Depends(get_admin_user)):
    await db.execute("INSERT INTO support_tickets(user_id,message,is_from_admin,is_read) VALUES($1,$2,TRUE,FALSE)", req.ticket_user_id, req.message)
    bg.add_task(push, req.ticket_user_id, f"📨 Ответ поддержки:\n{req.message}")
    return {"ok": True}

@app.get("/api/admin/stories")
async def admin_stories(_: dict = Depends(get_admin_user)):
    return [dict(r) for r in await db.fetch("SELECT sc.*,u.username,u.first_name FROM stories_claims sc JOIN users u ON u.id=sc.user_id WHERE sc.status='pending' ORDER BY sc.created_at")]

@app.post("/api/admin/stories/approve")
async def admin_approve_story(claim_id: int, bg: BackgroundTasks, _: dict = Depends(get_admin_user)):
    c = await db.fetchrow("SELECT * FROM stories_claims WHERE id=$1", claim_id)
    if not c or c['status'] != 'pending': raise HTTPException(400)
    await db.execute("UPDATE stories_claims SET status='approved' WHERE id=$1", claim_id)
    await db.execute("UPDATE users SET balance=balance+250 WHERE id=$1", c['user_id'])
    return {"ok": True, "item": "Бонус 250 RC"}

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
    await db.execute("UPDATE users SET balance=50,current_price=100,owner_id=NULL,custom_name=NULL,job_id=NULL,job_assigned_at=NULL,shield_until=NULL,chains_until=NULL,booster_mult=1.0,booster_until=NULL,stealth_until=NULL")
    return {"ok": True}

@app.post("/api/admin/db/clear")
async def clear_database(_: dict = Depends(get_admin_user)):
    async with db.acquire() as conn:
        async with conn.transaction():
            for t in ["pending_purchases", "promo_uses", "user_cosmetics", "user_sponsors", "stories_claims", "support_tickets", "transactions", "season_snapshots", "users"]:
                await conn.execute(f"DELETE FROM {t}")
    return {"ok": True}

@app.post("/api/admin/promo/create")
async def create_promo(req: PromoCreateReq, _: dict = Depends(get_admin_user)):
    await db.execute("INSERT INTO promo_codes(code,reward_rc,max_uses) VALUES($1,$2,$3)", req.code.upper(), req.reward_rc, req.max_uses)
    return {"ok": True}

# ─── ADMIN SPONSORS ───
@app.get("/api/admin/sponsors")
async def admin_get_sponsors(_: dict = Depends(get_admin_user)):
    return [dict(r) for r in await db.fetch("SELECT * FROM sponsors ORDER BY is_main DESC, id ASC")]

@app.post("/api/admin/sponsors/add")
async def admin_add_sponsor(req: SponsorAddReq, _: dict = Depends(get_admin_user)):
    await db.execute("INSERT INTO sponsors(channel_id, channel_title, channel_url, reward_rc, is_main) VALUES($1,$2,$3,$4,$5) ON CONFLICT(channel_id) DO UPDATE SET channel_title=$2, channel_url=$3, reward_rc=$4, is_main=$5", req.channel_id, req.channel_title, req.channel_url, req.reward_rc, req.is_main)
    return {"ok": True}

@app.post("/api/admin/sponsors/delete")
async def admin_del_sponsor(req: SponsorDelReq, _: dict = Depends(get_admin_user)):
    await db.execute("DELETE FROM sponsors WHERE id=$1", req.sponsor_id)
    return {"ok": True}

# ─── SUPER ADMIN ───
@app.get("/api/admin/admins_list")
async def get_admins_list(_: dict = Depends(get_super_admin)):
    rows = await db.fetch("SELECT au.user_id, u.username, u.first_name FROM admin_users au LEFT JOIN users u ON au.user_id = u.id")
    return {"db_admins": [dict(r) for r in rows]}

@app.post("/api/admin/admins_list/add")
async def add_admin(req: AdminManageReq, u: dict = Depends(get_super_admin)):
    await db.execute("INSERT INTO admin_users(user_id, added_by) VALUES($1,$2) ON CONFLICT DO NOTHING", req.user_id, u['id'])
    await db.execute("UPDATE users SET is_admin_flag=TRUE WHERE id=$1", req.user_id)
    return {"ok": True}

@app.post("/api/admin/admins_list/remove")
async def remove_admin(req: AdminManageReq, _: dict = Depends(get_super_admin)):
    if req.user_id in ADMIN_IDS: raise HTTPException(400, "Env Admin")
    await db.execute("DELETE FROM admin_users WHERE user_id=$1", req.user_id)
    await db.execute("UPDATE users SET is_admin_flag=FALSE WHERE id=$1", req.user_id)
    return {"ok": True}

# ─── BOT HANDLERS ─────────────────────────────────────────────────────────────

def _register_bot_handlers():
    @dp.message(CommandStart())
    async def cmd_start(msg: types.Message):
        uid = msg.from_user.id
        ref_id = None
        if msg.text and "ref_" in msg.text:
            try: ref_id = int(msg.text.split("ref_")[1])
            except: pass
        existing = await db.fetchrow("SELECT id FROM users WHERE id=$1", uid)
        if not existing:
            owner_id = ref_id if ref_id and ref_id != uid else None
            await db.execute("INSERT INTO users(id,username,first_name,balance,current_price,owner_id) VALUES($1,$2,$3,50,100,$4) ON CONFLICT DO NOTHING", uid, msg.from_user.username, msg.from_user.first_name, owner_id)
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⛓ Открыть Рабство", web_app=WebAppInfo(url=WEBAPP_URL))]])
        await msg.answer("⛓ <b>РАБСТВО</b>\n\nСоциальная экономическая стратегия внутри Telegram.\nПокупай людей → назначай работу → собирай доход.", parse_mode="HTML", reply_markup=kb)

    @dp.pre_checkout_query()
    async def pre_checkout(query: PreCheckoutQuery):
        await query.answer(ok=True)

    @dp.message(F.successful_payment)
    async def successful_payment(msg: types.Message):
        payload = msg.successful_payment.invoice_payload
        try:
            parts = payload.split(":")
            purchase_id, uid, item_type = int(parts[1]), int(parts[2]), parts[3]
            purchase = await db.fetchrow("SELECT * FROM pending_purchases WHERE id=$1 AND user_id=$2 AND status='pending'", purchase_id, uid)
            if not purchase: return await msg.answer("⚠️ Покупка уже обработана.")
            await db.execute("UPDATE pending_purchases SET status='completed' WHERE id=$1", purchase_id)
            await _grant_shop_item(uid, item_type, purchase['cosmetic_id'], purchase['slave_id'])
            await msg.answer("✅ Покупка успешна! Предмет активирован.")
        except Exception as e: print(f"[payment] error: {e}")

# ─── BACKGROUND ───────────────────────────────────────────────────────────────

async def _income_cron():
    while True:
        await asyncio.sleep(1800)
        try:
            for row in await db.fetch("SELECT DISTINCT owner_id FROM users WHERE owner_id IS NOT NULL AND job_id IS NOT NULL"):
                try: await collect_income(row['owner_id'])
                except: pass
        except: pass

async def _bot_polling():
    try: await dp.start_polling(bot, skip_updates=True)
    except: pass

@app.get("/")
async def serve(): return FileResponse("index.html")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), workers=1)
