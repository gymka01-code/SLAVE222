#!/usr/bin/env python3
"""
⛓ РАБСТВО — Telegram Mini App
Backend: FastAPI + Aiogram 3 + PostgreSQL + Redis
Single-file, fully self-contained.
"""

import asyncio, os, random, json, hmac, hashlib, time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional, List

import asyncpg
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from pydantic import BaseModel

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BOT_TOKEN    = os.getenv("BOT_TOKEN",    "8789914334:AAH7zS72v3-LMmIsavViB8W_SrvlQ_7-jFU")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://rabstvo:rabstvo@localhost/rabstvo")
REDIS_URL    = os.getenv("REDIS_URL",    "redis://localhost:6379")
WEBAPP_URL   = os.getenv("WEBAPP_URL",   "https://slave222-production.up.railway.app")
ADMIN_IDS    = list(map(int, os.getenv("ADMIN_IDS", "7502434760").split(",")))
SEASON_PASS  = "Niva01102007"

# ─── GLOBALS ──────────────────────────────────────────────────────────────────

bot:  Bot           = None
dp:   Dispatcher    = None
db:   asyncpg.Pool  = None
rdb:  aioredis.Redis = None

# ─── DB SCHEMA ────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id                    BIGINT PRIMARY KEY,
    username              TEXT,
    first_name            TEXT,
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
    unsub_penalty_notified BOOLEAN DEFAULT FALSE,
    is_banned             BOOLEAN  DEFAULT FALSE,
    created_at            TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jobs (
    id              SERIAL PRIMARY KEY,
    title           TEXT    NOT NULL,
    income_per_hour DECIMAL NOT NULL,
    drop_chance     INT     NOT NULL,
    requires_vip    BOOLEAN DEFAULT FALSE,
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

CREATE TABLE IF NOT EXISTS sponsors (
    id            SERIAL  PRIMARY KEY,
    channel_id    BIGINT  UNIQUE,
    channel_title TEXT,
    reward_rc     DECIMAL DEFAULT 250,
    is_active     BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS user_sponsors (
    user_id      BIGINT,
    sponsor_id   INT,
    subscribed_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, sponsor_id)
);

CREATE TABLE IF NOT EXISTS cosmetics (
    id          SERIAL PRIMARY KEY,
    type        TEXT,
    name        TEXT,
    value       TEXT,
    price_stars INT,
    css_class   TEXT
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

INSERT INTO jobs (title, income_per_hour, drop_chance, emoji) VALUES
  ('Подметать полы',    2.5,  70, '🧹'),
  ('Раздавать листовки',3.0,  70, '📄'),
  ('Майнить крипту',    8.0,  25, '⛏'),
  ('Петь на улице',     7.5,  25, '🎤'),
  ('Тапать хомяка',    20.0,   5, '🐹'),
  ('Просить милостыню',18.0,   5, '🙏')
ON CONFLICT DO NOTHING;

INSERT INTO cosmetics (type, name, value, price_stars, css_class) VALUES
  ('color', 'Неоновый',    'neon',    50, 'clr-neon'),
  ('color', 'Красный',     'red',     30, 'clr-red'),
  ('color', 'Золотой',     'gold',    40, 'clr-gold'),
  ('color', 'Градиент',    'gradient',70, 'clr-gradient'),
  ('frame', 'Огонь',       'fire',    80, 'frame-fire'),
  ('frame', 'Киберпанк',   'cyber',   80, 'frame-cyber'),
  ('frame', 'Неон',        'neon',    60, 'frame-neon'),
  ('frame', 'Цветы',       'flowers', 60, 'frame-flowers'),
  ('emoji', '👑 Корона',   '👑',     100, ''),
  ('emoji', '💎 Бриллиант','💎',      70, ''),
  ('emoji', '🔥 Огонь',    '🔥',      50, ''),
  ('emoji', '⚡ Молния',   '⚡',      40, '')
ON CONFLICT DO NOTHING;
"""

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def verify_webapp(init_data: str) -> Optional[dict]:
    try:
        from urllib.parse import unquote
        parsed = dict(x.split("=", 1) for x in init_data.split("&") if "=" in x)
        check_hash = parsed.pop("hash", "")
        data_check = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret   = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        computed = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        print(f"[auth] hash_ok={hmac.compare_digest(computed, check_hash)} check_hash={check_hash[:10]}... computed={computed[:10]}...")
        if hmac.compare_digest(computed, check_hash):
            return json.loads(unquote(parsed.get("user", "{}")))
    except Exception as e:
        print(f"[auth] exception: {e}")
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

async def collect_income(owner_id: int) -> float:
    now = datetime.utcnow()
    slaves = await db.fetch(
        """SELECT u.id, u.job_assigned_at, u.booster_mult, u.booster_until,
                  j.income_per_hour
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

# ─── AUTH ─────────────────────────────────────────────────────────────────────

async def _resolve_uid(x_init_data: str) -> int:
    if x_init_data.startswith("dev:"):           # dev bypass
        return int(x_init_data.split(":")[1])
    data = verify_webapp(x_init_data)
    if not data:
        raise HTTPException(401, "Invalid Telegram auth")
    return int(data['id'])

async def get_current_user(x_init_data: str = Header(...)) -> dict:
    uid = await _resolve_uid(x_init_data)
    user = await db.fetchrow("SELECT * FROM users WHERE id=$1", uid)
    if not user:
        raise HTTPException(404, "User not found — call /api/init first")
    if user['is_banned']:
        raise HTTPException(403, "Banned")
    return dict(user)

async def get_admin_user(x_init_data: str = Header(...)) -> dict:
    user = await get_current_user(x_init_data)
    if user['id'] not in ADMIN_IDS:
        raise HTTPException(403, "Admins only")
    return user

# ─── LIFESPAN ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot, dp, db, rdb

    # Диагностика: показываем хост подключения (пароль скрыт)
    import urllib.parse as _up
    _parsed = _up.urlparse(DATABASE_URL)
    print(f"[db] connecting → host={_parsed.hostname} port={_parsed.port} db={_parsed.path}")
    _ssl = "require" if _parsed.hostname and "railway" in _parsed.hostname else False

    # Retry-loop: 12 попыток × 5 сек = до 60 секунд
    for attempt in range(1, 13):
        try:
            _pool_kwargs = {"ssl": _ssl} if _ssl else {}
            db = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=15, **_pool_kwargs)
            print(f"[db] connected on attempt {attempt}")
            break
        except Exception as e:
            print(f"[db] attempt {attempt}/12 failed: {e}")
            if attempt == 12:
                raise
            await asyncio.sleep(5)

    rdb = aioredis.from_url(REDIS_URL, decode_responses=True)
    async with db.acquire() as c:
        await c.execute(SCHEMA)
    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher()
    _register_bot_handlers()
    asyncio.create_task(_bot_polling())
    asyncio.create_task(_income_cron())
    yield
    await db.close()
    await rdb.aclose()

# ─── APP ──────────────────────────────────────────────────────────────────────

app = FastAPI(lifespan=lifespan, title="Рабство API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

# ─── SCHEMAS ──────────────────────────────────────────────────────────────────

class InitReq(BaseModel):
    init_data: str
    ref_id:    Optional[int] = None

class BuyReq(BaseModel):
    target_id: int

class RenameReq(BaseModel):
    slave_id: int
    new_name: str

class SupportReq(BaseModel):
    message:       str
    photo_file_id: Optional[str] = None

class ShopBuyReq(BaseModel):
    item_type:      str
    cosmetic_id:    Optional[int] = None
    target_slave_id: Optional[int] = None

class PromoUseReq(BaseModel):
    code: str

class AdminEditReq(BaseModel):
    user_id:    int
    balance:    Optional[float] = None
    price:      Optional[float] = None
    custom_name: Optional[str] = None
    is_banned:  Optional[bool] = None
    free_slave: Optional[bool] = None

class SeasonReq(BaseModel):
    password: str

class BroadcastReq(BaseModel):
    text: str

class TicketReplyReq(BaseModel):
    ticket_user_id: int
    message: str

class PromoCreateReq(BaseModel):
    code:      str
    reward_rc: float
    max_uses:  int = 1

class SponsorReq(BaseModel):
    channel_id:    int
    channel_title: str
    reward_rc:     float = 250.0

# ─── PROFILE HELPER ───────────────────────────────────────────────────────────

async def _full_profile(uid: int) -> dict:
    u = await db.fetchrow("SELECT * FROM users WHERE id=$1", uid)
    if not u:
        raise HTTPException(404)
    u = dict(u)
    now = datetime.utcnow()

    owner = None
    if u['owner_id']:
        o = await db.fetchrow("SELECT id,username,first_name FROM users WHERE id=$1", u['owner_id'])
        if o: owner = dict(o)

    slaves_raw = await db.fetch(
        """SELECT u.id,u.username,u.first_name,u.custom_name,u.current_price,
                  j.title as job_title, j.emoji as job_emoji, j.income_per_hour
           FROM users u LEFT JOIN jobs j ON u.job_id=j.id
           WHERE u.owner_id=$1""", uid
    )
    slaves = [{
        "id":          s['id'],
        "username":    s['username'],
        "first_name":  s['first_name'],
        "custom_name": s['custom_name'],
        "price":       float(s['current_price']),
        "job_title":   s['job_title'],
        "job_emoji":   s['job_emoji'],
    } for s in slaves_raw]

    return {
        "id":           u['id'],
        "username":     u['username'],
        "first_name":   u['first_name'],
        "balance":      round(float(u['balance']), 2),
        "price":        round(float(u['current_price']), 2),
        "owner":        owner,
        "custom_name":  u['custom_name'],
        "slaves":       slaves,
        "slaves_count": len(slaves),
        "shield_active": bool(u['shield_until'] and u['shield_until'] > now),
        "shield_until": u['shield_until'].isoformat() if u['shield_until'] else None,
        "stealth_active": bool(u['stealth_until'] and u['stealth_until'] > now),
        "booster_active": bool(u['booster_until'] and u['booster_until'] > now),
        "booster_mult":   float(u['booster_mult']),
        "chains_active":  bool(u['chains_until'] and u['chains_until'] > now),
        "vip_level":  u['vip_level'],
        "name_color": u['name_color'],
        "avatar_frame": u['avatar_frame'],
        "emoji_status": u['emoji_status'],
        "is_admin":   u['id'] in ADMIN_IDS,
    }

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.post("/api/init")
async def init_user(req: InitReq):
    if req.init_data.startswith("dev:"):
        uid = int(req.init_data.split(":")[1])
        tg  = {"id": uid, "first_name": f"User{uid}", "username": f"user{uid}"}
    else:
        tg = verify_webapp(req.init_data)
        if not tg: raise HTTPException(401, "Bad initData")
        uid = int(tg['id'])

    existing = await db.fetchrow("SELECT id FROM users WHERE id=$1", uid)
    if not existing:
        owner_id = None
        if req.ref_id and req.ref_id != uid:
            ref_ok = await db.fetchrow("SELECT id FROM users WHERE id=$1", req.ref_id)
            if ref_ok: owner_id = req.ref_id

        jobs = await get_jobs_list()
        job  = pick_job(0, jobs) if owner_id else None
        now  = datetime.utcnow()

        await db.execute(
            """INSERT INTO users(id,username,first_name,balance,current_price,owner_id,job_id,job_assigned_at)
               VALUES($1,$2,$3,50,100,$4,$5,$6) ON CONFLICT DO NOTHING""",
            uid, tg.get('username'), tg.get('first_name'),
            owner_id, job['id'] if job else None, now if job else None
        )
        if owner_id:
            asyncio.create_task(push(owner_id,
                f"🎣 По вашей ссылке перешёл @{tg.get('username', uid)}! Новый раб готов."))
    else:
        await db.execute(
            "UPDATE users SET username=$1,first_name=$2 WHERE id=$3",
            tg.get('username'), tg.get('first_name'), uid
        )

    return await _full_profile(uid)


@app.get("/api/profile")
async def get_profile(user: dict = Depends(get_current_user)):
    await collect_income(user['id'])
    return await _full_profile(user['id'])


@app.post("/api/buy")
async def buy_player(req: BuyReq, bg: BackgroundTasks, user: dict = Depends(get_current_user)):
    buyer_id  = user['id']
    target_id = req.target_id
    if buyer_id == target_id:
        raise HTTPException(400, "Нельзя купить самого себя")

    await collect_income(buyer_id)
    buyer  = dict(await db.fetchrow("SELECT * FROM users WHERE id=$1", buyer_id))
    target = await db.fetchrow("SELECT * FROM users WHERE id=$1", target_id)
    if not target:
        raise HTTPException(404, "Игрок не найден")
    target = dict(target)

    now = datetime.utcnow()
    if target['shield_until'] and target['shield_until'] > now:
        raise HTTPException(400, "Цель защищена Щитом 🛡")

    price = float(target['current_price'])
    if target['chains_until'] and target['chains_until'] > now:
        price = round(price * 1.5, 2)  # Оковы +50%

    if float(buyer['balance']) < price:
        raise HTTPException(400, f"Недостаточно RC. Нужно {price:.0f}")

    fee       = round(price * 0.10, 2)
    payout    = round(price - fee, 2)
    seller_id = target['owner_id']
    inflation = random.uniform(0.10, 0.20)
    new_price = round(float(target['current_price']) * (1 + inflation), 2)

    jobs    = await get_jobs_list()
    new_job = pick_job(buyer['vip_level'], jobs)

    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE users SET balance=balance-$1 WHERE id=$2", price, buyer_id)
            await conn.execute(
                """UPDATE users SET current_price=$1,owner_id=$2,custom_name=NULL,
                   job_id=$3,job_assigned_at=$4 WHERE id=$5""",
                new_price, buyer_id, new_job['id'], now, target_id
            )
            if seller_id:
                await conn.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", payout, seller_id)
            else:
                await conn.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", payout, target_id)
            await conn.execute(
                "INSERT INTO transactions(buyer_id,slave_id,seller_id,amount,fee) VALUES($1,$2,$3,$4,$5)",
                buyer_id, target_id, seller_id, price, fee
            )

    t_name = f"@{target['username']}" if target['username'] else str(target_id)
    b_name = f"@{buyer['username']}"  if buyer['username']  else str(buyer_id)
    bg.add_task(push, target_id, f"⛓ Вас купил {b_name}! Назначена работа: {new_job['emoji']} {new_job['title']}.")
    if seller_id:
        bg.add_task(push, seller_id, f"💰 Вашего раба {t_name} перекупили! Прибыль: <b>{payout:.0f} RC</b>.")

    return {"ok": True, "paid": price, "new_price": new_price, "job": new_job['title']}


@app.post("/api/selfbuy")
async def self_buy(bg: BackgroundTasks, user: dict = Depends(get_current_user)):
    uid = user['id']
    await collect_income(uid)
    u = dict(await db.fetchrow("SELECT * FROM users WHERE id=$1", uid))
    if not u['owner_id']:
        raise HTTPException(400, "Вы уже свободны")

    price     = float(u['current_price'])
    if float(u['balance']) < price:
        raise HTTPException(400, f"Нужно {price:.0f} RC для самовыкупа")

    fee       = round(price * 0.10, 2)
    payout    = round(price - fee, 2)
    owner_id  = u['owner_id']
    new_price = round(price * (1 + random.uniform(0.10, 0.20)), 2)

    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE users SET balance=balance-$1 WHERE id=$2", price, uid)
            await conn.execute(
                "UPDATE users SET current_price=$1,owner_id=NULL,custom_name=NULL,job_id=NULL,job_assigned_at=NULL WHERE id=$2",
                new_price, uid
            )
            await conn.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", payout, owner_id)

    u_name = f"@{u['username']}" if u['username'] else str(uid)
    bg.add_task(push, owner_id, f"💸 {u_name} выкупил себя на свободу! Вы получили <b>{payout:.0f} RC</b>.")
    return {"ok": True}


@app.get("/api/search")
async def search_players(
    q:            Optional[str]   = None,
    min_price:    Optional[float] = None,
    max_price:    Optional[float] = None,
    random_pick:  bool            = False,
    newcomers:    bool            = False,
    user: dict = Depends(get_current_user)
):
    now = datetime.utcnow()
    base = "SELECT id,username,first_name,current_price,custom_name,vip_level,avatar_frame,name_color,emoji_status FROM users WHERE is_banned=FALSE"
    stealth = " AND (stealth_until IS NULL OR stealth_until<$1)"

    if newcomers:
        rows = await db.fetch(
            base + stealth + " ORDER BY created_at DESC LIMIT 50", now
        )
    elif random_pick:
        bal = float((await db.fetchrow("SELECT balance FROM users WHERE id=$1", user['id']))['balance'])
        rows = await db.fetch(
            base + stealth + " AND id!=$2 AND current_price<=$3 ORDER BY RANDOM() LIMIT 1",
            now, user['id'], bal
        )
    elif q:
        rows = await db.fetch(
            base + " AND (stealth_until IS NULL OR stealth_until<$1) AND (username ILIKE $2 OR first_name ILIKE $2) LIMIT 20",
            now, f"%{q}%"
        )
    else:
        params  = [now]
        conds   = [stealth[5:]]  # strip leading " AND "
        where   = base + stealth
        if min_price is not None:
            params.append(min_price); where += f" AND current_price>=${len(params)}"
        if max_price is not None:
            params.append(max_price); where += f" AND current_price<=${len(params)}"
        where += " ORDER BY created_at DESC LIMIT 50"
        rows = await db.fetch(where, *params)

    return [dict(r) for r in rows]


@app.get("/api/top")
async def get_top(cat: str = "forbes"):
    ck = f"top:{cat}"
    if cached := await rdb.get(ck):
        return json.loads(cached)
    now = datetime.utcnow()
    if cat == "forbes":
        rows = await db.fetch(
            """SELECT id,username,first_name,balance as value,vip_level,name_color,avatar_frame,emoji_status
               FROM users WHERE (stealth_until IS NULL OR stealth_until<$1) AND is_banned=FALSE
               ORDER BY balance DESC LIMIT 100""", now)
    elif cat == "owners":
        rows = await db.fetch(
            """SELECT u.id,u.username,u.first_name,u.vip_level,u.name_color,u.avatar_frame,u.emoji_status,
                      COUNT(s.id) as value
               FROM users u LEFT JOIN users s ON s.owner_id=u.id
               WHERE (u.stealth_until IS NULL OR u.stealth_until<$1) AND u.is_banned=FALSE
               GROUP BY u.id ORDER BY value DESC LIMIT 100""", now)
    else:
        rows = await db.fetch(
            """SELECT id,username,first_name,current_price as value,vip_level,name_color,avatar_frame,emoji_status
               FROM users WHERE (stealth_until IS NULL OR stealth_until<$1) AND is_banned=FALSE
               ORDER BY current_price DESC LIMIT 100""", now)
    result = [dict(r) for r in rows]
    await rdb.setex(ck, 900, json.dumps(result, default=str))
    return result


@app.post("/api/rename")
async def rename_slave(req: RenameReq, user: dict = Depends(get_current_user)):
    s = await db.fetchrow("SELECT owner_id FROM users WHERE id=$1", req.slave_id)
    if not s or s['owner_id'] != user['id']:
        raise HTTPException(403, "Это не ваш раб")
    if len(req.new_name) > 32:
        raise HTTPException(400, "Кличка не более 32 символов")
    await db.execute("UPDATE users SET custom_name=$1 WHERE id=$2", req.new_name.strip(), req.slave_id)
    return {"ok": True}


@app.post("/api/stories/claim")
async def stories_claim(user: dict = Depends(get_current_user)):
    uid  = user['id']
    last = await db.fetchrow(
        "SELECT created_at FROM stories_claims WHERE user_id=$1 AND status='approved' ORDER BY created_at DESC LIMIT 1", uid
    )
    if last and (datetime.utcnow() - last['created_at']).total_seconds() < 86400:
        raise HTTPException(400, "Кулдаун: 24 часа")
    pending = await db.fetchrow("SELECT id FROM stories_claims WHERE user_id=$1 AND status='pending'", uid)
    if pending:
        raise HTTPException(400, "Заявка уже на рассмотрении")
    await db.execute("INSERT INTO stories_claims(user_id) VALUES($1)", uid)
    return {"ok": True, "msg": "Заявка отправлена на проверку администратору"}


@app.post("/api/support")
async def send_support(req: SupportReq, user: dict = Depends(get_current_user)):
    await db.execute(
        "INSERT INTO support_tickets(user_id,message,photo_file_id) VALUES($1,$2,$3)",
        user['id'], req.message, req.photo_file_id
    )
    return {"ok": True}

@app.get("/api/support/history")
async def support_history(user: dict = Depends(get_current_user)):
    rows = await db.fetch(
        "SELECT id,message,is_from_admin,created_at FROM support_tickets WHERE user_id=$1 ORDER BY created_at ASC",
        user['id']
    )
    return [dict(r) for r in rows]


@app.post("/api/promo")
async def use_promo(req: PromoUseReq, user: dict = Depends(get_current_user)):
    uid   = user['id']
    promo = await db.fetchrow("SELECT * FROM promo_codes WHERE code=$1", req.code.upper())
    if not promo: raise HTTPException(404, "Промокод не найден")
    if promo['expires_at'] and promo['expires_at'] < datetime.utcnow():
        raise HTTPException(400, "Промокод истёк")
    if promo['used_count'] >= promo['max_uses']:
        raise HTTPException(400, "Промокод исчерпан")
    used = await db.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND promo_id=$2", uid, promo['id'])
    if used: raise HTTPException(400, "Уже использован")
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE users SET balance=balance+$1 WHERE id=$2", promo['reward_rc'], uid)
            await conn.execute("UPDATE promo_codes SET used_count=used_count+1 WHERE id=$1", promo['id'])
            await conn.execute("INSERT INTO promo_uses(user_id,promo_id) VALUES($1,$2)", uid, promo['id'])
    return {"ok": True, "reward": float(promo['reward_rc'])}


@app.get("/api/shop")
async def get_shop(user: dict = Depends(get_current_user)):
    cosmetics = await db.fetch("SELECT * FROM cosmetics ORDER BY type,id")
    owned_ids = {r['cosmetic_id'] for r in await db.fetch(
        "SELECT cosmetic_id FROM user_cosmetics WHERE user_id=$1", user['id']
    )}
    return {
        "items": [
            {"id":"shield_24",  "name":"🛡 Щит 24ч",     "price_stars":50,  "desc":"Защита от покупки на 24 часа"},
            {"id":"shield_48",  "name":"🛡 Щит 48ч",     "price_stars":90,  "desc":"Защита от покупки на 48 часов"},
            {"id":"chains",     "name":"⛓ Оковы",        "price_stars":30,  "desc":"Выкуп вашего раба другими дороже на 50%"},
            {"id":"boost_15",   "name":"🚀 Буст x1.5",   "price_stars":40,  "desc":"+50% к пассивному доходу на 24ч"},
            {"id":"boost_20",   "name":"🚀 Буст x2.0",   "price_stars":70,  "desc":"+100% к пассивному доходу на 24ч"},
            {"id":"stealth",    "name":"👻 Стелс",        "price_stars":35,  "desc":"Невидим в рейтингах и поиске на 24ч"},
            {"id":"vip_1",      "name":"🥉 Богач",        "price_stars":150, "desc":"Бронзовая рамка, +2% к доходу. 30 дней"},
            {"id":"vip_2",      "name":"🥈 Элита",        "price_stars":300, "desc":"Серебряная рамка, +5% к доходу. 30 дней"},
            {"id":"vip_3",      "name":"👑 Король",       "price_stars":500, "desc":"Золотая корона, налог 8%, редкие работы. 30 дней"},
        ],
        "cosmetics": [{**dict(c), "owned": c['id'] in owned_ids} for c in cosmetics],
        "user_vip": user['vip_level'],
    }


@app.post("/api/shop/buy")
async def shop_buy(req: ShopBuyReq, bg: BackgroundTasks, user: dict = Depends(get_current_user)):
    """
    In production: verify Telegram Stars invoice before calling this.
    Here we trust the client (add Stars invoice verification for real deployment).
    """
    uid = user['id']
    now = datetime.utcnow()
    t   = req.item_type

    if t == "shield_24":
        await db.execute("UPDATE users SET shield_until=$1 WHERE id=$2", now+timedelta(hours=24), uid)
    elif t == "shield_48":
        await db.execute("UPDATE users SET shield_until=$1 WHERE id=$2", now+timedelta(hours=48), uid)
    elif t == "boost_15":
        await db.execute("UPDATE users SET booster_mult=1.5,booster_until=$1 WHERE id=$2", now+timedelta(hours=24), uid)
    elif t == "boost_20":
        await db.execute("UPDATE users SET booster_mult=2.0,booster_until=$1 WHERE id=$2", now+timedelta(hours=24), uid)
    elif t == "stealth":
        await db.execute("UPDATE users SET stealth_until=$1 WHERE id=$2", now+timedelta(hours=24), uid)
    elif t == "chains":
        if not req.target_slave_id:
            raise HTTPException(400, "Укажите раба")
        s = await db.fetchrow("SELECT owner_id FROM users WHERE id=$1", req.target_slave_id)
        if not s or s['owner_id'] != uid:
            raise HTTPException(403, "Это не ваш раб")
        await db.execute("UPDATE users SET chains_until=$1 WHERE id=$2", now+timedelta(days=7), req.target_slave_id)
        bg.add_task(push, req.target_slave_id, "🔒 На вас повесили Оковы! Ваш выкуп стал на 50% дороже.")
    elif t in ("vip_1","vip_2","vip_3"):
        lvl = int(t[-1])
        await db.execute("UPDATE users SET vip_level=$1,vip_until=$2 WHERE id=$3", lvl, now+timedelta(days=30), uid)
    elif t == "cosmetic":
        if not req.cosmetic_id:
            raise HTTPException(400, "Укажите cosmetic_id")
        c = await db.fetchrow("SELECT * FROM cosmetics WHERE id=$1", req.cosmetic_id)
        if not c: raise HTTPException(404, "Косметика не найдена")
        await db.execute(
            "INSERT INTO user_cosmetics(user_id,cosmetic_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
            uid, req.cosmetic_id
        )
        if c['type'] == 'color':
            await db.execute("UPDATE users SET name_color=$1 WHERE id=$2", c['value'], uid)
        elif c['type'] == 'frame':
            await db.execute("UPDATE users SET avatar_frame=$1 WHERE id=$2", c['value'], uid)
        elif c['type'] == 'emoji':
            await db.execute("UPDATE users SET emoji_status=$1 WHERE id=$2", c['value'], uid)
    else:
        raise HTTPException(400, "Неизвестный товар")

    return {"ok": True}

# ─── ADMIN ────────────────────────────────────────────────────────────────────

@app.get("/api/admin/dashboard")
async def admin_dashboard(_: dict = Depends(get_admin_user)):
    stats = await db.fetchrow(
        """SELECT COUNT(*)                                                    AS total_users,
                  COUNT(CASE WHEN created_at>NOW()-INTERVAL '1 day' THEN 1 END) AS new_today,
                  COALESCE(SUM(balance),0)                                   AS total_rc,
                  (SELECT COUNT(*) FROM support_tickets WHERE is_from_admin=FALSE)::INT AS tickets,
                  (SELECT COUNT(*) FROM stories_claims WHERE status='pending')::INT     AS stories_pending
           FROM users WHERE is_banned=FALSE"""
    )
    return dict(stats)


@app.get("/api/admin/users")
async def admin_users(q: Optional[str] = None, _: dict = Depends(get_admin_user)):
    if q:
        rows = await db.fetch(
            "SELECT * FROM users WHERE username ILIKE $1 OR first_name ILIKE $1 OR id::text=$2 LIMIT 100",
            f"%{q}%", q
        )
    else:
        rows = await db.fetch("SELECT * FROM users ORDER BY created_at DESC LIMIT 100")
    return [dict(r) for r in rows]


@app.post("/api/admin/users/edit")
async def admin_edit(req: AdminEditReq, _: dict = Depends(get_admin_user)):
    parts, vals = [], []
    def add(col, v):
        vals.append(v); parts.append(f"{col}=${len(vals)}")

    if req.balance    is not None: add("balance",      req.balance)
    if req.price      is not None: add("current_price",req.price)
    if req.is_banned  is not None: add("is_banned",    req.is_banned)
    if req.custom_name is not None:
        add("custom_name", req.custom_name or None)
    if req.free_slave:
        parts += ["owner_id=NULL","custom_name=NULL","job_id=NULL","job_assigned_at=NULL"]

    if parts:
        vals.append(req.user_id)
        await db.execute(f"UPDATE users SET {','.join(parts)} WHERE id=${len(vals)}", *vals)
    return {"ok": True}


@app.get("/api/admin/tickets")
async def admin_tickets(_: dict = Depends(get_admin_user)):
    rows = await db.fetch(
        """SELECT st.*,u.username,u.first_name FROM support_tickets st
           JOIN users u ON u.id=st.user_id ORDER BY st.created_at DESC LIMIT 500"""
    )
    return [dict(r) for r in rows]


@app.post("/api/admin/tickets/reply")
async def admin_reply(req: TicketReplyReq, bg: BackgroundTasks, _: dict = Depends(get_admin_user)):
    await db.execute(
        "INSERT INTO support_tickets(user_id,message,is_from_admin) VALUES($1,$2,TRUE)",
        req.ticket_user_id, req.message
    )
    bg.add_task(push, req.ticket_user_id, f"📨 Ответ поддержки: {req.message}")
    return {"ok": True}


@app.get("/api/admin/stories")
async def admin_stories(_: dict = Depends(get_admin_user)):
    rows = await db.fetch(
        """SELECT sc.*,u.username,u.first_name FROM stories_claims sc
           JOIN users u ON u.id=sc.user_id WHERE sc.status='pending' ORDER BY sc.created_at"""
    )
    return [dict(r) for r in rows]


@app.post("/api/admin/stories/approve")
async def admin_approve_story(claim_id: int, bg: BackgroundTasks, _: dict = Depends(get_admin_user)):
    claim = await db.fetchrow("SELECT * FROM stories_claims WHERE id=$1", claim_id)
    if not claim: raise HTTPException(404)
    items = ["🛡 Щит", "⛓ Оковы", "🚀 Бустер", "👻 Стелс"]
    item  = random.choice(items)
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE stories_claims SET status='approved' WHERE id=$1", claim_id)
            await conn.execute("UPDATE users SET balance=balance+250 WHERE id=$1", claim['user_id'])
    bg.add_task(push, claim['user_id'], f"🎁 Ваша Story одобрена! +250 RC и предмет: {item}.")
    return {"ok": True, "item": item}


@app.post("/api/admin/broadcast")
async def admin_broadcast(req: BroadcastReq, _: dict = Depends(get_admin_user)):
    users = await db.fetch("SELECT id FROM users WHERE is_banned=FALSE")
    ids   = [u['id'] for u in users]
    asyncio.create_task(_broadcast(req.text, ids))
    return {"ok": True, "count": len(ids)}


async def _broadcast(text: str, ids: list):
    for i in range(0, len(ids), 25):
        for uid in ids[i:i+25]:
            await push(uid, text)
        await asyncio.sleep(1)


@app.post("/api/admin/season/start")
async def season_start(req: SeasonReq, _: dict = Depends(get_admin_user)):
    if req.password != SEASON_PASS:
        raise HTTPException(403, "Неверный пароль")
    users = await db.fetch("SELECT * FROM users")
    snap  = [dict(u) for u in users]
    await db.execute(
        "INSERT INTO season_snapshots(snapshot_data) VALUES($1)",
        json.dumps(snap, default=str)
    )
    await db.execute(
        """UPDATE users SET balance=50,current_price=100,owner_id=NULL,custom_name=NULL,
           job_id=NULL,job_assigned_at=NULL,shield_until=NULL,chains_until=NULL,
           booster_mult=1.0,booster_until=NULL,stealth_until=NULL"""
    )
    # Kosmetika и VIP остаются
    asyncio.create_task(_broadcast(
        "🔥 <b>Новый сезон начался!</b> Все на равных. Покажи, кто здесь настоящий король!",
        [u['id'] for u in users]
    ))
    return {"ok": True, "snapshotted": len(snap)}


@app.post("/api/admin/season/rollback")
async def season_rollback(req: SeasonReq, _: dict = Depends(get_admin_user)):
    if req.password != SEASON_PASS:
        raise HTTPException(403, "Неверный пароль")
    snap = await db.fetchrow("SELECT * FROM season_snapshots ORDER BY created_at DESC LIMIT 1")
    if not snap: raise HTTPException(404, "Снимок не найден")
    data = json.loads(snap['snapshot_data'])
    async with db.acquire() as conn:
        async with conn.transaction():
            for u in data:
                await conn.execute(
                    """UPDATE users SET balance=$1,current_price=$2,owner_id=$3,custom_name=$4,job_id=$5
                       WHERE id=$6""",
                    u['balance'], u['current_price'], u['owner_id'], u['custom_name'], u['job_id'], u['id']
                )
    return {"ok": True}


@app.post("/api/admin/promo/create")
async def admin_create_promo(req: PromoCreateReq, _: dict = Depends(get_admin_user)):
    await db.execute(
        "INSERT INTO promo_codes(code,reward_rc,max_uses) VALUES($1,$2,$3)",
        req.code.upper(), req.reward_rc, req.max_uses
    )
    return {"ok": True}


@app.post("/api/admin/sponsor/add")
async def admin_add_sponsor(req: SponsorReq, _: dict = Depends(get_admin_user)):
    await db.execute(
        "INSERT INTO sponsors(channel_id,channel_title,reward_rc) VALUES($1,$2,$3) ON CONFLICT(channel_id) DO UPDATE SET channel_title=$2,reward_rc=$3,is_active=TRUE",
        req.channel_id, req.channel_title, req.reward_rc
    )
    return {"ok": True}

# ─── BOT HANDLERS ─────────────────────────────────────────────────────────────

def _register_bot_handlers():
    @dp.message(CommandStart())
    async def cmd_start(msg: types.Message):
        uid    = msg.from_user.id
        ref_id = None
        if msg.text and "ref_" in msg.text:
            try: ref_id = int(msg.text.split("ref_")[1])
            except (ValueError, IndexError): pass

        existing = await db.fetchrow("SELECT id FROM users WHERE id=$1", uid)
        if not existing:
            owner_id = ref_id if ref_id and ref_id != uid else None
            jobs = await get_jobs_list()
            job  = pick_job(0, jobs) if owner_id else None
            now  = datetime.utcnow()
            await db.execute(
                """INSERT INTO users(id,username,first_name,balance,current_price,owner_id,job_id,job_assigned_at)
                   VALUES($1,$2,$3,50,100,$4,$5,$6) ON CONFLICT DO NOTHING""",
                uid, msg.from_user.username, msg.from_user.first_name,
                owner_id, job['id'] if job else None, now if job else None
            )
            if owner_id:
                asyncio.create_task(push(owner_id,
                    f"🎣 По вашей ссылке перешёл @{msg.from_user.username or uid}! Новый раб!"))

        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="⛓ Открыть Рабство", web_app=WebAppInfo(url=WEBAPP_URL))
        ]])
        await msg.answer(
            "⛓ <b>РАБСТВО</b>\n\n"
            "Социальная экономическая стратегия внутри Telegram.\n"
            "Покупай людей → назначай работу → собирай доход.\n\n"
            "💰 Стартовый баланс: <b>50 RC</b>\n"
            "📈 Ваша стоимость: <b>100 RC</b>\n\n"
            "Нажмите кнопку, чтобы начать игру:",
            parse_mode="HTML", reply_markup=kb
        )

# ─── BACKGROUND ───────────────────────────────────────────────────────────────

async def _income_cron():
    """Collect passive income for all owners every 30 min."""
    while True:
        await asyncio.sleep(1800)
        try:
            owners = await db.fetch(
                "SELECT DISTINCT owner_id FROM users WHERE owner_id IS NOT NULL AND job_id IS NOT NULL"
            )
            for row in owners:
                try: await collect_income(row['owner_id'])
                except Exception: pass
        except Exception as e:
            print(f"[cron] error: {e}")

async def _bot_polling():
    try:
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        print(f"[bot] error: {e}")

# ─── STATIC ───────────────────────────────────────────────────────────────────

@app.get("/")
async def serve():
    return FileResponse("index.html")

# ─── ENTRY ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))   # Railway задаёт PORT сам
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False, workers=1)
