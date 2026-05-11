import os, asyncio, random
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select, update, Integer, String, Float, ForeignKey, desc, func

# --- КОНФИГ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://slave222-production.up.railway.app")
ADMIN_IDS = [int(i) for i in os.getenv("ADMIN_IDS", "123456789").split(",")]
DB_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///game.db").replace("postgres://", "postgresql+asyncpg://")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()
engine = create_async_engine(DB_URL)
async_session = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase): pass
class User(Base):
    __tablename__ = "users"
    id = mapped_column(Integer, primary_key=True)
    username = mapped_column(String)
    balance = mapped_column(Float, default=50.0)
    price = mapped_column(Float, default=100.0)
    owner_id = mapped_column(Integer, ForeignKey("users.id"), nullable=True)
    job = mapped_column(String, default="Безработный")

# --- ЛОГИКА ---
def get_job():
    r = random.random()
    if r < 0.7: return "Подметать полы"
    if r < 0.95: return "Майнить крипту"
    return "Тапать хомяка"

@dp.message(CommandStart())
async def start(msg):
    async with async_session() as s:
        u = await s.get(User, msg.from_user.id)
        if not u:
            s.add(User(id=msg.from_user.id, username=msg.from_user.username or "Anon"))
            await s.commit()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎮 Открыть игру", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await msg.answer("Добро пожаловать в «Рабство»!", reply_markup=kb)

# --- API ---
@app.get("/")
async def root(): return FileResponse("index.html")

@app.get("/api/me")
async def get_me(user_id: int):
    async with async_session() as s:
        u = await s.get(User, user_id)
        if not u: raise HTTPException(404)
        slaves = (await s.execute(select(User).where(User.owner_id == u.id))).scalars().all()
        return {"user": {"username": u.username, "balance": u.balance, "price": u.price, "job": u.job}, "slaves": [{"username": s.username, "price": s.price} for s in slaves]}

@app.post("/api/buy")
async def buy(data: dict):
    async with async_session() as s:
        buyer = await s.get(User, data['buyer_id'])
        target = await s.get(User, data['target_id'])
        if buyer.balance < target.price: return {"error": "Мало денег"}
        buyer.balance -= target.price
        target.owner_id = buyer.id
        target.price *= 1.15
        target.job = get_job()
        await s.commit()
        return {"success": True}

@app.post("/api/admin/reset")
async def reset(data: dict):
    if data['pass'] != "Niva01102007": raise HTTPException(403)
    async with async_session() as s:
        await s.execute(update(User).values(balance=50.0, price=100.0, owner_id=None))
        await s.commit()
    return {"status": "ok"}

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn: await conn.run_sync(Base.metadata.create_all)
    asyncio.create_task(dp.start_polling(bot))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
