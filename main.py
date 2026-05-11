import asyncio
import random
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select, update, Integer, String, Float, Boolean, ForeignKey

# ================= КОНФИГ =================
BOT_TOKEN = "8606742629:AAE0l0AylYWg7Rjgk167GBkchFjBEtf2JFw"
WEBAPP_URL = "https://slave222-production.up.railway.app" # Замени на свой URL
ADMIN_IDS = [123456789] # Замени на свой Telegram ID

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ================= БАЗА ДАННЫХ =================
engine = create_async_engine("sqlite+aiosqlite:///game.db", echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase): pass

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True) # TG ID
    username: Mapped[str] = mapped_column(String, nullable=True)
    balance: Mapped[float] = mapped_column(Float, default=50.0)
    price: Mapped[float] = mapped_column(Float, default=100.0)
    owner_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=True)
    custom_name: Mapped[str] = mapped_column(String, nullable=True)
    job_title: Mapped[str] = mapped_column(String, nullable=True)
    vip_status: Mapped[int] = mapped_column(Integer, default=0) # 0-none, 1-bronze, 2-silver, 3-gold

class SeasonSnapshot(Base):
    __tablename__ = "season_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(Integer)
    balance: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)

# ================= AIOGRAM БОТ =================
@dp.message(CommandStart())
async def start_cmd(message: types.Message):
    args = message.text.split()
    ref_id = int(args[1].replace("ref_", "")) if len(args) > 1 and "ref_" in args[1] else None

    async with async_session() as session:
        user = await session.get(User, message.from_user.id)
        if not user:
            user = User(id=message.from_user.id, username=message.from_user.username)
            if ref_id and ref_id != message.from_user.id:
                user.owner_id = ref_id
                # Логика выдачи случайной работы при захвате
                user.job_title = assign_job()
                await bot.send_message(ref_id, f"🎣 По ссылке перешел новичок @{user.username}! Теперь он ваш раб.")
            session.add(user)
            await session.commit()

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🎮 Играть", web_app=types.WebAppInfo(url=WEBAPP_URL))]
    ])
    await message.answer("Добро пожаловать в «Рабство»! Нажми кнопку ниже, чтобы войти в игру.", reply_markup=kb)

# Логика выпадения работ
def assign_job(is_vip=False):
    rand = random.randint(1, 100)
    if is_vip and rand <= 5:
        return random.choice(["Тапать хомяка", "Просить милостыню"]) # 5%
    elif rand <= 30: # 25% + 5%
        return random.choice(["Майнить крипту", "Петь на улице"])
    return random.choice(["Подметать полы", "Раздавать листовки"]) # 70%

# ================= FASTAPI (API & WEBAPP) =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # Фоновая задача: начисление пассивного дохода (раз в 60 сек для теста)
    asyncio.create_task(bot_polling())
    yield

async def bot_polling():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

app = FastAPI(lifespan=lifespan)

# Эндпоинт для отдачи HTML фронтенда
@app.get("/")
async def get_index():
    return FileResponse("index.html")

# Схемы API
class BuyRequest(BaseModel):
    buyer_id: int
    target_id: int

class AdminSeasonRequest(BaseModel):
    admin_id: int
    password: str

@app.get("/api/me")
async def get_profile(user_id: int):
    async with async_session() as session:
        user = await session.get(User, user_id)
        if not user: return {"error": "User not found"}
        
        # Получаем рабов пользователя
        result = await session.execute(select(User).where(User.owner_id == user_id))
        slaves = result.scalars().all()
        
        # Узнаем владельца, если есть
        owner_name = None
        if user.owner_id:
            owner = await session.get(User, user.owner_id)
            owner_name = owner.username if owner else "Неизвестный"

        return {
            "id": user.id, "username": user.username, "balance": user.balance, 
            "price": user.price, "owner_name": owner_name, "is_admin": user.id in ADMIN_IDS,
            "slaves": [{"id": s.id, "username": s.custom_name or s.username, "price": s.price, "job": s.job_title} for s in slaves]
        }

@app.post("/api/buy")
async def buy_slave(req: BuyRequest):
    async with async_session() as session:
        buyer = await session.get(User, req.buyer_id)
        target = await session.get(User, req.target_id)
        
        if not buyer or not target: raise HTTPException(400, "User not found")
        if buyer.balance < target.price: raise HTTPException(400, "Недостаточно средств")
        if target.owner_id == buyer.id: raise HTTPException(400, "Уже ваш раб")

        pay_amount = target.price
        tax = pay_amount * 0.10 # 10% казне
        net_profit = pay_amount - tax

        buyer.balance -= pay_amount
        
        old_owner_id = target.owner_id
        if old_owner_id:
            old_owner = await session.get(User, old_owner_id)
            if old_owner: old_owner.balance += net_profit
        else:
            target.balance += net_profit # Если свободен, деньги идут ему
            
        target.owner_id = buyer.id
        target.price *= (1 + random.uniform(0.10, 0.20)) # Инфляция 10-20%
        target.custom_name = None # Сброс клички
        target.job_title = assign_job(is_vip=buyer.vip_status == 3)

        await session.commit()
        
        # Уведомления
        await bot.send_message(target.id, f"⛓ Вас купил @{buyer.username}! Назначена работа: {target.job_title}.")
        if old_owner_id:
            await bot.send_message(old_owner_id, f"💰 Вашего раба перекупил @{buyer.username}! Прибыль: {net_profit:.2f} RC.")
        
        return {"success": True, "new_balance": buyer.balance}

@app.post("/api/admin/season_reset")
async def season_reset(req: AdminSeasonRequest):
    if req.admin_id not in ADMIN_IDS: raise HTTPException(403, "Not admin")
    if req.password != "Niva01102007": raise HTTPException(403, "Wrong password")

    async with async_session() as session:
        # 1. Snapshot
        users = (await session.execute(select(User))).scalars().all()
        await session.execute(SeasonSnapshot.__table__.delete()) # Очистка старого бекапа
        for u in users:
            session.add(SeasonSnapshot(user_id=u.id, balance=u.balance, price=u.price))
            
        # 2. Reset
        await session.execute(update(User).values(
            balance=50.0, price=100.0, owner_id=None, custom_name=None, job_title=None
        ))
        await session.commit()
        
        # Рассылка (Упрощенно)
        for u in users:
            try: await bot.send_message(u.id, "🔥 Начался новый сезон! Все на равных.")
            except: pass
            
        return {"success": True, "message": "Сезон сброшен!"}

# Инициализация статики для фронтенда (если понадобятся картинки, но пока всё в index.html)
app.mount("/static", StaticFiles(directory=".", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    # Запуск: uvicorn main:app --reload
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
