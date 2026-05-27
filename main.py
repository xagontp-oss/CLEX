import asyncio
import aiosqlite
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from aiogram.enums import ParseMode

from fastapi import FastAPI, Request, HTTPException
import uvicorn
from contextlib import asynccontextmanager

# ================= ENV =================
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DB_PATH = "monitored.db"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change_this")

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ================= BOT INIT =================
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# ================= FUN ASCII BANNER =================
BANNER = """
…………………▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
……………▄▄█▓▓▓▒▒▒▒▒▒▒▒▒▒▓▓▓▓█▄▄
…………▄▀▀▓▒░░░░░░░░░░░░░░░░▒▓▓▀▄
………▄▀▓▒▒░░░░░░░░░░░░░░░░░░░▒▒▓▀▄
……..█▓█▒░░░░░░░░░░░░░░░░░░░░░▒▓▒▓█
…..▌▓▀▒░░░░░░░░░░░░░░░░░░░░░░░░▒▀▓█
…..█▌▓▒▒░░░░░░░░░░░░░░░░░░░░░░░░░▒▓█
…▐█▓▒░░░░░░░░░░░░░░░░░░░░░░░░░░░▒▓█▌
…█▓▒▒░░░░░░░░░░░░░░░░░░░░░░░░░░░░▒▓█
..█▐▒▒░░░░░░░░░░░░░░░░░░░░░░░░░░░▒▒█▓█
…█▓█▒░░░░░░░░░░░░░░░░░░░░░░░░░░░▒█▌▓█
..█▓▓█▒░░░░▒█▄▒▒░░░░░░░░░▒▒▄█▒░░░░▒█▓▓█
..█▓█▒░▒▒▒▒░░▀▀█▄▄░░░░░▄▄█▀▀░░▒▒▒▒░▒█▓█
.█▓▌▒▒▓▓▓▓▄▄▄▒▒▒▀█░░░░█▀▒▒▒▄▄▄▓▓▓▓▒▒▐▓█
.██▌▒▓███▓█████▓▒▐▌░░▐▌▒▓████▓████▓▒▐██
..██▒▒▓███▓▓▓████▓▄░░░▄▓████▓▓▓███▓▒▒██
..█▓▒▒▓██████████▓▒░░░▒▓██████████▓▒▒▓█
..█▓▒░▒▓███████▓▓▄▀░░▀▄▓▓███████▓▒░▒▓█
….█▓▒░▒▒▓▓▓▓▄▄▄▀▒░░░░░▒▀▄▄▄▓▓▓▓▒▒░▓█
……█▓▒░▒▒▒▒░░░░░░▒▒▒▒░░░░░░▒▒▒▒░▒▓█
………█▓▓▒▒▒░░██░░▒▓██▓▒░░██░░▒▒▒▓▓█
………▀██▓▓▓▒░░▀░▒▓████▓▒░▀░░▒▓▓▓██▀
………….░▀█▓▒▒░░░▓█▓▒▒▓█▓▒░░▒▒▓█▀░
…………█░░██▓▓▒░░▒▒▒░▒▒▒░░▒▓▓██░░█
………….█▄░░▀█▓▒░░░░░░░░░░▒▓█▀░░▄█
…………..█▓█░░█▓▒▒▒░░░░░▒▒▒▓█░░█▓█
…………….█▓█░░█▀█▓▓▓▓▓▓█▀░░█░█▓█▌
……………..█▓▓█░█░█░█▀▀▀█░█░▄▀░█▓█
……………..█▓▓█░░▀█▀█░█░█▄█▀░░█▓▓█
………………█▓▒▓█░░░░▀▀▀▀░░░░░█▓▓█
………………█▓▒▒▓█░░░░ ░░░░░░░█▓▓█
………………..█▓▒▓██▄█░░░▄░░▄██▓▒▓█
………………..█▓▒▒▓█▒█▀█▄█▀█▒█▓▒▓█
………………..█▓▓▒▒▓██▒▒██▒██▓▒▒▓█
………………….█▓▓▒▒▓▀▀███▀▀▒▒▓▓█
……………………▀█▓▓▓▓▒▒▒▒▓▓▓▓█▀
………………………..▀▀██▓▓▓▓██▀

"""

# ================= MENU =================
menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="➕ Add Wallet"), KeyboardButton(text="📋 My Wallets")],
        [KeyboardButton(text="❌ Delete Wallet"), KeyboardButton(text="ℹ️ Help")]
    ],
    resize_keyboard=True
)

# ================= DATABASE =================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS addresses (
                user_id INTEGER,
                solana_address TEXT,
                custom_message TEXT DEFAULT 'New buy detected!',
                PRIMARY KEY (user_id, solana_address)
            )
        """)
        await db.commit()

async def add_address(user_id: int, address: str, msg: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO addresses VALUES (?, ?, ?)",
            (user_id, address, msg)
        )
        await db.commit()

async def delete_address(user_id: int, address: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM addresses WHERE user_id=? AND solana_address=?",
            (user_id, address)
        )
        await db.commit()

async def get_all():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, solana_address, custom_message FROM addresses")
        return await cur.fetchall()

# ================= IMPROVED SWAP DETECTION =================
def extract_buy_info(tx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        transfers = tx.get("tokenTransfers", [])
        signature = tx.get("signature")

        if not transfers:
            return None

        received = []

        for t in transfers:
            try:
                amt = float(t.get("tokenAmount") or 0)
            except:
                continue

            if amt > 0 and t.get("toUserAccount"):
                received.append((t.get("mint"), amt))

        if not received:
            return None

        # biggest received token = bought coin
        mint, amt = max(received, key=lambda x: x[1])

        return {
            "ca": mint,
            "amount": amt,
            "signature": signature,
            "timestamp": tx.get("timestamp", int(datetime.now().timestamp()))
        }

    except Exception as e:
        logger.error(f"extract error: {e}")
        return None

# ================= HUMAN ALERT FORMAT =================
async def send_alert(user_id, wallet, buy, custom_msg):
    text = f"""
{BANNER}

🚨 NEW PUMP.FUN BUY DETECTED 🚨

👛 Wallet:
{wallet}

🪙 Token CA:
{buy['ca']}

💰 Amount Bought:
{buy['amount']}

🧠 Info:
{custom_msg}

🔗 Tx:
https://solscan.io/tx/{buy['signature']}

━━━━━━━━━━━━━━━━━━━━
"""

    await bot.send_message(
        user_id,
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

# ================= PROCESSOR =================
async def process_payload(payload: List[Dict[str, Any]]):
    if not isinstance(payload, list):
        return

    monitored = await get_all()
    address_map = {row[1]: (row[0], row[2]) for row in monitored}

    for tx in payload:
        wallet = tx.get("feePayer")

        if wallet not in address_map:
            continue

        buy = extract_buy_info(tx)

        if not buy:
            continue

        user_id, custom_msg = address_map[wallet]

        await send_alert(user_id, wallet, buy, custom_msg)

# ================= TELEGRAM UI =================
@router.message(Command("start"))
async def start(m: Message):
    await m.answer(BANNER + "\nGreetings, and welcome to the CLEX wallet tracker 🚀", reply_markup=menu)

@router.message(lambda m: m.text == "➕ Add Wallet")
async def add_ui(m: Message):
    await m.answer("Send wallet address to monitor 👇")

@router.message(lambda m: m.text == "📋 My Wallets")
async def list_ui(m: Message):
    data = await get_all()
    user = [d for d in data if d[0] == m.from_user.id]

    if not user:
        return await m.answer("No wallets tracked.")

    text = "📊 Your Monitored Wallets:\n\n"
    for u in user:
        text += f"👛 {u[1]}\n🧠 {u[2]}\n\n"

    await m.answer(text)

@router.message(lambda m: m.text == "❌ Delete Wallet")
async def delete_ui(m: Message):
    await m.answer("Send wallet address to delete ❌")

@router.message(lambda m: m.text and len(m.text) > 30)
async def handle_wallet_input(m: Message):
    # auto add/delete simplified UX
    await add_address(m.from_user.id, m.text, "Pump.fun tracking active 🔥")
    await m.answer("Wallet added to tracker 🚀", reply_markup=menu)

# ================= FASTAPI =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    asyncio.create_task(dp.start_polling(bot))
    logger.info("Pump.fun bot running")
    yield
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(req: Request):
    if req.headers.get("x-helius-secret") != WEBHOOK_SECRET:
        raise HTTPException(403)

    payload = await req.json()
    asyncio.create_task(process_payload(payload))

    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "pump.fun bot alive"}

# ================= RUN =================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
