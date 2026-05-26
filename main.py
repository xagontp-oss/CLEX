# main.py - Enhanced Production Solana Buy Alert Bot
# Same features, improved speed + accuracy + stability

import asyncio
import aiosqlite
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message
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

# ================= FAST SWAP EXTRACTION =================
def extract_buy_info(tx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Fast + accurate Helius swap extractor
    Focus: tokenTransfers only (fastest reliable signal)
    """

    try:
        transfers = tx.get("tokenTransfers", [])
        signature = tx.get("signature")

        if not transfers:
            return None

        # fastest path: filter valid received tokens
        best_mint = None
        best_amount = 0

        for t in transfers:
            mint = t.get("mint")
            amt = t.get("tokenAmount")

            if not mint or not amt:
                continue

            # ignore tiny dust transfers
            if amt <= 0:
                continue

            # pick largest received token = actual buy
            if amt > best_amount:
                best_amount = amt
                best_mint = mint

        if not best_mint:
            return None

        return {
            "ca": best_mint,
            "amount": best_amount,
            "signature": signature,
            "timestamp": tx.get("timestamp", int(datetime.now().timestamp()))
        }

    except Exception as e:
        logger.error(f"extract error: {e}")
        return None

# ================= FAST WEBHOOK PROCESSOR =================
async def process_payload(payload: List[Dict[str, Any]]):
    """
    Optimized:
    - no DB queries inside loop
    - preloaded map once per request
    """

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

        text = (
            f"🔔 BUY DETECTED\n\n"
            f"Wallet: {wallet}\n"
            f"CA: {buy['ca']}\n"
            f"Amount: {buy['amount']}\n"
            f"{custom_msg}\n\n"
            f"https://solscan.io/tx/{buy['signature']}"
        )

        try:
            await bot.send_message(
                user_id,
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"Telegram send error: {e}")

# ================= TELEGRAM COMMANDS =================
@router.message(Command("start"))
async def start(m: Message):
    await m.answer("Bot running.\nUse /add /delete /list")

@router.message(Command("add"))
async def add(m: Message):
    parts = m.text.split(maxsplit=2)
    if len(parts) < 2:
        return await m.answer("Usage: /add <address>")

    await add_address(m.from_user.id, parts[1], parts[2] if len(parts) > 2 else "New buy detected!")
    await m.answer("Added.")

@router.message(Command("delete"))
async def delete(m: Message):
    parts = m.text.split()
    if len(parts) != 2:
        return await m.answer("Usage: /delete <address>")

    await delete_address(m.from_user.id, parts[1])
    await m.answer("Deleted.")

@router.message(Command("list"))
async def list_cmd(m: Message):
    data = await get_all()
    user = [d for d in data if d[0] == m.from_user.id]

    if not user:
        return await m.answer("No addresses.")

    text = "\n".join([f"{x[1]} | {x[2]}" for x in user])
    await m.answer(text)

# ================= FASTAPI =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    asyncio.create_task(dp.start_polling(bot))
    logger.info("Bot running")
    yield
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(req: Request):
    if req.headers.get("x-helius-secret") != WEBHOOK_SECRET:
        raise HTTPException(403)

    payload = await req.json()

    # fire-and-forget (fast response to Helius)
    asyncio.create_task(process_payload(payload))

    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "ok"}

# ================= RUN =================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)