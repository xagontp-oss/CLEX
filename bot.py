import asyncio
import aiosqlite
import aiohttp
import logging
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError

from fastapi import FastAPI, Request, HTTPException
import uvicorn
from contextlib import asynccontextmanager

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DB_PATH = "monitored.db"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change_this")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

user_states: Dict[int, Dict[str, Any]] = {}
token_cache: Dict[str, Dict[str, Any]] = {}
cache_time: Dict[str, float] = {}

PUMP_FUN_PROGRAM = "6EF8rQNi1oDEZ7zrKsCauKMorruBaGECQw6B469Z7z8"
PUMP_FUN_BOUND_ACCOUNT = "SoLari2oNaLqgxGX2VT6Gau6d6f6vgYdvM3jdQcQjhS"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                user_id INTEGER, solana_address TEXT, custom_msg TEXT DEFAULT 'Pump.fun buy! 🚀',
                PRIMARY KEY (user_id, solana_address)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                user_id INTEGER, wallet TEXT, token_ca TEXT, amount REAL,
                signature TEXT UNIQUE, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def add_wallet(user_id: int, address: str, msg: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO wallets VALUES (?, ?, ?)", (user_id, address, msg))
        await db.commit()

async def del_wallet(user_id: int, address: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM wallets WHERE user_id=? AND solana_address=?", (user_id, address))
        await db.commit()

async def get_wallets(user_id: int) -> List[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT solana_address, custom_msg FROM wallets WHERE user_id=?", (user_id,))
        return await cur.fetchall()

async def get_all_wallets() -> List[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, solana_address, custom_msg FROM wallets")
        return await cur.fetchall()

async def log_alert(user_id: int, wallet: str, ca: str, amount: float, sig: str):
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute("INSERT INTO alerts VALUES (?, ?, ?, ?, ?, ?)", 
                           (user_id, wallet, ca, amount, sig, datetime.now().isoformat()))
            await db.commit()
        except:
            pass

async def is_duplicate(sig: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM alerts WHERE signature=? LIMIT 1", (sig,))
        return await cur.fetchone() is not None

async def get_token_metadata(mint: str) -> Optional[Dict]:
    if mint in token_cache and time.time() - cache_time.get(mint, 0) < 3600:
        return token_cache[mint]
    
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}",
                json={"jsonrpc":"2.0","id":"1","method":"getAsset","params":{"id":mint}},
                timeout=aiohttp.ClientTimeout(total=3)) as r:
                if r.status == 200:
                    data = await r.json()
                    if "result" in data and data["result"]:
                        meta = data["result"].get("content",{}).get("metadata",{})
                        info = {"name": meta.get("name","Unknown"), "symbol": meta.get("symbol","??")}
                        token_cache[mint] = info
                        cache_time[mint] = time.time()
                        return info
    except:
        pass
    return None

def extract_buy(tx: Dict) -> Optional[Dict]:
    try:
        sig = tx.get("signature")
        payer = tx.get("feePayer")
        if not sig or not payer:
            return None

        transfers = tx.get("tokenTransfers", [])
        received = []
        
        for t in transfers:
            if t.get("tokenStandard") == "NonFungible":
                continue
            if t.get("mint") == "So11111111111111111111111111111111111111112":
                continue
            
            try:
                amt = float(t.get("tokenAmount") or 0)
                if amt > 0 and t.get("toUserAccount") == payer:
                    decimals = int(t.get("decimals", 6))
                    received.append((t.get("mint"), amt / (10 ** decimals)))
            except:
                continue
        
        if not received:
            return None

        ca, amount = max(received, key=lambda x: x[1])
        return {"ca": ca, "amount": amount, "sig": sig, "ts": tx.get("timestamp", int(time.time()))}
    except:
        return None

async def send_alert(uid: int, wallet: str, buy: Dict, msg: str):
    meta = await get_token_metadata(buy["ca"])
    sym = meta["symbol"] if meta else "?"
    name = meta["name"] if meta else "Token"
    
    text = f"""
🚀 **PUMP.FUN BUY**

🪙 **{sym}** - {name}

💼 **CA:** `{buy['ca']}`
💰 **Amount:** {buy['amount']:.2f} {sym}
💬 {msg}

🔗 [Solscan](https://solscan.io/tx/{buy['sig']})
"""
    
    try:
        await bot.send_message(uid, text, parse_mode=ParseMode.MARKDOWN)
    except TelegramAPIError as e:
        logger.error(f"Send error to {uid}: {e}")

async def process_payload(payload: List[Dict]):
    if not isinstance(payload, list):
        return
    
    wallets = await get_all_wallets()
    wmap = {w[1]: (w[0], w[2]) for w in wallets}
    
    for tx in payload:
        payer = tx.get("feePayer")
        if payer not in wmap:
            continue
        
        buy = extract_buy(tx)
        if not buy or await is_duplicate(buy["sig"]):
            continue
        
        uid, msg = wmap[payer]
        await send_alert(uid, payer, buy, msg)
        await log_alert(uid, payer, buy["ca"], buy["amount"], buy["sig"])

@router.message(Command("start"))
async def start(m: Message):
    user_states[m.from_user.id] = {"state": "menu"}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add", callback_data="add"), 
         InlineKeyboardButton(text="📋 List", callback_data="list")],
        [InlineKeyboardButton(text="❌ Delete", callback_data="del"),
         InlineKeyboardButton(text="ℹ️ Help", callback_data="help")]
    ])
    await m.answer("🎯 **CLEX Pump.fun Tracker**\n\nMonitor wallets for buys!\n\nChoose action:", 
                   reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "add")
async def add_start(q: CallbackQuery):
    user_states[q.from_user.id] = {"state": "add_wallet"}
    await q.message.edit_text("📍 Send wallet address:")

@router.callback_query(F.data == "list")
async def list_wallets(q: CallbackQuery):
    wallets = await get_wallets(q.from_user.id)
    if not wallets:
        await q.message.edit_text("No wallets tracked.")
        return
    
    text = "📊 **Your Wallets:**\n\n"
    kb_btns = []
    for addr, msg in wallets:
        text += f"👛 `{addr[:20]}...`\n   {msg}\n\n"
        kb_btns.append([InlineKeyboardButton(text=f"🗑 {addr[:15]}", callback_data=f"d_{addr}")])
    
    kb_btns.append([InlineKeyboardButton(text="⬅️ Back", callback_data="back")])
    await q.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_btns), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "del")
async def del_start(q: CallbackQuery):
    user_states[q.from_user.id] = {"state": "del_wallet"}
    await q.message.edit_text("❌ Send wallet to delete:")

@router.callback_query(F.data == "help")
async def help_cmd(q: CallbackQuery):
    text = """
🎯 **CLEX Help**

**Features:**
• Add Solana wallets to track
• Get alerts when they buy on Pump.fun
• See token CA in each alert
• Delete anytime

**Speed:**
⚡ Alerts in 2-5 seconds
✅ Zero spam
✅ Always shows Token CA

**Setup:**
1. Add wallet address
2. Wait for transaction
3. Get instant alert!
"""
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data="back")]])
    await q.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "back")
async def back(q: CallbackQuery):
    await start(q.message)

@router.callback_query(F.data.startswith("d_"))
async def delete_wallet_cb(q: CallbackQuery):
    addr = q.data[2:]
    await del_wallet(q.from_user.id, addr)
    await q.answer("✅ Deleted")
    await list_wallets(q)

@router.message()
async def handle_input(m: Message):
    uid = m.from_user.id
    state = user_states.get(uid, {}).get("state", "menu")
    text = m.text.strip()
    
    if state == "add_wallet":
        if len(text) >= 43 and len(text) <= 44:
            await add_wallet(uid, text, "Pump.fun buy! 🚀")
            await m.answer("✅ Wallet added!\n\nWaiting for transactions...", 
                          reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]))
            user_states[uid] = {"state": "menu"}
        else:
            await m.answer("❌ Invalid address (43-44 chars)")
    
    elif state == "del_wallet":
        if len(text) >= 43 and len(text) <= 44:
            await del_wallet(uid, text)
            await m.answer("✅ Deleted!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]))
            user_states[uid] = {"state": "menu"}
        else:
            await m.answer("❌ Invalid address")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("DB ready")
    poll = asyncio.create_task(dp.start_polling(bot))
    logger.info("Bot live")
    yield
    poll.cancel()
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
    return {"status": "alive"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
