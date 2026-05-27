import asyncio
import aiosqlite
import aiohttp
import logging
import os
import time
import json as jsonlib
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError

from fastapi import FastAPI, Request
import uvicorn
from contextlib import asynccontextmanager

load_dotenv()

# ── ENV ───────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN")
HELIUS_API_KEY    = os.getenv("HELIUS_API_KEY")
HELIUS_WEBHOOK_ID = os.getenv("HELIUS_WEBHOOK_ID")
HELIUS_BASE       = "https://api.helius.xyz/v0"
HELIUS_RPC        = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
DB_PATH           = "clex.db"
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET", "change_this")

# ── TUNING ────────────────────────────────────────────────────────────────────
WATCHLIST_TTL       = 300    # seconds a coin stays in watchlist before dropped
CHECK_INTERVAL      = 15     # seconds between watchlist sweeps
MAX_ALERTS_PER_HOUR = 5      # anti-spam
MIN_ALERT_GAP       = 180    # seconds minimum between any two alerts
MAX_RUG_SCORE       = int(os.getenv("MAX_RUG_SCORE", "55"))

# conviction gate thresholds
MIN_CURVE_VELOCITY  = 0.4    # %/min minimum bonding curve fill rate
MIN_HOLDERS         = 12     # absolute holder count
MIN_HOLDER_DELTA    = 2      # new holders since last check
MAX_TOP1_PCT        = 50     # max % held by single wallet

# ── PUMP.FUN ──────────────────────────────────────────────────────────────────
PUMP_PROGRAM  = "6EF8rQNi1oDEZ7zrKsCauKMorruBaGECQw6B469Z7z8"
WSOL_MINT     = "So11111111111111111111111111111111111111112"
TOTAL_SUPPLY  = 1_000_000_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

bot    = Bot(token=TELEGRAM_TOKEN)
dp     = Dispatcher()
router = Router()
dp.include_router(router)

# ── KEYWORD LISTS ─────────────────────────────────────────────────────────────
RUG_NAME_HARD = {"rugpull","honeypot","scam","ponzi","exit","drain"}
RUG_NAME_SOFT = {
    "elon","trump","biden","musk","shib","doge","pepe","inu","safe","moon",
    "gem","100x","1000x","rich","lambo","presale","airdrop","giveaway",
    "free","official","real","legit","verified","guaranteed","pump",
    "based","chad","wojak","bonk","wif","bome","catwif","michi",
    "notcoin","hamster","clown","x100","x1000","moonshot","nextbig",
    "nextgem","callout","fair","launch","stealth","kek","frog",
}
COPYCAT_SYMBOLS = {
    "BTC","ETH","SOL","BNB","DOGE","SHIB","PEPE","WIF","BONK",
    "TRUMP","MAGA","BOME","WEN","SAMO","COPE","FLOKI","KISHU",
}

# ── WATCHLIST ─────────────────────────────────────────────────────────────────
@dataclass
class Snapshot:
    t: float
    curve_pct: float
    holder_count: int
    top1_pct: float
    tx_count: int

@dataclass
class WatchlistEntry:
    mint: str
    dev_wallet: str
    tx_sig: str
    added_at: float
    meta: Dict
    dev: Dict
    rug_risk: int
    risk_flags: List[str]
    snapshots: List[Snapshot] = field(default_factory=list)

watchlist: Dict[str, WatchlistEntry] = {}

# ── ALERT RATE LIMITING ───────────────────────────────────────────────────────
alert_times: List[float] = []
last_alert_at: float = 0.0

def can_alert() -> bool:
    global alert_times, last_alert_at
    now = time.time()
    alert_times = [t for t in alert_times if now - t < 3600]
    if len(alert_times) >= MAX_ALERTS_PER_HOUR:
        return False
    if now - last_alert_at < MIN_ALERT_GAP:
        return False
    return True

def record_alert():
    global last_alert_at
    alert_times.append(time.time())
    last_alert_at = time.time()

# ── DB ────────────────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS subscribers (
            chat_id INTEGER PRIMARY KEY, joined_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS seen_tokens (
            mint TEXT PRIMARY KEY, alerted_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS dev_cache (
            address TEXT PRIMARY KEY, data TEXT, cached_at REAL)""")
        await db.commit()

async def get_subscribers() -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT chat_id FROM subscribers")
        return [r[0] for r in await cur.fetchall()]

async def add_subscriber(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO subscribers VALUES (?, CURRENT_TIMESTAMP)", (chat_id,))
        await db.commit()

async def remove_subscriber(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
        await db.commit()

async def already_seen(mint: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM seen_tokens WHERE mint=?", (mint,))
        return await cur.fetchone() is not None

async def mark_seen(mint: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO seen_tokens VALUES (?, CURRENT_TIMESTAMP)", (mint,))
        await db.commit()

# ── HELIUS SYNC ───────────────────────────────────────────────────────────────
async def helius_set_pump_watch():
    if not HELIUS_WEBHOOK_ID:
        logger.warning("HELIUS_WEBHOOK_ID not set")
        return
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{HELIUS_BASE}/webhooks/{HELIUS_WEBHOOK_ID}",
                params={"api-key": HELIUS_API_KEY},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                r.raise_for_status()
                existing = await r.json()
            payload = {
                "webhookURL":       existing.get("webhookURL"),
                "transactionTypes": existing.get("transactionTypes", ["Any"]),
                "accountAddresses": [PUMP_PROGRAM],
                "webhookType":      existing.get("webhookType", "enhanced"),
                "authHeader":       existing.get("authHeader", WEBHOOK_SECRET),
            }
            async with s.put(f"{HELIUS_BASE}/webhooks/{HELIUS_WEBHOOK_ID}",
                params={"api-key": HELIUS_API_KEY}, json=payload,
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                r.raise_for_status()
                logger.info("Helius webhook → watching pump.fun program ✅")
    except Exception as e:
        logger.error(f"Helius webhook setup error: {e}")

# ── RPC ───────────────────────────────────────────────────────────────────────
async def rpc(method: str, params: list, timeout: int = 6) -> Optional[Dict]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(HELIUS_RPC,
                json={"jsonrpc":"2.0","id":1,"method":method,"params":params},
                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status == 200:
                    return (await r.json()).get("result")
    except Exception as e:
        logger.debug(f"RPC {method}: {e}")
    return None

async def das(method: str, params: dict, timeout: int = 6) -> Optional[Dict]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(HELIUS_RPC,
                json={"jsonrpc":"2.0","id":1,"method":method,"params":params},
                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status == 200:
                    return (await r.json()).get("result")
    except Exception as e:
        logger.debug(f"DAS {method}: {e}")
    return None

# ── DATA FETCHERS ─────────────────────────────────────────────────────────────
async def fetch_token_metadata(mint: str) -> Dict:
    result = await das("getAsset", {"id": mint})
    if not result:
        return {}
    content  = result.get("content", {})
    metadata = content.get("metadata", {})
    links    = content.get("links", {})
    return {
        "name":        metadata.get("name", ""),
        "symbol":      metadata.get("symbol", ""),
        "description": metadata.get("description", ""),
        "uri":         content.get("json_uri", ""),
        "twitter":     links.get("twitter", ""),
        "telegram":    links.get("telegram", ""),
        "website":     links.get("external_url", ""),
    }

async def fetch_dev_history(dev_wallet: str) -> Dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT data, cached_at FROM dev_cache WHERE address=?", (dev_wallet,))
        row = await cur.fetchone()
        if row and (time.time() - row[1]) < 900:
            return jsonlib.loads(row[0])
    info = {
        "wallet_age_days": 0, "tokens_created": 0, "prior_rugs_est": 0,
        "sol_balance": 0, "is_fresh_wallet": True, "flags": [],
    }
    bal = await rpc("getBalance", [dev_wallet])
    if bal:
        info["sol_balance"] = round(bal.get("value", 0) / 1e9, 4)
    sigs = await rpc("getSignaturesForAddress", [dev_wallet, {"limit": 50, "commitment": "confirmed"}])
    if not sigs:
        info["flags"].append("NO_TX_HISTORY")
    else:
        oldest_ts = sigs[-1].get("blockTime")
        if oldest_ts:
            info["wallet_age_days"] = round((time.time() - oldest_ts) / 86400, 1)
            info["is_fresh_wallet"] = info["wallet_age_days"] < 3
        if info["is_fresh_wallet"]:
            info["flags"].append("FRESH_WALLET")
    created = await das("getAssetsByCreator",
        {"creatorAddress": dev_wallet, "onlyVerified": False, "limit": 20, "page": 1})
    if created:
        items = created.get("items", [])
        info["tokens_created"] = len(items)
        rugs = sum(1 for i in items
            if i.get("token_info", {}).get("supply") is not None
            and int(i["token_info"]["supply"]) < TOTAL_SUPPLY * 0.05)
        info["prior_rugs_est"] = rugs
        if rugs > 0: info["flags"].append(f"PRIOR_RUGS~{rugs}")
        if info["tokens_created"] > 5: info["flags"].append(f"SERIAL_LAUNCHER_{info['tokens_created']}")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO dev_cache VALUES (?,?,?)",
            (dev_wallet, jsonlib.dumps(info), time.time()))
        await db.commit()
    return info

async def fetch_snapshot(mint: str) -> Snapshot:
    """Single lightweight fetch: curve%, holder count, top1%, tx count."""
    holders_task = rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    sigs_task    = rpc("getSignaturesForAddress",  [mint, {"limit": 30, "commitment": "confirmed"}])
    holders_res, sigs_res = await asyncio.gather(holders_task, sigs_task)

    accounts = (holders_res or {}).get("value", [])
    amounts  = []
    for a in accounts:
        try: amounts.append(float(a.get("uiAmount") or 0))
        except: pass

    curve_pct    = 0.0
    top1_pct     = 0.0
    holder_count = len(amounts)
    if amounts:
        curve_pct = max(0.0, min(100.0, round((1 - amounts[0] / TOTAL_SUPPLY) * 100, 2)))
        top1_pct  = round(amounts[0] / TOTAL_SUPPLY * 100, 2)

    tx_count = len(sigs_res) if sigs_res else 0
    return Snapshot(t=time.time(), curve_pct=curve_pct,
                    holder_count=holder_count, top1_pct=top1_pct, tx_count=tx_count)

# ── PRE-FILTER (instant, at capture) ─────────────────────────────────────────
def pre_filter_rug(meta: Dict, dev: Dict) -> Tuple[int, List[str]]:
    """Quick rug score at capture time. Returns (score, flags)."""
    pts, flags = 0, []
    name = (meta.get("name") or "").lower()
    sym  = (meta.get("symbol") or "").upper()

    hard = [k for k in RUG_NAME_HARD if k in name]
    if hard:
        pts += 30; flags.append(f"🔴 Scam keywords: {', '.join(hard)}")
    soft = [k for k in RUG_NAME_SOFT if k in name]
    if len(soft) >= 3:
        pts += 10; flags.append(f"🟡 Noise keywords ({len(soft)})")
    elif soft:
        pts += 4
    if sym in COPYCAT_SYMBOLS:
        pts += 8; flags.append(f"🟡 Copycat symbol ({sym})")
    if not name or len(name) < 2:
        pts += 12; flags.append("🔴 No name")

    # dev
    if dev.get("is_fresh_wallet"):
        pts += 18; flags.append("🔴 Fresh wallet (<3d)")
    elif dev.get("wallet_age_days", 999) < 14:
        pts += 10; flags.append("🟡 Young wallet (<14d)")
    pr = dev.get("prior_rugs_est", 0)
    if pr >= 3:
        pts += 15; flags.append(f"🔴 Rug history (~{pr})")
    elif pr >= 1:
        pts += 8;  flags.append(f"🟡 Possible prior rug (~{pr})")
    tc = dev.get("tokens_created", 0)
    if tc >= 10:
        pts += 10; flags.append(f"🔴 Serial launcher ({tc})")
    elif tc >= 4:
        pts += 5;  flags.append(f"🟡 Repeat launcher ({tc})")
    if not any([meta.get("twitter"), meta.get("telegram"), meta.get("website")]):
        pts += 5;  flags.append("🟡 No socials")

    return min(pts, 100), flags

# ── CONVICTION GATE ───────────────────────────────────────────────────────────
def check_conviction(entry: WatchlistEntry) -> Tuple[bool, Dict]:
    """
    Returns (should_alert, momentum_data).
    Requires multiple momentum signals to align simultaneously.
    """
    snaps = entry.snapshots
    if len(snaps) < 2:
        return False, {}

    now     = snaps[-1]
    first   = snaps[0]
    prev    = snaps[-2]
    elapsed = max((now.t - first.t) / 60, 0.01)  # minutes since first snapshot

    # Velocity metrics (rate of change)
    curve_velocity  = (now.curve_pct - first.curve_pct) / elapsed   # %/min
    holder_velocity = (now.holder_count - first.holder_count) / elapsed  # holders/min
    holder_delta    = now.holder_count - prev.holder_count           # since last check
    curve_delta     = now.curve_pct - prev.curve_pct                 # since last check

    momentum = {
        "curve_pct":      now.curve_pct,
        "curve_velocity": round(curve_velocity, 3),
        "curve_delta":    round(curve_delta, 3),
        "holder_count":   now.holder_count,
        "holder_velocity": round(holder_velocity, 2),
        "holder_delta":   holder_delta,
        "top1_pct":       now.top1_pct,
        "tx_count":       now.tx_count,
        "age_secs":       round(now.t - entry.added_at),
    }

    # Hard gates — ALL must pass
    if entry.rug_risk > MAX_RUG_SCORE:          return False, momentum
    if curve_velocity < MIN_CURVE_VELOCITY:     return False, momentum
    if now.holder_count < MIN_HOLDERS:          return False, momentum
    if holder_delta < MIN_HOLDER_DELTA:         return False, momentum
    if now.top1_pct > MAX_TOP1_PCT:             return False, momentum
    if now.curve_pct < 1.0:                     return False, momentum

    # Continuity check: not just a spike — at least 2 consecutive positive curve deltas
    if len(snaps) >= 3:
        prev2 = snaps[-3]
        delta2 = prev.curve_pct - prev2.curve_pct
        if curve_delta <= 0 and delta2 <= 0:
            return False, momentum  # two consecutive flat/declining windows

    return True, momentum

# ── WATCHLIST BACKGROUND LOOP ─────────────────────────────────────────────────
async def watchlist_loop():
    logger.info("Watchlist engine started")
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now       = time.time()
        to_remove = []

        for mint, entry in list(watchlist.items()):
            age = now - entry.added_at

            # Expired
            if age > WATCHLIST_TTL:
                logger.debug(f"Watchlist TTL expired: {mint[:20]}")
                to_remove.append(mint)
                continue

            # Fetch current snapshot
            try:
                snap = await fetch_snapshot(mint)
                entry.snapshots.append(snap)
            except Exception as e:
                logger.debug(f"Snapshot error {mint[:20]}: {e}")
                continue

            # Dead coin early exit (no growth at all after 90s)
            if age > 90 and snap.curve_pct < 0.5 and snap.holder_count < 8:
                logger.debug(f"Dead coin dropped: {mint[:20]}")
                to_remove.append(mint)
                continue

            # Conviction check
            should_alert, momentum = check_conviction(entry)
            if not should_alert:
                continue

            # Rate limit check
            if not can_alert():
                logger.info(f"Rate limited — skipping {mint[:20]}")
                continue

            # Fire alert
            await fire_alert(entry, momentum)
            record_alert()
            to_remove.append(mint)

        for mint in to_remove:
            watchlist.pop(mint, None)

# ── ALERT ─────────────────────────────────────────────────────────────────────
def _bar(v: float, max_v: float = 100) -> str:
    filled = round(min(v, max_v) / max_v * 10)
    return "█" * filled + "░" * (10 - filled)

async def fire_alert(entry: WatchlistEntry, momentum: Dict):
    meta = entry.meta
    dev  = entry.dev
    name   = meta.get("name", "Unknown") or "Unknown"
    symbol = meta.get("symbol", "???")   or "???"
    mint   = entry.mint
    sig    = entry.tx_sig

    age_s  = momentum.get("age_secs", 0)
    age_str = f"{age_s//60}m{age_s%60}s" if age_s >= 60 else f"{age_s}s"

    socials = []
    if meta.get("twitter"):  socials.append(f"[Twitter]({meta['twitter']})")
    if meta.get("telegram"): socials.append(f"[Telegram]({meta['telegram']})")
    if meta.get("website"):  socials.append(f"[Web]({meta['website']})")
    social_line = " · ".join(socials) if socials else "None"

    risk_block = "\n".join(entry.risk_flags) or "None detected"

    cv  = momentum.get("curve_velocity", 0)
    hv  = momentum.get("holder_velocity", 0)
    cp  = momentum.get("curve_pct", 0)
    hc  = momentum.get("holder_count", 0)
    t1  = momentum.get("top1_pct", 0)
    txn = momentum.get("tx_count", 0)

    text = (
        f"😈 *CLEX CALLOUT*\n\n"
        f"🪙 *{name}* (${symbol})\n"
        f"`{mint}`\n\n"
        f"⏱ *{age_str} old* — caught rising\n\n"
        f"📈 *MOMENTUM*\n"
        f"Curve:   {cp}%  {_bar(cp, 100)}  (+{momentum.get('curve_delta',0):.2f}% last window)\n"
        f"Velocity: {cv:.2f}%/min  {_bar(cv, 3)}\n"
        f"Holders: {hc}  (+{momentum.get('holder_delta',0)} last window)\n"
        f"H-rate:  {hv:.1f}/min\n"
        f"Top1:    {t1}%   Txns: {txn}\n\n"
        f"⚠️ *RISK* (score: {entry.rug_risk}/100)\n"
        f"{risk_block}\n\n"
        f"👨‍💻 *DEV* `{entry.dev_wallet[:20]}...`\n"
        f"Age: {dev.get('wallet_age_days',0)}d · "
        f"Tokens: {dev.get('tokens_created',0)} · "
        f"Rugs: {dev.get('prior_rugs_est',0)}\n\n"
        f"🔗 [Pump.fun](https://pump.fun/{mint}) · "
        f"[Solscan](https://solscan.io/tx/{sig}) · "
        f"[GMGN](https://gmgn.ai/sol/token/{mint})\n"
        f"Socials: {social_line}"
    )

    subscribers = await get_subscribers()
    for chat_id in subscribers:
        try:
            await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN,
                                   disable_web_page_preview=True)
        except TelegramAPIError as e:
            logger.error(f"Send error to {chat_id}: {e}")

    logger.info(f"ALERT fired: {name} ({mint[:20]}) age={age_str} curve={cp}% vel={cv:.2f}%/min")

# ── CAPTURE (webhook → watchlist) ─────────────────────────────────────────────
def extract_pump_launch(tx: Dict) -> Optional[Tuple[str, str]]:
    if tx.get("type") != "CREATE":
        return None
    dev_wallet   = tx.get("feePayer", "")
    account_data = tx.get("accountData", [])
    for acct in account_data:
        addr = acct.get("account", "")
        if addr.endswith("pump") and len(addr) in (43, 44):
            return addr, dev_wallet
    for t in tx.get("tokenTransfers", []):
        mint = t.get("mint", "")
        if mint.endswith("pump"):
            return mint, dev_wallet
    return None

async def process_payload(payload: list):
    if not isinstance(payload, list):
        return
    for tx in payload:
        result = extract_pump_launch(tx)
        if not result:
            continue
        mint, dev_wallet = result

        if mint in watchlist:
            continue
        if await already_seen(mint):
            continue
        await mark_seen(mint)

        # Fetch metadata + dev in parallel (cheap, one-time)
        meta, dev = await asyncio.gather(
            fetch_token_metadata(mint),
            fetch_dev_history(dev_wallet),
        )

        rug_risk, risk_flags = pre_filter_rug(meta, dev)

        # Hard instant discard — obvious rugs not worth watching
        if rug_risk >= 85:
            logger.debug(f"Instant discard {mint[:20]}: rug={rug_risk}")
            continue

        entry = WatchlistEntry(
            mint=mint,
            dev_wallet=dev_wallet,
            tx_sig=tx.get("signature", ""),
            added_at=time.time(),
            meta=meta,
            dev=dev,
            rug_risk=rug_risk,
            risk_flags=risk_flags,
        )
        watchlist[mint] = entry
        logger.info(f"Watchlist +{meta.get('name','?')} ({mint[:20]}) rug={rug_risk} watching...")

# ── TELEGRAM BOT ──────────────────────────────────────────────────────────────
@router.message(Command("start"))
async def start(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔔 Subscribe",    callback_data="sub"),
         InlineKeyboardButton(text="🔕 Unsubscribe",  callback_data="unsub")],
        [InlineKeyboardButton(text="📊 Watchlist",    callback_data="wl"),
         InlineKeyboardButton(text="ℹ️ How it works", callback_data="help")],
    ])
    await m.answer(
        "😈 *CLEX Pump.fun Scanner*\n\n"
        "Every new launch enters a watchlist.\n"
        "Alerts only fire when momentum is *proven* — rising curve, growing holders, sustained buying.\n\n"
        "No spam. Only callouts worth trading.",
        reply_markup=kb, parse_mode=ParseMode.MARKDOWN
    )

@router.callback_query(F.data == "sub")
async def subscribe(q: CallbackQuery):
    await add_subscriber(q.from_user.id)
    await q.answer("✅ Subscribed!")
    await q.message.edit_text(
        "✅ *Subscribed!*\n\nYou'll receive alerts when CLEX confirms a coin is rising.",
        parse_mode=ParseMode.MARKDOWN
    )

@router.callback_query(F.data == "unsub")
async def unsubscribe(q: CallbackQuery):
    await remove_subscriber(q.from_user.id)
    await q.answer("🔕 Unsubscribed")
    await q.message.edit_text("🔕 Unsubscribed. Use /start to resubscribe.")

@router.callback_query(F.data == "wl")
async def show_watchlist(q: CallbackQuery):
    if not watchlist:
        await q.answer("Watchlist is empty right now")
        return
    lines = []
    now = time.time()
    for mint, entry in list(watchlist.items())[:10]:
        age = int(now - entry.added_at)
        snap = entry.snapshots[-1] if entry.snapshots else None
        curve = f"{snap.curve_pct:.1f}%" if snap else "..."
        holders = snap.holder_count if snap else "..."
        lines.append(f"• {entry.meta.get('name','?')[:16]} | {age}s | curve:{curve} | h:{holders}")
    await q.answer()
    await q.message.edit_text(
        f"*Watchlist ({len(watchlist)} coins)*\n\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN
    )

@router.callback_query(F.data == "help")
async def help_cb(q: CallbackQuery):
    await q.message.edit_text(
        "🔬 *How CLEX works*\n\n"
        "Every pump.fun launch enters the watchlist.\n"
        "Every 15s, CLEX checks:\n\n"
        "• Bonding curve velocity (must be rising ≥0.4%/min)\n"
        "• Holder count (must reach ≥12 and keep growing)\n"
        "• Top wallet concentration (must be <50%)\n"
        "• Continuity (two consecutive growing windows)\n\n"
        "Coins that don't prove themselves within 5 min are dropped silently.\n"
        "Max 5 alerts/hour · 3 min gap between alerts.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]
        ]),
        parse_mode=ParseMode.MARKDOWN
    )

@router.callback_query(F.data == "back")
async def back(q: CallbackQuery):
    await start(q.message)

@router.message(Command("stats"))
async def stats(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        subs = (await (await db.execute("SELECT COUNT(*) FROM subscribers")).fetchone())[0]
        seen = (await (await db.execute("SELECT COUNT(*) FROM seen_tokens")).fetchone())[0]
    now = time.time()
    recent = sum(1 for t in alert_times if now - t < 3600)
    await m.answer(
        f"📊 *Stats*\n\n"
        f"Subscribers: {subs}\n"
        f"Tokens scanned: {seen}\n"
        f"Watchlist now: {len(watchlist)}\n"
        f"Alerts last hour: {recent}/{MAX_ALERTS_PER_HOUR}",
        parse_mode=ParseMode.MARKDOWN
    )

# ── FASTAPI ────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await helius_set_pump_watch()
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(3)
    logger.info("DB ready · webhook synced")
    asyncio.create_task(watchlist_loop())
    poll = asyncio.create_task(dp.start_polling(
        bot, allowed_updates=["message", "callback_query"]))
    logger.info("Bot live · watchlist engine running")
    yield
    poll.cancel()
    try:
        await asyncio.wait_for(bot.session.close(), timeout=5)
    except:
        pass

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(req: Request):
    payload = await req.json()
    logger.info(f"Webhook: {len(payload)} txns")
    asyncio.create_task(process_payload(payload))
    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "alive", "watchlist": len(watchlist)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
