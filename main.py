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

import sniper as sn
from sniper import (
    RISK_PROFILES, init_sniper_db, get_sniper_user, save_sniper_user,
    delete_sniper_user, toggle_sniper, update_risk_profile,
    get_enabled_snipers, execute_user_buy, execute_full_sell, execute_manual_sell,
    get_open_positions, get_performance_stats, get_blacklist_count,
    get_sol_balance, validate_private_key, get_setup_state,
    set_setup_state, clear_setup_state, position_monitor_loop,
    set_alert_callback,
)

# ── ENV ───────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN")
HELIUS_API_KEY    = os.getenv("HELIUS_API_KEY")
HELIUS_WEBHOOK_ID = os.getenv("HELIUS_WEBHOOK_ID")
HELIUS_BASE       = "https://api.helius.xyz/v0"
HELIUS_RPC        = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
DB_PATH           = "clex.db"
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET", "change_this")

# ── TUNING ────────────────────────────────────────────────────────────────────
WATCHLIST_TTL       = 300
CHECK_INTERVAL      = 15
MAX_ALERTS_PER_HOUR = 5
MIN_ALERT_GAP       = 180
MAX_RUG_SCORE       = int(os.getenv("MAX_RUG_SCORE", "55"))
MIN_CURVE_VELOCITY  = 0.4
MIN_HOLDERS         = 12
MIN_HOLDER_DELTA    = 2
MAX_TOP1_PCT        = 50

PUMP_PROGRAM = "6EF8rQNi1oDEZ7zrKsCauKMorruBaGECQw6B469Z7z8"
WSOL_MINT    = "So11111111111111111111111111111111111111112"
TOTAL_SUPPLY = 1_000_000_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

bot    = Bot(token=TELEGRAM_TOKEN)
dp     = Dispatcher()
router = Router()
dp.include_router(router)

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
    t: float; curve_pct: float; holder_count: int; top1_pct: float; tx_count: int

@dataclass
class WatchlistEntry:
    mint: str; dev_wallet: str; tx_sig: str; added_at: float
    meta: Dict; dev: Dict; rug_risk: int; risk_flags: List[str]
    snapshots: List[Snapshot] = field(default_factory=list)

watchlist: Dict[str, WatchlistEntry] = {}
alert_times: List[float] = []
last_alert_at: float = 0.0

def can_alert() -> bool:
    global alert_times, last_alert_at
    now = time.time()
    alert_times = [t for t in alert_times if now - t < 3600]
    if len(alert_times) >= MAX_ALERTS_PER_HOUR: return False
    if now - last_alert_at < MIN_ALERT_GAP: return False
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

async def is_subscriber(chat_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM subscribers WHERE chat_id=?", (chat_id,))
        return await cur.fetchone() is not None

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
    holders_task = rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    sigs_task    = rpc("getSignaturesForAddress",  [mint, {"limit": 30, "commitment": "confirmed"}])
    holders_res, sigs_res = await asyncio.gather(holders_task, sigs_task)
    accounts = (holders_res or {}).get("value", [])
    amounts  = []
    for a in accounts:
        try: amounts.append(float(a.get("uiAmount") or 0))
        except: pass
    curve_pct = top1_pct = 0.0
    holder_count = len(amounts)
    if amounts:
        curve_pct = max(0.0, min(100.0, round((1 - amounts[0] / TOTAL_SUPPLY) * 100, 2)))
        top1_pct  = round(amounts[0] / TOTAL_SUPPLY * 100, 2)
    tx_count = len(sigs_res) if sigs_res else 0
    return Snapshot(t=time.time(), curve_pct=curve_pct,
                    holder_count=holder_count, top1_pct=top1_pct, tx_count=tx_count)

# ── PRE-FILTER ────────────────────────────────────────────────────────────────
def pre_filter_rug(meta: Dict, dev: Dict) -> Tuple[int, List[str]]:
    pts, flags = 0, []
    name = (meta.get("name") or "").lower()
    sym  = (meta.get("symbol") or "").upper()
    hard = [k for k in RUG_NAME_HARD if k in name]
    if hard: pts += 30; flags.append(f"🔴 Scam keywords: {', '.join(hard)}")
    soft = [k for k in RUG_NAME_SOFT if k in name]
    if len(soft) >= 3: pts += 10; flags.append(f"🟡 Noise keywords ({len(soft)})")
    elif soft: pts += 4
    if sym in COPYCAT_SYMBOLS: pts += 8; flags.append(f"🟡 Copycat symbol ({sym})")
    if not name or len(name) < 2: pts += 12; flags.append("🔴 No name")
    if dev.get("is_fresh_wallet"): pts += 18; flags.append("🔴 Fresh wallet (<3d)")
    elif dev.get("wallet_age_days", 999) < 14: pts += 10; flags.append("🟡 Young wallet (<14d)")
    pr = dev.get("prior_rugs_est", 0)
    if pr >= 3: pts += 15; flags.append(f"🔴 Rug history (~{pr})")
    elif pr >= 1: pts += 8; flags.append(f"🟡 Possible prior rug (~{pr})")
    tc = dev.get("tokens_created", 0)
    if tc >= 10: pts += 10; flags.append(f"🔴 Serial launcher ({tc})")
    elif tc >= 4: pts += 5; flags.append(f"🟡 Repeat launcher ({tc})")
    if not any([meta.get("twitter"), meta.get("telegram"), meta.get("website")]):
        pts += 5; flags.append("🟡 No socials")
    return min(pts, 100), flags

# ── CONVICTION GATE ───────────────────────────────────────────────────────────
def check_conviction(entry: WatchlistEntry) -> Tuple[bool, Dict]:
    snaps = entry.snapshots
    if len(snaps) < 2: return False, {}
    now = snaps[-1]; first = snaps[0]; prev = snaps[-2]
    elapsed = max((now.t - first.t) / 60, 0.01)
    curve_velocity  = (now.curve_pct - first.curve_pct) / elapsed
    holder_velocity = (now.holder_count - first.holder_count) / elapsed
    holder_delta    = now.holder_count - prev.holder_count
    curve_delta     = now.curve_pct - prev.curve_pct
    momentum = {
        "curve_pct": now.curve_pct, "curve_velocity": round(curve_velocity, 3),
        "curve_delta": round(curve_delta, 3), "holder_count": now.holder_count,
        "holder_velocity": round(holder_velocity, 2), "holder_delta": holder_delta,
        "top1_pct": now.top1_pct, "tx_count": now.tx_count,
        "age_secs": round(now.t - entry.added_at),
    }
    if entry.rug_risk > MAX_RUG_SCORE:      return False, momentum
    if curve_velocity < MIN_CURVE_VELOCITY: return False, momentum
    if now.holder_count < MIN_HOLDERS:      return False, momentum
    if holder_delta < MIN_HOLDER_DELTA:     return False, momentum
    if now.top1_pct > MAX_TOP1_PCT:         return False, momentum
    if now.curve_pct < 1.0:                 return False, momentum
    if len(snaps) >= 3:
        delta2 = prev.curve_pct - snaps[-3].curve_pct
        if curve_delta <= 0 and delta2 <= 0:
            return False, momentum
    return True, momentum

# ── WATCHLIST LOOP ────────────────────────────────────────────────────────────
async def watchlist_loop():
    logger.info("Watchlist engine started")
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        to_remove = []
        for mint, entry in list(watchlist.items()):
            age = now - entry.added_at
            if age > WATCHLIST_TTL:
                to_remove.append(mint); continue
            try:
                snap = await fetch_snapshot(mint)
                entry.snapshots.append(snap)
            except Exception as e:
                logger.debug(f"Snapshot error {mint[:20]}: {e}"); continue
            if age > 90 and snap.curve_pct < 0.5 and snap.holder_count < 8:
                to_remove.append(mint); continue
            should_alert, momentum = check_conviction(entry)
            if not should_alert: continue
            if not can_alert():
                logger.info(f"Rate limited — skipping {mint[:20]}"); continue
            await fire_alert(entry, momentum)
            record_alert()
            to_remove.append(mint)
        for mint in to_remove:
            watchlist.pop(mint, None)

# ── SNIPER MESSAGE CALLBACK ───────────────────────────────────────────────────
async def sniper_message_callback(user_id: int, text: str):
    try:
        await bot.send_message(user_id, text, parse_mode=ParseMode.MARKDOWN)
    except TelegramAPIError as e:
        logger.error(f"Sniper msg error to {user_id}: {e}")

# ── FIRE ALERT ────────────────────────────────────────────────────────────────
def _bar(v: float, max_v: float = 100) -> str:
    filled = round(min(v, max_v) / max_v * 10)
    return "█" * filled + "░" * (10 - filled)

async def fire_alert(entry: WatchlistEntry, momentum: Dict):
    meta   = entry.meta
    dev    = entry.dev
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
    risk_block  = "\n".join(entry.risk_flags) or "None detected"

    cv  = momentum.get("curve_velocity", 0)
    hv  = momentum.get("holder_velocity", 0)
    cp  = momentum.get("curve_pct", 0)
    hc  = momentum.get("holder_count", 0)
    t1  = momentum.get("top1_pct", 0)
    txn = momentum.get("tx_count", 0)

    text = (
        f"😈 *𝐂𝐋𝐄𝐗 𝐂𝐀𝐋𝐋𝐎𝐔𝐓*\n\n"
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

    # Send callout to all subscribers
    subscribers = await get_subscribers()
    for chat_id in subscribers:
        try:
            await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN,
                                   disable_web_page_preview=True)
        except TelegramAPIError as e:
            logger.error(f"Send error to {chat_id}: {e}")

    # Extract top holder addresses from latest snapshot for blacklist check
    top_holders = []
    if entry.snapshots:
        pass  # holder addresses not in snapshot — blacklist check uses mint scan

    # Trigger per-user snipers
    snipers = await get_enabled_snipers()
    for su in snipers:
        uid = su["user_id"]
        if not await is_subscriber(uid):
            continue
        asyncio.create_task(_run_user_snipe(uid, mint, name, symbol, momentum, su, top_holders))

    logger.info(f"ALERT fired: {name} ({mint[:20]}) age={age_str} curve={cp}% vel={cv:.2f}%/min")

async def _run_user_snipe(user_id: int, mint: str, name: str,
                          symbol: str, momentum: Dict, su: Dict,
                          top_holders: list = None):
    profile   = RISK_PROFILES[su["risk_profile"]]
    ok, sig, method, buy_sol, exit_mode = await execute_user_buy(
        user_id, mint, name, symbol, momentum, top_holders)
    if ok:
        mode_label = {"momentum":"🔥 Momentum exits","steady":"📈 Trailing stop","weak":"💤 Fixed TP/SL"}.get(exit_mode, exit_mode)
        msg = (
            f"✅ *Sniped via {method}*\n"
            f"*{name}* (${symbol})\n"
            f"Size: {buy_sol:.4f} SOL\n"
            f"Strategy: {mode_label}\n"
            f"Profile: {profile['label']}\n"
            f"`{sig[:20]}...`"
        )
    else:
        msg = f"❌ *Snipe failed:* {sig}"
    try:
        await bot.send_message(user_id, msg, parse_mode=ParseMode.MARKDOWN)
    except:
        pass

# ── CAPTURE ───────────────────────────────────────────────────────────────────
def extract_pump_launch(tx: Dict) -> Optional[Tuple[str, str]]:
    if tx.get("type") != "CREATE": return None
    dev_wallet = tx.get("feePayer", "")
    for acct in tx.get("accountData", []):
        addr = acct.get("account", "")
        if addr.endswith("pump") and len(addr) in (43, 44):
            return addr, dev_wallet
    for t in tx.get("tokenTransfers", []):
        mint = t.get("mint", "")
        if mint.endswith("pump"): return mint, dev_wallet
    return None

async def process_payload(payload: list):
    if not isinstance(payload, list): return
    for tx in payload:
        result = extract_pump_launch(tx)
        if not result: continue
        mint, dev_wallet = result
        if mint in watchlist or await already_seen(mint): continue
        await mark_seen(mint)
        meta, dev, fp = await asyncio.gather(
            fetch_token_metadata(mint),
            fetch_dev_history(dev_wallet),
            __import__('sniper').fingerprint_dev_wallet(dev_wallet))
        # Append fingerprint flag if single-funder detected
        rug_risk, risk_flags = pre_filter_rug(meta, dev)
        if fp.get('single_funder'):
            rug_risk = min(rug_risk + 20, 100)
            risk_flags.append(fp['flag'])
        if rug_risk >= 85:
            logger.debug(f"Instant discard {mint[:20]}: rug={rug_risk}"); continue
        # Record first-slot buyers for blacklist tracking
        create_slot = tx.get('slot', 0)
        for sig_acct in tx.get('accountData', []):
            acct_addr = sig_acct.get('account', '')
            if acct_addr and acct_addr != dev_wallet:
                asyncio.create_task(
                    __import__('sniper').record_first_buyer(acct_addr))
        entry = WatchlistEntry(
            mint=mint, dev_wallet=dev_wallet, tx_sig=tx.get("signature", ""),
            added_at=time.time(), meta=meta, dev=dev,
            rug_risk=rug_risk, risk_flags=risk_flags)
        watchlist[mint] = entry
        logger.info(f"Watchlist +{meta.get('name','?')} ({mint[:20]}) rug={rug_risk} watching...")

# ── KEYBOARDS ─────────────────────────────────────────────────────────────────
def main_kb(subscribed: bool) -> InlineKeyboardMarkup:
    sub_btn = InlineKeyboardButton(
        text="🔕 Unsubscribe" if subscribed else "🔔 Subscribe",
        callback_data="unsub" if subscribed else "sub")
    return InlineKeyboardMarkup(inline_keyboard=[
        [sub_btn, InlineKeyboardButton(text="📊 Watchlist", callback_data="wl")],
        [InlineKeyboardButton(text="🔫 Sniper Bot", callback_data="sniper_menu"),
         InlineKeyboardButton(text="ℹ️ How it works", callback_data="help")],
    ])

def sniper_menu_kb(user: Optional[Dict]) -> InlineKeyboardMarkup:
    if not user:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Set Up Sniper", callback_data="sniper_setup")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")],
        ])
    enabled     = user.get("sniper_enabled", False)
    toggle_text = "🔴 Turn Off" if enabled else "🟢 Turn On"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle_text, callback_data="sniper_toggle")],
        [InlineKeyboardButton(text="⚡ Change Risk Profile", callback_data="sniper_risk")],
        [InlineKeyboardButton(text="📈 My Positions",        callback_data="sniper_positions")],
        [InlineKeyboardButton(text="🗑 Delete Wallet",       callback_data="sniper_delete_confirm")],
        [InlineKeyboardButton(text="⬅️ Back",                callback_data="back")],
    ])

def risk_kb(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=RISK_PROFILES["low"]["label"],
                              callback_data=f"{prefix}_low")],
        [InlineKeyboardButton(text=RISK_PROFILES["moderate"]["label"],
                              callback_data=f"{prefix}_moderate")],
        [InlineKeyboardButton(text=RISK_PROFILES["psycho"]["label"],
                              callback_data=f"{prefix}_psycho")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="sniper_menu")],
    ])

# ── BOT HANDLERS ──────────────────────────────────────────────────────────────
@router.message(Command("start"))
async def start(m: Message):
    subscribed = await is_subscriber(m.from_user.id)
    await m.answer(
        "😈 *𝕎𝕖𝕝𝕔𝕠𝕞𝕖 𝕥𝕠 ℂ𝕃𝔼𝕏, 𝕙𝕦𝕞𝕒𝕟.*\n\n"
        "Every new launch enters a watchlist.\n"
        "Alerts only fire when momentum is *proven* — rising curve, growing holders, sustained buying.\n\n"
        "No spam. Only callouts worth trading.",
        reply_markup=main_kb(subscribed),
        parse_mode=ParseMode.MARKDOWN
    )

@router.callback_query(F.data == "sub")
async def subscribe(q: CallbackQuery):
    await add_subscriber(q.from_user.id)
    await q.answer("✅ Subscribed!")
    await q.message.edit_text(
        "✅ *Subscribed!*\n\nYou'll receive alerts when CLEX confirms a coin is rising.",
        reply_markup=main_kb(True), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "unsub")
async def unsubscribe(q: CallbackQuery):
    await remove_subscriber(q.from_user.id)
    await q.answer("🔕 Unsubscribed")
    await q.message.edit_text(
        "🔕 Unsubscribed. Use /start to resubscribe.",
        reply_markup=main_kb(False))

@router.callback_query(F.data == "back")
async def back(q: CallbackQuery):
    subscribed = await is_subscriber(q.from_user.id)
    await q.message.edit_text(
        "😈 *𝕎𝕖𝕝𝕔𝕠𝕞𝕖 𝕥𝕠 ℂ𝕃𝔼𝕏, 𝕙𝕦𝕞𝕒𝕟.*\n\n"
        "Every new launch enters a watchlist.\n"
        "Alerts only fire when momentum is *proven* — rising curve, growing holders, sustained buying.\n\n"
        "No spam. Only callouts worth trading.",
        reply_markup=main_kb(subscribed),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "wl")
async def show_watchlist(q: CallbackQuery):
    if not watchlist:
        await q.answer("Watchlist is empty right now"); return
    lines = []
    now = time.time()
    for mint, entry in list(watchlist.items())[:10]:
        age     = int(now - entry.added_at)
        snap    = entry.snapshots[-1] if entry.snapshots else None
        curve   = f"{snap.curve_pct:.1f}%" if snap else "..."
        holders = snap.holder_count if snap else "..."
        lines.append(f"• {entry.meta.get('name','?')[:16]} | {age}s | curve:{curve} | h:{holders}")
    await q.answer()
    await q.message.edit_text(
        f"*Watchlist ({len(watchlist)} coins)*\n\n" + "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]),
        parse_mode=ParseMode.MARKDOWN)

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
        "CLEX gives you the early position, includes risks.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.message(Command("performance"))
async def performance_cmd(m: Message):
    uid   = m.from_user.id
    stats = await get_performance_stats(uid)
    if not stats:
        await m.answer("No closed trades yet. Stats will appear after your first exit.")
        return
    recent_lines = []
    for r in stats.get("recent", []):
        sign = "+" if r["pnl"] >= 0 else ""
        emoji = "🟢" if r["pnl"] >= 0 else "🔴"
        recent_lines.append(
            f"{emoji} {r['name']} (${r['symbol']}) "
            f"{sign}{r['pnl']:.4f} SOL ({sign}{r['pct']:.1f}%) [{r['reason']}]")
    recent_block = "\n".join(recent_lines) or "None"
    await m.answer(
        f"📊 *CLEX Performance*\n\n"
        f"Trades: {stats['total']} · Win rate: {stats['win_rate']}%\n"
        f"Total PnL: {'+' if stats['total_pnl']>=0 else ''}{stats['total_pnl']:.4f} SOL\n"
        f"Avg win: +{stats['avg_win_pct']:.1f}% · Avg loss: {stats['avg_loss_pct']:.1f}%\n"
        f"Avg hold: {stats['avg_hold_mins']}min\n\n"
        f"🏆 Best: {stats['best']['name']} +{stats['best']['pnl']:.4f} SOL (+{stats['best']['pct']:.1f}%)\n"
        f"💀 Worst: {stats['worst']['name']} {stats['worst']['pnl']:.4f} SOL ({stats['worst']['pct']:.1f}%)\n\n"
        f"*Last 5 trades:*\n{recent_block}",
        parse_mode=ParseMode.MARKDOWN)

@router.message(Command("sell"))
async def sell_cmd(m: Message):
    uid       = m.from_user.id
    positions = await get_open_positions(uid)
    if not positions:
        await m.answer("No open positions to sell.")
        return
    if len(positions) == 1:
        pos = positions[0]
        val = await __import__("sniper").get_position_value_sol(pos["mint"], pos["token_amount"])
        pnl_pct = round((val / pos["sol_spent"] - 1) * 100, 1) if pos["sol_spent"] else 0
        sign = "+" if pnl_pct >= 0 else ""
        await m.answer(
            f"🔫 *Manual Sell*\n\n"
            f"*{pos['name']}* (${pos['symbol']})\n"
            f"Current value: {val:.4f} SOL ({sign}{pnl_pct:.1f}%)\n\n"
            f"Reply /sellconfirm to exit now.",
            parse_mode=ParseMode.MARKDOWN)
        # Store pending sell in user state
        user_sell_pending[uid] = pos["id"]
    else:
        lines = []
        for i, pos in enumerate(positions):
            lines.append(f"{i+1}. *{pos['name']}* (${pos['symbol']}) — {pos['sol_spent']} SOL")
        await m.answer(
            f"Open positions:\n\n" + "\n".join(lines) +
            "\n\nReply /sell <number> to select one.",
            parse_mode=ParseMode.MARKDOWN)

user_sell_pending: Dict[int, int] = {}

@router.message(Command("sellconfirm"))
async def sellconfirm_cmd(m: Message):
    uid    = m.from_user.id
    pos_id = user_sell_pending.pop(uid, None)
    if not pos_id:
        await m.answer("No pending sell. Use /sell first.")
        return
    await m.answer("⏳ Executing sell...")
    ok, sig, pnl_sol, pnl_pct = await execute_manual_sell(uid, pos_id)
    if ok:
        sign = "+" if pnl_sol >= 0 else ""
        await m.answer(
            f"✅ *Sold!*\n"
            f"PnL: {sign}{pnl_sol:.4f} SOL ({sign}{pnl_pct:.1f}%)\n"
            f"`{sig[:20]}...`",
            parse_mode=ParseMode.MARKDOWN)
    else:
        await m.answer(f"❌ Sell failed: {sig}")

@router.message(Command("stats"))
async def stats(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        subs = (await (await db.execute("SELECT COUNT(*) FROM subscribers")).fetchone())[0]
        seen = (await (await db.execute("SELECT COUNT(*) FROM seen_tokens")).fetchone())[0]
        snip = (await (await db.execute(
            "SELECT COUNT(*) FROM sniper_users WHERE sniper_enabled=1")).fetchone())[0]
    recent = sum(1 for t in alert_times if time.time() - t < 3600)
    await m.answer(
        f"📊 *Stats*\n\n"
        f"Subscribers: {subs}\n"
        f"Tokens scanned: {seen}\n"
        f"Active snipers: {snip}\n"
        f"Watchlist now: {len(watchlist)}\n"
        f"Alerts last hour: {recent}/{MAX_ALERTS_PER_HOUR}\n"
        f"Blacklisted wallets: {await get_blacklist_count()}",
        parse_mode=ParseMode.MARKDOWN)

# ── SNIPER MENU ───────────────────────────────────────────────────────────────
@router.callback_query(F.data == "sniper_menu")
async def sniper_menu(q: CallbackQuery):
    uid = q.from_user.id
    if not await is_subscriber(uid):
        await q.answer("Subscribe first to use the sniper!", show_alert=True); return
    user = await get_sniper_user(uid)
    await q.answer()
    if not user:
        await q.message.edit_text(
            "🔫 *CLEX Sniper Bot*\n\n"
            "Auto-buy every callout the moment it fires.\n\n"
            "Connect your own Solana wallet, choose a risk profile, toggle on/off anytime.\n\n"
            "⚠️ *Your key is encrypted with AES-256 and only you can delete it.*\n"
            "Use a dedicated wallet with only what you're willing to trade.",
            reply_markup=sniper_menu_kb(None),
            parse_mode=ParseMode.MARKDOWN)
    else:
        profile = RISK_PROFILES[user["risk_profile"]]
        status  = "🟢 ON" if user["sniper_enabled"] else "🔴 OFF"
        bal     = await get_sol_balance(user["pubkey"])
        await q.message.edit_text(
            f"🔫 *CLEX Sniper*  {status}\n\n"
            f"Wallet: `{user['pubkey'][:20]}...`\n"
            f"Balance: {bal} SOL\n"
            f"Profile: {profile['label']}\n"
            f"{profile['desc']}",
            reply_markup=sniper_menu_kb(user),
            parse_mode=ParseMode.MARKDOWN)

# ── SNIPER SETUP FLOW ─────────────────────────────────────────────────────────
@router.callback_query(F.data == "sniper_setup")
async def sniper_setup_start(q: CallbackQuery):
    await q.answer()
    await set_setup_state(q.from_user.id, "risk")
    await q.message.edit_text(
        "⚙️ *Sniper Setup — Step 1 of 2*\n\n"
        "Choose your risk profile:\n\n"
        f"🛡 *Low Risk* — {RISK_PROFILES['low']['desc']}\n\n"
        f"⚡ *Moderate* — {RISK_PROFILES['moderate']['desc']}\n\n"
        f"🤑 *Psycho* — {RISK_PROFILES['psycho']['desc']}\n\n"
        "You can change this anytime.",
        reply_markup=risk_kb("setup_risk"),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data.startswith("setup_risk_"))
async def sniper_setup_risk(q: CallbackQuery):
    risk = q.data.replace("setup_risk_", "")
    if risk not in RISK_PROFILES:
        await q.answer("Invalid"); return
    await set_setup_state(q.from_user.id, "key", risk)
    await q.answer()
    await q.message.edit_text(
        f"✅ *{RISK_PROFILES[risk]['label']}* selected\n\n"
        "⚙️ *Sniper Setup — Step 2 of 2*\n\n"
        "Send your Solana private key (base58 format) in the next message.\n\n"
        "🔒 It's encrypted with AES-256 before storage.\n"
        "🗑 You can delete it from the bot at any time.\n\n"
        "⚠️ *Recommended: use a dedicated trading wallet.*\n"
        "Never use your main wallet.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="sniper_cancel")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "sniper_cancel")
async def sniper_cancel(q: CallbackQuery):
    await clear_setup_state(q.from_user.id)
    await q.answer("Cancelled")
    await sniper_menu(q)

# ── PRIVATE KEY HANDLER ───────────────────────────────────────────────────────
@router.message()
async def handle_message(m: Message):
    uid   = m.from_user.id
    state = await get_setup_state(uid)
    if not state or state["step"] != "key":
        return

    raw_key = (m.text or "").strip()

    # Delete immediately for security
    try:
        await bot.delete_message(m.chat.id, m.message_id)
    except:
        pass

    valid, pubkey, err = validate_private_key(raw_key)
    if not valid:
        await bot.send_message(uid,
            f"❌ Invalid private key: {err}\n\nTry again or tap Cancel.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Cancel", callback_data="sniper_cancel")]]))
        return

    risk    = state["risk"]
    profile = RISK_PROFILES[risk]
    bal     = await get_sol_balance(pubkey)

    await save_sniper_user(uid, raw_key, pubkey, risk)
    await clear_setup_state(uid)

    await bot.send_message(uid,
        f"✅ *Wallet connected!*\n\n"
        f"Address: `{pubkey[:20]}...`\n"
        f"Balance: {bal} SOL\n\n"
        f"Profile: {profile['label']}\n"
        f"{profile['desc']}\n\n"
        f"Sniper is *OFF* by default — toggle it on from the Sniper menu when ready.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔫 Open Sniper Menu", callback_data="sniper_menu")]]),
        parse_mode=ParseMode.MARKDOWN)

# ── SNIPER CONTROLS ───────────────────────────────────────────────────────────
@router.callback_query(F.data == "sniper_toggle")
async def sniper_toggle(q: CallbackQuery):
    uid  = q.from_user.id
    user = await get_sniper_user(uid)
    if not user:
        await q.answer("Set up your sniper first"); return
    new_state = not user["sniper_enabled"]
    if new_state:
        profile = RISK_PROFILES[user["risk_profile"]]
        bal     = await get_sol_balance(user["pubkey"])
        needed  = profile["min_buy"] + profile["priority_fee"] + 0.01
        if bal < needed:
            await q.answer(
                f"Insufficient balance! Need {needed:.3f} SOL, have {bal:.3f} SOL",
                show_alert=True); return
    await toggle_sniper(uid, new_state)
    await q.answer("🟢 Sniper ON" if new_state else "🔴 Sniper OFF")
    await sniper_menu(q)

@router.callback_query(F.data == "sniper_risk")
async def sniper_change_risk(q: CallbackQuery):
    await q.answer()
    await q.message.edit_text(
        "⚡ *Change Risk Profile*\n\n"
        f"🛡 *Low Risk* — {RISK_PROFILES['low']['desc']}\n\n"
        f"⚡ *Moderate* — {RISK_PROFILES['moderate']['desc']}\n\n"
        f"🤑 *Psycho* — {RISK_PROFILES['psycho']['desc']}",
        reply_markup=risk_kb("risk_select"),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data.startswith("risk_select_"))
async def risk_selected(q: CallbackQuery):
    risk = q.data.replace("risk_select_", "")
    if risk not in RISK_PROFILES:
        await q.answer("Invalid"); return
    await update_risk_profile(q.from_user.id, risk)
    await q.answer(f"{RISK_PROFILES[risk]['label']} set!")
    await sniper_menu(q)

@router.callback_query(F.data == "sniper_positions")
async def show_positions(q: CallbackQuery):
    uid       = q.from_user.id
    positions = await get_open_positions(uid)
    if not positions:
        await q.answer("No open positions"); return
    lines = []
    now = time.time()
    for pos in positions:
        age = int((now - pos["bought_at"]) / 60)
        mode_label = {"momentum":"🔥","steady":"📈","weak":"💤"}.get(pos.get('exit_mode','?'),'?')
        lines.append(
            f"• *{pos['name']}* (${pos['symbol']})\n"
            f"  {pos['sol_spent']} SOL · {age}m ago · mode:{mode_label}")
    await q.answer()
    await q.message.edit_text(
        f"📈 *Open Positions ({len(positions)})*\n\n" + "\n\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="sniper_menu")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "sniper_delete_confirm")
async def delete_confirm(q: CallbackQuery):
    await q.answer()
    await q.message.edit_text(
        "🗑 *Delete Wallet*\n\n"
        "This permanently removes your private key from CLEX.\n"
        "Open positions will *not* be auto-sold.\n\n"
        "Are you sure?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Yes, Delete", callback_data="sniper_delete_go"),
             InlineKeyboardButton(text="❌ No, Keep",    callback_data="sniper_menu")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "sniper_delete_go")
async def delete_wallet(q: CallbackQuery):
    await delete_sniper_user(q.from_user.id)
    await q.answer("Wallet deleted")
    await q.message.edit_text(
        "🗑 Wallet deleted. Your private key has been permanently removed from CLEX.\n\n"
        "You can reconnect anytime from the Sniper menu.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]))

# ── FASTAPI ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_sniper_db()
    await helius_set_pump_watch()
    set_alert_callback(sniper_message_callback)
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(3)
    logger.info("DB ready · webhook synced")
    asyncio.create_task(watchlist_loop())
    asyncio.create_task(position_monitor_loop())
    poll = asyncio.create_task(dp.start_polling(
        bot, allowed_updates=["message", "callback_query"]))
    logger.info("Bot live · watchlist + position monitor running")
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
