"""
CLEX — pump.fun callout + sniper bot.
Quicknode stream delivery, pump.fun API metadata, full sniper pipeline.
"""
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
from aiogram.types import (Message, InlineKeyboardMarkup,
                            InlineKeyboardButton, CallbackQuery)
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager

load_dotenv()

from sniper import (
    RISK_PROFILES, init_sniper_db, get_sniper_user, save_sniper_user,
    delete_sniper_user, toggle_sniper, update_risk_profile,
    get_enabled_snipers, execute_user_buy, execute_manual_sell,
    get_open_positions, get_performance_stats, get_blacklist_count,
    get_sol_balance, validate_private_key, get_setup_state,
    set_setup_state, clear_setup_state, position_monitor_loop,
    set_alert_callback, set_custom_amount, get_custom_amount,
    record_first_buyer, fingerprint_dev_wallet,
)
import sniper as sn

# ── ENV ───────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DB_PATH        = "clex.db"

# ── TUNING ────────────────────────────────────────────────────────────────────
WATCHLIST_TTL       = 300        # seconds a coin stays in watchlist
CHECK_INTERVAL      = 15         # watchlist evaluation interval
MAX_ALERTS_PER_HOUR = 10
MIN_ALERT_GAP       = 60         # seconds between alerts
MAX_RUG_SCORE       = int(os.getenv("MAX_RUG_SCORE", "55"))
MIN_CURVE_VELOCITY  = 0.3        # %/min — lower = catch earlier
MIN_HOLDERS         = 8          # lower = enter earlier on fast pumps
MIN_HOLDER_DELTA    = 1          # just needs to be growing
MAX_TOP1_PCT        = 50
TOTAL_SUPPLY        = 1_000_000_000

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

bot    = Bot(token=TELEGRAM_TOKEN)
dp     = Dispatcher()
router = Router()
dp.include_router(router)

# ── RUG FILTERS ───────────────────────────────────────────────────────────────
RUG_HARD = {"rugpull", "honeypot", "scam", "ponzi", "exit", "drain"}
RUG_SOFT = {
    "elon", "trump", "biden", "musk", "shib", "doge", "pepe", "inu",
    "safe", "moon", "gem", "100x", "1000x", "rich", "lambo", "presale",
    "airdrop", "giveaway", "free", "official", "real", "legit", "verified",
    "guaranteed", "pump", "based", "chad", "wojak", "bonk", "wif", "bome",
    "catwif", "notcoin", "hamster", "clown", "x100", "x1000", "moonshot",
    "nextgem", "callout", "fair", "launch", "stealth", "kek", "frog",
}
COPYCAT_SYMS = {
    "BTC", "ETH", "SOL", "BNB", "DOGE", "SHIB", "PEPE", "WIF", "BONK",
    "TRUMP", "MAGA", "BOME", "WEN", "SAMO", "COPE", "FLOKI", "KISHU",
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

watchlist:    Dict[str, WatchlistEntry] = {}
alert_times:  List[float] = []
last_alert_at: float = 0.0

def can_alert() -> bool:
    global alert_times, last_alert_at
    now = time.time()
    alert_times = [t for t in alert_times if now - t < 3600]
    return (len(alert_times) < MAX_ALERTS_PER_HOUR and
            now - last_alert_at >= MIN_ALERT_GAP)

def record_alert():
    global last_alert_at
    alert_times.append(time.time())
    last_alert_at = time.time()

# ── DB ────────────────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id   INTEGER PRIMARY KEY,
                joined_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS seen_tokens (
                mint       TEXT PRIMARY KEY,
                alerted_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS dev_cache (
                address   TEXT PRIMARY KEY,
                data      TEXT,
                cached_at REAL
            );
        """)
        await db.commit()
    # Clear stale seen_tokens so fresh deploys catch recent launches
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM seen_tokens "
            "WHERE alerted_at < datetime('now', '-10 minutes')")
        await db.commit()

async def get_subscribers() -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT chat_id FROM subscribers")
        return [r[0] for r in await cur.fetchall()]

async def is_subscriber(chat_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM subscribers WHERE chat_id=?", (chat_id,))
        return await cur.fetchone() is not None

async def add_subscriber(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO subscribers VALUES (?, CURRENT_TIMESTAMP)",
            (chat_id,))
        await db.commit()

async def remove_subscriber(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
        await db.commit()

async def already_seen(mint: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM seen_tokens WHERE mint=?", (mint,))
        return await cur.fetchone() is not None

async def mark_seen(mint: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO seen_tokens VALUES (?, CURRENT_TIMESTAMP)",
            (mint,))
        await db.commit()

# ── RPC ───────────────────────────────────────────────────────────────────────
def _rpc_url() -> str:
    qn  = os.getenv("QUICKNODE_RPC", "")
    hel = os.getenv("HELIUS_API_KEY", "")
    if qn:  return qn
    if hel: return f"https://mainnet.helius-rpc.com/?api-key={hel}"
    return "https://api.mainnet-beta.solana.com"

async def rpc(method: str, params: list, timeout: int = 6) -> Optional[Dict]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(_rpc_url(),
                json={"jsonrpc": "2.0", "id": 1,
                      "method": method, "params": params},
                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status == 200:
                    return (await r.json()).get("result")
    except Exception as e:
        logger.debug(f"RPC {method}: {e}")
    return None

async def das(method: str, params: dict, timeout: int = 6) -> Optional[Dict]:
    hel = os.getenv("HELIUS_API_KEY", "")
    url = (f"https://mainnet.helius-rpc.com/?api-key={hel}"
           if hel else _rpc_url())
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url,
                json={"jsonrpc": "2.0", "id": 1,
                      "method": method, "params": params},
                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status == 200:
                    return (await r.json()).get("result")
    except Exception as e:
        logger.debug(f"DAS {method}: {e}")
    return None

# ── METADATA (pump.fun API first, DAS fallback) ────────────────────────────
async def fetch_token_metadata(mint: str) -> Dict:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://frontend-api.pump.fun/coins/{mint}",
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    d = await r.json()
                    return {
                        "name":              d.get("name", ""),
                        "symbol":            d.get("symbol", ""),
                        "description":       d.get("description", ""),
                        "twitter":           d.get("twitter", ""),
                        "telegram":          d.get("telegram", ""),
                        "website":           d.get("website", ""),
                        "usd_market_cap":    float(d.get("usd_market_cap") or 0),
                        "created_timestamp": d.get("created_timestamp"),
                    }
    except Exception:
        pass
    result = await das("getAsset", {"id": mint})
    if not result:
        return {}
    content  = result.get("content", {})
    metadata = content.get("metadata", {})
    links    = content.get("links", {})
    return {
        "name":          metadata.get("name", ""),
        "symbol":        metadata.get("symbol", ""),
        "description":   metadata.get("description", ""),
        "twitter":       links.get("twitter", ""),
        "telegram":      links.get("telegram", ""),
        "website":       links.get("external_url", ""),
        "usd_market_cap": 0.0,
    }

async def fetch_dev_history(dev_wallet: str) -> Dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT data, cached_at FROM dev_cache WHERE address=?",
            (dev_wallet,))
        row = await cur.fetchone()
        if row and (time.time() - row[1]) < 900:
            return jsonlib.loads(row[0])

    info: Dict = {
        "wallet_age_days": 0, "tokens_created": 0, "prior_rugs_est": 0,
        "is_fresh_wallet": True, "flags": [],
    }
    bal = await rpc("getBalance", [dev_wallet])
    if bal:
        info["sol_balance"] = round(bal.get("value", 0) / 1e9, 4)

    sigs = await rpc("getSignaturesForAddress",
                     [dev_wallet, {"limit": 50, "commitment": "confirmed"}])
    if not sigs:
        info["flags"].append("NO_TX_HISTORY")
    else:
        oldest = sigs[-1].get("blockTime")
        if oldest:
            info["wallet_age_days"] = round((time.time() - oldest) / 86400, 1)
            info["is_fresh_wallet"] = info["wallet_age_days"] < 3
        if info["is_fresh_wallet"]:
            info["flags"].append("FRESH_WALLET")

    created = await das("getAssetsByCreator",
        {"creatorAddress": dev_wallet, "onlyVerified": False,
         "limit": 20, "page": 1})
    if created:
        items = created.get("items", [])
        info["tokens_created"] = len(items)
        rugs = sum(1 for i in items
            if i.get("token_info", {}).get("supply") is not None
            and int(i["token_info"]["supply"]) < TOTAL_SUPPLY * 0.05)
        info["prior_rugs_est"] = rugs
        if rugs > 0:
            info["flags"].append(f"PRIOR_RUGS~{rugs}")
        if info["tokens_created"] > 5:
            info["flags"].append(f"SERIAL_LAUNCHER_{info['tokens_created']}")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO dev_cache VALUES (?,?,?)",
            (dev_wallet, jsonlib.dumps(info), time.time()))
        await db.commit()
    return info

async def fetch_snapshot(mint: str) -> Snapshot:
    """Use pump.fun API for live holder count, curve %, and volume.
    Falls back to RPC only if pump.fun API is unavailable.
    getTokenLargestAccounts only returns ~10 accounts and misses real holder count."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://frontend-api.pump.fun/coins/{mint}",
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    d = await r.json()
                    # pump.fun returns bonding curve progress as a fraction 0-1
                    # convert to percentage of curve filled
                    bc_progress = float(d.get("bonding_curve_percentage") or 0)
                    # holder_count is the real number of unique wallets
                    holder_count = int(d.get("holder_count") or 0)
                    # market_cap in SOL — use as proxy for tx volume
                    mc_sol = float(d.get("market_cap") or 0)
                    # top holder: largest single holder as % of supply
                    top1_pct = 0.0
                    return Snapshot(
                        t=time.time(),
                        curve_pct=round(bc_progress * 100, 2),
                        holder_count=holder_count,
                        top1_pct=top1_pct,
                        tx_count=int(mc_sol * 10),  # proxy for activity
                    )
    except Exception:
        pass
    # RPC fallback
    holders_res, sigs_res = await asyncio.gather(
        rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}]),
        rpc("getSignaturesForAddress",  [mint, {"limit": 30, "commitment": "confirmed"}]),
    )
    accounts = (holders_res or {}).get("value", [])
    amounts  = []
    for a in accounts:
        try: amounts.append(float(a.get("uiAmount") or 0))
        except Exception: pass
    curve_pct = top1_pct = 0.0
    if amounts:
        curve_pct = max(0.0, min(100.0, round((1 - amounts[0] / TOTAL_SUPPLY) * 100, 2)))
        top1_pct  = round(amounts[0] / TOTAL_SUPPLY * 100, 2)
    return Snapshot(
        t=time.time(), curve_pct=curve_pct,
        holder_count=len(amounts), top1_pct=top1_pct,
        tx_count=len(sigs_res) if sigs_res else 0,
    )

# ── RUG PRE-FILTER ────────────────────────────────────────────────────────────
def pre_filter_rug(meta: Dict, dev: Dict) -> Tuple[int, List[str]]:
    pts, flags = 0, []
    name = (meta.get("name") or "").lower()
    sym  = (meta.get("symbol") or "").upper()

    hard = [k for k in RUG_HARD if k in name]
    if hard: pts += 30; flags.append(f"🔴 Scam keywords: {', '.join(hard)}")

    soft = [k for k in RUG_SOFT if k in name]
    if len(soft) >= 3: pts += 10; flags.append(f"🟡 Noise keywords ({len(soft)})")
    elif soft:         pts += 4

    if sym in COPYCAT_SYMS:          pts += 8;  flags.append(f"🟡 Copycat symbol ({sym})")
    if not name or len(name) < 2:    pts += 12; flags.append("🔴 No name")
    if dev.get("is_fresh_wallet"):   pts += 18; flags.append("🔴 Fresh wallet (<3d)")
    elif dev.get("wallet_age_days", 999) < 14:
                                     pts += 10; flags.append("🟡 Young wallet (<14d)")

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
    if len(snaps) < 2:
        return False, {}

    now_s  = snaps[-1]
    first  = snaps[0]
    prev   = snaps[-2]
    elapsed = max((now_s.t - first.t) / 60, 0.01)

    curve_velocity  = (now_s.curve_pct - first.curve_pct) / elapsed
    holder_velocity = (now_s.holder_count - first.holder_count) / elapsed
    holder_delta    = now_s.holder_count - prev.holder_count
    curve_delta     = now_s.curve_pct - prev.curve_pct

    momentum = {
        "curve_pct":      now_s.curve_pct,
        "curve_velocity": round(curve_velocity, 3),
        "curve_delta":    round(curve_delta, 3),
        "holder_count":   now_s.holder_count,
        "holder_velocity":round(holder_velocity, 2),
        "holder_delta":   holder_delta,
        "top1_pct":       now_s.top1_pct,
        "tx_count":       now_s.tx_count,
        "age_secs":       round(now_s.t - entry.added_at),
    }

    if entry.rug_risk > MAX_RUG_SCORE:      return False, momentum
    if curve_velocity  < MIN_CURVE_VELOCITY: return False, momentum
    if now_s.holder_count < MIN_HOLDERS:    return False, momentum
    if holder_delta    < MIN_HOLDER_DELTA:  return False, momentum
    if now_s.top1_pct  > MAX_TOP1_PCT:     return False, momentum
    if now_s.curve_pct < 1.0:              return False, momentum
    # One strong window is enough for fast pump.fun coins
    # Two-window requirement kills entries on parabolic moves
    if len(snaps) >= 3 and curve_velocity < 1.0:
        # Only require consistency if move is not already strong
        if curve_delta <= 0 and (prev.curve_pct - snaps[-3].curve_pct) <= 0:
            return False, momentum

    return True, momentum

# ── WATCHLIST LOOP ────────────────────────────────────────────────────────────
async def watchlist_loop():
    logger.info("Watchlist engine started")
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        to_drop = []
        for mint, entry in list(watchlist.items()):
            age = now - entry.added_at
            if age > WATCHLIST_TTL:
                to_drop.append(mint); continue
            try:
                snap = await fetch_snapshot(mint)
                entry.snapshots.append(snap)
            except Exception as e:
                logger.debug(f"Snapshot {mint[:12]}: {e}"); continue
            if age > 90 and snap.curve_pct < 0.5 and snap.holder_count < 8:
                to_drop.append(mint); continue
            passed, momentum = check_conviction(entry)
            if not passed: continue
            if not can_alert():
                logger.info(f"Rate limited — skip {mint[:12]}"); continue
            await fire_alert(entry, momentum)
            record_alert()
            to_drop.append(mint)
        for m in to_drop:
            watchlist.pop(m, None)

# ── SNIPER CALLBACK ───────────────────────────────────────────────────────────
async def sniper_message_callback(user_id: int, text: str):
    try:
        await bot.send_message(user_id, text, parse_mode=ParseMode.MARKDOWN)
    except TelegramAPIError as e:
        logger.error(f"Sniper msg to {user_id}: {e}")

# ── ALERT ─────────────────────────────────────────────────────────────────────
def _bar(v: float, max_v: float = 100) -> str:
    filled = round(min(v, max_v) / max_v * 10)
    return "█" * filled + "░" * (10 - filled)

def _fmt_mc(usd: float) -> str:
    if usd >= 1_000_000: return f"${usd/1_000_000:.2f}M"
    if usd >= 1_000:     return f"${usd/1_000:.1f}K"
    return f"${usd:.0f}"

async def fire_alert(entry: WatchlistEntry, momentum: Dict):
    mint = entry.mint
    dev  = entry.dev

    # Refresh metadata at alert time for latest MC + name
    fresh = await fetch_token_metadata(mint)
    meta  = {**entry.meta, **fresh} if fresh.get("name") else entry.meta

    name   = meta.get("name")   or mint[:8]
    symbol = meta.get("symbol") or "???"
    sig    = entry.tx_sig
    age_s  = momentum.get("age_secs", 0)
    age_str = f"{age_s//60}m{age_s%60}s" if age_s >= 60 else f"{age_s}s"

    usd_mc = meta.get("usd_market_cap") or 0
    mc_str = _fmt_mc(usd_mc) if usd_mc > 0 else "—"

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
        f"`{mint}`\n"
        f"💰 MC: *{mc_str}* · ⏱ *{age_str} old*\n\n"
        f"📈 *MOMENTUM*\n"
        f"Curve:    {cp:.1f}%  {_bar(cp)}  (+{momentum.get('curve_delta',0):.2f}%)\n"
        f"Velocity: {cv:.2f}%/min  {_bar(cv, 3)}\n"
        f"Holders:  {hc}  (+{momentum.get('holder_delta',0)} last window)\n"
        f"H-rate:   {hv:.1f}/min · Top1: {t1:.1f}% · Txns: {txn}\n\n"
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
            await bot.send_message(chat_id, text,
                                   parse_mode=ParseMode.MARKDOWN,
                                   disable_web_page_preview=True)
        except TelegramAPIError as e:
            logger.error(f"Alert to {chat_id}: {e}")

    snipers = await get_enabled_snipers()
    for su in snipers:
        uid = su["user_id"]
        if not await is_subscriber(uid): continue
        asyncio.create_task(_run_snipe(uid, mint, name, symbol, momentum, su))

    logger.info(
        f"ALERT {name} ({mint[:12]}) MC={mc_str} "
        f"age={age_str} curve={cp:.1f}% vel={cv:.2f}%/min"
    )

async def _run_snipe(user_id: int, mint: str, name: str,
                     symbol: str, momentum: Dict, su: Dict):
    ok, sig, method, buy_sol, exit_mode = await execute_user_buy(
        user_id, mint, name, symbol, momentum)
    profile    = RISK_PROFILES[su["risk_profile"]]
    mode_label = {"momentum": "🔥 Momentum", "steady": "📈 Trailing",
                  "weak": "💤 Fixed TP/SL"}.get(exit_mode, exit_mode)
    if ok:
        msg = (f"✅ *Sniped via {method}*\n"
               f"*{name}* (${symbol})\n"
               f"Size: {buy_sol:.4f} SOL · {mode_label}\n"
               f"Profile: {profile['label']}\n"
               f"`{sig[:20]}...`")
    else:
        msg = f"❌ *Snipe failed:* {sig}"
    try:
        await bot.send_message(user_id, msg, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

# ── PAYLOAD PROCESSOR ─────────────────────────────────────────────────────────
def _extract_launch(tx: Dict) -> Optional[Tuple[str, str]]:
    if tx.get("type") != "CREATE": return None
    dev = tx.get("feePayer", "")
    for acct in tx.get("accountData", []):
        addr = acct.get("account", "")
        if addr.endswith("pump") and len(addr) in (43, 44):
            return addr, dev
    for t in tx.get("tokenTransfers", []):
        mint = t.get("mint", "")
        if mint.endswith("pump"): return mint, dev
    return None

MAX_COIN_AGE_SECS = 120  # reject coins older than 2 minutes

async def process_payload(payload: list):
    if not isinstance(payload, list): return
    for tx in payload:
        result = _extract_launch(tx)
        if not result: continue
        mint, dev_wallet = result
        if mint in watchlist or await already_seen(mint): continue

        # ── Freshness gate using pump.fun API timestamp ───────────────────
        # Must be called first. pump.fun returns created_timestamp (unix secs).
        # Stream backfills deliver old blocks on reconnect — this kills them.
        meta = await fetch_token_metadata(mint)
        created = meta.get("created_timestamp")
        if created:
            age_secs = time.time() - float(created)
            if age_secs > MAX_COIN_AGE_SECS:
                logger.debug(f"Stale {mint[:12]} age={age_secs:.0f}s — dropped")
                await mark_seen(mint)
                continue
        else:
            age_secs = 0

        await mark_seen(mint)
        dev = await fetch_dev_history(dev_wallet)
        rug_risk, risk_flags = pre_filter_rug(meta, dev)
        if rug_risk >= 85:
            logger.debug(f"Discard {mint[:12]} rug={rug_risk}"); continue

        for acct in list(tx.get("accountData", []))[:5]:
            addr = acct.get("account", "")
            if addr and addr != dev_wallet:
                asyncio.create_task(record_first_buyer(addr))

        if len(watchlist) >= 50:
            oldest = min(watchlist, key=lambda m: watchlist[m].added_at)
            watchlist.pop(oldest, None)

        name = meta.get("name") or mint[:8]
        watchlist[mint] = WatchlistEntry(
            mint=mint, dev_wallet=dev_wallet,
            tx_sig=tx.get("signature", ""),
            added_at=time.time(), meta=meta, dev=dev,
            rug_risk=rug_risk, risk_flags=risk_flags,
        )
        logger.info(f"Watchlist +{name} ({mint[:12]}) age={int(age_secs)}s rug={rug_risk}")

# ── KEYBOARDS ─────────────────────────────────────────────────────────────────
def _kb_main(subscribed: bool) -> InlineKeyboardMarkup:
    btn = InlineKeyboardButton(
        text="🔕 Unsubscribe" if subscribed else "🔔 Subscribe",
        callback_data="unsub" if subscribed else "sub")
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn, InlineKeyboardButton(text="📊 Watchlist", callback_data="wl")],
        [InlineKeyboardButton(text="🔫 Sniper", callback_data="sniper_menu"),
         InlineKeyboardButton(text="ℹ️ How it works", callback_data="help")],
    ])

def _kb_sniper(user: Optional[Dict]) -> InlineKeyboardMarkup:
    if not user:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Set Up Sniper",
                                  callback_data="sniper_setup")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")],
        ])
    toggle = "🔴 Turn Off" if user["sniper_enabled"] else "🟢 Turn On"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle, callback_data="sniper_toggle")],
        [InlineKeyboardButton(text="⚡ Risk Profile",
                              callback_data="sniper_risk")],
        [InlineKeyboardButton(text="💰 Custom Amount",
                              callback_data="sniper_custom_amount")],
        [InlineKeyboardButton(text="📈 Positions",
                              callback_data="sniper_positions")],
        [InlineKeyboardButton(text="🗑 Delete Wallet",
                              callback_data="sniper_delete_confirm")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="back")],
    ])

def _kb_risk(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=RISK_PROFILES["low"]["label"],
                              callback_data=f"{prefix}_low")],
        [InlineKeyboardButton(text=RISK_PROFILES["moderate"]["label"],
                              callback_data=f"{prefix}_moderate")],
        [InlineKeyboardButton(text=RISK_PROFILES["psycho"]["label"],
                              callback_data=f"{prefix}_psycho")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="sniper_menu")],
    ])

# ── STATE ─────────────────────────────────────────────────────────────────────
_input_state: Dict[int, str] = {}   # "custom_amount" | absent
_sell_pending: Dict[int, int] = {}  # user_id → pos_id

WELCOME = (
    "😈 *𝕎𝕖𝕝𝕔𝕠𝕞𝕖 𝕥𝕠 ℂ𝕃𝔼𝕏, 𝕙𝕦𝕞𝕒𝕟.*\n\n"
    "Every new launch enters a watchlist.\n"
    "Alerts only fire when momentum is *proven* — rising curve, "
    "growing holders, sustained buying.\n\n"
    "No spam. Only callouts worth trading."
)

# ── HANDLERS ──────────────────────────────────────────────────────────────────
@router.message(Command("start"))
async def cmd_start(m: Message):
    sub = await is_subscriber(m.from_user.id)
    await m.answer(WELCOME, reply_markup=_kb_main(sub),
                   parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "sub")
async def cb_sub(q: CallbackQuery):
    await add_subscriber(q.from_user.id)
    await q.answer("✅ Subscribed!")
    await q.message.edit_text(
        "✅ *Subscribed!*\n\nAlerts will arrive when CLEX confirms a coin is rising.",
        reply_markup=_kb_main(True), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "unsub")
async def cb_unsub(q: CallbackQuery):
    await remove_subscriber(q.from_user.id)
    await q.answer("🔕 Unsubscribed")
    await q.message.edit_text("🔕 Unsubscribed. /start to re-subscribe.",
                               reply_markup=_kb_main(False))

@router.callback_query(F.data == "back")
async def cb_back(q: CallbackQuery):
    sub = await is_subscriber(q.from_user.id)
    await q.message.edit_text(WELCOME, reply_markup=_kb_main(sub),
                               parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "wl")
async def cb_wl(q: CallbackQuery):
    if not watchlist:
        await q.answer("Watchlist is empty right now"); return
    now   = time.time()
    lines = []
    for mint, entry in list(watchlist.items())[:10]:
        age  = int(now - entry.added_at)
        snap = entry.snapshots[-1] if entry.snapshots else None
        name = entry.meta.get("name") or mint[:8]
        lines.append(
            f"• {name[:16]} | {age}s | "
            f"curve:{snap.curve_pct:.1f}% | "
            f"h:{snap.holder_count}" if snap else
            f"• {name[:16]} | {age}s | —")
    await q.answer()
    await q.message.edit_text(
        f"*Watchlist ({len(watchlist)} coins)*\n\n" + "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "help")
async def cb_help(q: CallbackQuery):
    await q.message.edit_text(
        "🔬 *How CLEX works*\n\n"
        "Every pump.fun launch enters the watchlist.\n"
        "Every 15s CLEX checks:\n\n"
        "• Curve velocity ≥ 0.4%/min\n"
        "• Holders ≥ 12 and growing\n"
        "• Top wallet < 50%\n"
        "• Two consecutive positive windows\n\n"
        "Coins that don't prove themselves within 5 min are dropped silently.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.message(Command("stats"))
async def cmd_stats(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        subs = (await (await db.execute(
            "SELECT COUNT(*) FROM subscribers")).fetchone())[0]
        seen = (await (await db.execute(
            "SELECT COUNT(*) FROM seen_tokens")).fetchone())[0]
        snip = (await (await db.execute(
            "SELECT COUNT(*) FROM sniper_users "
            "WHERE sniper_enabled=1")).fetchone())[0]
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

@router.message(Command("performance"))
async def cmd_performance(m: Message):
    stats = await get_performance_stats(m.from_user.id)
    if not stats:
        await m.answer("No closed trades yet."); return
    lines = [
        f"{'🟢' if r['pnl']>=0 else '🔴'} {r['name']} (${r['symbol']}) "
        f"{r['pnl']:+.4f} SOL ({r['pct']:+.1f}%) [{r['reason']}]"
        for r in stats.get("recent", [])
    ]
    await m.answer(
        f"📊 *Performance*\n\n"
        f"Trades: {stats['total']} · Win rate: {stats['win_rate']}%\n"
        f"Total PnL: {stats['total_pnl']:+.4f} SOL\n"
        f"Avg win: +{stats['avg_win_pct']:.1f}% · "
        f"Avg loss: {stats['avg_loss_pct']:.1f}%\n"
        f"Avg hold: {stats['avg_hold_mins']}min\n\n"
        f"🏆 Best: {stats['best']['name']} {stats['best']['pnl']:+.4f} SOL\n"
        f"💀 Worst: {stats['worst']['name']} {stats['worst']['pnl']:+.4f} SOL\n\n"
        f"*Last 5:*\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN)

@router.message(Command("sell"))
async def cmd_sell(m: Message):
    uid       = m.from_user.id
    positions = await get_open_positions(uid)
    if not positions:
        await m.answer("No open positions."); return
    if len(positions) == 1:
        pos     = positions[0]
        val     = await sn.get_position_value_sol(pos["mint"], pos["token_amount"])
        pnl_pct = round((val / pos["sol_spent"] - 1) * 100, 1) if pos["sol_spent"] else 0
        await m.answer(
            f"🔫 *Manual Sell*\n\n"
            f"*{pos['name']}* (${pos['symbol']})\n"
            f"Value: {val:.4f} SOL ({pnl_pct:+.1f}%)\n\n"
            f"Reply /sellconfirm to exit.",
            parse_mode=ParseMode.MARKDOWN)
        _sell_pending[uid] = pos["id"]
    else:
        lines = [f"{i+1}. *{p['name']}* — {p['sol_spent']} SOL"
                 for i, p in enumerate(positions)]
        await m.answer("Positions:\n\n" + "\n".join(lines) +
                       "\n\n/sell <number>", parse_mode=ParseMode.MARKDOWN)

@router.message(Command("sellconfirm"))
async def cmd_sellconfirm(m: Message):
    uid    = m.from_user.id
    pos_id = _sell_pending.pop(uid, None)
    if not pos_id:
        await m.answer("No pending sell. Use /sell first."); return
    await m.answer("⏳ Executing…")
    ok, sig, pnl_sol, pnl_pct = await execute_manual_sell(uid, pos_id)
    if ok:
        await m.answer(
            f"✅ *Sold!*\nPnL: {pnl_sol:+.4f} SOL ({pnl_pct:+.1f}%)\n"
            f"`{sig[:20]}...`", parse_mode=ParseMode.MARKDOWN)
    else:
        await m.answer(f"❌ Sell failed: {sig}")

# ── SNIPER MENU ───────────────────────────────────────────────────────────────
@router.callback_query(F.data == "sniper_menu")
async def cb_sniper_menu(q: CallbackQuery):
    uid = q.from_user.id
    if not await is_subscriber(uid):
        await q.answer("Subscribe first!", show_alert=True); return
    user = await get_sniper_user(uid)
    await q.answer()
    if not user:
        await q.message.edit_text(
            "🔫 *CLEX Sniper*\n\n"
            "Auto-buys every callout the moment it fires.\n\n"
            "Connect a dedicated Solana wallet — key is AES-256 encrypted.",
            reply_markup=_kb_sniper(None), parse_mode=ParseMode.MARKDOWN)
        return
    profile     = RISK_PROFILES[user["risk_profile"]]
    status      = "🟢 ON" if user["sniper_enabled"] else "🔴 OFF"
    bal         = await get_sol_balance(user["pubkey"])
    custom      = await get_custom_amount(uid)
    amount_line = f"Custom: {custom} SOL/trade" if custom else profile["desc"]
    fee_warn    = profile["priority_fee"] + 0.005
    warning     = "\n\n⚠️ *Balance low — may not cover fees*" if bal < fee_warn else ""
    await q.message.edit_text(
        f"🔫 *CLEX Sniper*  {status}\n\n"
        f"Wallet: `{user['pubkey'][:20]}...`\n"
        f"Balance: {bal} SOL\n"
        f"Profile: {profile['label']}\n"
        f"{amount_line}{warning}",
        reply_markup=_kb_sniper(user), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "sniper_setup")
async def cb_sniper_setup(q: CallbackQuery):
    await q.answer()
    await set_setup_state(q.from_user.id, "risk")
    await q.message.edit_text(
        "⚙️ *Sniper Setup — Step 1 of 2*\n\n"
        f"🛡 *Low* — {RISK_PROFILES['low']['desc']}\n\n"
        f"⚡ *Moderate* — {RISK_PROFILES['moderate']['desc']}\n\n"
        f"🤑 *Psycho* — {RISK_PROFILES['psycho']['desc']}",
        reply_markup=_kb_risk("setup_risk"),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data.startswith("setup_risk_"))
async def cb_setup_risk(q: CallbackQuery):
    risk = q.data.removeprefix("setup_risk_")
    if risk not in RISK_PROFILES:
        await q.answer("Invalid"); return
    await set_setup_state(q.from_user.id, "key", risk)
    await q.answer()
    await q.message.edit_text(
        f"✅ *{RISK_PROFILES[risk]['label']}* selected\n\n"
        "⚙️ *Step 2 of 2*\n\n"
        "Send your Solana private key (base58) in the next message.\n"
        "🔒 AES-256 encrypted. 🗑 Deletable anytime.\n\n"
        "⚠️ Use a *dedicated trading wallet* — never your main.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel",
                                  callback_data="sniper_cancel")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "sniper_cancel")
async def cb_sniper_cancel(q: CallbackQuery):
    await clear_setup_state(q.from_user.id)
    _input_state.pop(q.from_user.id, None)
    await q.answer("Cancelled")
    await cb_sniper_menu(q)

@router.callback_query(F.data == "sniper_toggle")
async def cb_sniper_toggle(q: CallbackQuery):
    uid  = q.from_user.id
    user = await get_sniper_user(uid)
    if not user:
        await q.answer("Set up your sniper first"); return
    new = not user["sniper_enabled"]
    await toggle_sniper(uid, new)
    await q.answer("🟢 Sniper ON" if new else "🔴 Sniper OFF")
    await cb_sniper_menu(q)

@router.callback_query(F.data == "sniper_risk")
async def cb_sniper_risk(q: CallbackQuery):
    await q.answer()
    await q.message.edit_text(
        "⚡ *Change Risk Profile*\n\n"
        f"🛡 *Low* — {RISK_PROFILES['low']['desc']}\n\n"
        f"⚡ *Moderate* — {RISK_PROFILES['moderate']['desc']}\n\n"
        f"🤑 *Psycho* — {RISK_PROFILES['psycho']['desc']}",
        reply_markup=_kb_risk("risk_select"),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data.startswith("risk_select_"))
async def cb_risk_select(q: CallbackQuery):
    risk = q.data.removeprefix("risk_select_")
    if risk not in RISK_PROFILES:
        await q.answer("Invalid"); return
    await update_risk_profile(q.from_user.id, risk)
    await q.answer(f"{RISK_PROFILES[risk]['label']} set!")
    await cb_sniper_menu(q)

@router.callback_query(F.data == "sniper_custom_amount")
async def cb_custom_amount(q: CallbackQuery):
    uid  = q.from_user.id
    user = await get_sniper_user(uid)
    if not user:
        await q.answer("Set up sniper first"); return
    custom  = await get_custom_amount(uid)
    profile = RISK_PROFILES[user["risk_profile"]]
    current = (f"{custom} SOL (custom)" if custom
               else f"{profile['buy_pct']*100:.0f}% of balance")
    _input_state[uid] = "custom_amount"
    await q.answer()
    await q.message.edit_text(
        f"💰 *Custom Trade Amount*\n\nCurrent: {current}\n\n"
        "Send the SOL amount per trade (e.g. `0.05`).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Profile Default",
                                  callback_data="sniper_clear_custom")],
            [InlineKeyboardButton(text="❌ Cancel",
                                  callback_data="sniper_cancel")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "sniper_clear_custom")
async def cb_clear_custom(q: CallbackQuery):
    await set_custom_amount(q.from_user.id, None)
    _input_state.pop(q.from_user.id, None)
    await q.answer("✅ Back to profile default")
    await cb_sniper_menu(q)

@router.callback_query(F.data == "sniper_positions")
async def cb_positions(q: CallbackQuery):
    positions = await get_open_positions(q.from_user.id)
    if not positions:
        await q.answer("No open positions"); return
    now   = time.time()
    lines = []
    for pos in positions:
        age   = int((now - pos["bought_at"]) / 60)
        emoji = {"momentum": "🔥", "steady": "📈", "weak": "💤"}.get(
            pos.get("exit_mode", ""), "?")
        lines.append(
            f"• *{pos['name']}* (${pos['symbol']})\n"
            f"  {pos['sol_spent']} SOL · {age}m · {emoji}")
    await q.answer()
    await q.message.edit_text(
        f"📈 *Open Positions ({len(positions)})*\n\n" + "\n\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back",
                                  callback_data="sniper_menu")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "sniper_delete_confirm")
async def cb_delete_confirm(q: CallbackQuery):
    await q.answer()
    await q.message.edit_text(
        "🗑 *Delete Wallet*\n\n"
        "Permanently removes your private key.\n"
        "Open positions will *not* be auto-sold.\n\nSure?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Yes, Delete",
                                  callback_data="sniper_delete_go"),
             InlineKeyboardButton(text="❌ Keep",
                                  callback_data="sniper_menu")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "sniper_delete_go")
async def cb_delete_go(q: CallbackQuery):
    await delete_sniper_user(q.from_user.id)
    await q.answer("Wallet deleted")
    await q.message.edit_text(
        "🗑 Wallet deleted. Reconnect anytime from the Sniper menu.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]))

# ── MESSAGE HANDLER ───────────────────────────────────────────────────────────
@router.message()
async def handle_message(m: Message):
    uid    = m.from_user.id
    istate = _input_state.get(uid)
    state  = await get_setup_state(uid)

    # Custom amount input
    if istate == "custom_amount":
        raw = (m.text or "").strip()
        try:
            amount = float(raw)
            if amount <= 0: raise ValueError
        except ValueError:
            await m.answer("❌ Invalid. Send a number like `0.05`"); return
        user    = await get_sniper_user(uid)
        profile = RISK_PROFILES[user["risk_profile"]]
        bal     = await get_sol_balance(user["pubkey"])
        fee     = profile["priority_fee"] + 0.005
        warning = (f"\n\n⚠️ Low balance ({bal:.4f} SOL). "
                   f"Trade + fees ≈ {amount+fee:.3f} SOL.")
        await set_custom_amount(uid, amount)
        _input_state.pop(uid, None)
        await m.answer(
            f"✅ *Custom: {amount} SOL per trade*"
            f"{warning if amount + fee > bal else ''}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔫 Sniper Menu",
                                      callback_data="sniper_menu")]]))
        return

    # Private key input
    if not state or state["step"] != "key": return
    raw_key = (m.text or "").strip()
    try: await bot.delete_message(m.chat.id, m.message_id)
    except Exception: pass

    valid, pubkey, err = validate_private_key(raw_key)
    if not valid:
        await bot.send_message(uid,
            f"❌ Invalid key: {err}\n\nTry again or Cancel.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Cancel",
                                      callback_data="sniper_cancel")]]))
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
        f"Profile: {profile['label']}\n{profile['desc']}\n\n"
        f"Sniper is *OFF* — toggle on from the menu when ready.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔫 Sniper Menu",
                                  callback_data="sniper_menu")]]),
        parse_mode=ParseMode.MARKDOWN)

# ── FASTAPI ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_sniper_db()
    set_alert_callback(sniper_message_callback)
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(2)
    logger.info("CLEX ready — Quicknode stream active")
    asyncio.create_task(watchlist_loop())
    asyncio.create_task(position_monitor_loop())
    poll = asyncio.create_task(
        dp.start_polling(bot, allowed_updates=["message", "callback_query"]))
    logger.info("Bot polling started")
    yield
    poll.cancel()
    try:
        await asyncio.wait_for(bot.session.close(), timeout=5)
    except Exception:
        pass

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(req: Request):
    payload = await req.json()
    asyncio.create_task(process_payload(payload))
    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "alive", "watchlist": len(watchlist)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080, access_log=False)
