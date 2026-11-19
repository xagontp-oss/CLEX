"""
CLEX — pump.fun callout + sniper bot
Helius webhook delivery. Proven binary conviction gate.

Efficiency build (Helius-credit conscious):
  • Snapshot loop fetches holders only; tx_count is fetched once at alert time.
  • Per-coin polling cadence is adaptive (hot/warm/cold) instead of fixed 15s.
  • Dev history is fetched only for coins that PASS, at alert time.
  • Token metadata comes from the webhook first; DAS is a fallback only.
  • Graduation is resolved once per alert and shared across all snipers.
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
                            InlineKeyboardButton, CallbackQuery, BotCommand)
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
import hmac
from fastapi import FastAPI, Request, HTTPException
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
    record_first_buyer, fingerprint_dev_wallet, is_graduated,
)
import sniper as sn
import apex

# ── ENV ───────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DB_PATH        = os.getenv("DB_PATH", "clex.db")
# The pump.fun callout engine is OFF by default: Apex owns detection and the credit budget.
LEGACY_PUMP    = os.getenv("LEGACY_PUMP_ENGINE", "0") == "1"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ── TUNING — original values that produced good callouts ─────────────────────
WATCHLIST_TTL       = 300
CHECK_INTERVAL      = 5      # fine scheduler tick; real cadence is per-coin adaptive
MAX_ALERTS_PER_HOUR = 10
MIN_ALERT_GAP       = 60
MAX_RUG_SCORE       = int(os.getenv("MAX_RUG_SCORE", "55"))
MIN_CURVE_VELOCITY  = 0.4    # proven threshold
MIN_HOLDERS         = 12     # proven threshold
MIN_HOLDER_DELTA    = 2      # proven threshold
MAX_TOP1_PCT        = 50
TOTAL_SUPPLY        = 1_000_000_000
MAX_COIN_AGE_SLOTS  = 200    # ~80 seconds at 400ms/slot
FRESHNESS_MAX_SECS  = 90     # drop launches whose webhook timestamp is older

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
dp.include_router(apex.router)   # Apex pages take priority
dp.include_router(router)

# ── RPC ───────────────────────────────────────────────────────────────────────
def _rpc_url() -> str:
    qn  = os.getenv("QUICKNODE_RPC", "")
    hel = os.getenv("HELIUS_API_KEY", "")
    if qn:  return qn
    if hel: return f"https://mainnet.helius-rpc.com/?api-key={hel}"
    return "https://api.mainnet-beta.solana.com"

def _das_url() -> str:
    """Helius DAS for metadata — best source for token name/symbol."""
    hel = os.getenv("HELIUS_API_KEY", "")
    if hel: return f"https://mainnet.helius-rpc.com/?api-key={hel}"
    return _rpc_url()

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
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(_das_url(),
                json={"jsonrpc": "2.0", "id": 1,
                      "method": method, "params": params},
                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status == 200:
                    return (await r.json()).get("result")
    except Exception as e:
        logger.debug(f"DAS {method}: {e}")
    return None

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
    last_snap_at: float = 0.0

watchlist:     Dict[str, WatchlistEntry] = {}
alert_times:   List[float] = []
last_alert_at: float = 0.0
_startup_slot: int   = 0    # set at boot for freshness gating

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

def _snap_interval(age_s: float) -> float:
    """Adaptive polling: spend credits in the window that actually matters."""
    if age_s < 60:  return 10    # hot — first minute decides everything
    if age_s < 180: return 20    # warm
    return 45                     # cold — barely poll

# ── DB ────────────────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
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
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "DELETE FROM seen_tokens "
            "WHERE alerted_at < datetime('now', '-10 minutes')")
        await db.commit()

async def get_subscribers() -> List[int]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute("SELECT chat_id FROM subscribers")
        return [r[0] for r in await cur.fetchall()]

async def is_subscriber(chat_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute(
            "SELECT 1 FROM subscribers WHERE chat_id=?", (chat_id,))
        return await cur.fetchone() is not None

async def add_subscriber(chat_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT OR IGNORE INTO subscribers VALUES (?, CURRENT_TIMESTAMP)",
            (chat_id,))
        await db.commit()

async def remove_subscriber(chat_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
        await db.commit()

async def already_seen(mint: str) -> bool:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute(
            "SELECT 1 FROM seen_tokens WHERE mint=?", (mint,))
        return await cur.fetchone() is not None

async def mark_seen(mint: str):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT OR IGNORE INTO seen_tokens VALUES (?, CURRENT_TIMESTAMP)",
            (mint,))
        await db.commit()

# ── METADATA ─────────────────────────────────────────────────────────────────
def _meta_from_webhook(tx: Dict) -> Dict:
    """Zero-cost metadata straight from the webhook payload, when present.

    Helius/Quicknode enhanced CREATE payloads vary in shape — MAP THIS to the
    exact fields you see in your `PAYLOAD_KEYS` logs. It returns {} whenever it
    can't find a usable name, so fetch_token_metadata falls through to the
    metered DAS call only when the webhook genuinely lacked the data.
    """
    if not isinstance(tx, dict):
        return {}
    name   = tx.get("name") or tx.get("tokenName") or ""
    symbol = tx.get("symbol") or tx.get("tokenSymbol") or ""
    # Some enhanced streams nest token metadata under 'events' / 'meta'.
    ev = tx.get("events") or {}
    if isinstance(ev, dict):
        token_ev = ev.get("token") or ev.get("nft") or {}
        if isinstance(token_ev, dict):
            name   = name   or token_ev.get("name", "")
            symbol = symbol or token_ev.get("symbol", "")
    if not name:
        return {}
    return {
        "name": name, "symbol": symbol,
        "description": tx.get("description", ""),
        "twitter": tx.get("twitter", ""),
        "telegram": tx.get("telegram", ""),
        "website": tx.get("website", ""),
        "usd_market_cap": float(tx.get("usd_market_cap") or 0),
    }

async def fetch_token_metadata(mint: str, tx: Optional[Dict] = None) -> Dict:
    """Webhook first (free), Helius DAS fallback (metered), pump.fun enrich."""
    # 1) Free path — use what the webhook already gave us.
    if tx is not None:
        wm = _meta_from_webhook(tx)
        if wm.get("name"):
            return wm

    # 2) Metered fallback — DAS getAsset gives reliable names.
    result = await das("getAsset", {"id": mint})
    meta: Dict = {}
    if result:
        content  = result.get("content", {})
        metadata = content.get("metadata", {})
        links    = content.get("links", {})
        meta = {
            "name":        metadata.get("name", ""),
            "symbol":      metadata.get("symbol", ""),
            "description": metadata.get("description", ""),
            "twitter":     links.get("twitter", ""),
            "telegram":    links.get("telegram", ""),
            "website":     links.get("external_url", ""),
            "usd_market_cap": 0.0,
        }

    # 3) Enrich with pump.fun API (MC + socials) — best effort, may 403.
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://frontend-api.pump.fun/coins/{mint}",
                timeout=aiohttp.ClientTimeout(total=4)) as r:
                if r.status == 200:
                    d = await r.json()
                    if not meta.get("name"):
                        meta["name"]   = d.get("name", "")
                        meta["symbol"] = d.get("symbol", "")
                    if not meta.get("twitter"):
                        meta["twitter"]  = d.get("twitter", "")
                        meta["telegram"] = d.get("telegram", "")
                        meta["website"]  = d.get("website", "")
                    meta["usd_market_cap"] = float(d.get("usd_market_cap") or 0)
    except Exception:
        pass

    return meta

async def fetch_dev_history(dev_wallet: str) -> Dict:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
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

    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT OR REPLACE INTO dev_cache VALUES (?,?,?)",
            (dev_wallet, jsonlib.dumps(info), time.time()))
        await db.commit()
    return info

async def fetch_snapshot(mint: str) -> Snapshot:
    """Bonding curve % + holder count via Helius RPC — ONE call per snapshot.
    tx_count is deliberately omitted here (it's not used by the conviction gate)
    and fetched once at alert time via fetch_tx_count()."""
    holders_res = await rpc("getTokenLargestAccounts",
                            [mint, {"commitment": "processed"}])
    accounts = (holders_res or {}).get("value", [])
    amounts  = []
    for a in accounts:
        v = a.get("uiAmount")
        if v is not None:
            try:
                amounts.append(float(v))
            except Exception:
                pass

    curve_pct = top1_pct = 0.0
    holder_count = len(amounts)
    if amounts:
        curve_pct = max(0.0, min(100.0, round(
            (1 - amounts[0] / TOTAL_SUPPLY) * 100, 2)))
        top1_pct  = round(amounts[0] / TOTAL_SUPPLY * 100, 2)

    logger.warning(
        f"SNAP {mint[:12]} curve={curve_pct:.2f}% "
        f"h={holder_count} top1={top1_pct:.1f}%")
    return Snapshot(
        t=time.time(), curve_pct=curve_pct,
        holder_count=holder_count, top1_pct=top1_pct, tx_count=0,
    )

async def fetch_tx_count(mint: str) -> int:
    """Single signatures lookup — called once when a coin actually passes."""
    sigs = await rpc("getSignaturesForAddress",
                     [mint, {"limit": 30, "commitment": "processed"}])
    return len(sigs) if sigs else 0

# ── PRE-FILTER ────────────────────────────────────────────────────────────────
def pre_filter_rug(meta: Dict, dev: Dict) -> Tuple[int, List[str]]:
    """Momentum-only — every coin evaluated on price action, not reputation."""
    return 0, []

# ── CONVICTION GATE — original proven binary gate ─────────────────────────────
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
        "curve_pct":       now_s.curve_pct,
        "curve_velocity":  round(curve_velocity, 3),
        "curve_delta":     round(curve_delta, 3),
        "holder_count":    now_s.holder_count,
        "holder_velocity": round(holder_velocity, 2),
        "holder_delta":    holder_delta,
        "top1_pct":        now_s.top1_pct,
        "tx_count":        now_s.tx_count,
        "age_secs":        round(now_s.t - entry.added_at),
    }

    # ── Original proven gates ─────────────────────────────────────────────
    m = entry.mint[:12]
    if curve_velocity  < MIN_CURVE_VELOCITY:
        logger.warning(f"FAIL {m} cv={round(curve_velocity,3)} < {MIN_CURVE_VELOCITY}"); return False, momentum
    if now_s.holder_count < MIN_HOLDERS:
        logger.warning(f"FAIL {m} holders={now_s.holder_count} < {MIN_HOLDERS}"); return False, momentum
    if holder_delta    < MIN_HOLDER_DELTA:
        logger.warning(f"FAIL {m} hd={holder_delta} < {MIN_HOLDER_DELTA}"); return False, momentum
    if now_s.top1_pct  > MAX_TOP1_PCT:
        logger.warning(f"FAIL {m} top1={now_s.top1_pct} > {MAX_TOP1_PCT}"); return False, momentum
    if now_s.curve_pct < 1.0:
        logger.warning(f"FAIL {m} curve={now_s.curve_pct} < 1.0"); return False, momentum
    if len(snaps) >= 3:
        delta2 = prev.curve_pct - snaps[-3].curve_pct
        if curve_delta <= 0 and delta2 <= 0:
            logger.warning(f"FAIL {m} two_neg_windows cv={round(curve_velocity,3)}"); return False, momentum

    logger.warning(f"PASS {entry.mint[:12]} cv={round(curve_velocity,2)} h={now_s.holder_count} hd={holder_delta} cp={now_s.curve_pct:.1f}%")
    return True, momentum

# ── WATCHLIST LOOP ────────────────────────────────────────────────────────────
async def watchlist_loop():
    logger.info("Watchlist engine started")
    while True:
        await asyncio.sleep(CHECK_INTERVAL)   # fine tick; per-coin cadence is adaptive
        now     = time.time()
        to_drop = []
        for mint, entry in list(watchlist.items()):
            age = now - entry.added_at
            if age > WATCHLIST_TTL:
                to_drop.append(mint); continue
            # Adaptive throttle — only snapshot when this coin is due.
            if now - entry.last_snap_at < _snap_interval(age):
                continue
            entry.last_snap_at = now
            try:
                snap = await fetch_snapshot(mint)
                entry.snapshots.append(snap)
            except Exception as e:
                logger.debug(f"Snapshot {mint[:12]}: {e}"); continue
            # Early cull — no activity after 90 seconds
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

    # tx_count is fetched once, here, only for a coin that passed.
    try:
        momentum["tx_count"] = await fetch_tx_count(mint)
    except Exception:
        momentum["tx_count"] = momentum.get("tx_count", 0)

    # Dev history is fetched once, here, only for a coin that passed (cached 15m).
    try:
        dev = await fetch_dev_history(entry.dev_wallet)
    except Exception:
        dev = {}
    dev_flags = dev.get("flags", [])
    dev_age   = dev.get("wallet_age_days", 0)
    dev_toks  = dev.get("tokens_created", 0)
    risk_line = " · ".join(dev_flags) if dev_flags else "None detected"

    # Refresh metadata at alert time for the freshest MC (DAS, ≤10/hr).
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

    cv  = momentum.get("curve_velocity", 0)
    hv  = momentum.get("holder_velocity", 0)
    cp  = momentum.get("curve_pct", 0)
    hc  = momentum.get("holder_count", 0)
    t1  = momentum.get("top1_pct", 0)
    txn = momentum.get("tx_count", 0)

    text = (
        f"👁 *CLEX CALLOUT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"*{name}* · ${symbol}\n"
        f"`{mint}`\n"
        f"💰 MC: *{mc_str}* · ⏱ *{age_str} old*\n\n"
        f"📈 *MOMENTUM*\n"
        f"Curve:    {cp:.1f}%  {_bar(cp)}  (+{momentum.get('curve_delta',0):.2f}%)\n"
        f"Velocity: {cv:.2f}%/min  {_bar(cv, 3)}\n"
        f"Holders:  {hc}  (+{momentum.get('holder_delta',0)} this window)\n"
        f"H-rate:   {hv:.1f}/min · Txns: {txn}\n"
        f"Top wallet: {t1:.1f}%\n\n"
        f"👨‍💻 *DEV* `{entry.dev_wallet[:20]}...`\n"
        f"Age: {dev_age}d · Launches: {dev_toks}\n"
        f"Flags: {risk_line}\n\n"
        f"🔗 [Pump.fun](https://pump.fun/{mint}) · "
        f"[GMGN](https://gmgn.ai/sol/token/{mint}) · "
        f"[Solscan](https://solscan.io/tx/{sig})\n"
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

    # Resolve graduation ONCE for this mint, shared across every sniper.
    try:
        graduated = await is_graduated(mint)
    except Exception:
        graduated = None

    snipers = await get_enabled_snipers()
    for su in snipers:
        uid = su["user_id"]
        if not await is_subscriber(uid): continue
        asyncio.create_task(
            _run_snipe(uid, mint, name, symbol, momentum, su, graduated))

    logger.info(
        f"ALERT {name} ({mint[:12]}) MC={mc_str} "
        f"age={age_str} curve={cp:.1f}% vel={cv:.2f}%/min grad={graduated}"
    )

async def _run_snipe(user_id: int, mint: str, name: str,
                     symbol: str, momentum: Dict, su: Dict,
                     graduated: Optional[bool] = None):
    """Calls execute_user_buy with the pre-resolved graduation flag."""
    ok, sig, method, buy_sol, exit_mode = await execute_user_buy(
        user_id, mint, name, symbol, momentum, graduated=graduated)
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
    # tokenTransfers FIRST — the Helius stream filter explicitly
    # identified this address as the mint ending in "pump".
    # accountData contains ALL keys including bonding curve PDAs
    # which can also end in "pump" and would be selected incorrectly.
    for t in tx.get("tokenTransfers", []):
        mint = t.get("mint", "")
        if mint and len(mint) >= 32:
            return mint, dev
    # fallback: accountData
    for acct in tx.get("accountData", []):
        addr = acct.get("account", "")
        if addr.endswith("pump") and len(addr) in (43, 44):
            logger.warning(f"EXTRACT_ACCT: {addr[:20]}")
            return addr, dev
    return None

async def process_payload(payload: list):
    global _startup_slot
    if not isinstance(payload, list): return
    for tx in payload:
        # ── Slot-based freshness gate ─────────────────────────────────────
        # Helius/stream backfill on reconnect: drop anything older than startup.
        tx_slot = tx.get("slot", 0)
        if _startup_slot > 0 and tx_slot > 0:
            if tx_slot < (_startup_slot - MAX_COIN_AGE_SLOTS):
                continue  # stale block from backfill

        # ── Timestamp freshness gate (zero cost) ──────────────────────────
        ts = tx.get("timestamp", 0)
        if ts and isinstance(ts, (int, float)) and ts > 1_000_000_000:
            if time.time() - ts > FRESHNESS_MAX_SECS:
                continue

        result = _extract_launch(tx)
        if not result: continue
        mint, dev_wallet = result
        if mint in watchlist or await already_seen(mint): continue
        await mark_seen(mint)

        # Metadata: webhook first (free), DAS only if the payload lacked a name.
        meta = await fetch_token_metadata(mint, tx)

        # Dev history is DEFERRED to alert time — most coins never pass, so we
        # don't spend 3 calls/launch enriching a wallet we'll never show.
        rug_risk, risk_flags = pre_filter_rug(meta, {})

        # Record first buyers for blacklist (fire and forget, limit 5)
        for acct in list(tx.get("accountData", []))[:5]:
            addr = acct.get("account", "")
            if addr and addr != dev_wallet:
                asyncio.create_task(record_first_buyer(addr))

        # Cap watchlist to control RPC usage
        if len(watchlist) >= 50:
            oldest = min(watchlist, key=lambda m: watchlist[m].added_at)
            watchlist.pop(oldest, None)

        name = meta.get("name") or mint[:8]
        watchlist[mint] = WatchlistEntry(
            mint=mint, dev_wallet=dev_wallet,
            tx_sig=tx.get("signature", ""),
            added_at=time.time(), meta=meta, dev={},
            rug_risk=rug_risk, risk_flags=risk_flags,
        )
        logger.info(f"Watchlist +{name} ({mint[:12]}) slot={tx_slot}")

# ── KEYBOARDS ─────────────────────────────────────────────────────────────────
def _kb_main(subscribed: bool) -> InlineKeyboardMarkup:
    btn = InlineKeyboardButton(
        text="🔕 Unsubscribe" if subscribed else "🔔 Subscribe",
        callback_data="unsub" if subscribed else "sub")
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn, InlineKeyboardButton(text="📊 Watchlist", callback_data="wl")],
        [InlineKeyboardButton(text="🔫 Sniper", callback_data="sniper_menu"),
         InlineKeyboardButton(text="ℹ️ How It Works", callback_data="help")],
        [InlineKeyboardButton(text="📋 All Commands", callback_data="cmds")],
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

_input_state:  Dict[int, str] = {}
_sell_pending: Dict[int, int] = {}

WELCOME = (
    "👁 *CLEX — Real-Time Pump Intelligence*\n\n"
    "Every pump.fun launch is tracked from birth.\n"
    "Callouts only fire when momentum is *confirmed* — "
    "rising curve, accelerating holders, sustained buy pressure.\n\n"
    "High signal. No noise. Early entries only."
)

COMMANDS = (
    "📋 *CLEX Command Reference*\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "*General*\n"
    "/start — Open the main control panel\n"
    "/commands — Show this reference\n"
    "/stats — Live network statistics\n\n"
    "*Trading*\n"
    "/performance — Your full trading scorecard\n"
    "/sell — List open positions & stage an exit\n"
    "/sellconfirm — Confirm the staged manual exit\n\n"
    "*Sniper* — via the 🔫 menu\n"
    "• Set up wallet & risk profile\n"
    "• Toggle automation on / off\n"
    "• Set a custom per-trade size\n"
    "• View open positions\n"
    "• Delete wallet\n\n"
    "_Start as low as ~$5 — sizing scales with your balance._"
)

# ── HANDLERS ──────────────────────────────────────────────────────────────────
@router.message(Command("start"))
async def cmd_start(m: Message):
    sub = await is_subscriber(m.from_user.id)
    await m.answer(WELCOME, reply_markup=_kb_main(sub),
                   parse_mode=ParseMode.MARKDOWN)

@router.message(Command("commands"))
async def cmd_commands(m: Message):
    await m.answer(COMMANDS, parse_mode=ParseMode.MARKDOWN,
                   disable_web_page_preview=True)

@router.message(Command("help"))
async def cmd_help_command(m: Message):
    await m.answer(COMMANDS, parse_mode=ParseMode.MARKDOWN,
                   disable_web_page_preview=True)

@router.callback_query(F.data == "cmds")
async def cb_cmds(q: CallbackQuery):
    await q.answer()
    await q.message.edit_text(
        COMMANDS,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]),
        parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

@router.callback_query(F.data == "sub")
async def cb_sub(q: CallbackQuery):
    await add_subscriber(q.from_user.id)
    await q.answer("✅ Subscribed!")
    await q.message.edit_text(
        "✅ *Subscribed.*\n\nCallouts arrive the moment conviction is confirmed.",
        reply_markup=_kb_main(True), parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "unsub")
async def cb_unsub(q: CallbackQuery):
    await remove_subscriber(q.from_user.id)
    await q.answer("🔕 Unsubscribed")
    await q.message.edit_text("🔕 Unsubscribed. /start to return.",
                               reply_markup=_kb_main(False))

@router.callback_query(F.data == "back")
async def cb_back(q: CallbackQuery):
    sub = await is_subscriber(q.from_user.id)
    await q.message.edit_text(WELCOME, reply_markup=_kb_main(sub),
                               parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "wl")
async def cb_wl(q: CallbackQuery):
    if not watchlist:
        await q.answer("Watchlist is empty"); return
    now   = time.time()
    lines = []
    for mint, entry in list(watchlist.items())[:10]:
        age  = int(now - entry.added_at)
        snap = entry.snapshots[-1] if entry.snapshots else None
        name = entry.meta.get("name") or mint[:8]
        if snap:
            lines.append(f"• {name[:16]} | {age}s | curve:{snap.curve_pct:.1f}% | h:{snap.holder_count}")
        else:
            lines.append(f"• {name[:16]} | {age}s | —")
    await q.answer()
    await q.message.edit_text(
        f"*Watchlist ({len(watchlist)} coins)*\n\n" + "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.callback_query(F.data == "help")
async def cb_help(q: CallbackQuery):
    await q.message.edit_text(
        "🔬 *How CLEX Works*\n\n"
        "Every pump.fun launch is detected at the moment of creation.\n\n"
        "On an adaptive cadence (10s in the first minute), each coin is scored on:\n\n"
        "• Bonding curve velocity ≥ 0.4%/min\n"
        "• Holder count ≥ 12 and growing by ≥ 2\n"
        "• Top wallet concentration < 50%\n"
        "• Two consecutive positive windows\n\n"
        "Coins that fail to prove momentum within 5 minutes are dropped silently.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="back")]]),
        parse_mode=ParseMode.MARKDOWN)

@router.message(Command("stats"))
async def cmd_stats(m: Message):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        subs = (await (await db.execute(
            "SELECT COUNT(*) FROM subscribers")).fetchone())[0]
        seen = (await (await db.execute(
            "SELECT COUNT(*) FROM seen_tokens")).fetchone())[0]
        snip = (await (await db.execute(
            "SELECT COUNT(*) FROM sniper_users "
            "WHERE sniper_enabled=1")).fetchone())[0]
    recent = sum(1 for t in alert_times if time.time() - t < 3600)
    await m.answer(
        f"📊 *Network Stats*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Subscribers: {subs}\n"
        f"Tokens scanned: {seen}\n"
        f"Active snipers: {snip}\n"
        f"Watchlist: {len(watchlist)}\n"
        f"Alerts last hour: {recent}/{MAX_ALERTS_PER_HOUR}\n"
        f"Blacklisted wallets: {await get_blacklist_count()}",
        parse_mode=ParseMode.MARKDOWN)

@router.message(Command("performance"))
async def cmd_performance(m: Message):
    stats = await get_performance_stats(m.from_user.id)
    if not stats:
        await m.answer("No closed trades yet."); return
    lines = [
        f"{'🟢' if r['pnl']>=0 else '🔴'} {r['name']} "
        f"{r['pnl']:+.4f} SOL ({r['pct']:+.1f}%) [{r['reason']}]"
        for r in stats.get("recent", [])
    ]
    await m.answer(
        f"📊 *Performance*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
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
            f"Confirm with /sellconfirm",
            parse_mode=ParseMode.MARKDOWN)
        _sell_pending[uid] = pos["id"]
    else:
        lines = [f"{i+1}. *{p['name']}* — {p['sol_spent']} SOL"
                 for i, p in enumerate(positions)]
        await m.answer("Open positions:\n\n" + "\n".join(lines),
                       parse_mode=ParseMode.MARKDOWN)

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
            f"✅ *Sold*\nPnL: {pnl_sol:+.4f} SOL ({pnl_pct:+.1f}%)\n"
            f"`{sig[:20]}...`", parse_mode=ParseMode.MARKDOWN)
    else:
        await m.answer(f"❌ Sell failed: {sig}")

# ── SNIPER MENU ───────────────────────────────────────────────────────────────
@router.callback_query(F.data == "sniper_menu")
async def cb_sniper_menu(q: CallbackQuery):
    uid = q.from_user.id
    if not await is_subscriber(uid):
        await q.answer("Subscribe first.", show_alert=True); return
    user = await get_sniper_user(uid)
    await q.answer()
    if not user:
        await q.message.edit_text(
            "🔫 *CLEX Sniper*\n\n"
            "Executes on every confirmed callout the moment it fires.\n\n"
            "Connect a dedicated trading wallet. "
            "Your private key is AES-256 encrypted and never leaves the system.",
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
        "Send the SOL amount per trade (e.g. `0.05`).\n"
        "_Tip: ~$5 ≈ a small fraction of 1 SOL — size to your wallet._",
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
        "Open positions will *not* be auto-sold.\n\nConfirm?",
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

    if istate == "custom_amount":
        raw = (m.text or "").strip()
        try:
            amount = float(raw)
            if amount <= 0: raise ValueError
        except ValueError:
            await m.answer("❌ Send a number like `0.05`"); return
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
        f"✅ *Wallet connected*\n\n"
        f"Address: `{pubkey[:20]}...`\n"
        f"Balance: {bal} SOL\n\n"
        f"Profile: {profile['label']}\n{profile['desc']}\n\n"
        f"Sniper is *OFF* by default — enable it from the menu when ready.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔫 Sniper Menu",
                                  callback_data="sniper_menu")]]),
        parse_mode=ParseMode.MARKDOWN)

# ── FASTAPI ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _startup_slot
    await init_db()
    await init_sniper_db()
    set_alert_callback(sniper_message_callback)
    await apex.startup(bot)
    await bot.delete_webhook(drop_pending_updates=True)

    # Register the Telegram command menu (the “/” autocomplete list).
    try:
        await bot.set_my_commands(
                [BotCommand(command=c, description=d) for c, d in apex.BOT_COMMANDS])
    except Exception as e:
        logger.warning(f"set_my_commands failed: {e}")

    # Capture startup slot for freshness gating
    slot_result = await rpc("getSlot", [])
    if slot_result:
        _startup_slot = int(slot_result)
        logger.info(f"Startup slot: {_startup_slot} — freshness gate active")

    await asyncio.sleep(2)
    logger.info(f"CLEX ready — Apex engine active (legacy pump engine: {'on' if LEGACY_PUMP else 'off'})")
    asyncio.create_task(watchlist_loop())
    asyncio.create_task(position_monitor_loop())
    poll = asyncio.create_task(
        dp.start_polling(bot, allowed_updates=["message", "callback_query"]))
    logger.info("Bot polling · watchlist engine · position monitor — all running")
    yield
    poll.cancel()
    try:
        await asyncio.wait_for(bot.session.close(), timeout=5)
    except Exception:
        pass

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(req: Request):
    # Helius sends the configured authHeader value in the Authorization header.
    # Fail closed: reject everything unless a secret is configured and matches.
    got = req.headers.get("authorization", "")
    if got.startswith("Bearer "):
        got = got[7:]
    if not WEBHOOK_SECRET or not hmac.compare_digest(got, WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="unauthorized")
    payload = await req.json()
    apex.note_webhook()
    items = payload if isinstance(payload, list) else []
    for tx in items:
        t = tx.get("type")
        if t == "CREATE_POOL":
            asyncio.create_task(apex.on_pool_event(tx))
        elif t == "CREATE" and LEGACY_PUMP:
            asyncio.create_task(process_payload([tx]))
    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "alive", "watchlist": len(watchlist),
            "startup_slot": _startup_slot}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), access_log=False)
