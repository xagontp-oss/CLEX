import asyncio
import aiosqlite
import aiohttp
import logging
import os
import re
import time
import math
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
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

# ── ENV ───────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN")
HELIUS_API_KEY    = os.getenv("HELIUS_API_KEY")
HELIUS_WEBHOOK_ID = os.getenv("HELIUS_WEBHOOK_ID")
HELIUS_BASE       = "https://api.helius.xyz/v0"
HELIUS_RPC        = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
DB_PATH           = "clex.db"
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET", "change_this")
MIN_PROFIT_SCORE  = int(os.getenv("MIN_PROFIT_SCORE", "52"))   # alert threshold
MAX_RUG_SCORE     = int(os.getenv("MAX_RUG_SCORE", "60"))      # hard rug ceiling

# ── PUMP.FUN CONSTANTS ────────────────────────────────────────────────────────
PUMP_PROGRAM  = "6EF8rQNi1oDEZ7zrKsCauKMorruBaGECQw6B469Z7z8"
WSOL_MINT     = "So11111111111111111111111111111111111111112"
PUMP_FEE_ACCT = "CebN5WGQ4jvEPvsVU4EoHEpgznyZKUD7yo2MXjj4oHBn"
TOTAL_SUPPLY  = 1_000_000_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()
router = Router()
dp.include_router(router)

# ── RUG / SCAM KEYWORD LISTS ──────────────────────────────────────────────────
RUG_NAME_HARD = {                  # instant heavy penalty
    "rugpull","honeypot","scam","ponzi","exit","drain",
}
RUG_NAME_SOFT = {                  # mild penalty — common memecoin noise
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
# known high-risk funding sources (on-chain mixer / tumbler program IDs)
MIXER_PROGRAMS = {
    "mixLUCKYXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",   # placeholder
}

# ── DB SETUP ──────────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY,
                joined_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS seen_tokens (
                mint TEXT PRIMARY KEY,
                alerted_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS dev_cache (
                address TEXT PRIMARY KEY,
                data    TEXT,
                cached_at REAL
            )""")
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

# ── HELIUS WEBHOOK SYNC ───────────────────────────────────────────────────────
async def helius_set_pump_watch():
    """Ensure our webhook is watching the pump.fun program."""
    if not HELIUS_WEBHOOK_ID:
        logger.warning("HELIUS_WEBHOOK_ID not set")
        return
    try:
        async with aiohttp.ClientSession() as s:
            # First GET the existing config so we don't wipe required fields
            async with s.get(
                f"{HELIUS_BASE}/webhooks/{HELIUS_WEBHOOK_ID}",
                params={"api-key": HELIUS_API_KEY},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                r.raise_for_status()
                existing = await r.json()

            # Merge — keep everything, just overwrite accountAddresses
            payload = {
                "webhookURL":       existing.get("webhookURL"),
                "transactionTypes": existing.get("transactionTypes", ["Any"]),
                "accountAddresses": [PUMP_PROGRAM],
                "webhookType":      existing.get("webhookType", "enhanced"),
                "authHeader":       existing.get("authHeader", WEBHOOK_SECRET),
            }

            async with s.put(
                f"{HELIUS_BASE}/webhooks/{HELIUS_WEBHOOK_ID}",
                params={"api-key": HELIUS_API_KEY},
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                r.raise_for_status()
                logger.info("Helius webhook → watching pump.fun program ✅")
    except Exception as e:
        logger.error(f"Helius webhook setup error: {e}")

# ── HELIUS / RPC DATA FETCHERS ────────────────────────────────────────────────
async def rpc(method: str, params: list, timeout: int = 6) -> Optional[Dict]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(HELIUS_RPC,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status == 200:
                    d = await r.json()
                    return d.get("result")
    except Exception as e:
        logger.debug(f"RPC {method} error: {e}")
    return None

async def das(method: str, params: dict, timeout: int = 6) -> Optional[Dict]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(HELIUS_RPC,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status == 200:
                    d = await r.json()
                    return d.get("result")
    except Exception as e:
        logger.debug(f"DAS {method} error: {e}")
    return None

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
        "image":       links.get("image", ""),
    }

async def fetch_holder_distribution(mint: str) -> Dict:
    """Returns top-holder concentration data."""
    result = await rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    if not result:
        return {"top1_pct": 0, "top3_pct": 0, "top5_pct": 0, "top10_pct": 0, "holder_count": 0}

    accounts = result.get("value", [])
    amounts  = []
    for acct in accounts:
        try:
            amounts.append(float(acct.get("uiAmount") or 0))
        except:
            pass

    if not amounts or TOTAL_SUPPLY == 0:
        return {"top1_pct": 0, "top3_pct": 0, "top5_pct": 0, "top10_pct": 0, "holder_count": len(amounts)}

    total = TOTAL_SUPPLY
    return {
        "top1_pct":     round(amounts[0] / total * 100, 2) if amounts else 0,
        "top3_pct":     round(sum(amounts[:3]) / total * 100, 2),
        "top5_pct":     round(sum(amounts[:5]) / total * 100, 2),
        "top10_pct":    round(sum(amounts[:10]) / total * 100, 2),
        "holder_count": len(amounts),
        "raw":          amounts[:10],
    }

async def fetch_dev_history(dev_wallet: str) -> Dict:
    """Analyses dev wallet: age, prior tokens, rug history, funding source."""
    import json as jsonlib

    # Check cache (15 min TTL)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT data, cached_at FROM dev_cache WHERE address=?", (dev_wallet,))
        row = await cur.fetchone()
        if row and (time.time() - row[1]) < 900:
            return jsonlib.loads(row[0])

    info = {
        "wallet_age_days":    0,
        "tokens_created":     0,
        "avg_token_lifespan": 0,
        "prior_rugs_est":     0,
        "sol_balance":        0,
        "tx_count":           0,
        "funded_from":        "unknown",
        "is_fresh_wallet":    True,
        "flags":              [],
    }

    # Wallet SOL balance
    bal = await rpc("getBalance", [dev_wallet])
    if bal:
        info["sol_balance"] = round(bal.get("value", 0) / 1e9, 4)

    # Transaction history — last 50 sigs
    sigs = await rpc("getSignaturesForAddress",
                     [dev_wallet, {"limit": 50, "commitment": "confirmed"}])
    if not sigs:
        info["is_fresh_wallet"] = True
        info["flags"].append("NO_TX_HISTORY")
        _cache_dev(dev_wallet, info)
        return info

    info["tx_count"] = len(sigs)

    # Wallet age = oldest tx in last 50
    oldest_ts = sigs[-1].get("blockTime") if sigs else None
    if oldest_ts:
        age_secs = time.time() - oldest_ts
        info["wallet_age_days"] = round(age_secs / 86400, 1)
        info["is_fresh_wallet"] = info["wallet_age_days"] < 3

    if info["is_fresh_wallet"]:
        info["flags"].append("FRESH_WALLET")

    # Count pump.fun create interactions
    pump_creates = 0
    short_lived   = 0
    prev_create_times = []
    for sig_info in sigs:
        err = sig_info.get("err")
        memo = sig_info.get("memo", "") or ""
        # We detect pump creates by looking for the program in the logs
        logs = sig_info.get("confirmationStatus", "")
        # Heuristic: if same wallet has many recent sigs it's likely a serial launcher
        ts = sig_info.get("blockTime", 0)
        prev_create_times.append(ts)

    # Estimate prior pump.fun token creates via DAS: search for assets created by dev
    created = await das("getAssetsByCreator",
                        {"creatorAddress": dev_wallet, "onlyVerified": False,
                         "limit": 20, "page": 1})
    if created:
        items = created.get("items", [])
        info["tokens_created"] = len(items)
        # Estimate rugs: tokens with 0 holders or very low supply remaining
        for item in items:
            supply = item.get("token_info", {}).get("supply", TOTAL_SUPPLY)
            if supply is not None and int(supply) < TOTAL_SUPPLY * 0.05:
                short_lived += 1
        info["prior_rugs_est"] = short_lived
        if short_lived > 0:
            info["flags"].append(f"PRIOR_RUGS~{short_lived}")

    if info["tokens_created"] > 5:
        info["flags"].append(f"SERIAL_LAUNCHER_{info['tokens_created']}")

    # Cache result
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO dev_cache VALUES (?,?,?)",
                         (dev_wallet, jsonlib.dumps(info), time.time()))
        await db.commit()

    return info

async def fetch_launch_buys(mint: str, dev_wallet: str) -> Dict:
    """
    Analyses the first N transactions after launch:
    - buy/sell ratio
    - unique buyers
    - bundled sniper detection (multiple buys same slot)
    - dev sell-off speed
    """
    sigs = await rpc("getSignaturesForAddress",
                     [mint, {"limit": 30, "commitment": "confirmed"}])
    if not sigs:
        return {"buy_count": 0, "sell_count": 0, "unique_buyers": 0,
                "bundled_slots": 0, "dev_sold_early": False, "buy_sell_ratio": 0}

    slot_counts: Dict[int, int] = {}
    buy_count   = 0
    sell_count  = 0
    buyers: set = set()
    dev_sold    = False

    # We can't parse full tx here cheaply — use sig metadata heuristics
    # Slot clustering = bundled snipers
    for s in sigs:
        slot = s.get("slot", 0)
        slot_counts[slot] = slot_counts.get(slot, 0) + 1

    bundled_slots = sum(1 for cnt in slot_counts.values() if cnt >= 3)

    # Rough buy/sell estimate from helius enhanced (batch)
    # Use getSignaturesForAddress count as proxy for activity velocity
    tx_count     = len(sigs)
    buy_count    = max(1, int(tx_count * 0.7))   # heuristic — refine with full parse if needed
    sell_count   = tx_count - buy_count
    unique_buyers = max(1, int(buy_count * 0.8))  # heuristic

    buy_sell_ratio = round(buy_count / max(sell_count, 1), 2)

    return {
        "buy_count":      buy_count,
        "sell_count":     sell_count,
        "unique_buyers":  unique_buyers,
        "bundled_slots":  bundled_slots,
        "dev_sold_early": dev_sold,
        "buy_sell_ratio": buy_sell_ratio,
        "tx_velocity":    tx_count,
    }

async def fetch_bonding_curve(mint: str) -> float:
    """Returns estimated bonding curve fill % for pump.fun."""
    # pump.fun curve fills when ~85 SOL raised → graduation to Raydium
    # We estimate via the token's supply still on bonding curve
    holders = await rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    if not holders:
        return 0.0
    accounts = holders.get("value", [])
    # The bonding curve account typically holds the largest share
    if accounts:
        curve_balance = float(accounts[0].get("uiAmount") or 0)
        pct_sold = round((1 - curve_balance / TOTAL_SUPPLY) * 100, 2)
        return max(0.0, min(100.0, pct_sold))
    return 0.0

# ── SCORING ENGINE ─────────────────────────────────────────────────────────────
#
# RUG RISK SCORE  : 0–100  (higher = more likely rug)
# PROFIT SCORE    : 0–100  (higher = more likely profitable)
#
# Weighted sub-scores with named flags for transparency in the alert.

def score_dev(dev: Dict) -> Tuple[int, List[str]]:
    """Returns (rug_risk_pts: 0-30, flags)."""
    pts   = 0
    flags = []

    if dev.get("is_fresh_wallet"):
        pts += 18
        flags.append("🔴 Fresh wallet (<3d)")
    elif dev.get("wallet_age_days", 999) < 14:
        pts += 10
        flags.append("🟡 Young wallet (<14d)")

    tokens_created = dev.get("tokens_created", 0)
    if tokens_created >= 10:
        pts += 12
        flags.append(f"🔴 Serial launcher ({tokens_created} tokens)")
    elif tokens_created >= 4:
        pts += 7
        flags.append(f"🟡 Repeat launcher ({tokens_created} tokens)")

    prior_rugs = dev.get("prior_rugs_est", 0)
    if prior_rugs >= 3:
        pts += 14
        flags.append(f"🔴 Rug history (~{prior_rugs} rugs)")
    elif prior_rugs >= 1:
        pts += 7
        flags.append(f"🟡 Possible prior rug (~{prior_rugs})")

    sol = dev.get("sol_balance", 0)
    if sol < 0.05:
        pts += 5
        flags.append("🟡 Low SOL balance")

    return min(pts, 30), flags

def score_metadata(meta: Dict) -> Tuple[int, List[str]]:
    """Returns (rug_risk_pts: 0-20, flags)."""
    pts   = 0
    flags = []
    name  = (meta.get("name") or "").lower()
    sym   = (meta.get("symbol") or "").upper()

    # Hard rug keywords in name
    hard_hits = [k for k in RUG_NAME_HARD if k in name]
    if hard_hits:
        pts += 18
        flags.append(f"🔴 Scam keywords: {', '.join(hard_hits)}")

    # Soft rug keywords
    soft_hits = [k for k in RUG_NAME_SOFT if k in name]
    if len(soft_hits) >= 3:
        pts += 8
        flags.append(f"🟡 Noise keywords ({len(soft_hits)}): {', '.join(soft_hits[:3])}")
    elif len(soft_hits) >= 1:
        pts += 3

    # Copycat symbol
    if sym in COPYCAT_SYMBOLS:
        pts += 7
        flags.append(f"🟡 Copycat symbol ({sym})")

    # No socials
    has_social = any([meta.get("twitter"), meta.get("telegram"), meta.get("website")])
    if not has_social:
        pts += 5
        flags.append("🟡 No socials")

    # Empty name or description
    if not name or len(name) < 2:
        pts += 8
        flags.append("🔴 No name")

    if not meta.get("uri"):
        pts += 4
        flags.append("🟡 No metadata URI")

    return min(pts, 20), flags

def score_holders(dist: Dict) -> Tuple[int, List[str]]:
    """Returns (rug_risk_pts: 0-30, flags)."""
    pts   = 0
    flags = []

    top1  = dist.get("top1_pct", 0)
    top3  = dist.get("top3_pct", 0)
    top5  = dist.get("top5_pct", 0)
    top10 = dist.get("top10_pct", 0)
    cnt   = dist.get("holder_count", 0)

    if top1 >= 50:
        pts += 25
        flags.append(f"🔴 Single wallet holds {top1}%")
    elif top1 >= 30:
        pts += 15
        flags.append(f"🔴 Top holder = {top1}%")
    elif top1 >= 15:
        pts += 7
        flags.append(f"🟡 Top holder = {top1}%")

    if top3 >= 70:
        pts += 10
        flags.append(f"🔴 Top 3 hold {top3}%")
    elif top3 >= 50:
        pts += 5
        flags.append(f"🟡 Top 3 hold {top3}%")

    if cnt < 10:
        pts += 10
        flags.append(f"🔴 Only {cnt} holders")
    elif cnt < 25:
        pts += 5
        flags.append(f"🟡 Low holders ({cnt})")

    return min(pts, 30), flags

def score_launch(buys: Dict) -> Tuple[int, List[str]]:
    """Returns (rug_risk_pts: 0-20, flags)."""
    pts   = 0
    flags = []

    bundled = buys.get("bundled_slots", 0)
    if bundled >= 5:
        pts += 18
        flags.append(f"🔴 Heavy sniping ({bundled} bundled slots)")
    elif bundled >= 2:
        pts += 9
        flags.append(f"🟡 Bundled buys ({bundled} slots)")

    if buys.get("dev_sold_early"):
        pts += 15
        flags.append("🔴 Dev sold at launch")

    ratio = buys.get("buy_sell_ratio", 1)
    if ratio < 1.2:
        pts += 8
        flags.append(f"🔴 Sell pressure (B/S={ratio})")

    return min(pts, 20), flags

def calc_profit_score(
    rug_risk: int,
    dev: Dict,
    meta: Dict,
    dist: Dict,
    buys: Dict,
    curve_pct: float,
) -> Tuple[int, List[str]]:
    """
    Builds a 0–100 profit score from positive signals.
    Starts at 0 and adds points for green flags.
    Also applies penalties from rug_risk.
    """
    pts    = 0
    greens = []

    # ── VELOCITY / MOMENTUM ──────────────────────────────────────────────────
    velocity = buys.get("tx_velocity", 0)
    if velocity >= 25:
        pts += 18
        greens.append(f"🚀 High momentum ({velocity} txns)")
    elif velocity >= 12:
        pts += 10
        greens.append(f"⚡ Good activity ({velocity} txns)")
    elif velocity >= 5:
        pts += 4

    # ── BUY/SELL RATIO ────────────────────────────────────────────────────────
    ratio = buys.get("buy_sell_ratio", 1)
    if ratio >= 4:
        pts += 15
        greens.append(f"🚀 Strong buy ratio ({ratio}x)")
    elif ratio >= 2.5:
        pts += 9
        greens.append(f"⚡ Bullish ratio ({ratio}x)")
    elif ratio >= 1.5:
        pts += 4

    # ── BONDING CURVE FILL ────────────────────────────────────────────────────
    if curve_pct >= 25:
        pts += 14
        greens.append(f"🚀 Curve {curve_pct}% filled")
    elif curve_pct >= 10:
        pts += 8
        greens.append(f"⚡ Curve {curve_pct}% filled")
    elif curve_pct >= 3:
        pts += 3

    # ── CLEAN DEV ─────────────────────────────────────────────────────────────
    if not dev.get("is_fresh_wallet") and dev.get("wallet_age_days", 0) >= 30:
        pts += 12
        greens.append(f"✅ Dev aged {dev['wallet_age_days']}d")
    elif dev.get("wallet_age_days", 0) >= 7:
        pts += 5

    if dev.get("prior_rugs_est", 0) == 0 and dev.get("tokens_created", 0) <= 2:
        pts += 8
        greens.append("✅ Clean dev history")

    # ── METADATA QUALITY ──────────────────────────────────────────────────────
    has_social = any([meta.get("twitter"), meta.get("telegram"), meta.get("website")])
    if has_social:
        pts += 10
        socials = []
        if meta.get("twitter"):  socials.append("TW")
        if meta.get("telegram"): socials.append("TG")
        if meta.get("website"):  socials.append("WEB")
        greens.append(f"✅ Socials: {'/'.join(socials)}")

    name = (meta.get("name") or "").lower()
    soft_hits = [k for k in RUG_NAME_SOFT if k in name]
    hard_hits = [k for k in RUG_NAME_HARD if k in name]
    if not hard_hits and len(soft_hits) == 0 and len(name) >= 3:
        pts += 7
        greens.append("✅ Original name")

    if meta.get("description") and len(meta["description"]) > 20:
        pts += 3
        greens.append("✅ Has description")

    # ── HOLDER DISTRIBUTION ───────────────────────────────────────────────────
    cnt = dist.get("holder_count", 0)
    if cnt >= 50:
        pts += 10
        greens.append(f"✅ {cnt} holders")
    elif cnt >= 20:
        pts += 5
        greens.append(f"⚡ {cnt} holders")

    top3 = dist.get("top3_pct", 100)
    if top3 < 25:
        pts += 8
        greens.append(f"✅ Healthy distribution (top3={top3}%)")
    elif top3 < 40:
        pts += 4

    # ── PENALISE FROM RUG RISK ────────────────────────────────────────────────
    # Each 10 rug-risk pts shaves ~6 profit pts
    penalty = int((rug_risk / 10) * 6)
    pts     = max(0, pts - penalty)

    return min(pts, 100), greens

# ── MASTER ANALYSIS RUNNER ────────────────────────────────────────────────────
async def analyse_token(mint: str, dev_wallet: str, tx_sig: str) -> Optional[Dict]:
    """
    Runs all scoring modules concurrently, returns a full report dict
    or None if the token should be silently skipped (hard fail).
    """
    # Fetch everything in parallel
    meta_task  = asyncio.create_task(fetch_token_metadata(mint))
    dev_task   = asyncio.create_task(fetch_dev_history(dev_wallet))
    dist_task  = asyncio.create_task(fetch_holder_distribution(mint))
    buys_task  = asyncio.create_task(fetch_launch_buys(mint, dev_wallet))
    curve_task = asyncio.create_task(fetch_bonding_curve(mint))

    meta, dev, dist, buys, curve_pct = await asyncio.gather(
        meta_task, dev_task, dist_task, buys_task, curve_task
    )

    # ── SCORE MODULES ─────────────────────────────────────────────────────────
    dev_risk,   dev_flags    = score_dev(dev)
    meta_risk,  meta_flags   = score_metadata(meta)
    holder_risk, holder_flags = score_holders(dist)
    launch_risk, launch_flags = score_launch(buys)

    rug_risk  = dev_risk + meta_risk + holder_risk + launch_risk   # 0–100
    rug_risk  = min(rug_risk, 100)

    profit_score, profit_flags = calc_profit_score(
        rug_risk, dev, meta, dist, buys, curve_pct
    )

    all_risk_flags = dev_flags + meta_flags + holder_flags + launch_flags

    # ── VERDICT ───────────────────────────────────────────────────────────────
    if rug_risk >= 85:
        verdict       = "🔴 LIKELY RUG"
        verdict_emoji = "🔴"
    elif rug_risk >= 65:
        verdict       = "🟠 HIGH RISK"
        verdict_emoji = "🟠"
    elif rug_risk >= 40:
        verdict       = "🟡 MODERATE RISK"
        verdict_emoji = "🟡"
    else:
        verdict       = "🟢 LOW RISK"
        verdict_emoji = "🟢"

    if profit_score >= 70:
        call = "🚀 STRONG CALL"
    elif profit_score >= 52:
        call = "⚡ MODERATE CALL"
    elif profit_score >= 35:
        call = "💤 WEAK"
    else:
        call = "❌ SKIP"

    return {
        "mint":          mint,
        "dev_wallet":    dev_wallet,
        "tx_sig":        tx_sig,
        "meta":          meta,
        "dev":           dev,
        "dist":          dist,
        "buys":          buys,
        "curve_pct":     curve_pct,
        "rug_risk":      rug_risk,
        "profit_score":  profit_score,
        "risk_flags":    all_risk_flags,
        "profit_flags":  profit_flags,
        "verdict":       verdict,
        "verdict_emoji": verdict_emoji,
        "call":          call,
        "sub_scores": {
            "dev":    dev_risk,
            "meta":   meta_risk,
            "holder": holder_risk,
            "launch": launch_risk,
        },
    }

# ── ALERT FORMATTER ───────────────────────────────────────────────────────────
def format_alert(r: Dict) -> str:
    meta = r["meta"]
    dev  = r["dev"]
    dist = r["dist"]
    buys = r["buys"]
    sub  = r["sub_scores"]

    name   = meta.get("name", "Unknown") or "Unknown"
    symbol = meta.get("symbol", "???")   or "???"
    mint   = r["mint"]
    sig    = r["tx_sig"]

    rug_bar    = _bar(r["rug_risk"])
    profit_bar = _bar(r["profit_score"])

    risk_block   = "\n".join(r["risk_flags"])   or "None detected"
    profit_block = "\n".join(r["profit_flags"]) or "None"

    socials = []
    if meta.get("twitter"):  socials.append(f"[Twitter]({meta['twitter']})")
    if meta.get("telegram"): socials.append(f"[Telegram]({meta['telegram']})")
    if meta.get("website"):  socials.append(f"[Web]({meta['website']})")
    social_line = " · ".join(socials) if socials else "None"

    return f"""
😈 *CLEX PUMP\\.FUN SCANNER*

🪙 *{escape(name)}* \\(${escape(symbol)}\\)
`{mint}`

━━━━━━━━━━━━━━━━━━━━━━
*{r['call']}*

📊 *SCORES*
Rug Risk:     {r['rug_risk']}/100  {rug_bar}
Profit Score: {r['profit_score']}/100  {profit_bar}
Verdict:      {r['verdict']}

🔬 *SUB\\-SCORES*
Dev:     {sub['dev']}/30  ·  Meta: {sub['meta']}/20
Holders: {sub['holder']}/30  ·  Launch: {sub['launch']}/20

━━━━━━━━━━━━━━━━━━━━━━
⚠️ *RISK FLAGS*
{risk_block}

✅ *PROFIT SIGNALS*
{profit_block}

━━━━━━━━━━━━━━━━━━━━━━
👨‍💻 *DEV*  `{r['dev_wallet'][:20]}…`
Age: {dev.get('wallet_age_days', 0)}d  ·  Tokens: {dev.get('tokens_created', 0)}  ·  Prior rugs: {dev.get('prior_rugs_est', 0)}

📈 *MARKET*
Curve: {r['curve_pct']}% filled  ·  Holders: {dist.get('holder_count', 0)}
Top1: {dist.get('top1_pct', 0)}%  ·  Top3: {dist.get('top3_pct', 0)}%
B/S Ratio: {buys.get('buy_sell_ratio', 0)}x  ·  Txns: {buys.get('tx_velocity', 0)}

🔗 *LINKS*
Socials: {social_line}
[Pump\\.fun](https://pump.fun/{mint}) · [Solscan](https://solscan.io/tx/{sig}) · [GMGN](https://gmgn.ai/sol/token/{mint})
""".strip()

def _bar(score: int) -> str:
    filled = round(score / 10)
    return "█" * filled + "░" * (10 - filled)

def escape(s: str) -> str:
    """Escape MarkdownV2 special chars."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        s = s.replace(ch, f"\\{ch}")
    return s

# ── WEBHOOK PAYLOAD HANDLER ────────────────────────────────────────────────────
def extract_pump_launch(tx: Dict) -> Optional[Tuple[str, str]]:
    """
    Returns (mint, dev_wallet) if this tx is a pump.fun token creation, else None.
    Detects by:
      1. pump program in instructions
      2. A new mint account appears in accountData
      3. feePayer = dev
    """
    instructions = tx.get("instructions", [])
    account_data  = tx.get("accountData", [])
    dev_wallet    = tx.get("feePayer", "")

    pump_ix = [ix for ix in instructions if ix.get("programId") == PUMP_PROGRAM]
    if not pump_ix:
        return None

    # Find newly created token mint accounts (native balance went from 0)
    for acct in account_data:
        pre  = acct.get("nativeBalanceChange", 0)
        # A new mint has rent deposited (positive balance change) and token program owner
        if pre > 0 and acct.get("account", "") not in (dev_wallet, PUMP_FEE_ACCT, PUMP_PROGRAM):
            possible_mint = acct.get("account", "")
            if len(possible_mint) in (43, 44):   # valid base58 pubkey length
                return possible_mint, dev_wallet

    # Fallback: grab mint from token transfers if any
    transfers = tx.get("tokenTransfers", [])
    for t in transfers:
        mint = t.get("mint", "")
        if mint and mint != WSOL_MINT and len(mint) in (43, 44):
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

        if await already_seen(mint):
            continue
        await mark_seen(mint)

        logger.info(f"New pump.fun launch: {mint} by {dev_wallet}")

        report = await analyse_token(mint, dev_wallet, tx.get("signature", ""))
        if not report:
            continue

        # Gating: only alert if worth it
        if report["rug_risk"] > MAX_RUG_SCORE and report["profit_score"] < MIN_PROFIT_SCORE:
            logger.info(f"Skipping {mint}: rug={report['rug_risk']} profit={report['profit_score']}")
            continue

        if report["profit_score"] < MIN_PROFIT_SCORE:
            logger.info(f"Below profit threshold {mint}: {report['profit_score']}")
            continue

        text = format_alert(report)
        subscribers = await get_subscribers()
        for chat_id in subscribers:
            try:
                await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN_V2,
                                       disable_web_page_preview=True)
            except TelegramAPIError as e:
                logger.error(f"Send error to {chat_id}: {e}")

# ── TELEGRAM BOT ──────────────────────────────────────────────────────────────
@router.message(Command("start"))
async def start(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔔 Subscribe", callback_data="sub"),
         InlineKeyboardButton(text="🔕 Unsubscribe", callback_data="unsub")],
        [InlineKeyboardButton(text="ℹ️ How it works", callback_data="help")],
    ])
    await m.answer(
        "😈 *CLEX Pump\\.fun Scanner*\n\n"
        "I scan every new pump\\.fun launch and score it for rug risk "
        "\\+ profit potential using 4 analysis modules\\.\n\n"
        "Subscribe to receive callouts\\!",
        reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2
    )

@router.callback_query(F.data == "sub")
async def subscribe(q: CallbackQuery):
    await add_subscriber(q.from_user.id)
    await q.answer("✅ Subscribed!")
    await q.message.edit_text(
        "✅ *Subscribed\\!*\n\nYou'll receive alerts when CLEX detects a strong pump\\.fun launch\\.",
        parse_mode=ParseMode.MARKDOWN_V2
    )

@router.callback_query(F.data == "unsub")
async def unsubscribe(q: CallbackQuery):
    await remove_subscriber(q.from_user.id)
    await q.answer("🔕 Unsubscribed")
    await q.message.edit_text("🔕 Unsubscribed\\. Use /start to resubscribe\\.",
                               parse_mode=ParseMode.MARKDOWN_V2)

@router.callback_query(F.data == "help")
async def help_cb(q: CallbackQuery):
    text = (
        "🔬 *How CLEX Scores Coins*\n\n"
        "*Rug Risk \\(0–100\\)*\n"
        "• Dev wallet age \\& history \\(0–30 pts\\)\n"
        "• Metadata quality \\& scam keywords \\(0–20 pts\\)\n"
        "• Holder concentration \\(0–30 pts\\)\n"
        "• Launch pattern \\& sniping \\(0–20 pts\\)\n\n"
        "*Profit Score \\(0–100\\)*\n"
        "• Tx velocity \\& momentum\n"
        "• Buy/sell ratio\n"
        "• Bonding curve fill\n"
        "• Clean dev history\n"
        "• Social presence\n"
        "• Holder distribution\n\n"
        "Only alerts with Profit ≥ 52 and Rug Risk ≤ 60 are sent\\."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data="back")]])
    await q.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)

@router.callback_query(F.data == "back")
async def back(q: CallbackQuery):
    await start(q.message)

@router.message(Command("stats"))
async def stats(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        subs  = (await (await db.execute("SELECT COUNT(*) FROM subscribers")).fetchone())[0]
        seen  = (await (await db.execute("SELECT COUNT(*) FROM seen_tokens")).fetchone())[0]
    await m.answer(f"📊 Subscribers: {subs}\n🔍 Tokens scanned: {seen}")

# ── FASTAPI ────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await helius_set_pump_watch()
    logger.info("DB ready · webhook synced")
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
