import asyncio
import aiosqlite
import aiohttp
import logging
import os
import time
import json as jsonlib
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
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
HELIUS_API_KEY   = os.getenv("HELIUS_API_KEY")
HELIUS_WEBHOOK_ID = os.getenv("HELIUS_WEBHOOK_ID")
HELIUS_BASE      = "https://api.helius.xyz/v0"
HELIUS_RPC       = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
DB_PATH          = "clex.db"
WEBHOOK_SECRET   = os.getenv("WEBHOOK_SECRET", "change_this")
MIN_PROFIT_SCORE = int(os.getenv("MIN_PROFIT_SCORE", "5"))
MAX_RUG_SCORE    = int(os.getenv("MAX_RUG_SCORE", "50"))

# ── PUMP.FUN CONSTANTS ────────────────────────────────────────────────────────
PUMP_PROGRAM  = "6EF8rQNi1oDEZ7zrKsCauKMorruBaGECQw6B469Z7z8"
WSOL_MINT     = "So11111111111111111111111111111111111111112"
PUMP_FEE_ACCT = "CebN5WGQ4jvEPvsVU4EoHEpgznyZKUD7yo2MXjj4oHBn"
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

# ── HELIUS WEBHOOK SYNC ───────────────────────────────────────────────────────
async def helius_set_pump_watch():
    if not HELIUS_WEBHOOK_ID:
        logger.warning("HELIUS_WEBHOOK_ID not set")
        return
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{HELIUS_BASE}/webhooks/{HELIUS_WEBHOOK_ID}",
                params={"api-key": HELIUS_API_KEY},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                r.raise_for_status()
                existing = await r.json()
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

# ── RPC HELPERS ───────────────────────────────────────────────────────────────
async def rpc(method: str, params: list, timeout: int = 6) -> Optional[Dict]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(HELIUS_RPC,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status == 200:
                    return (await r.json()).get("result")
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
                    return (await r.json()).get("result")
    except Exception as e:
        logger.debug(f"DAS {method} error: {e}")
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

async def fetch_holder_distribution(mint: str) -> Dict:
    result = await rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    if not result:
        return {"top1_pct": 0, "top3_pct": 0, "top5_pct": 0, "holder_count": 0}
    accounts = result.get("value", [])
    amounts  = []
    for acct in accounts:
        try:
            amounts.append(float(acct.get("uiAmount") or 0))
        except:
            pass
    if not amounts:
        return {"top1_pct": 0, "top3_pct": 0, "top5_pct": 0, "holder_count": 0}
    total = TOTAL_SUPPLY
    return {
        "top1_pct":     round(amounts[0] / total * 100, 2),
        "top3_pct":     round(sum(amounts[:3]) / total * 100, 2),
        "top5_pct":     round(sum(amounts[:5]) / total * 100, 2),
        "holder_count": len(amounts),
    }

async def fetch_dev_history(dev_wallet: str) -> Dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT data, cached_at FROM dev_cache WHERE address=?", (dev_wallet,))
        row = await cur.fetchone()
        if row and (time.time() - row[1]) < 900:
            return jsonlib.loads(row[0])

    info = {
        "wallet_age_days": 0,
        "tokens_created":  0,
        "prior_rugs_est":  0,
        "sol_balance":     0,
        "is_fresh_wallet": True,
        "flags":           [],
    }

    bal = await rpc("getBalance", [dev_wallet])
    if bal:
        info["sol_balance"] = round(bal.get("value", 0) / 1e9, 4)

    sigs = await rpc("getSignaturesForAddress",
                     [dev_wallet, {"limit": 50, "commitment": "confirmed"}])
    if not sigs:
        info["flags"].append("NO_TX_HISTORY")
    else:
        oldest_ts = sigs[-1].get("blockTime") if sigs else None
        if oldest_ts:
            info["wallet_age_days"] = round((time.time() - oldest_ts) / 86400, 1)
            info["is_fresh_wallet"] = info["wallet_age_days"] < 3
        if info["is_fresh_wallet"]:
            info["flags"].append("FRESH_WALLET")

    created = await das("getAssetsByCreator",
                        {"creatorAddress": dev_wallet, "onlyVerified": False,
                         "limit": 20, "page": 1})
    if created:
        items = created.get("items", [])
        info["tokens_created"] = len(items)
        short_lived = sum(
            1 for item in items
            if item.get("token_info", {}).get("supply", TOTAL_SUPPLY) is not None
            and int(item.get("token_info", {}).get("supply", TOTAL_SUPPLY)) < TOTAL_SUPPLY * 0.05
        )
        info["prior_rugs_est"] = short_lived
        if short_lived > 0:
            info["flags"].append(f"PRIOR_RUGS~{short_lived}")
        if info["tokens_created"] > 5:
            info["flags"].append(f"SERIAL_LAUNCHER_{info['tokens_created']}")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO dev_cache VALUES (?,?,?)",
                         (dev_wallet, jsonlib.dumps(info), time.time()))
        await db.commit()

    return info

async def fetch_launch_buys(mint: str, dev_wallet: str) -> Dict:
    sigs = await rpc("getSignaturesForAddress",
                     [mint, {"limit": 30, "commitment": "confirmed"}])
    if not sigs:
        return {"bundled_slots": 0, "dev_sold_early": False,
                "buy_sell_ratio": 0, "tx_velocity": 0}

    slot_counts: Dict[int, int] = {}
    for s in sigs:
        slot = s.get("slot", 0)
        slot_counts[slot] = slot_counts.get(slot, 0) + 1

    tx_count      = len(sigs)
    buy_count     = max(1, int(tx_count * 0.7))
    sell_count    = tx_count - buy_count
    bundled_slots = sum(1 for cnt in slot_counts.values() if cnt >= 3)

    return {
        "bundled_slots":  bundled_slots,
        "dev_sold_early": False,
        "buy_sell_ratio": round(buy_count / max(sell_count, 1), 2),
        "tx_velocity":    tx_count,
    }

async def fetch_bonding_curve(mint: str) -> float:
    holders = await rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    if not holders:
        return 0.0
    accounts = holders.get("value", [])
    if accounts:
        curve_balance = float(accounts[0].get("uiAmount") or 0)
        return max(0.0, min(100.0, round((1 - curve_balance / TOTAL_SUPPLY) * 100, 2)))
    return 0.0

# ── SCORING ENGINE ────────────────────────────────────────────────────────────
def score_dev(dev: Dict) -> Tuple[int, List[str]]:
    pts, flags = 0, []
    if dev.get("is_fresh_wallet"):
        pts += 18; flags.append("🔴 Fresh wallet (<3d)")
    elif dev.get("wallet_age_days", 999) < 14:
        pts += 10; flags.append("🟡 Young wallet (<14d)")
    tc = dev.get("tokens_created", 0)
    if tc >= 10:
        pts += 12; flags.append(f"🔴 Serial launcher ({tc} tokens)")
    elif tc >= 4:
        pts += 7;  flags.append(f"🟡 Repeat launcher ({tc} tokens)")
    pr = dev.get("prior_rugs_est", 0)
    if pr >= 3:
        pts += 14; flags.append(f"🔴 Rug history (~{pr} rugs)")
    elif pr >= 1:
        pts += 7;  flags.append(f"🟡 Possible prior rug (~{pr})")
    if dev.get("sol_balance", 0) < 0.05:
        pts += 5;  flags.append("🟡 Low SOL balance")
    return min(pts, 30), flags

def score_metadata(meta: Dict) -> Tuple[int, List[str]]:
    pts, flags = 0, []
    name = (meta.get("name") or "").lower()
    sym  = (meta.get("symbol") or "").upper()
    hard_hits = [k for k in RUG_NAME_HARD if k in name]
    if hard_hits:
        pts += 18; flags.append(f"🔴 Scam keywords: {', '.join(hard_hits)}")
    soft_hits = [k for k in RUG_NAME_SOFT if k in name]
    if len(soft_hits) >= 3:
        pts += 8;  flags.append(f"🟡 Noise keywords ({len(soft_hits)}): {', '.join(soft_hits[:3])}")
    elif soft_hits:
        pts += 3
    if sym in COPYCAT_SYMBOLS:
        pts += 7;  flags.append(f"🟡 Copycat symbol ({sym})")
    if not any([meta.get("twitter"), meta.get("telegram"), meta.get("website")]):
        pts += 5;  flags.append("🟡 No socials")
    if not name or len(name) < 2:
        pts += 8;  flags.append("🔴 No name")
    if not meta.get("uri"):
        pts += 4;  flags.append("🟡 No metadata URI")
    return min(pts, 20), flags

def score_holders(dist: Dict) -> Tuple[int, List[str]]:
    pts, flags = 0, []
    top1 = dist.get("top1_pct", 0)
    top3 = dist.get("top3_pct", 0)
    cnt  = dist.get("holder_count", 0)
    if top1 >= 50:
        pts += 25; flags.append(f"🔴 Single wallet holds {top1}%")
    elif top1 >= 30:
        pts += 15; flags.append(f"🔴 Top holder = {top1}%")
    elif top1 >= 15:
        pts += 7;  flags.append(f"🟡 Top holder = {top1}%")
    if top3 >= 70:
        pts += 10; flags.append(f"🔴 Top 3 hold {top3}%")
    elif top3 >= 50:
        pts += 5;  flags.append(f"🟡 Top 3 hold {top3}%")
    if cnt < 10:
        pts += 10; flags.append(f"🔴 Only {cnt} holders")
    elif cnt < 25:
        pts += 5;  flags.append(f"🟡 Low holders ({cnt})")
    return min(pts, 30), flags

def score_launch(buys: Dict) -> Tuple[int, List[str]]:
    pts, flags = 0, []
    bundled = buys.get("bundled_slots", 0)
    if bundled >= 5:
        pts += 18; flags.append(f"🔴 Heavy sniping ({bundled} bundled slots)")
    elif bundled >= 2:
        pts += 9;  flags.append(f"🟡 Bundled buys ({bundled} slots)")
    if buys.get("dev_sold_early"):
        pts += 15; flags.append("🔴 Dev sold at launch")
    if buys.get("buy_sell_ratio", 1) < 1.2:
        pts += 8;  flags.append(f"🔴 Sell pressure (B/S={buys.get('buy_sell_ratio',0)})")
    return min(pts, 20), flags

def calc_profit_score(rug_risk, dev, meta, dist, buys, curve_pct) -> Tuple[int, List[str]]:
    pts, greens = 0, []
    v = buys.get("tx_velocity", 0)
    if v >= 25:
        pts += 18; greens.append(f"🚀 High momentum ({v} txns)")
    elif v >= 12:
        pts += 10; greens.append(f"⚡ Good activity ({v} txns)")
    elif v >= 5:
        pts += 4
    ratio = buys.get("buy_sell_ratio", 1)
    if ratio >= 4:
        pts += 15; greens.append(f"🚀 Strong buy ratio ({ratio}x)")
    elif ratio >= 2.5:
        pts += 9;  greens.append(f"⚡ Bullish ratio ({ratio}x)")
    elif ratio >= 1.5:
        pts += 4
    if curve_pct >= 25:
        pts += 14; greens.append(f"🚀 Curve {curve_pct}% filled")
    elif curve_pct >= 10:
        pts += 8;  greens.append(f"⚡ Curve {curve_pct}% filled")
    elif curve_pct >= 3:
        pts += 3
    if not dev.get("is_fresh_wallet") and dev.get("wallet_age_days", 0) >= 30:
        pts += 12; greens.append(f"✅ Dev aged {dev['wallet_age_days']}d")
    elif dev.get("wallet_age_days", 0) >= 7:
        pts += 5
    if dev.get("prior_rugs_est", 0) == 0 and dev.get("tokens_created", 0) <= 2:
        pts += 8;  greens.append("✅ Clean dev history")
    if any([meta.get("twitter"), meta.get("telegram"), meta.get("website")]):
        s = []
        if meta.get("twitter"):  s.append("TW")
        if meta.get("telegram"): s.append("TG")
        if meta.get("website"):  s.append("WEB")
        pts += 10; greens.append(f"✅ Socials: {'/'.join(s)}")
    name = (meta.get("name") or "").lower()
    soft = [k for k in RUG_NAME_SOFT if k in name]
    hard = [k for k in RUG_NAME_HARD if k in name]
    if not hard and not soft and len(name) >= 3:
        pts += 7;  greens.append("✅ Original name")
    if meta.get("description") and len(meta["description"]) > 20:
        pts += 3;  greens.append("✅ Has description")
    cnt = dist.get("holder_count", 0)
    if cnt >= 50:
        pts += 10; greens.append(f"✅ {cnt} holders")
    elif cnt >= 20:
        pts += 5;  greens.append(f"⚡ {cnt} holders")
    if dist.get("top3_pct", 100) < 25:
        pts += 8;  greens.append(f"✅ Healthy distribution (top3={dist.get('top3_pct',0)}%)")
    elif dist.get("top3_pct", 100) < 40:
        pts += 4
    pts = max(0, pts - int((rug_risk / 10) * 6))
    return min(pts, 100), greens

# ── ANALYSIS RUNNER ───────────────────────────────────────────────────────────
async def analyse_token(mint: str, dev_wallet: str, tx_sig: str) -> Optional[Dict]:
    meta, dev, dist, buys, curve_pct = await asyncio.gather(
        fetch_token_metadata(mint),
        fetch_dev_history(dev_wallet),
        fetch_holder_distribution(mint),
        fetch_launch_buys(mint, dev_wallet),
        fetch_bonding_curve(mint),
    )
    dev_risk,    dev_flags    = score_dev(dev)
    meta_risk,   meta_flags   = score_metadata(meta)
    holder_risk, holder_flags = score_holders(dist)
    launch_risk, launch_flags = score_launch(buys)
    rug_risk = min(dev_risk + meta_risk + holder_risk + launch_risk, 100)
    profit_score, profit_flags = calc_profit_score(rug_risk, dev, meta, dist, buys, curve_pct)

    if rug_risk >= 85:   verdict = "🔴 LIKELY RUG"
    elif rug_risk >= 65: verdict = "🟠 HIGH RISK"
    elif rug_risk >= 40: verdict = "🟡 MODERATE RISK"
    else:                verdict = "🟢 LOW RISK"

    if profit_score >= 70:   call = "🚀 STRONG CALL"
    elif profit_score >= 52: call = "⚡ MODERATE CALL"
    elif profit_score >= 35: call = "💤 WEAK"
    else:                    call = "❌ SKIP"

    return {
        "mint": mint, "dev_wallet": dev_wallet, "tx_sig": tx_sig,
        "meta": meta, "dev": dev, "dist": dist, "buys": buys,
        "curve_pct": curve_pct, "rug_risk": rug_risk,
        "profit_score": profit_score, "verdict": verdict, "call": call,
        "risk_flags":   dev_flags + meta_flags + holder_flags + launch_flags,
        "profit_flags": profit_flags,
        "sub_scores": {"dev": dev_risk, "meta": meta_risk,
                       "holder": holder_risk, "launch": launch_risk},
    }

# ── ALERT FORMATTER ───────────────────────────────────────────────────────────
def _bar(score: int) -> str:
    filled = round(score / 10)
    return "█" * filled + "░" * (10 - filled)

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

    socials = []
    if meta.get("twitter"):  socials.append(f"[Twitter]({meta['twitter']})")
    if meta.get("telegram"): socials.append(f"[Telegram]({meta['telegram']})")
    if meta.get("website"):  socials.append(f"[Web]({meta['website']})")
    social_line  = " · ".join(socials) if socials else "None"
    risk_block   = "\n".join(r["risk_flags"])   or "None detected"
    profit_block = "\n".join(r["profit_flags"]) or "None"

    return (
        f"😈 *CLEX PUMP.FUN SCANNER*\n\n"
        f"🪙 *{name}* (${symbol})\n"
        f"`{mint}`\n\n"
        f"*{r['call']}*\n\n"
        f"📊 *SCORES*\n"
        f"Rug Risk:  {r['rug_risk']}/100  {_bar(r['rug_risk'])}\n"
        f"Profit:    {r['profit_score']}/100  {_bar(r['profit_score'])}\n"
        f"Verdict:   {r['verdict']}\n\n"
        f"🔬 *SUB-SCORES*\n"
        f"Dev: {sub['dev']}/30 · Meta: {sub['meta']}/20 · "
        f"Holders: {sub['holder']}/30 · Launch: {sub['launch']}/20\n\n"
        f"⚠️ *RISK FLAGS*\n{risk_block}\n\n"
        f"✅ *PROFIT SIGNALS*\n{profit_block}\n\n"
        f"👨‍💻 *DEV* `{r['dev_wallet'][:20]}...`\n"
        f"Age: {dev.get('wallet_age_days',0)}d · "
        f"Tokens: {dev.get('tokens_created',0)} · "
        f"Prior rugs: {dev.get('prior_rugs_est',0)}\n\n"
        f"📈 *MARKET*\n"
        f"Curve: {r['curve_pct']}% · Holders: {dist.get('holder_count',0)} · "
        f"B/S: {buys.get('buy_sell_ratio',0)}x · Txns: {buys.get('tx_velocity',0)}\n\n"
        f"🔗 [Pump.fun](https://pump.fun/{mint}) · "
        f"[Solscan](https://solscan.io/tx/{sig}) · "
        f"[GMGN](https://gmgn.ai/sol/token/{mint})\n"
        f"Socials: {social_line}"
    )

# ── WEBHOOK HANDLER ───────────────────────────────────────────────────────────
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
        if await already_seen(mint):
            continue
        await mark_seen(mint)
        logger.info(f"New pump.fun launch: {mint} by {dev_wallet}")
        report = await analyse_token(mint, dev_wallet, tx.get("signature", ""))
        if not report:
            continue
        if report["rug_risk"] > MAX_RUG_SCORE:
            logger.info(f"Skipping {mint}: rug={report['rug_risk']} profit={report['profit_score']}")
            continue
        if report["profit_score"] < MIN_PROFIT_SCORE:
            logger.info(f"Below profit threshold {mint}: {report['profit_score']}")
            continue
        text = format_alert(report)
        for chat_id in await get_subscribers():
            try:
                await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN,
                                       disable_web_page_preview=True)
            except TelegramAPIError as e:
                logger.error(f"Send error to {chat_id}: {e}")

# ── TELEGRAM BOT ──────────────────────────────────────────────────────────────
@router.message(Command("start"))
async def start(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔔 Subscribe",   callback_data="sub"),
         InlineKeyboardButton(text="🔕 Unsubscribe", callback_data="unsub")],
        [InlineKeyboardButton(text="ℹ️ How it works", callback_data="help")],
    ])
    await m.answer(
        "😈 *CLEX Pump.fun Scanner*\n\n"
        "I scan every new pump.fun launch and score it for rug risk "
        "+ profit potential using 4 analysis modules.\n\n"
        "Subscribe to receive callouts!",
        reply_markup=kb, parse_mode=ParseMode.MARKDOWN
    )

@router.callback_query(F.data == "sub")
async def subscribe(q: CallbackQuery):
    await add_subscriber(q.from_user.id)
    await q.answer("✅ Subscribed!")
    await q.message.edit_text(
        "✅ *Subscribed!*\n\nYou'll receive alerts when CLEX detects a strong pump.fun launch.",
        parse_mode=ParseMode.MARKDOWN
    )

@router.callback_query(F.data == "unsub")
async def unsubscribe(q: CallbackQuery):
    await remove_subscriber(q.from_user.id)
    await q.answer("🔕 Unsubscribed")
    await q.message.edit_text("🔕 Unsubscribed. Use /start to resubscribe.")

@router.callback_query(F.data == "help")
async def help_cb(q: CallbackQuery):
    await q.message.edit_text(
        "🔬 *How CLEX Scores Coins*\n\n"
        "*Rug Risk (0-100)*\n"
        "• Dev wallet age & history (0-30 pts)\n"
        "• Metadata quality & scam keywords (0-20 pts)\n"
        "• Holder concentration (0-30 pts)\n"
        "• Launch pattern & sniping (0-20 pts)\n\n"
        "*Profit Score (0-100)*\n"
        "• Tx velocity & momentum\n"
        "• Buy/sell ratio\n"
        "• Bonding curve fill\n"
        "• Clean dev history\n"
        "• Social presence\n"
        "• Holder distribution",
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
    await m.answer(f"📊 Subscribers: {subs}\n🔍 Tokens scanned: {seen}")

# ── FASTAPI ────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await helius_set_pump_watch()
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(3)
    logger.info("DB ready · webhook synced")
    poll = asyncio.create_task(dp.start_polling(
        bot, allowed_updates=["message", "callback_query"]))
    logger.info("Bot live")
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
    logger.info(f"Webhook hit: {len(payload)} txns, type: {payload[0].get('type') if payload else 'empty'}")
    asyncio.create_task(process_payload(payload))
    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "alive"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
