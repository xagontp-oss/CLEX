"""
CLEX APEX — new-pool strategy engine.

Detection : Helius enhanced webhook (CREATE_POOL on Raydium) -> on_pool_event()
Screening : fail-fast, two stages (cheap on-chain checks now, confirmation at T+60s)
Execution : Jupiter swap API — live trading from a connected Phantom wallet
Exits     : four fixed profiles (A/B/C/D), hard stop, trailing stop, time stop
Interface : Telegram pages (router) — see the PAGES section.
"""
import asyncio
import base64
import html
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
import aiosqlite
import base58
from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.filters import Command, Filter
from aiogram.types import (BufferedInputFile, CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message)

import sniper as sn

logger = logging.getLogger("apex")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — every number from the strategy lives here
# ══════════════════════════════════════════════════════════════════════════════
DB_PATH = os.getenv("DB_PATH", "clex.db")

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
QUOTE_MINTS = {WSOL, USDC, USDT}
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# Screening
MIN_SUPPLY, MAX_SUPPLY = 100_000_000, 1_000_000_000_000
MAX_TOP10_PCT, MAX_SINGLE_PCT, MAX_DEV_PCT = 40.0, 15.0, 5.0
MIN_POOL_USD = 3_000.0
MAX_MCAP_USD = 500_000.0
MAX_MINT_TO_POOL_SECS = 30 * 60
MIN_UNIQUE_BUYERS = 5
CONFIRM_WINDOW_SECS = 60
MAX_TAX_PCT = 10.0                 # per side
PROBE_SOL = 0.05                   # size of the honeypot probe quotes
MAX_EVENT_AGE_SECS = 120           # ignore stale / backfilled webhook events

# pump.fun bonding-curve constants (liquidity is read from the curve's token balance, free)
PUMP_SUPPLY = 1_000_000_000
PUMP_K = 1_073_000_000 * 30.0          # virtual token reserve x virtual SOL reserve
PUMP_VIRT_OFFSET = 279_900_000         # virtual tokens - real tokens on the curve
PUMP_START_SOL = 30.0                  # virtual SOL at launch
PUMP_CHECK_AGES = [60, 120, 180, 240, 330, 450, 600]   # seconds after launch
PUMP_MAX_WATCHERS = 60                 # cap on coins being re-checked at once (credit guard)

# Execution
ENTRY_SLIPPAGE_BPS = 1500          # 15%
EXIT_SLIPPAGE_BPS = 2000           # 20%
PRIORITY_MAX_LAMPORTS = 1_000_000  # 0.001 SOL cap
PAPER_FEE_SOL = 0.0015             # simulated network+priority cost per paper trade
MIN_TRADE_USD = 5.0
MIN_WALLET_USD = 4.0
MAX_OPEN_POSITIONS = 20
MONITOR_INTERVAL = 30

# Risk
HARD_STOP_RATIO = 0.55             # -45%
TRAIL_ACTIVATE_RATIO = 2.0         # +100%
TRAIL_KEEP = 0.75                  # sell if price falls 25% from peak
TIME_STOP_SECS = 72 * 3600
TIME_STOP_RATIO = 1.20
WEBHOOK_SILENCE_SECS = 600
ACTIVE_HOURS_UTC = (8, 24)
DEAD_AFTER_FAILS = 10              # consecutive failed quotes before a position is written off

# Helius credit guard
MONTHLY_CREDIT_CEILING = 10_000_000
CREDIT_HALT_AT = 9_500_000
CREDIT_COST = {"getAsset": 10}     # everything else is estimated at 1 credit

# Exit profiles: tiers are (price ratio vs entry, fraction of the *remaining* bag)
PROFILES: Dict[str, Dict] = {
    "A": {"name": "Conservative", "icon": "🛡", "trail": False,
          "tiers": [(2.0, 1.0)],
          "blurb": "Sell everything at 2x. Simple, clean, repeatable.",
          "rows": ["+100% → sell 100%"]},
    "B": {"name": "Balanced", "icon": "⚖️", "trail": True,
          "tiers": [(2.0, 0.5), (5.0, 0.5)],
          "blurb": "Take your stake off the table at 2x, ride the rest.",
          "rows": ["+100% → sell 50% (stake recovered)",
                   "+400% → sell half of what's left",
                   "Final slice rides a 25% trailing stop"]},
    "C": {"name": "Aggressive", "icon": "🚀", "trail": True,
          "tiers": [(2.0, 0.30), (5.0, 0.25), (10.0, 0.25)],
          "blurb": "Keep a heavy moon bag for the rare 10x.",
          "rows": ["+100% → sell 30%", "+400% → sell 25% of the rest",
                   "+900% → sell 25% of the rest",
                   "Final slice rides a 25% trailing stop"]},
    "D": {"name": "Sniper Fast Exit", "icon": "⚡", "trail": False,
          "tiers": [(1.5, 1.0)],
          "blurb": "In, +50%, out. Compound the small wins.",
          "rows": ["+50% → sell 100%"]},
}
SIZE_OPTIONS_USD = [5, 10, 25, 50, 100]

# LP verification
BURN_OWNERS = {
    "1nc1nerator11111111111111111111111111111111",
    "11111111111111111111111111111111",
    "deadDeaDDeaDDeaDDeaDDeaDDeaDDeaDDeaDDeaDDeaD",
}
LOCK_PROGRAMS = {
    "strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m",   # Streamflow
    "LockrWmn6K5twhz3y9w1dQERbmgSaRkfnTeTKbpofwE",   # Raydium Burn & Earn / LP Lock (per docs.raydium.io)
}

# ══════════════════════════════════════════════════════════════════════════════
# JUPITER (free of Helius credits)
# ══════════════════════════════════════════════════════════════════════════════
JUP_KEY = os.getenv("JUPITER_API_KEY", "")
JUP_BASE = os.getenv("JUPITER_BASE") or (
    "https://api.jup.ag/swap/v1" if JUP_KEY else "https://lite-api.jup.ag/swap/v1")
_jup_sem = asyncio.Semaphore(3)
_decimals: Dict[str, int] = {}


def _jup_headers() -> Dict[str, str]:
    return {"x-api-key": JUP_KEY} if JUP_KEY else {}


async def jup_quote(inp: str, out: str, amount: int, slippage_bps: int) -> Optional[Dict]:
    if amount <= 0:
        return None
    try:
        async with _jup_sem:
            async with aiohttp.ClientSession(headers=_jup_headers()) as s:
                async with s.get(f"{JUP_BASE}/quote",
                                 params={"inputMint": inp, "outputMint": out,
                                         "amount": int(amount),
                                         "slippageBps": slippage_bps,
                                         "restrictIntermediateTokens": "true"},
                                 timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200:
                        return None
                    q = await r.json()
        if not isinstance(q, dict) or "error" in q or not q.get("outAmount"):
            return None
        return q
    except Exception as e:
        logger.debug(f"jup_quote: {e}")
        return None


async def jup_swap_tx(quote: Dict, pubkey: str) -> Optional[bytes]:
    try:
        async with aiohttp.ClientSession(headers=_jup_headers()) as s:
            async with s.post(f"{JUP_BASE}/swap",
                              json={"quoteResponse": quote, "userPublicKey": pubkey,
                                    "wrapAndUnwrapSol": True,
                                    "dynamicComputeUnitLimit": True,
                                    "prioritizationFeeLamports": {
                                        "priorityLevelWithMaxLamports": {
                                            "maxLamports": PRIORITY_MAX_LAMPORTS,
                                            "priorityLevel": "high"}}},
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return None
                data = await r.json()
        raw = data.get("swapTransaction")
        return base64.b64decode(raw) if raw else None
    except Exception as e:
        logger.debug(f"jup_swap_tx: {e}")
        return None


_sol_px = {"usd": 0.0, "at": 0.0}


async def sol_usd() -> float:
    if _sol_px["usd"] and time.time() - _sol_px["at"] < 60:
        return _sol_px["usd"]
    q = await jup_quote(WSOL, USDC, 1_000_000_000, 100)
    if q:
        _sol_px.update(usd=int(q["outAmount"]) / 1e6, at=time.time())
    return _sol_px["usd"]


# ══════════════════════════════════════════════════════════════════════════════
# HELIUS CALLS + CREDIT METER
# ══════════════════════════════════════════════════════════════════════════════
_credits_pending = 0
_month_cache = {"month": "", "credits": 0, "at": 0.0}


def _meter(method: str):
    global _credits_pending
    _credits_pending += CREDIT_COST.get(method, 1)


async def _h(method: str, params: list):
    _meter(method)
    return await sn._rpc(method, params)


async def _das(method: str, params: dict) -> Optional[Dict]:
    key = os.getenv("HELIUS_API_KEY", "")
    if not key:
        return None
    _meter(method)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://mainnet.helius-rpc.com/?api-key={key}",
                              json={"jsonrpc": "2.0", "id": 1,
                                    "method": method, "params": params},
                              timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    return (await r.json()).get("result")
    except Exception:
        pass
    return None


async def _flush_credits():
    global _credits_pending
    n, _credits_pending = _credits_pending, 0
    if not n:
        return
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT INTO apex_usage(day,credits) VALUES(?,?) "
            "ON CONFLICT(day) DO UPDATE SET credits=credits+excluded.credits", (day, n))
        await db.commit()
    _month_cache["at"] = 0.0


async def credits_used() -> Tuple[int, int]:
    """(today, this month) — estimated Helius credits spent by Apex."""
    now = datetime.now(timezone.utc)
    day, month = now.strftime("%Y-%m-%d"), now.strftime("%Y-%m")
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute("SELECT credits FROM apex_usage WHERE day=?", (day,))
        row = await cur.fetchone()
        cur = await db.execute(
            "SELECT COALESCE(SUM(credits),0) FROM apex_usage WHERE day LIKE ?",
            (month + "%",))
        m = (await cur.fetchone())[0]
    return (row[0] if row else 0) + _credits_pending, int(m) + _credits_pending


async def _budget_ok() -> bool:
    if time.time() - _month_cache["at"] > 60:
        _, m = await credits_used()
        _month_cache.update(credits=m, at=time.time())
    return _month_cache["credits"] < CREDIT_HALT_AT


# ══════════════════════════════════════════════════════════════════════════════
# ED25519 — wallet vs. program-owned account (pool vaults are off-curve PDAs)
# ══════════════════════════════════════════════════════════════════════════════
_P = 2 ** 255 - 19
_D = (-121665 * pow(121666, -1, _P)) % _P


def is_wallet_address(addr: str) -> bool:
    """True when the address is a normal ed25519 key (a person), False for PDAs."""
    try:
        raw = base58.b58decode(addr)
        if len(raw) != 32:
            return False
        y = int.from_bytes(raw, "little") & ((1 << 255) - 1)
        if y >= _P:
            return False
        u, v = (y * y - 1) % _P, (_D * y * y + 1) % _P
        x2 = u * pow(v, -1, _P) % _P
        return x2 == 0 or pow(x2, (_P - 1) // 2, _P) == 1
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# STORAGE
# ══════════════════════════════════════════════════════════════════════════════
_blacklist: set = set()
_stats = {"events": 0, "unparsed": 0, "idle_skips": 0, "stage1_pass": 0,
          "confirmed": 0, "entries": 0, "since": time.time()}
_reject_reasons: Dict[str, int] = {}


async def init_db():
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS apex_settings (
                user_id   INTEGER PRIMARY KEY,
                profile   TEXT NOT NULL DEFAULT 'B',
                size_usd  REAL NOT NULL DEFAULT 10,
                size_sol  REAL DEFAULT NULL,
                mode      TEXT NOT NULL DEFAULT 'live',
                paused    INTEGER NOT NULL DEFAULT 0,
                low_alert REAL NOT NULL DEFAULT 0,
                risk_ack  INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS apex_positions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id       INTEGER NOT NULL,
                mode          TEXT NOT NULL,
                mint          TEXT NOT NULL,
                name          TEXT, symbol TEXT,
                status        TEXT NOT NULL DEFAULT 'open',
                entry_ts      REAL NOT NULL,
                entry_sol     REAL NOT NULL,
                entry_tokens  REAL NOT NULL,
                entry_price   REAL NOT NULL,
                entry_mcap    REAL,
                decimals      INTEGER NOT NULL,
                tokens_left   REAL NOT NULL,
                profile       TEXT NOT NULL,
                tiers_hit     INTEGER NOT NULL DEFAULT 0,
                peak_ratio    REAL NOT NULL DEFAULT 1,
                clock_start   REAL NOT NULL,
                last_ratio    REAL DEFAULT 1,
                realized_sol  REAL NOT NULL DEFAULT 0,
                fails         INTEGER NOT NULL DEFAULT 0,
                exit_ts       REAL, exit_reason TEXT, buy_tx TEXT
            );
            CREATE INDEX IF NOT EXISTS ix_apex_pos ON apex_positions(user_id, status);
            CREATE TABLE IF NOT EXISTS apex_blacklist (
                mint TEXT PRIMARY KEY, reason TEXT, ts REAL
            );
            CREATE TABLE IF NOT EXISTS apex_usage (
                day TEXT PRIMARY KEY, credits INTEGER NOT NULL DEFAULT 0
            );
        """)
        await db.commit()
        try:
            await db.execute("ALTER TABLE apex_settings ADD COLUMN risk_ack INTEGER NOT NULL DEFAULT 0")
            await db.commit()
        except Exception:
            pass                                    # column already exists
        # One-time migration: paper mode is gone. Everyone starts STOPPED, in live mode.
        await db.execute("UPDATE apex_settings SET mode='live', paused=1 WHERE mode='paper'")
        await db.commit()
        cur = await db.execute("SELECT mint FROM apex_blacklist")
        _blacklist.update(r[0] for r in await cur.fetchall())
    logger.info(f"Apex DB ready — {len(_blacklist)} blacklisted tokens loaded")


async def blacklist_add(mint: str, reason: str):
    _blacklist.add(mint)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("INSERT OR REPLACE INTO apex_blacklist VALUES (?,?,?)",
                         (mint, reason, time.time()))
        await db.commit()


async def get_settings(uid: int, create: bool = False) -> Tuple[Optional[Dict], bool]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM apex_settings WHERE user_id=?", (uid,))
        row = await cur.fetchone()
        if row:
            return dict(row), False
        if not create:
            return None, False
        await db.execute("INSERT INTO apex_settings(user_id, paused, mode) VALUES (?,1,'live')", (uid,))
        await db.commit()
        cur = await db.execute("SELECT * FROM apex_settings WHERE user_id=?", (uid,))
        return dict(await cur.fetchone()), True


async def set_setting(uid: int, **kv):
    cols = ",".join(f"{k}=?" for k in kv)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(f"UPDATE apex_settings SET {cols} WHERE user_id=?",
                         (*kv.values(), uid))
        await db.commit()


async def _active_users() -> List[Dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM apex_settings WHERE paused=0")
        return [dict(r) for r in await cur.fetchall()]


async def positions(uid: Optional[int] = None, status: str = "open",
                    limit: int = 50) -> List[Dict]:
    q = "SELECT * FROM apex_positions WHERE status=?"
    args: list = [status]
    if uid is not None:
        q += " AND user_id=?"
        args.append(uid)
    q += " ORDER BY " + ("entry_ts" if status == "open" else "exit_ts") + " DESC LIMIT ?"
    args.append(limit)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(q, args)
        return [dict(r) for r in await cur.fetchall()]


async def _update_pos(pid: int, **kv):
    cols = ",".join(f"{k}=?" for k in kv)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(f"UPDATE apex_positions SET {cols} WHERE id=?",
                         (*kv.values(), pid))
        await db.commit()


# ══════════════════════════════════════════════════════════════════════════════
# POOL EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class Pool:
    mint: str
    dev: str
    ts: float
    sig: str
    pool_sol: float = 0.0
    stable_usd: float = 0.0
    lp_mint: str = ""
    lp_minted: float = 0.0
    kind: str = "raydium"          # "raydium" | "pump"
    dev_pct0: float = 0.0          # dev's initial buy, % of supply (pump only)


_logged_shapes = 0


def extract_pool(tx: Dict) -> Optional[Pool]:
    """Pull the new token, dev wallet and liquidity out of an enhanced CREATE_POOL tx."""
    global _logged_shapes
    dev = tx.get("feePayer", "")
    transfers = tx.get("tokenTransfers") or []
    others: Dict[str, Dict] = {}
    pool_sol = stable = 0.0
    for t in transfers:
        m = t.get("mint", "")
        amt = float(t.get("tokenAmount") or 0)
        if m == WSOL:
            pool_sol = max(pool_sol, amt)
        elif m in (USDC, USDT):
            stable = max(stable, amt)
        elif m:
            rec = others.setdefault(m, {"from_dev": False, "amount": 0.0})
            rec["amount"] = max(rec["amount"], amt)
            if t.get("fromUserAccount") == dev:
                rec["from_dev"] = True
    for n in tx.get("nativeTransfers") or []:
        pool_sol = max(pool_sol, float(n.get("amount") or 0) / 1e9)
    if not others:
        return None
    base = next((m for m, r in others.items() if r["from_dev"]), next(iter(others)))
    lp = next((m for m in others if m != base), "")
    if _logged_shapes < 5:
        _logged_shapes += 1
        logger.info(f"APEX shape#{_logged_shapes}: base={base[:8]} lp={lp[:8] or '-'} "
                    f"sol={pool_sol:.2f} stable={stable:.0f} keys={sorted(tx.keys())[:12]}")
    return Pool(mint=base, dev=dev, ts=float(tx.get("timestamp") or time.time()),
                sig=tx.get("signature", ""), pool_sol=pool_sol, stable_usd=stable,
                lp_mint=lp, lp_minted=others.get(lp, {}).get("amount", 0.0))


def extract_pump(tx: Dict) -> Optional[Pool]:
    """New pump.fun launch: mint = the account ending in 'pump', dev = fee payer."""
    dev = tx.get("feePayer", "")
    mint = ""
    for a in tx.get("accountData") or []:
        addr = a.get("account", "")
        if addr.endswith("pump") and len(addr) in (43, 44):
            mint = addr
            break
    if not mint:
        for t in tx.get("tokenTransfers") or []:
            if str(t.get("mint", "")).endswith("pump"):
                mint = t["mint"]
                break
    if not mint:
        return None
    dev_tokens = sum(float(t.get("tokenAmount") or 0) for t in tx.get("tokenTransfers") or []
                     if t.get("mint") == mint and t.get("toUserAccount") == dev)
    return Pool(mint=mint, dev=dev, ts=float(tx.get("timestamp") or time.time()),
                sig=tx.get("signature", ""), kind="pump",
                dev_pct0=dev_tokens / PUMP_SUPPLY * 100.0)


def pump_real_sol(curve_tokens_ui: float) -> float:
    """SOL actually raised on the bonding curve, derived from the curve's token balance."""
    vt = curve_tokens_ui + PUMP_VIRT_OFFSET
    return max(0.0, PUMP_K / vt - PUMP_START_SOL) if vt > 0 else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# SCREENING
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class Verdict:
    ok: bool
    reason: str = ""
    permanent: bool = False        # contract-security failure -> blacklist forever
    data: Dict = field(default_factory=dict)


def _reject(reason: str, permanent: bool = False) -> Verdict:
    return Verdict(False, reason, permanent)


async def _holder_view(mint: str, supply_ui: float):
    """([(owner, pct_of_supply)] for real wallets only, biggest vault balance).
    Pool vaults / bonding curves are program-owned (off-curve) and excluded from holders."""
    res = await _h("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    accts = (res or {}).get("value") or []
    if not accts:
        return None
    infos = await _h("getMultipleAccounts",
                     [[a["address"] for a in accts],
                      {"encoding": "jsonParsed", "commitment": "confirmed"}])
    vals = (infos or {}).get("value") or []
    held: Dict[str, float] = {}
    vault_max = 0.0
    for a, info in zip(accts, vals):
        try:
            owner = info["data"]["parsed"]["info"]["owner"]
        except Exception:
            continue
        ui = float(a.get("uiAmount") or 0)
        if ui <= 0:
            continue
        if is_wallet_address(owner):
            held[owner] = held.get(owner, 0.0) + ui
        else:
            vault_max = max(vault_max, ui)
    return (sorted(((o, v / supply_ui * 100.0) for o, v in held.items()),
                   key=lambda x: -x[1]), vault_max)


async def stage1(pool: Pool) -> Verdict:
    """Cheap, fail-fast checks the moment the pool appears."""
    if pool.mint in _blacklist:
        return _reject("blacklisted")
    px = await sol_usd()
    if not px:
        return _reject("no_sol_price")
    pool_usd = max(pool.pool_sol * px, pool.stable_usd)
    if pool_usd < MIN_POOL_USD:
        return _reject("liquidity_low")

    # mint authority / freeze authority / token program / supply — one call
    info = await _h("getAccountInfo", [pool.mint, {"encoding": "jsonParsed"}])
    val = (info or {}).get("value")
    try:
        parsed = val["data"]["parsed"]["info"]
        program = val["owner"]
        decimals = int(parsed["decimals"])
        supply_ui = int(parsed["supply"]) / 10 ** decimals
    except Exception:
        return _reject("mint_unreadable")
    if program != TOKEN_PROGRAM:
        return _reject("token2022", permanent=True)
    if parsed.get("mintAuthority"):
        return _reject("mint_authority", permanent=True)
    if parsed.get("freezeAuthority"):
        return _reject("freeze_authority", permanent=True)
    if not (MIN_SUPPLY <= supply_ui <= MAX_SUPPLY):
        return _reject("supply_range")

    hv = await _holder_view(pool.mint, supply_ui)
    if hv is None:
        return _reject("holders_unreadable")
    holders = hv[0]
    if holders and holders[0][1] > MAX_SINGLE_PCT:
        return _reject("whale_over_15")
    if sum(p for _, p in holders[:10]) > MAX_TOP10_PCT:
        return _reject("top10_over_40")
    dev_pct = next((p for o, p in holders if o == pool.dev), 0.0)
    if dev_pct > MAX_DEV_PCT:
        return _reject("dev_over_5")

    # deployment -> pool delay (a young mint has a short signature history)
    sigs = await _h("getSignaturesForAddress", [pool.mint, {"limit": 1000}])
    if sigs is not None:
        if len(sigs) >= 1000:
            return _reject("mint_too_old")
        oldest = next((s.get("blockTime") for s in reversed(sigs) if s.get("blockTime")), None)
        if oldest and pool.ts - oldest > MAX_MINT_TO_POOL_SECS:
            return _reject("dormant_mint")

    return Verdict(True, data={"decimals": decimals, "supply_ui": supply_ui,
                               "pool_usd": pool_usd, "dev_pct": dev_pct})


async def _lp_locked(pool: Pool) -> Tuple[bool, str]:
    """LP must be burned or sit in a recognised lock program."""
    if not pool.lp_mint:
        return False, "lp_unverifiable"
    sup = await _h("getTokenSupply", [pool.lp_mint])
    try:
        now_ui = float(sup["value"]["uiAmount"] or 0)
    except Exception:
        return False, "lp_unverifiable"
    if pool.lp_minted > 0 and now_ui <= pool.lp_minted * 0.02:
        return True, "burned"
    big = await _h("getTokenLargestAccounts", [pool.lp_mint])
    accts = (big or {}).get("value") or []
    if not accts:
        return False, "lp_unverifiable"
    info = await _h("getAccountInfo", [accts[0]["address"], {"encoding": "jsonParsed"}])
    try:
        owner = info["value"]["data"]["parsed"]["info"]["owner"]
    except Exception:
        return False, "lp_unverifiable"
    if owner in BURN_OWNERS:
        return True, "burned"
    oinfo = await _h("getAccountInfo", [owner, {"encoding": "base64"}])
    prog = ((oinfo or {}).get("value") or {}).get("owner", "")
    if prog in LOCK_PROGRAMS:
        return True, "locked"
    return False, "lp_unlocked"


async def stage2(pool: Pool, s1: Dict) -> Verdict:
    """Confirmation after the 60-second organic-discovery window."""
    hv = await _holder_view(pool.mint, s1["supply_ui"])
    if hv is None:
        return _reject("holders_unreadable")
    holders = hv[0]
    if holders and holders[0][1] > MAX_SINGLE_PCT:
        return _reject("whale_over_15")
    if sum(p for _, p in holders[:10]) > MAX_TOP10_PCT:
        return _reject("top10_over_40")
    if next((p for o, p in holders if o == pool.dev), 0.0) > MAX_DEV_PCT:
        return _reject("dev_over_5")
    buyers = sum(1 for o, _ in holders if o != pool.dev)
    if buyers < MIN_UNIQUE_BUYERS:
        return _reject("few_buyers")

    ok, why = await _lp_locked(pool)
    if not ok:
        return _reject(why)

    return await _jupiter_checks(pool.mint, s1["decimals"], s1["supply_ui"])


async def _jupiter_checks(mint: str, dec: int, supply_ui: float) -> Verdict:
    """Honeypot / tax / market-cap via Jupiter quotes (no Helius credits)."""
    buy = await jup_quote(WSOL, mint, int(PROBE_SOL * 1e9), ENTRY_SLIPPAGE_BPS)
    if not buy:
        return _reject("no_buy_route")
    got = int(buy["outAmount"])
    sell = await jup_quote(mint, WSOL, got, EXIT_SLIPPAGE_BPS)
    if not sell:
        return _reject("honeypot", permanent=True)
    back = int(sell["outAmount"]) / 1e9
    loss_pct = (1 - back / PROBE_SOL) * 100
    if loss_pct > 2 * MAX_TAX_PCT + 8:      # 2 sides of tax + fees/impact allowance
        return _reject("tax_over_10", permanent=True)
    px = await sol_usd()
    price_sol = PROBE_SOL / (got / 10 ** dec)
    mcap = price_sol * supply_ui * px
    if mcap > MAX_MCAP_USD:
        return _reject("mcap_over_500k")
    return Verdict(True, data={"mcap": mcap, "price_sol": price_sol, "loss_pct": loss_pct})


async def pump_final(pool: Pool) -> Verdict:
    """Last gates for a pump.fun coin that already cleared holders + liquidity."""
    info = await _h("getAccountInfo", [pool.mint, {"encoding": "jsonParsed"}])
    try:
        parsed = info["value"]["data"]["parsed"]["info"]
        dec = int(parsed["decimals"])
    except Exception:
        return _reject("mint_unreadable")
    if info["value"].get("owner") != TOKEN_PROGRAM:
        return _reject("token2022", permanent=True)
    if parsed.get("mintAuthority"):
        return _reject("mint_authority", permanent=True)
    if parsed.get("freezeAuthority"):
        return _reject("freeze_authority", permanent=True)
    v = await _jupiter_checks(pool.mint, dec, PUMP_SUPPLY)
    if v.ok:
        v.data["decimals"] = dec
    return v


# ══════════════════════════════════════════════════════════════════════════════
# EXIT DECISIONS (pure — easy to test)
# ══════════════════════════════════════════════════════════════════════════════
def decide_exit(profile: str, ratio: float, tiers_hit: int, peak: float,
                clock_start: float, now: float) -> Dict:
    """
    Returns {"actions": [(kind, fraction_of_remaining, reason, tier_index)], "peak": x,
             "clock_start": y}. kind is "partial" or "all".
    """
    prof = PROFILES[profile]
    peak = max(peak, ratio)
    if ratio > TIME_STOP_RATIO:
        clock_start = now
    out = {"actions": [], "peak": peak, "clock_start": clock_start}

    if prof["trail"] and peak >= TRAIL_ACTIVATE_RATIO and ratio <= peak * TRAIL_KEEP:
        out["actions"].append(("all", 1.0, "trailing_stop", -1))
        return out
    if ratio <= HARD_STOP_RATIO:
        out["actions"].append(("all", 1.0, "stop_loss", -1))
        return out
    if now - clock_start >= TIME_STOP_SECS and ratio <= TIME_STOP_RATIO:
        out["actions"].append(("all", 1.0, "time_stop", -1))
        return out
    for i, (thr, frac) in enumerate(prof["tiers"]):
        if tiers_hit & (1 << i) or ratio < thr:
            continue
        last = i == len(prof["tiers"]) - 1
        if frac >= 1.0:
            out["actions"].append(("all", 1.0, "target", i))
        else:
            out["actions"].append(("partial", frac, f"target_{int(thr)}x", i))
        if last and frac >= 1.0:
            break
    return out


# ══════════════════════════════════════════════════════════════════════════════
# TRADING (live)
# ══════════════════════════════════════════════════════════════════════════════
_bot = None
_halted = {"on": False, "alerted": False, "credit_alerted": False}
_last_webhook = time.time()
_seen: Dict[str, float] = {}
_pipeline_sem = asyncio.Semaphore(12)
_pos_locks: Dict[int, asyncio.Lock] = {}


async def _notify(uid: int, text: str, kb: Optional[InlineKeyboardMarkup] = None):
    if not _bot:
        return
    try:
        await _bot.send_message(uid, text, parse_mode=ParseMode.HTML,
                                reply_markup=kb, disable_web_page_preview=True)
    except TelegramAPIError as e:
        logger.warning(f"notify {uid}: {e}")


async def _wallet(uid: int) -> Optional[Dict]:
    return await sn.get_sniper_user(uid)


async def _live_buy(uid: int, mint: str, sol: float, dec: int) -> Tuple[bool, float, str, str]:
    w = await _wallet(uid)
    enc = await sn._get_enc_key(uid)
    if not w or not enc:
        return False, 0.0, "", "no wallet connected"
    q = await jup_quote(WSOL, mint, int(sol * 1e9), ENTRY_SLIPPAGE_BPS)
    if not q:
        return False, 0.0, "", "no route within 15% slippage"
    raw = await jup_swap_tx(q, w["pubkey"])
    if not raw:
        return False, 0.0, "", "swap build failed"
    kp = sn.load_keypair(enc)
    sig = await sn.send_transaction_fast(raw, kp, True)
    if not sig:
        return False, 0.0, "", "send failed"
    if not await sn.confirm_transaction(sig):
        return False, 0.0, sig, "unconfirmed"
    bal = await sn.get_token_balance(w["pubkey"], mint)
    return True, bal or int(q["outAmount"]) / 10 ** dec, sig, ""


async def _sell(pos: Dict, tokens: float) -> Tuple[bool, float, str]:
    """Returns (ok, sol_out, tx). Paper sells simply realise the live quote."""
    dec = pos["decimals"]
    amount = int(tokens * 10 ** dec)
    q = await jup_quote(pos["mint"], WSOL, amount, EXIT_SLIPPAGE_BPS)
    if not q:
        return False, 0.0, ""
    sol_out = int(q["outAmount"]) / 1e9
    if pos["mode"] == "paper":
        return True, max(0.0, sol_out - PAPER_FEE_SOL), "paper"
    w = await _wallet(pos["user_id"])
    enc = await sn._get_enc_key(pos["user_id"])
    if not w or not enc:
        return False, 0.0, ""
    raw = await jup_swap_tx(q, w["pubkey"])
    if not raw:
        return False, 0.0, ""
    sig = await sn.send_transaction_fast(raw, sn.load_keypair(enc), True)
    if not sig or not await sn.confirm_transaction(sig):
        return False, 0.0, sig or ""
    return True, sol_out, sig


async def _enter_one(u: Dict, mint: str, name: str, symbol: str, dec: int, mcap: float):
    uid = u["user_id"]
    try:
        if _halted["on"]:
            return
        open_now = await positions(uid, "open", 100)
        if len(open_now) >= MAX_OPEN_POSITIONS or any(p["mint"] == mint for p in open_now):
            return
        px = await sol_usd()
        if not px:
            return
        sol = u["size_sol"] or (u["size_usd"] / px)
        if sol * px < MIN_TRADE_USD - 0.01:
            return
        mode = "live"
        w = await _wallet(uid)
        if not w:
            return
        bal = await sn.get_sol_balance(w["pubkey"])
        if bal * px < MIN_WALLET_USD:
            if time.time() - u["low_alert"] > 3600:
                await set_setting(uid, low_alert=time.time())
                await _notify(uid, "⚠️ <b>Wallet running low</b>\n"
                                   f"Balance is {bal:.4f} SOL — below the ${MIN_WALLET_USD:.0f} "
                                   "reserve. New entries are paused so exits can still pay gas.")
            return
        if bal < sol + 0.004:
            return
        ok, tokens, sig, err = await _live_buy(uid, mint, sol, dec)
        if not ok:
            logger.warning(f"live buy failed {mint[:8]}: {err}")
            return
        if tokens <= 0:
            return
        entry_price = sol / tokens
        now = time.time()
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "INSERT INTO apex_positions(user_id,mode,mint,name,symbol,entry_ts,entry_sol,"
                "entry_tokens,entry_price,entry_mcap,decimals,tokens_left,profile,peak_ratio,"
                "clock_start,buy_tx) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (uid, mode, mint, name, symbol, now, sol, tokens, entry_price, mcap, dec,
                 tokens, u["profile"], 1.0, now, sig))
            await db.commit()
        _stats["entries"] += 1
        prof = PROFILES[u["profile"]]
        tag = "🔴 LIVE"
        await _notify(uid, (
            f"🎯 <b>Position opened</b>  ·  {tag}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<b>{html.escape(name)}</b> <code>${html.escape(symbol)}</code>\n"
            f"<code>{mint}</code>\n\n"
            f"Entry      <code>{entry_price:.3e} SOL</code>\n"
            f"Size       <code>{sol:.4f} SOL</code> (≈ ${sol * px:,.2f})\n"
            f"Mkt cap    <code>${mcap:,.0f}</code>\n"
            f"Exit plan  {prof['icon']} {prof['name']}\n"
            f"Stop       <code>-45%</code> hard · trailing after +100%\n\n"
            f"<a href=\"https://dexscreener.com/solana/{mint}\">Chart</a> · "
            f"<a href=\"https://solscan.io/token/{mint}\">Solscan</a>"))
    except Exception as e:
        logger.error(f"enter_one {uid} {mint[:8]}: {e}", exc_info=True)


async def _close_or_reduce(pos: Dict, tokens: float, reason: str, tier: int,
                           full: bool) -> bool:
    ok, sol_out, tx = await _sell(pos, tokens)
    if not ok:
        return False
    left = 0.0 if full else max(0.0, pos["tokens_left"] - tokens)
    realized = pos["realized_sol"] + sol_out
    hit = pos["tiers_hit"] | (1 << tier if tier >= 0 else 0)
    uid = pos["user_id"]
    nm = f"<b>{html.escape(pos['name'] or pos['mint'][:6])}</b>"
    px = await sol_usd()
    if left <= 0:
        pnl = realized - pos["entry_sol"]
        await _update_pos(pos["id"], status="closed", tokens_left=0, realized_sol=realized,
                          exit_ts=time.time(), exit_reason=reason, tiers_hit=hit)
        if reason == "stop_loss":
            await blacklist_add(pos["mint"], "stop_loss")
            head = "🛑 <b>Stop-loss triggered</b>"
        elif reason == "trailing_stop":
            head = "🪂 <b>Trailing stop — profit secured</b>"
        elif reason == "time_stop":
            head = "⏳ <b>Time-stop — capital freed</b>"
        else:
            head = "✅ <b>Position closed — target hit</b>"
        sign = "+" if pnl >= 0 else ""
        await _notify(uid, (
            f"{head}\n━━━━━━━━━━━━━━━\n{nm}\n\n"
            f"Entry     <code>{pos['entry_price']:.3e}</code>\n"
            f"Returned  <code>{realized:.4f} SOL</code>\n"
            f"Net       <code>{sign}{pnl:.4f} SOL</code> (≈ {sign}${pnl * px:,.2f})\n"
            f"Reason    <code>{reason}</code>"))
    else:
        await _update_pos(pos["id"], tokens_left=left, realized_sol=realized, tiers_hit=hit)
        pct = tokens / pos["tokens_left"] * 100 if pos["tokens_left"] else 0
        await _notify(uid, (
            f"💸 <b>Partial exit</b>\n━━━━━━━━━━━━━━━\n{nm}\n\n"
            f"Sold       <code>{pct:.0f}%</code> of the remaining bag\n"
            f"Received   <code>{sol_out:.4f} SOL</code>\n"
            f"Still held <code>{left / pos['entry_tokens'] * 100:.0f}%</code> of the original\n"
            f"Tier       <code>{reason}</code>"))
    return True


async def _manage(pos: Dict):
    lock = _pos_locks.setdefault(pos["id"], asyncio.Lock())
    if lock.locked():
        return
    async with lock:
        tokens = pos["tokens_left"]
        if tokens <= 0:
            return
        q = await jup_quote(pos["mint"], WSOL, int(tokens * 10 ** pos["decimals"]),
                            EXIT_SLIPPAGE_BPS)
        if not q:
            fails = pos["fails"] + 1
            if fails >= DEAD_AFTER_FAILS:
                await _update_pos(pos["id"], status="closed", exit_ts=time.time(),
                                  exit_reason="illiquid_writeoff", tokens_left=0, fails=fails)
                await blacklist_add(pos["mint"], "illiquid")
                await _notify(pos["user_id"],
                              f"☠️ <b>Written off</b> — {html.escape(pos['name'] or pos['mint'][:6])} "
                              "has had no sell route for 5 minutes.")
            else:
                await _update_pos(pos["id"], fails=fails)
            return
        value = int(q["outAmount"]) / 1e9
        ratio = (value / tokens) / pos["entry_price"] if pos["entry_price"] else 1.0
        d = decide_exit(pos["profile"], ratio, pos["tiers_hit"], pos["peak_ratio"],
                        pos["clock_start"], time.time())
        await _update_pos(pos["id"], peak_ratio=d["peak"], clock_start=d["clock_start"],
                          last_ratio=ratio, fails=0)
        cur = dict(pos, tokens_left=tokens)
        for kind, frac, reason, tier in d["actions"]:
            amt = tokens if kind == "all" else cur["tokens_left"] * frac
            if not await _close_or_reduce(cur, amt, reason, tier, kind == "all"):
                break
            cur = (await positions(pos["user_id"], "open", 100) or [None])
            cur = next((p for p in cur if p and p["id"] == pos["id"]), None)
            if not cur:
                break


async def monitor_loop():
    logger.info("Apex position monitor started")
    while True:
        try:
            ps = await positions(None, "open", 500)
            if ps:
                await asyncio.gather(*[_manage(p) for p in ps], return_exceptions=True)
        except Exception as e:
            logger.error(f"monitor: {e}", exc_info=True)
        await asyncio.sleep(MONITOR_INTERVAL)


async def housekeeping_loop():
    """Credit flush, webhook-silence watchdog, seen-set pruning."""
    while True:
        await asyncio.sleep(30)
        try:
            await _flush_credits()
            global _last_summary
            if time.time() - _last_summary > 300:
                _last_summary = time.time()
                today, month = await credits_used()
                top = sorted(_reject_reasons.items(), key=lambda x: -x[1])[:3]
                logger.info(f"APEX summary: launches={_stats['events']} stage1={_stats['stage1_pass']} "
                            f"confirmed={_stats['confirmed']} entries={_stats['entries']} "
                            f"idle_skips={_stats['idle_skips']} top_rejects={top} "
                            f"credits today={today:,} month={month:,}")
            for m in [m for m, t in _seen.items() if time.time() - t > 6 * 3600]:
                _seen.pop(m, None)
            hr = datetime.now(timezone.utc).hour
            silent = time.time() - _last_webhook
            users = await _active_users()
            if (users and ACTIVE_HOURS_UTC[0] <= hr < ACTIVE_HOURS_UTC[1]
                    and silent > WEBHOOK_SILENCE_SECS and not _halted["alerted"]):
                _halted.update(on=True, alerted=True)
                for u in users:
                    await _notify(u["user_id"],
                                  "📡 <b>Feed went quiet</b>\nNo pool events for 10 minutes. "
                                  "New entries are paused until the feed reconnects — open "
                                  "positions are still protected.")
            if not await _budget_ok() and not _halted["credit_alerted"]:
                _halted["credit_alerted"] = True
                for u in users:
                    await _notify(u["user_id"],
                                  "🧮 <b>Credit guard engaged</b>\nApex has used ~95% of this "
                                  "month's Helius credit ceiling. Screening is paused to protect "
                                  "your budget; exits keep running.")
        except Exception as e:
            logger.error(f"housekeeping: {e}")


def note_webhook():
    global _last_webhook
    _last_webhook = time.time()
    if _halted["on"] and _halted["alerted"]:
        _halted.update(on=False, alerted=False)
        asyncio.create_task(_announce_reconnect())


async def _announce_reconnect():
    for u in await _active_users():
        await _notify(u["user_id"], "📡 <b>Feed reconnected</b> — scanning again.")


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
def _count_reject(reason: str):
    _reject_reasons[reason] = _reject_reasons.get(reason, 0) + 1


_pump_watchers = 0
_last_summary = 0.0


async def _finish_entry(pool: Pool, dec: int, mcap: float):
    meta = await _das("getAsset", {"id": pool.mint}) or {}
    md = (meta.get("content") or {}).get("metadata") or {}
    name = md.get("name") or pool.mint[:6]
    symbol = md.get("symbol") or "?"
    for u in await _active_users():
        asyncio.create_task(_enter_one(u, pool.mint, name, symbol, dec, mcap))
    logger.info(f"APEX PASS [{pool.kind}] {name} ({pool.mint[:8]}) mcap=${mcap:,.0f}")


async def pump_pipeline(pool: Pool):
    """
    pump.fun launches. Same gates as the Raydium path, adapted to a bonding curve:
      free  : dev's initial buy <= 5% (read straight from the webhook payload)
      T+60s : holders (2 calls) -> top10/whale/dev gates, 5+ unique buyers
              curve liquidity is derived from the curve's token balance (free)
      later : re-check at a few widening intervals until liquidity >= $3,000
    A bonding curve cannot be pulled by the dev, which is the curve-stage equivalent
    of 'LP burned or locked'.
    """
    global _pump_watchers
    if pool.dev_pct0 > MAX_DEV_PCT:
        _count_reject("dev_over_5")
        return
    if _pump_watchers >= PUMP_MAX_WATCHERS:
        _count_reject("capacity")
        return
    _pump_watchers += 1
    first_pass = True
    try:
        for i, age in enumerate(PUMP_CHECK_AGES):
            wait = pool.ts + age - time.time()
            if wait > 0:
                await asyncio.sleep(wait)
            if not await _budget_ok() or not await _active_users():
                return
            last = i == len(PUMP_CHECK_AGES) - 1
            async with _pipeline_sem:
                hv = await _holder_view(pool.mint, PUMP_SUPPLY)
            if hv is None:
                if last:
                    _count_reject("holders_unreadable")
                continue
            holders, curve_ui = hv
            if holders and holders[0][1] > MAX_SINGLE_PCT:
                _count_reject("whale_over_15")
                return
            if sum(p for _, p in holders[:10]) > MAX_TOP10_PCT:
                _count_reject("top10_over_40")
                return
            if next((p for o, p in holders if o == pool.dev), 0.0) > MAX_DEV_PCT:
                _count_reject("dev_over_5")
                return
            buyers = sum(1 for o, _ in holders if o != pool.dev)
            if buyers < MIN_UNIQUE_BUYERS:
                # Strategy rule: 5 unique buyers within the first 60 seconds, or it's out.
                # Dropping these here keeps the re-check queue (and credits) for real contenders.
                _count_reject("few_buyers")
                return
            if first_pass:
                first_pass = False
                _stats["stage1_pass"] += 1
            px = await sol_usd()
            if pump_real_sol(curve_ui) * px < MIN_POOL_USD:
                if last:
                    _count_reject("liquidity_low")
                continue
            async with _pipeline_sem:
                v = await pump_final(pool)
            if not v.ok:
                _count_reject(v.reason)
                if v.permanent:
                    await blacklist_add(pool.mint, v.reason)
                return
            _stats["confirmed"] += 1
            await _finish_entry(pool, v.data["decimals"], v.data["mcap"])
            return
    finally:
        _pump_watchers -= 1


async def on_pool_event(tx: Dict):
    _stats["events"] += 1
    try:
        is_pump = tx.get("type") == "CREATE"
        if is_pump and tx.get("source") != "PUMP_FUN":
            return
        pool = extract_pump(tx) if is_pump else extract_pool(tx)
        if not pool:
            _stats["unparsed"] += 1
            return
        if time.time() - pool.ts > MAX_EVENT_AGE_SECS:
            return
        if pool.mint in _seen:
            return
        _seen[pool.mint] = time.time()
        if not await _active_users():
            _stats["idle_skips"] += 1     # nobody is trading: spend zero credits
            return
        if not await _budget_ok():
            return
        if pool.mint in _blacklist:
            return
        if is_pump:
            _stats["pump_launches"] = _stats.get("pump_launches", 0) + 1
            await pump_pipeline(pool)
            return
        async with _pipeline_sem:
            v1 = await stage1(pool)
            if not v1.ok:
                _count_reject(v1.reason)
                if v1.permanent:
                    await blacklist_add(pool.mint, v1.reason)
                return
            _stats["stage1_pass"] += 1
            wait = pool.ts + CONFIRM_WINDOW_SECS - time.time()
        if wait > 0:
            await asyncio.sleep(wait)
        async with _pipeline_sem:
            v2 = await stage2(pool, v1.data)
        if not v2.ok:
            _count_reject(v2.reason)
            if v2.permanent:
                await blacklist_add(pool.mint, v2.reason)
            return
        _stats["confirmed"] += 1
        await _finish_entry(pool, v1.data["decimals"], v2.data["mcap"])
    except Exception as e:
        logger.error(f"on_pool_event: {e}", exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGES — Telegram interface
# ══════════════════════════════════════════════════════════════════════════════
router = Router()
_await: Dict[int, str] = {}       # uid -> "wallet" | "size"
_wizard: set = set()
RULE = "━━━━━━━━━━━━━━━━━━"


def _short(a: str) -> str:
    return f"{a[:4]}…{a[-4:]}" if a and len(a) > 9 else a or "—"


def _ago(ts: float) -> str:
    s = max(0, int(time.time() - ts))
    if s < 90:
        return f"{s}s"
    if s < 5400:
        return f"{s // 60}m"
    if s < 172800:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


def _kb(rows: List[List[Tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=d) for t, d in row] for row in rows])


HOME_BTN = [("‹ Home", "ax:home")]


async def _send(target, text: str, kb: InlineKeyboardMarkup):
    """Edit in place when we can, send fresh when we can't."""
    if isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(text, parse_mode=ParseMode.HTML,
                                           reply_markup=kb, disable_web_page_preview=True)
        except TelegramBadRequest as e:
            if "not modified" not in str(e):
                await target.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb,
                                            disable_web_page_preview=True)
        await target.answer()
    else:
        await target.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb,
                            disable_web_page_preview=True)


async def _home(uid: int, banner: str = "") -> Tuple[str, InlineKeyboardMarkup]:
    s, _ = await get_settings(uid, create=True)
    prof = PROFILES[s["profile"]]
    w = await _wallet(uid)
    px = await sol_usd()
    size = (f"{s['size_sol']:g} SOL" if s["size_sol"] else f"${s['size_usd']:g}")
    if w:
        bal = await sn.get_sol_balance(w["pubkey"])
        wal = f"{_short(w['pubkey'])} · {bal:.3f} SOL"
    else:
        wal = "not connected"
    opened = await positions(uid, "open", 100)
    running = not s["paused"]
    if not running:
        state = "■ STOPPED"
    elif _halted["on"]:
        state = "◌ RUNNING · feed quiet"
    else:
        state = "● RUNNING · hunting"
    text = (
        (f"{banner}\n\n" if banner else "") +
        f"<b>◆ CLEX APEX</b>\n<i>Precision entries on fresh Solana launches.</i>\n{RULE}\n"
        f"<code>Sniper   </code><b>{state}</b>\n"
        f"<code>Exit     </code>{prof['icon']} {prof['name']}\n"
        f"<code>Size     </code>{size}"
        + (f" ≈ {s['size_sol'] * px:.2f} USD" if s["size_sol"] and px else "") + "\n"
        f"<code>Wallet   </code>{wal}\n"
        f"<code>Open     </code>{len(opened)} position{'s' if len(opened) != 1 else ''}\n"
        f"{RULE}\n"
        "<i>Roughly 95% of launches never pass the gauntlet. That's the edge.</i>")
    toggle = [("■ STOP SNIPER", "ax:stop")] if running else [("▶ START SNIPER", "ax:start")]
    kb = _kb([
        toggle,
        [("🎯 Positions", "ax:pos"), ("📜 History", "ax:hist")],
        [("🧭 Strategy", "ax:strat"), ("💵 Size", "ax:size")],
        [("👛 Wallet", "ax:wallet"), ("📡 Status", "ax:stat")],
        [("🚫 Blacklist", "ax:bl"), ("📖 How it works", "ax:how")],
    ])
    return text, kb


WELCOME = (
    "<b>◆ CLEX APEX</b>\n<i>Precision entries on fresh Solana launches.</i>\n" + RULE + "\n"
    "Apex watches every new launch, throws out the junk, and only moves when a coin "
    "clears <b>every</b> safety gate. Then it manages the exit for you — targets, "
    "stops, all of it.\n\n"
    "It trades <b>real SOL</b> from a dedicated Phantom wallet, but only while you "
    "have it <b>started</b>. Nothing happens until you press ▶ START.\n\n"
    "<b>Three quick steps:</b> exit strategy, trade size, wallet.")


async def _strategy_page() -> Tuple[str, InlineKeyboardMarkup]:
    lines = ["<b>🧭 Exit Strategy</b>\n" + RULE + "\nPick how Apex takes profit. "
             "Applies to every <i>new</i> trade.\n"]
    for k, p in PROFILES.items():
        lines.append(f"\n<b>{k} · {p['icon']} {p['name']}</b>\n<i>{p['blurb']}</i>\n" +
                     "\n".join(f"  • {r}" for r in p["rows"]))
    lines.append("\n\n<b>Always on:</b> −45% hard stop · 72h time-stop if flat.")
    kb = _kb([[(f"{k} · {p['name']}", f"ax:prof:{k}")] for k, p in PROFILES.items()] +
             [HOME_BTN])
    return "\n".join(lines), kb


def _size_page(s: Dict) -> Tuple[str, InlineKeyboardMarkup]:
    cur = f"{s['size_sol']:g} SOL" if s["size_sol"] else f"${s['size_usd']:g}"
    text = (f"<b>💵 Position Size</b>\n{RULE}\nCurrent: <b>{cur}</b> per trade\n\n"
            "Apex always trades exactly this amount — no scaling up, no leverage, no "
            "averaging down. Minimum is $5.")
    rows = [[(f"${v}", f"ax:sz:{v}") for v in SIZE_OPTIONS_USD[:3]],
            [(f"${v}", f"ax:sz:{v}") for v in SIZE_OPTIONS_USD[3:]] + [("Custom SOL", "ax:szc")]]
    return text, _kb(rows + [HOME_BTN])


@router.message(Command("start"))
async def cmd_start(m: Message):
    s, new = await get_settings(m.from_user.id, create=True)
    if new:
        _wizard.add(m.from_user.id)
        t, kb = await _strategy_page()
        await m.answer(WELCOME, parse_mode=ParseMode.HTML)
        await m.answer("<b>Step 1 of 2</b> — " + t, parse_mode=ParseMode.HTML, reply_markup=kb)
        return
    t, kb = await _home(m.from_user.id)
    await _send(m, t, kb)


@router.message(Command("settings"))
async def cmd_settings(m: Message):
    t, kb = await _strategy_page()
    await _send(m, t, kb)


@router.callback_query(F.data == "ax:home")
async def cb_home(q: CallbackQuery):
    _await.pop(q.from_user.id, None)
    t, kb = await _home(q.from_user.id)
    await _send(q, t, kb)


@router.callback_query(F.data == "ax:strat")
async def cb_strat(q: CallbackQuery):
    t, kb = await _strategy_page()
    await _send(q, t, kb)


@router.callback_query(F.data.startswith("ax:prof:"))
async def cb_prof(q: CallbackQuery):
    k = q.data.split(":")[2]
    if k not in PROFILES:
        await q.answer("Unknown profile")
        return
    await get_settings(q.from_user.id, create=True)
    await set_setting(q.from_user.id, profile=k)
    if q.from_user.id in _wizard:
        s, _ = await get_settings(q.from_user.id)
        t, kb = _size_page(s)
        await _send(q, "<b>Step 2 of 2</b> — " + t, kb)
        return
    t, kb = await _home(q.from_user.id, f"✅ Exit strategy set to <b>{PROFILES[k]['name']}</b>.")
    await _send(q, t, kb)


@router.callback_query(F.data == "ax:size")
async def cb_size(q: CallbackQuery):
    s, _ = await get_settings(q.from_user.id, create=True)
    t, kb = _size_page(s)
    await _send(q, t, kb)


@router.callback_query(F.data.startswith("ax:sz:"))
async def cb_sz(q: CallbackQuery):
    v = float(q.data.split(":")[2])
    await set_setting(q.from_user.id, size_usd=v, size_sol=None)
    done = q.from_user.id in _wizard
    _wizard.discard(q.from_user.id)
    banner = ("🎉 <b>Almost there.</b> Connect your wallet, then press ▶ START SNIPER."
              if done else f"✅ Size set to <b>${v:g}</b> per trade.")
    t, kb = await _home(q.from_user.id, banner)
    await _send(q, t, kb)


@router.callback_query(F.data == "ax:szc")
async def cb_szc(q: CallbackQuery):
    _await[q.from_user.id] = "size"
    await _send(q, "<b>Custom size</b>\n" + RULE + "\nSend the SOL amount per trade, e.g. "
                "<code>0.05</code>.", _kb([HOME_BTN]))


async def _do_start(uid: int, target):
    s, _ = await get_settings(uid, create=True)
    w = await _wallet(uid)
    if not w:
        t, kb = await _wallet_page(uid)
        await _send(target, "🔐 <b>Connect a wallet first</b> — Apex needs a funded Phantom "
                            "account to trade from.\n\n" + t, kb)
        return
    px = await sol_usd()
    bal = await sn.get_sol_balance(w["pubkey"])
    size_sol = s["size_sol"] or ((s["size_usd"] / px) if px else 0)
    need = size_sol + 0.004 + (MIN_WALLET_USD / px if px else 0)
    if bal < need:
        await _send(target, f"<b>💸 Fund your wallet first</b>\n{RULE}\n"
                            f"Balance <b>{bal:.4f} SOL</b> · needed about <b>{need:.4f} SOL</b>\n"
                            "(one trade + fees + the $4 gas reserve).\n\n"
                            f"Send SOL to:\n<code>{w['pubkey']}</code>",
                    _kb([[("↻ Check again", "ax:start")], HOME_BTN]))
        return
    if not s["risk_ack"]:
        await _send(target, f"<b>▶ Start sniping?</b>\n{RULE}\nApex will spend <b>real SOL</b> from "
                            f"<code>{_short(w['pubkey'])}</code>, automatically, with no confirmation "
                            "per trade.\n\n"
                            "• Memecoins can go to zero — the −45% stop cannot guarantee an exit\n"
                            "• Press ■ STOP any time: no new entries, open positions stay protected\n"
                            "• Only fund the wallet with money you can lose",
                    _kb([[("I understand — START", "ax:startgo")], [("Cancel", "ax:home")]]))
        return
    await set_setting(uid, paused=0)
    t, kb = await _home(uid, "▶ <b>Sniper started.</b> Apex is hunting and will place trades "
                             "when a launch clears every gate.")
    await _send(target, t, kb)


@router.callback_query(F.data == "ax:start")
async def cb_start(q: CallbackQuery):
    await _do_start(q.from_user.id, q)


@router.callback_query(F.data == "ax:startgo")
async def cb_startgo(q: CallbackQuery):
    if not await _wallet(q.from_user.id):
        await q.answer("Connect a wallet first")
        return
    await set_setting(q.from_user.id, risk_ack=1)
    await _do_start(q.from_user.id, q)


async def _do_stop(uid: int, target):
    await get_settings(uid, create=True)
    await set_setting(uid, paused=1)
    t, kb = await _home(uid, "■ <b>Sniper stopped.</b> No new entries. Open positions stay "
                             "protected — stops and targets keep running.")
    await _send(target, t, kb)


@router.callback_query(F.data == "ax:stop")
async def cb_stop(q: CallbackQuery):
    await _do_stop(q.from_user.id, q)


@router.message(Command("run"))
@router.message(Command("resume"))
async def cmd_run(m: Message):
    await _do_start(m.from_user.id, m)


@router.message(Command("stop"))
@router.message(Command("pause"))
async def cmd_stop(m: Message):
    await _do_stop(m.from_user.id, m)


async def _positions_page(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    ps = await positions(uid, "open", 25)
    if not ps:
        return (f"<b>🎯 Open Positions</b>\n{RULE}\nNothing open. Apex only fires when a pool "
                "clears every gate — patience is the strategy.", _kb([HOME_BTN]))
    px = await sol_usd()
    out = [f"<b>🎯 Open Positions</b> · {len(ps)}\n{RULE}"]
    for p in ps:
        r = p["last_ratio"] or 1.0
        gain = (r - 1) * 100
        dot = "🟢" if gain >= 0 else "🔴"
        floor = ""
        if PROFILES[p["profile"]]["trail"] and p["peak_ratio"] >= TRAIL_ACTIVATE_RATIO:
            floor = f"trail {(p['peak_ratio'] * TRAIL_KEEP - 1) * 100:+.0f}%"
        else:
            floor = "stop −45%"
        tag = "🧪" if p["mode"] == "paper" else ""
        out.append(
            f"\n{dot} <b>{html.escape(p['name'] or p['mint'][:6])}</b> "
            f"<code>${html.escape(p['symbol'] or '?')}</code> {tag}\n"
            f"   <code>{gain:+.1f}%</code> · {p['tokens_left'] / p['entry_tokens'] * 100:.0f}% held · "
            f"{_ago(p['entry_ts'])} · {floor}")
    return "\n".join(out), _kb([[("↻ Refresh", "ax:pos")], HOME_BTN])


@router.callback_query(F.data == "ax:pos")
async def cb_pos(q: CallbackQuery):
    t, kb = await _positions_page(q.from_user.id)
    await _send(q, t, kb)


@router.message(Command("positions"))
async def cmd_pos(m: Message):
    t, kb = await _positions_page(m.from_user.id)
    await _send(m, t, kb)


async def _history_page(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    ps = await positions(uid, "closed", 20)
    if not ps:
        return (f"<b>📜 History</b>\n{RULE}\nNo closed trades yet.", _kb([HOME_BTN]))
    wins = sum(1 for p in ps if p["realized_sol"] > p["entry_sol"])
    net = sum(p["realized_sol"] - p["entry_sol"] for p in ps)
    out = [f"<b>📜 Last {len(ps)} trades</b>\n{RULE}\n"
           f"Win rate <b>{wins / len(ps) * 100:.0f}%</b> · Net <b>{net:+.4f} SOL</b>\n"]
    for p in ps:
        pnl = p["realized_sol"] - p["entry_sol"]
        pct = pnl / p["entry_sol"] * 100 if p["entry_sol"] else 0
        out.append(f"{'🟢' if pnl >= 0 else '🔴'} <b>{html.escape(p['name'] or p['mint'][:6])}</b> "
                   f"<code>{pct:+.0f}%</code> · {pnl:+.4f} SOL · "
                   f"<i>{(p['exit_reason'] or '').replace('_', ' ')}</i>"
                   f"{' 🧪' if p['mode'] == 'paper' else ''}")
    return "\n".join(out), _kb([HOME_BTN])


@router.callback_query(F.data == "ax:hist")
async def cb_hist(q: CallbackQuery):
    t, kb = await _history_page(q.from_user.id)
    await _send(q, t, kb)


@router.message(Command("history"))
async def cmd_hist(m: Message):
    t, kb = await _history_page(m.from_user.id)
    await _send(m, t, kb)


SEND_FEE_LAMPORTS = 5_000
RENT_MIN_LAMPORTS = 890_880            # a system account must hold 0 or >= this after a transfer
OPEN_POS_RESERVE = 10_000_000          # 0.01 SOL kept for exit gas while positions are open
WD_TTL = 300                           # a staged withdrawal expires after 5 minutes
_wd: Dict[int, Dict] = {}


def valid_address(addr: str) -> bool:
    try:
        return len(base58.b58decode(addr.strip())) == 32
    except Exception:
        return False


def parse_amount(text: str, balance: int, reserve: int = 0) -> Tuple[int, str]:
    """Returns (lamports, error). 'max' sends everything except fee + reserve."""
    t = text.strip().lower()
    if t in ("max", "all"):
        lam = balance - SEND_FEE_LAMPORTS - reserve
    else:
        try:
            lam = int(round(float(t) * 1e9))
        except ValueError:
            return 0, "Send a number like <code>0.25</code>, or <code>max</code>."
    if lam <= 0:
        return 0, "Nothing to send after the network fee" + (" and the gas reserve." if reserve else ".")
    left = balance - lam - SEND_FEE_LAMPORTS
    if left < reserve:
        return 0, (f"That leaves too little. Max you can send is "
                   f"<b>{max(0, balance - SEND_FEE_LAMPORTS - reserve) / 1e9:.6f} SOL</b>.")
    if 0 < left < RENT_MIN_LAMPORTS:
        return 0, ("That would leave a dust balance Solana won't allow. Send <code>max</code> "
                   "or leave at least 0.001 SOL.")
    return lam, ""


async def _wallet_page(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    w = await _wallet(uid)
    if w:
        bal = await sn.get_sol_balance(w["pubkey"])
        px = await sol_usd()
        text = (f"<b>👛 Your Apex wallet</b>\n{RULE}\n<code>{w['pubkey']}</code>\n\n"
                f"Balance  <b>{bal:.4f} SOL</b>" + (f"  ≈ ${bal * px:,.2f}" if px else "") +
                "\n\nKeep only what you're willing to trade in here. Apex halts new entries "
                "below $4 so exits can always pay gas.")
        return text, _kb([[("📥 Deposit", "ax:dep"), ("📤 Send funds", "ax:send")],
                          [("🔑 Export key", "ax:wexp"), ("🗑 Disconnect", "ax:wdisc")],
                          [("↻ Refresh", "ax:wallet")], HOME_BTN])
    text = (f"<b>👛 Set up your wallet</b>\n{RULE}\n"
            "Apex trades from its own Solana wallet. Pick one:\n\n"
            "<b>✨ Create a new wallet</b> <i>(recommended)</i>\n"
            "Apex generates a fresh wallet. You fund it from Phantom or anywhere else — "
            "no keys to hunt for.\n\n"
            "<b>🔐 Import a key</b>\n"
            "Use a dedicated Phantom account's private key. Never your main wallet.\n\n"
            "⚠️ Either way, the bot holds the key (AES-encrypted). Only fund what you can afford to lose.")
    return text, _kb([[("✨ Create new wallet", "ax:wcreate")],
                      [("🔐 Import a private key", "ax:wconnect")], HOME_BTN])


@router.callback_query(F.data == "ax:wallet")
async def cb_wallet(q: CallbackQuery):
    t, kb = await _wallet_page(q.from_user.id)
    await _send(q, t, kb)


@router.callback_query(F.data == "ax:wconnect")
async def cb_wconnect(q: CallbackQuery):
    _await[q.from_user.id] = "wallet"
    await _send(q, "<b>🔐 Send your private key</b>\n" + RULE +
                "\nPaste the base58 key from your dedicated Phantom account in your next message.",
                _kb([[("Cancel", "ax:home")]]))


def _qr_png(data: str) -> Optional[bytes]:
    try:
        import io
        import segno
        buf = io.BytesIO()
        segno.make(data, error="m").save(buf, kind="png", scale=8, border=2)
        return buf.getvalue()
    except Exception as e:
        logger.warning(f"qr failed: {e}")
        return None


@router.callback_query(F.data == "ax:wcreate")
async def cb_wcreate(q: CallbackQuery):
    uid = q.from_user.id
    if await _wallet(uid):
        t, kb = await _wallet_page(uid)
        await _send(q, "You already have a wallet connected.\n\n" + t, kb)
        return
    from solders.keypair import Keypair
    kp = Keypair()
    pub = str(kp.pubkey())
    await sn.save_sniper_user(uid, base58.b58encode(bytes(kp)).decode(), pub, "low")
    await _send(q, (f"<b>✨ Wallet created</b>\n{RULE}\n<code>{pub}</code>\n\n"
                    "Next:\n1. <b>Export your key now</b> and save it in a password manager — "
                    "it's your only way to recover the funds if the bot is ever unavailable.\n"
                    "2. <b>Deposit SOL</b> to this address (Solana network only).\n"
                    "3. Press ▶ START SNIPER."),
                _kb([[("🔑 Export my key", "ax:wexp"), ("📥 Deposit", "ax:dep")], HOME_BTN]))


@router.callback_query(F.data == "ax:dep")
async def cb_dep(q: CallbackQuery):
    w = await _wallet(q.from_user.id)
    if not w:
        t, kb = await _wallet_page(q.from_user.id)
        await _send(q, t, kb)
        return
    bal = await sn.get_sol_balance(w["pubkey"])
    caption = (f"<b>📥 Deposit SOL</b>\n{RULE}\nSend SOL to:\n<code>{w['pubkey']}</code>\n"
               "<i>(tap the address to copy)</i>\n\n"
               f"Balance now: <b>{bal:.4f} SOL</b>\n\n"
               "• <b>Solana network only</b> — other networks lose the funds\n"
               "• Works from Phantom, Coinbase, Binance, Robinhood, or any wallet that can "
               "send SOL on Solana\n"
               "• Press ↻ after sending to see the new balance")
    kb = _kb([[("↻ Check balance", "ax:dep")], [("‹ Wallet", "ax:wallet")], HOME_BTN])
    png = _qr_png(w["pubkey"])
    if png:
        try:
            await q.message.answer_photo(BufferedInputFile(png, "deposit.png"), caption=caption,
                                         parse_mode=ParseMode.HTML, reply_markup=kb)
            await q.answer()
            return
        except TelegramAPIError as e:
            logger.warning(f"deposit photo: {e}")
    await _send(q, caption, kb)


@router.callback_query(F.data == "ax:wexp")
async def cb_wexp(q: CallbackQuery):
    if not await _wallet(q.from_user.id):
        await q.answer("No wallet yet")
        return
    await _send(q, f"<b>🔑 Export private key</b>\n{RULE}\nThe key will appear in a message that "
                   "<b>deletes itself after 60 seconds</b>.\n\n"
                   "• Anyone who sees it controls this wallet's funds\n"
                   "• Make sure nobody is looking at your screen\n"
                   "• Paste it straight into a password manager",
                _kb([[("Show my key", "ax:wexpgo")], [("Cancel", "ax:wallet")]]))


@router.callback_query(F.data == "ax:wexpgo")
async def cb_wexpgo(q: CallbackQuery):
    uid = q.from_user.id
    enc = await sn._get_enc_key(uid)
    if not enc:
        await q.answer("No wallet")
        return
    try:
        key = sn.decrypt_key(enc)
    except Exception:
        await q.answer("Could not decrypt — check MASTER_ENCRYPTION_KEY", show_alert=True)
        return
    await q.answer()
    msg = await q.message.answer(
        "🔑 <b>Private key</b> — deleting in 60s\n<tg-spoiler>" + html.escape(key) + "</tg-spoiler>",
        parse_mode=ParseMode.HTML, protect_content=True)
    del key

    async def _wipe():
        await asyncio.sleep(60)
        try:
            await msg.delete()
        except Exception:
            pass
    asyncio.create_task(_wipe())


@router.callback_query(F.data == "ax:wdisc")
async def cb_wdisc(q: CallbackQuery):
    w = await _wallet(q.from_user.id)
    bal = await sn.get_sol_balance(w["pubkey"]) if w else 0.0
    warn = (f"\n\n⚠️ This wallet holds <b>{bal:.4f} SOL</b>. Once the key is deleted from Apex, "
            "those funds are only recoverable if you exported the key. "
            "<b>Send them out or export first.</b>") if bal > 0.001 else ""
    await _send(q, "<b>Disconnect wallet?</b>\nThe encrypted key is deleted from Apex and the "
                   "sniper stops. Open positions will no longer be managed." + warn,
                _kb([[("📤 Send funds first", "ax:send")],
                     [("Yes, delete it", "ax:wdiscgo"), ("Keep", "ax:wallet")]]))


@router.callback_query(F.data == "ax:wdiscgo")
async def cb_wdiscgo(q: CallbackQuery):
    await sn.delete_sniper_user(q.from_user.id)
    await set_setting(q.from_user.id, paused=1)
    t, kb = await _home(q.from_user.id, "🗑 Wallet disconnected. Sniper stopped.")
    await _send(q, t, kb)


# ── SEND FUNDS (withdraw to another wallet) ──────────────────────────────────
@router.callback_query(F.data == "ax:send")
async def cb_send(q: CallbackQuery):
    uid = q.from_user.id
    if not await _wallet(uid):
        await q.answer("No wallet")
        return
    s, _ = await get_settings(uid, create=True)
    if not s["paused"]:
        await _send(q, f"<b>📤 Send funds</b>\n{RULE}\nStop the sniper first so a trade can't "
                       "land in the middle of a transfer. Open positions stay protected.",
                    _kb([[("■ Stop sniper & continue", "ax:sendstop")], [("Cancel", "ax:wallet")]]))
        return
    _await[uid] = "wd_addr"
    _wd.pop(uid, None)
    await _send(q, f"<b>📤 Send funds</b>\n{RULE}\nPaste the <b>Solana address</b> to send SOL to.\n\n"
                   "• Must be a Solana (SOL) address — a Phantom address or an exchange's "
                   "<i>Solana</i> deposit address\n• Transfers are irreversible\n"
                   "• Tip: send a small test amount first",
                _kb([[("Cancel", "ax:wallet")]]))


@router.callback_query(F.data == "ax:sendstop")
async def cb_sendstop(q: CallbackQuery):
    await set_setting(q.from_user.id, paused=1)
    await cb_send(q)


async def _reserve_for(uid: int) -> int:
    return OPEN_POS_RESERVE if await positions(uid, "open", 1) else 0


async def _stage_send(target, uid: int, text: str):
    w = await _wallet(uid)
    d = _wd.get(uid)
    if not w or not d:
        return
    bal = int(round(await sn.get_sol_balance(w["pubkey"]) * 1e9))
    reserve = await _reserve_for(uid)
    lam, err = parse_amount(text, bal, reserve)
    if err:
        await _send(target, f"❌ {err}", _kb([[("Send max", "ax:sendmax")], [("Cancel", "ax:wallet")]]))
        return
    d.update(lamports=lam, expires=time.time() + WD_TTL)
    _await.pop(uid, None)
    px = await sol_usd()
    usd = f" (≈ ${lam / 1e9 * px:,.2f})" if px else ""
    await _send(target, (
        f"<b>📤 Confirm transfer</b>\n{RULE}\n"
        f"Amount   <b>{lam / 1e9:.6f} SOL</b>{usd}\n"
        f"Network fee  ≈ {SEND_FEE_LAMPORTS / 1e9:.6f} SOL\n"
        f"To:\n<code>{d['dest']}</code>\n\n"
        "Check the <b>whole address</b>. Transfers can't be undone."
        + ("\n\n<i>0.01 SOL stays behind so open positions can still exit.</i>" if reserve else "")),
        _kb([[("✅ Confirm & send", "ax:sendgo")], [("Cancel", "ax:wallet")]]))


@router.callback_query(F.data == "ax:sendmax")
async def cb_sendmax(q: CallbackQuery):
    if q.from_user.id not in _wd:
        await q.answer("Start again from Wallet")
        return
    await _stage_send(q, q.from_user.id, "max")


@router.callback_query(F.data == "ax:sendgo")
async def cb_sendgo(q: CallbackQuery):
    uid = q.from_user.id
    d = _wd.get(uid)
    if not d or not d.get("lamports") or time.time() > d.get("expires", 0):
        _wd.pop(uid, None)
        await _send(q, "That transfer expired. Start again from Wallet → Send funds.",
                    _kb([[("‹ Wallet", "ax:wallet")]]))
        return
    _wd.pop(uid, None)                       # one shot: a double-tap can never send twice
    ok, sig, err = await send_sol(uid, d["dest"], d["lamports"])
    if ok:
        await _send(q, f"✅ <b>Sent {d['lamports'] / 1e9:.6f} SOL</b>\n{RULE}\n"
                       f"To <code>{_short(d['dest'])}</code>\n"
                       f"<a href=\"https://solscan.io/tx/{sig}\">View on Solscan</a>",
                    _kb([[("‹ Wallet", "ax:wallet")], HOME_BTN]))
    else:
        await _send(q, f"❌ <b>Transfer failed</b>\n{html.escape(err)}\nNothing was sent.",
                    _kb([[("‹ Wallet", "ax:wallet")]]))


async def send_sol(uid: int, dest: str, lamports: int) -> Tuple[bool, str, str]:
    """Plain SOL transfer from the user's Apex wallet."""
    try:
        from solders.hash import Hash
        from solders.message import Message as SMessage
        from solders.pubkey import Pubkey
        from solders.system_program import TransferParams, transfer
        from solders.transaction import Transaction
        enc = await sn._get_enc_key(uid)
        if not enc:
            return False, "", "no wallet"
        kp = sn.load_keypair(enc)
        bh = await sn._rpc("getLatestBlockhash", [{"commitment": "finalized"}])
        blockhash = Hash.from_string(bh["value"]["blockhash"])
        ix = transfer(TransferParams(from_pubkey=kp.pubkey(), to_pubkey=Pubkey.from_string(dest),
                                     lamports=int(lamports)))
        tx = Transaction([kp], SMessage.new_with_blockhash([ix], kp.pubkey(), blockhash), blockhash)
        sig = await sn.send_transaction_fast(bytes(tx), kp, False)
        if not sig:
            return False, "", "the network rejected the transaction"
        if not await sn.confirm_transaction(sig, 40):
            return False, sig, "not confirmed in time — check Solscan before retrying"
        return True, sig, ""
    except Exception as e:
        logger.error(f"send_sol: {e}", exc_info=True)
        return False, "", "unexpected error"


async def _status_page() -> Tuple[str, InlineKeyboardMarkup]:
    today, month = await credits_used()
    px = await sol_usd()
    reasons = sorted(_reject_reasons.items(), key=lambda x: -x[1])[:5]
    rtxt = "\n".join(f"  • {k.replace('_', ' ')} — {v}" for k, v in reasons) or "  • nothing yet"
    silent = int(time.time() - _last_webhook)
    text = (f"<b>📡 Engine Status</b>\n{RULE}\n"
            f"Feed       {'🟢 live' if silent < 300 else '🟠 quiet'} · last event {silent}s ago\n"
            f"SOL        ${px:,.2f}\n\n"
            f"<b>Since last restart</b>\n"
            f"Launches seen     <code>{_stats['events']}</code>\n"
            f"Passed stage 1    <code>{_stats['stage1_pass']}</code>\n"
            f"Fully confirmed   <code>{_stats['confirmed']}</code>\n"
            f"Entries           <code>{_stats['entries']}</code>\n\n"
            f"<b>Top rejections</b>\n{rtxt}\n\n"
            f"<b>Helius credits (est.)</b>\n"
            f"Today <code>{today:,}</code> · Month <code>{month:,}</code> / "
            f"<code>{MONTHLY_CREDIT_CEILING:,}</code>")
    return text, _kb([[("↻ Refresh", "ax:stat")], HOME_BTN])


@router.callback_query(F.data == "ax:stat")
async def cb_stat(q: CallbackQuery):
    t, kb = await _status_page()
    await _send(q, t, kb)


@router.message(Command("status"))
async def cmd_status(m: Message):
    t, kb = await _status_page()
    await _send(m, t, kb)


@router.callback_query(F.data == "ax:bl")
async def cb_bl(q: CallbackQuery):
    await _send(q, await _bl_text(), _kb([HOME_BTN]))


@router.message(Command("blacklist"))
async def cmd_bl(m: Message):
    await _send(m, await _bl_text(), _kb([HOME_BTN]))


async def _bl_text() -> str:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute(
            "SELECT mint,reason FROM apex_blacklist ORDER BY ts DESC LIMIT 15")
        rows = await cur.fetchall()
    body = "\n".join(f"• <code>{_short(m)}</code> — {r.replace('_', ' ')}" for m, r in rows) \
        or "Empty — nothing has been banned yet."
    return (f"<b>🚫 Blacklist</b> · {len(_blacklist)} tokens\n{RULE}\n"
            f"Honeypots, tax traps, open authorities and stop-outs are never touched again.\n\n{body}")


@router.callback_query(F.data == "ax:how")
async def cb_how(q: CallbackQuery):
    await _send(q, (
        f"<b>📖 How Apex Works</b>\n{RULE}\n"
        "<b>1 · Detect</b> — every new pump.fun launch and Raydium pool, in seconds. "
        "Most launches are dropped for free before a single paid call.\n\n"
        "<b>2 · Gauntlet</b> — a launch must clear <i>all</i> of these:\n"
        "  • Mint &amp; freeze authority renounced\n"
        "  • Supply 100M – 1T, standard SPL token\n"
        "  • Top 10 wallets ≤ 40%, none above 15%, dev ≤ 5%\n"
        "  • Liquidity ≥ $3,000 (a bonding curve can't be pulled; Raydium LP must be burned or locked)\n"
        "  • Minted within 30 min of the pool\n"
        "  • 5+ unique buyers in the first 60 seconds\n"
        "  • Buy &amp; sell both route, tax ≤ 10%, market cap &lt; $500K\n\n"
        "<b>3 · Enter</b> — your fixed size, never more, via Jupiter.\n\n"
        "<b>4 · Manage</b> — your exit profile, a −45% hard stop, a 25% trailing stop "
        "after +100%, and a 72-hour time-stop.\n\n"
        "<i>Most launches fail. That's the feature.</i>"), _kb([HOME_BTN]))


@router.message(Command("balance"))
async def cmd_balance(m: Message):
    t, kb = await _wallet_page(m.from_user.id)
    await _send(m, t, kb)


@router.message(Command("help"))
async def cmd_help(m: Message):
    await _send(m, (
        f"<b>Commands</b>\n{RULE}\n/start — home\n/positions — what's open\n"
        "/history — last 20 trades\n/settings — exit strategy\n/run — start sniping · /stop — stop sniping\n"
        "/balance — wallet\n/status — engine &amp; credits\n/blacklist — banned tokens"),
        _kb([HOME_BTN]))


class _Awaiting(Filter):
    async def __call__(self, m: Message) -> bool:
        return bool(m.from_user and m.from_user.id in _await and m.text
                    and not m.text.startswith("/"))


@router.message(_Awaiting())
async def on_text(m: Message):
    uid = m.from_user.id
    kind = _await.get(uid)
    raw = (m.text or "").strip()
    if kind == "wd_addr":
        w = await _wallet(uid)
        if not valid_address(raw):
            await m.answer("❌ That isn't a valid Solana address. Paste it again, or cancel.",
                           reply_markup=_kb([[("Cancel", "ax:wallet")]]))
            return
        if w and raw == w["pubkey"]:
            await m.answer("That's this wallet's own address. Paste a different one.")
            return
        _wd[uid] = {"dest": raw}
        _await[uid] = "wd_amt"
        bal = await sn.get_sol_balance(w["pubkey"]) if w else 0.0
        await m.answer(f"<b>How much?</b>\nBalance: <b>{bal:.4f} SOL</b>\n"
                       "Send an amount like <code>0.25</code>, or tap Send max.",
                       parse_mode=ParseMode.HTML,
                       reply_markup=_kb([[("Send max", "ax:sendmax")], [("Cancel", "ax:wallet")]]))
        return
    if kind == "wd_amt":
        await _stage_send(m, uid, raw)
        return
    if kind == "wallet":
        try:
            await m.delete()
        except Exception:
            pass
        ok, pub, err = sn.validate_private_key(raw)
        if not ok:
            await m.answer("❌ That doesn't look like a valid private key. Try again, or cancel.",
                           reply_markup=_kb([[("Cancel", "ax:home")]]))
            return
        await sn.save_sniper_user(uid, raw, pub, "low")   # legacy sniper stays disabled
        _await.pop(uid, None)
        bal = await sn.get_sol_balance(pub)
        t, kb = await _home(uid, f"✅ <b>Wallet connected</b> · <code>{_short(pub)}</code> · "
                                 f"{bal:.3f} SOL\nPress ▶ START SNIPER when you're ready.")
        await m.answer(t, parse_mode=ParseMode.HTML, reply_markup=kb)
        return
    if kind == "size":
        try:
            v = float(raw)
            if v <= 0:
                raise ValueError
        except ValueError:
            await m.answer("Send a number like <code>0.05</code>.", parse_mode=ParseMode.HTML)
            return
        px = await sol_usd()
        if px and v * px < MIN_TRADE_USD:
            await m.answer(f"Minimum is $5 — that's about {MIN_TRADE_USD / px:.3f} SOL.")
            return
        await set_setting(uid, size_sol=v)
        _await.pop(uid, None)
        done = uid in _wizard
        _wizard.discard(uid)
        t, kb = await _home(uid, "🎉 <b>Almost there.</b> Connect your wallet, then press ▶ START SNIPER."
                            if done else f"✅ Size set to <b>{v:g} SOL</b> per trade.")
        await m.answer(t, parse_mode=ParseMode.HTML, reply_markup=kb)


# ══════════════════════════════════════════════════════════════════════════════
# LIFECYCLE
# ══════════════════════════════════════════════════════════════════════════════
BOT_COMMANDS = [
    ("start", "Open Apex"), ("positions", "Open positions"), ("history", "Last 20 trades"),
    ("settings", "Exit strategy"), ("run", "▶ Start sniping"), ("stop", "■ Stop sniping"),
    ("balance", "Wallet balance"), ("status", "Engine & credit status"),
    ("blacklist", "Banned tokens"), ("help", "All commands"),
]


async def startup(bot):
    global _bot
    _bot = bot
    await init_db()
    px = await sol_usd()
    logger.info(f"Apex online — Jupiter {'OK' if px else 'UNREACHABLE'} "
                f"({JUP_BASE}{' key' if JUP_KEY else ' keyless'}), SOL=${px:,.2f}")
    if not px:
        logger.error("Apex cannot reach Jupiter — set JUPITER_API_KEY. Scanning will reject all pools.")
    asyncio.create_task(monitor_loop())
    asyncio.create_task(housekeeping_loop())
