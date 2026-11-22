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
import json
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
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
# Token-2022 extensions that cannot be used to tax, freeze, redirect or confiscate a holder's tokens.
# pump.fun's current launches carry exactly metadataPointer + tokenMetadata. Anything else
# (transferFeeConfig, permanentDelegate, transferHook, defaultAccountState, nonTransferable, ...) is out.
SAFE_2022_EXTENSIONS = {"metadataPointer", "tokenMetadata", "groupPointer", "groupMemberPointer",
                        "tokenGroup", "tokenGroupMember"}

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
PUMP_VIRT_OFFSET = 73_000_000          # virtual token reserves = curve account balance + 73M
PUMP_START_SOL = 30.0                  # virtual SOL at launch
PUMP_CHECK_AGES = [60, 120, 180, 240, 330, 450, 600]   # seconds after launch
PUMP_MAX_WATCHERS = 140                # cap on coins being re-checked at once (credit guard)

# Apex FLOW (the high-frequency entry strategy)
FLOW_TARGET_PER_HOUR = 20
FLOW_MAX_DEV = 8.0                     # dev's opening buy, % of supply
FLOW_MIN_BUYERS = 3                    # unique non-dev wallets in the top-20 holder list
FLOW_TOP10 = 45.0
FLOW_MAX_CLUSTER = 4                   # 4+ wallets with near-identical balances = bundled launch
FLOW_MIN_SOL = 1.0                     # SOL actually raised on the curve (kills dead launches)
FLOW_CHECKS = 2                        # evaluated at the first two check times (T+60s, T+120s)
FLOW_SCORE_START, FLOW_SCORE_FLOOR, FLOW_SCORE_CAP = 0.50, 0.30, 0.80
FLOW_TUNE_WARMUP, FLOW_TUNE_EVERY, FLOW_TUNE_STEP = 600, 300, 0.05

# Apex SCALP (loosest gates, fast in-and-out, ~50 signals an hour)
SCALP_TARGET_PER_HOUR = 50
SCALP_MAX_DEV = 15.0
SCALP_MAX_SINGLE = 25.0
SCALP_TOP10 = 60.0
SCALP_MAX_CLUSTER = 6
SCALP_MIN_BUYERS = 2
SCALP_MIN_SOL = 0.5
SCALP_MOVE_SPEED = 0.8                 # SOL raised per minute that counts as "movement"
SCALP_MOVE_DELTA = 0.4                 # ...or SOL added since the previous look
SCALP_CHECKS = 2
SCALP_SCORE_START, SCALP_SCORE_FLOOR, SCALP_SCORE_CAP = 0.30, 0.10, 0.70
SCALP_FAST_INTERVAL = 6                # seconds between price checks on scalp positions
SCALP_SESSION_LOSSES = 20              # circuit breaker: stop after 20 losing scalp trades in ONE session
SCALP_ENTRY_SLIPPAGE_BPS = 800         # Rule 3: 8% (was 15%). A missed fill costs nothing
SCALP_ACCEL_WINDOW = 30                # Rule 4: watch the curve for the 30s right before the fill
SCALP_SLOPE_WINDOW = 10                # journal: curve price change over the last 10s before the fill
PUMP_GRADUATION_SOL = 85.0             # SOL raised on the curve at graduation (100% of the curve)

# Exit workers (Finding 1): one worker per position, supervised. No position can block another.
WORKER_STEP_TIMEOUT = 100              # one check-and-act cycle may not take longer than this
WORKER_STALL_SECS = 120                # no heartbeat for this long = the worker is dead
SUPERVISOR_INTERVAL = 2                # how often the supervisor looks at every worker
EXIT_FAILS_TO_EMERGENCY = 3            # a triggered exit that fails this many times in a row
EMERGENCY_SLIPPAGE_BPS = 9900          # "any slippage": the most Jupiter accepts short of 100%
EMERGENCY_ATTEMPTS = 3
EMERGENCY_RETRY_SECS = 3

# Rule 5: SOL kept untouchable for gas. No new position may take the wallet below this.
GAS_RESERVE_SOL = 0.15

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
# The scalp exit is automatic for Apex Scalp (not shown in the exit menu).
PROFILES["S"] = {"name": "Scalp", "icon": "🗡", "trail": True, "hidden": True,
                 # Rule 2: tier fractions are of the ORIGINAL position (40% then another 30%)
                 "tiers": [(1.50, 0.40), (2.50, 0.30)], "tier_basis": "original",
                 "stop": 0.90,                        # Rule 1: −10% flat, don't wait for −15%
                 "verdict_secs": 90, "verdict_ratio": 1.08,   # Rule 1: the 90-second verdict
                 "trail_keep": 0.65,                  # Rule 2: remainder trails 35% below its peak
                 "time_stop_secs": None,              # Rule 2: no time limit on the runner
                 "blurb": "90-second verdict, partials into strength, runner with no time limit.",
                 "rows": ["−10% hard stop",
                          "T+90s: not above +8% → sell at market",
                          "Above +8% at T+90s → trailing stop engaged",
                          "+50% → sell 40% of the position",
                          "+150% → sell another 30%",
                          "Remainder trails 35% below its peak, no time limit"]}
VERDICT_BIT = 1 << 6                   # set in tiers_hit once a scalp position passes its 90s verdict
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


_jup_exit_sem = asyncio.Semaphore(8)   # exits get their own lane: screening can never starve them
_jup_limited = {"n": 0, "logged": 0.0}


async def jup_quote(inp: str, out: str, amount: int, slippage_bps: int,
                    lane: str = "scan") -> Optional[Dict]:
    if amount <= 0:
        return None
    try:
        async with (_jup_exit_sem if lane == "exit" else _jup_sem):
            async with aiohttp.ClientSession(headers=_jup_headers()) as s:
                async with s.get(f"{JUP_BASE}/quote",
                                 params={"inputMint": inp, "outputMint": out,
                                         "amount": int(amount),
                                         "slippageBps": slippage_bps,
                                         "restrictIntermediateTokens": "true"},
                                 timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200:
                        if r.status == 429:
                            _jup_limited["n"] += 1
                            if time.time() - _jup_limited["logged"] > 60:
                                _jup_limited["logged"] = time.time()
                                logger.warning(f"Jupiter rate limit hit ({_jup_limited['n']} times so far) "
                                               f"— lane={lane}")
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


def mint_program_check(owner: str, parsed: Dict) -> Tuple[bool, str]:
    """(ok, reason). Classic SPL token: fine. Token-2022: only with harmless extensions."""
    if owner == TOKEN_PROGRAM:
        return True, ""
    if owner == TOKEN_2022_PROGRAM:
        bad = [e.get("extension", "?") for e in (parsed.get("extensions") or [])
               if e.get("extension") not in SAFE_2022_EXTENSIONS]
        if bad:
            return False, "t22_" + str(bad[0])
        return True, ""
    return False, "unknown_token_program"


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
                risk_ack  INTEGER NOT NULL DEFAULT 0,
                entry     TEXT NOT NULL DEFAULT 'apex'
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
            CREATE TABLE IF NOT EXISTS apex_wallets (
                user_id INTEGER NOT NULL, pubkey TEXT NOT NULL, encrypted_key TEXT NOT NULL,
                kind TEXT NOT NULL, created REAL NOT NULL, PRIMARY KEY (user_id, pubkey)
            );
        """)
        await db.commit()
        try:
            await db.execute("ALTER TABLE apex_settings ADD COLUMN risk_ack INTEGER NOT NULL DEFAULT 0")
            await db.commit()
        except Exception:
            pass                                    # column already exists
        try:
            await db.execute("ALTER TABLE apex_settings ADD COLUMN entry TEXT NOT NULL DEFAULT 'apex'")
            await db.commit()
        except Exception:
            pass                                    # column already exists
        # Rule 6: the entry journal on every trade record
        for col, typ in (("buyers_30s_before_entry", "INTEGER"), ("curve_slope", "REAL"),
                         ("dev_holding_pct", "REAL"), ("entry_rank_on_curve", "REAL")):
            try:
                await db.execute(f"ALTER TABLE apex_positions ADD COLUMN {col} {typ}")
                await db.commit()
            except Exception:
                pass                                # column already exists
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


_win_hook = {"fn": None}


def set_win_hook(fn):
    """Wired by vault.py (Fund Collections). Called as fn(user_id, profit_sol, token_name)
    whenever a position closes at a net profit."""
    _win_hook["fn"] = fn


_access = {"deny": lambda uid: False, "admin": 0}


def can_export(uid: int) -> bool:
    """Only the owner may view private keys. Everyone else can deposit, trade and send funds."""
    return bool(_access["admin"]) and uid == _access["admin"]


def set_access_hooks(is_blocked, admin_id: int):
    """Wired by access.py. `is_blocked(uid)` -> True means: no NEW trades for that user
    (their open positions are still managed). `admin_id` shows the owner's Admin button."""
    _access["deny"] = is_blocked
    _access["admin"] = admin_id


async def _active_users() -> List[Dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM apex_settings WHERE paused=0")
        return [dict(r) for r in await cur.fetchall() if not _access["deny"](r["user_id"])]


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


PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
_curve_cache: Dict[str, str] = {}


def curve_pda(mint: str) -> str:
    """The pump.fun bonding-curve account for a mint. Its token account is the curve's inventory.
    (Some launches have 2B supply, with a second ~1B vault run by pump.fun's agent — so we can't
    just take the biggest vault; we look for the one the curve PDA owns.)"""
    if mint not in _curve_cache:
        try:
            from solders.pubkey import Pubkey
            _curve_cache[mint] = str(Pubkey.find_program_address(
                [b"bonding-curve", bytes(Pubkey.from_string(mint))], Pubkey.from_string(PUMP_PROGRAM_ID))[0])
        except Exception:
            return ""                                   # malformed mint: fall back to the biggest vault
        if len(_curve_cache) > 5000:
            _curve_cache.pop(next(iter(_curve_cache)))
    return _curve_cache[mint]


def pump_real_sol(curve_tokens_ui: float) -> float:
    """SOL actually raised on the bonding curve, derived from the curve's token balance.
    Check: balance 1,000M -> 0 SOL (launch); balance 206.9M -> 85 SOL (graduation)."""
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


async def _holder_view(mint: str, supply_ui: float, curve_owner: Optional[str] = None):
    """([(owner, pct_of_supply)] for real wallets only, biggest vault balance, wallet balances).
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
    curve_bal: Optional[float] = None
    for a, info in zip(accts, vals):
        try:
            owner = info["data"]["parsed"]["info"]["owner"]
        except Exception:
            continue
        ui = float(a.get("uiAmount") or 0)
        if curve_owner and owner == curve_owner:
            curve_bal = ui
        if ui <= 0:
            continue
        if is_wallet_address(owner):
            held[owner] = held.get(owner, 0.0) + ui
        else:
            vault_max = max(vault_max, ui)
    return (sorted(((o, v / supply_ui * 100.0) for o, v in held.items()),
                   key=lambda x: -x[1]), (curve_bal if curve_bal is not None else vault_max),
            sorted(held.values(), reverse=True))


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
    prog_ok, prog_why = mint_program_check(program, parsed)
    if not prog_ok:
        return _reject(prog_why, permanent=True)
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
        await asyncio.sleep(1.0)                       # fresh coins / rate limits: one more try
        buy = await jup_quote(WSOL, mint, int(PROBE_SOL * 1e9), ENTRY_SLIPPAGE_BPS)
    if not buy:
        return _reject("no_buy_route")
    got = int(buy["outAmount"])
    sell = await jup_quote(mint, WSOL, got, EXIT_SLIPPAGE_BPS)
    if not sell:
        await asyncio.sleep(1.0)
        sell = await jup_quote(mint, WSOL, got, EXIT_SLIPPAGE_BPS)
    if not sell:
        # Can't price a sale: don't buy. Not blacklisted forever — a missing route is often just
        # Jupiter not having indexed a brand-new pool yet, or a rate limit.
        return _reject("no_sell_route")
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
    prog_ok, prog_why = mint_program_check(info["value"].get("owner", ""), parsed)
    if not prog_ok:
        return _reject(prog_why, permanent=True)
    if parsed.get("mintAuthority"):
        return _reject("mint_authority", permanent=True)
    if parsed.get("freezeAuthority"):
        return _reject("freeze_authority", permanent=True)
    try:
        supply_ui = int(parsed["supply"]) / 10 ** dec
    except Exception:
        supply_ui = float(PUMP_SUPPLY)
    v = await _jupiter_checks(pool.mint, dec, supply_ui)
    if v.ok:
        v.data["decimals"] = dec
    return v


# ══════════════════════════════════════════════════════════════════════════════
# EXIT DECISIONS (pure — easy to test)
# ══════════════════════════════════════════════════════════════════════════════
def decide_exit(profile: str, ratio: float, tiers_hit: int, peak: float,
                clock_start: float, now: float) -> Dict:
    """
    Returns {"actions": [(kind, fraction, reason, tier_index)], "peak": x, "clock_start": y,
             "flags": bits to OR into tiers_hit}. kind is "partial" or "all".
    `fraction` is of the remaining bag, or of the ORIGINAL position when the profile has
    tier_basis="original" (the caller converts it to a token amount).

    Profiles with a 90-second verdict (Scalp) use clock_start as the entry time and never move it.
    """
    prof = PROFILES[profile]
    stop = prof.get("stop", HARD_STOP_RATIO)
    trail_keep = prof.get("trail_keep", TRAIL_KEEP)
    verdict_secs = prof.get("verdict_secs")
    peak = max(peak, ratio)
    out = {"actions": [], "peak": peak, "clock_start": clock_start, "flags": 0}

    if verdict_secs:
        # Trailing stop is engaged once the verdict is passed, or once the first partial has sold
        # (from then on the position is a runner and the verdict no longer applies).
        runner = bool(tiers_hit & 1)
        engaged = bool(tiers_hit & VERDICT_BIT) or runner
        if engaged and ratio <= peak * trail_keep:
            out["actions"].append(("all", 1.0, "trailing_stop", -1))
            return out
        if ratio <= stop:
            out["actions"].append(("all", 1.0, "stop_loss", -1))
            return out
        if not engaged and now - clock_start >= verdict_secs:
            if ratio > prof["verdict_ratio"]:
                out["flags"] |= VERDICT_BIT                      # impulse shown: normal flow continues
            else:
                out["actions"].append(("all", 1.0, "verdict_90s", -1))
                return out
    else:
        trail_on = prof.get("trail_activate", TRAIL_ACTIVATE_RATIO)
        ts_secs = prof.get("time_stop_secs", TIME_STOP_SECS)
        ts_ratio = prof.get("time_stop_ratio", TIME_STOP_RATIO)
        if ratio > ts_ratio:
            clock_start = now
            out["clock_start"] = now
        if prof["trail"] and peak >= trail_on and ratio <= peak * trail_keep:
            out["actions"].append(("all", 1.0, "trailing_stop", -1))
            return out
        if ratio <= stop:
            out["actions"].append(("all", 1.0, "stop_loss", -1))
            return out
        if ts_secs and now - clock_start >= ts_secs and ratio <= ts_ratio:
            out["actions"].append(("all", 1.0, "time_stop", -1))
            return out

    for i, (thr, frac) in enumerate(prof["tiers"]):
        if tiers_hit & (1 << i) or ratio < thr:
            continue
        last = i == len(prof["tiers"]) - 1
        if frac >= 1.0:
            out["actions"].append(("all", 1.0, "target", i))
        else:
            out["actions"].append(("partial", frac, f"target_{int(round((thr - 1) * 100))}pct"
                                   if thr < 2 else f"target_{thr:g}x", i))
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


# ── a user's wallet list ─────────────────────────────────────────────────────
# `sniper_users` always holds the ACTIVE trading wallet (everything else reads it).
# `apex_wallets` is the list of wallets the user owns: kind "phantom" (their own, connected
# first) or "apex" (generated by the bot, only allowed once a Phantom is connected).
async def wl_sync(uid: int):
    """Wallets connected before this list existed are counted as the user's own Phantom."""
    w = await _wallet(uid)
    if not w:
        return
    enc = await sn._get_enc_key(uid)
    if not enc:
        return
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("INSERT OR IGNORE INTO apex_wallets(user_id,pubkey,encrypted_key,kind,created) "
                         "VALUES (?,?,?,?,?)", (uid, w["pubkey"], enc, "phantom", time.time()))
        await db.commit()


async def wl_list(uid: int) -> List[Dict]:
    await wl_sync(uid)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT pubkey,kind,created FROM apex_wallets WHERE user_id=? ORDER BY created",
                               (uid,))
        return [dict(r) for r in await cur.fetchall()]


async def wl_has_phantom(uid: int) -> bool:
    return any(r["kind"] == "phantom" for r in await wl_list(uid))


async def wl_add(uid: int, raw_b58: str, pub: str, kind: str):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("INSERT OR IGNORE INTO apex_wallets(user_id,pubkey,encrypted_key,kind,created) "
                         "VALUES (?,?,?,?,?)", (uid, pub, sn.encrypt_key(raw_b58), kind, time.time()))
        await db.commit()


async def wl_remove(uid: int, pub: str):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("DELETE FROM apex_wallets WHERE user_id=? AND pubkey=?", (uid, pub))
        await db.commit()


async def wl_activate(uid: int, pub: str, force: bool = False) -> Tuple[bool, str]:
    """Make `pub` the wallet Apex trades with. Not while trading, so no position is orphaned."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute("SELECT encrypted_key FROM apex_wallets WHERE user_id=? AND pubkey=?", (uid, pub))
        row = await cur.fetchone()
    if not row:
        return False, "That wallet isn't in your list."
    cur_w = await _wallet(uid)
    if cur_w and cur_w["pubkey"] == pub:
        return True, ""
    if cur_w and not force:
        s, _ = await get_settings(uid, create=True)
        if not s["paused"]:
            return False, "Stop the sniper first, then switch wallets."
        if await positions(uid):
            return False, "You have open positions on the current wallet. Let them close, then switch."
    await sn.save_sniper_user(uid, sn.decrypt_key(row[0]), pub, "low")
    await set_setting(uid, paused=1)
    return True, ""


async def _confirm_tx(sig: str, timeout_s: int = 30) -> Tuple[str, str]:
    """('ok'|'failed'|'timeout', err). A transaction can land on-chain AND have failed
    (e.g. slippage exceeded) — sn.confirm_transaction reports that as success, so we don't use it."""
    deadline = time.time() + timeout_s
    async with aiohttp.ClientSession() as s:
        while time.time() < deadline:
            try:
                async with s.post(sn.PUBLIC_RPC,
                                  json={"jsonrpc": "2.0", "id": 1, "method": "getSignatureStatuses",
                                        "params": [[sig], {"searchTransactionHistory": False}]},
                                  timeout=aiohttp.ClientTimeout(total=4)) as r:
                    val = (((await r.json()).get("result") or {}).get("value") or [None])[0]
                if val:
                    if val.get("err"):
                        return "failed", json.dumps(val["err"])
                    if val.get("confirmationStatus") in ("confirmed", "finalized"):
                        return "ok", ""
            except Exception:
                pass
            await asyncio.sleep(2)
    return "timeout", ""


def friendly_err(err: str) -> str:
    e = (err or "").lower()
    if "6001" in e or "slippage" in e:
        return "price moved too fast — slippage limit exceeded"
    if "insufficient" in e or "0x1" in e:
        return "not enough SOL to cover the trade and fees"
    if "route" in e:
        return "no route within the slippage limit"
    if "build" in e:
        return "couldn't build the swap"
    if err:
        return err
    return "the transaction failed"


_fail_notes: Dict[int, List[float]] = {}
_exit_warned: Dict[int, float] = {}


async def _notify_fail(uid: int, head: str, reason: str, sig: str = "", cost_note: str = ""):
    """Failure alerts, capped at 8 an hour per user so a bad stretch can't flood the chat."""
    now = time.time()
    log = [t for t in _fail_notes.get(uid, []) if now - t < 3600]
    if len(log) >= 8:
        _fail_notes[uid] = log
        return
    log.append(now)
    _fail_notes[uid] = log
    link = f'\n<a href="https://solscan.io/tx/{sig}">View transaction</a>' if sig else ""
    await _notify(uid, f"⚠️ <b>{head}</b>\n{RULE}\nReason: {html.escape(reason)}"
                       f"{(chr(10) + cost_note) if cost_note else ''}{link}")


async def _live_buy(uid: int, mint: str, sol: float, dec: int,
                    slippage_bps: int = ENTRY_SLIPPAGE_BPS) -> Tuple[bool, float, str, str]:
    w = await _wallet(uid)
    enc = await sn._get_enc_key(uid)
    if not w or not enc:
        return False, 0.0, "", "no wallet connected"
    q = await jup_quote(WSOL, mint, int(sol * 1e9), slippage_bps)
    if not q:
        return False, 0.0, "", f"no route within {slippage_bps / 100:g}% slippage"
    raw = await jup_swap_tx(q, w["pubkey"])
    if not raw:
        return False, 0.0, "", "swap build failed"
    kp = sn.load_keypair(enc)
    sig = await sn.send_transaction_fast(raw, kp, True)
    if not sig:
        return False, 0.0, "", "the network rejected the transaction"
    status, err = await _confirm_tx(sig)
    if status == "failed":
        return False, 0.0, sig, friendly_err(err)
    if status == "timeout":
        await asyncio.sleep(8)               # it may still have landed — check the wallet itself
    bal = await sn.get_token_balance(w["pubkey"], mint)
    if bal <= 0 and status == "ok":
        await asyncio.sleep(3)
        bal = await sn.get_token_balance(w["pubkey"], mint)
    if bal <= 0:
        return False, 0.0, sig, ("not confirmed in time and no tokens arrived" if status == "timeout"
                                 else "confirmed but no tokens arrived")
    return True, bal, sig, ""


async def _sell(pos: Dict, tokens: float,
                slippage_bps: int = EXIT_SLIPPAGE_BPS) -> Tuple[bool, float, str]:
    """Returns (ok, sol_out, tx). Paper sells simply realise the live quote."""
    dec = pos["decimals"]
    amount = int(tokens * 10 ** dec)
    q = await jup_quote(pos["mint"], WSOL, amount, slippage_bps, lane="exit")
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
    if not sig:
        return False, 0.0, ""
    status, _err = await _confirm_tx(sig)
    if status != "ok":
        return False, 0.0, sig
    return True, sol_out, sig


JOURNAL_FIELDS = ("buyers_30s_before_entry", "curve_slope", "dev_holding_pct", "entry_rank_on_curve")


async def _enter_one(u: Dict, mint: str, name: str, symbol: str, dec: int, mcap: float,
                     journal: Optional[Dict] = None, buyers_task: Optional["asyncio.Task"] = None):
    uid = u["user_id"]
    journal = journal or {}
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
        profile = "S" if u.get("entry") == "scalp" else u["profile"]
        w = await _wallet(uid)
        if not w:
            return
        bal = await sn.get_sol_balance(w["pubkey"])
        # Rule 5: available SOL minus this position minus the 0.15 SOL gas reserve must not go negative
        if bal - sol - GAS_RESERVE_SOL < 0:
            if time.time() - u["low_alert"] > 3600:
                await set_setting(uid, low_alert=time.time())
                await _notify(uid, "⚠️ <b>Gas reserve protected — entries paused</b>\n"
                                   f"Balance <b>{bal:.4f} SOL</b>. A trade needs <b>{sol:.4f} SOL</b> plus "
                                   f"the <b>{GAS_RESERVE_SOL:g} SOL</b> that is always kept for exits "
                                   f"(≈ {sol + GAS_RESERVE_SOL:.4f} SOL). Open positions are still managed. "
                                   "Top up the wallet to keep sniping.")
            return
        slip = SCALP_ENTRY_SLIPPAGE_BPS if profile == "S" else ENTRY_SLIPPAGE_BPS
        ok, tokens, sig, err = await _live_buy(uid, mint, sol, dec, slippage_bps=slip)
        if not ok:
            logger.warning(f"live buy failed {mint[:8]}: {err}")
            await _notify_fail(
                uid, f"Buy didn't go through — {name}", err, sig,
                "A failed on-chain attempt costs only the network fee." if sig else
                "No SOL was spent.")
            return
        if tokens <= 0:
            return
        entry_price = sol / tokens
        now = time.time()
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            cur = await db.execute(
                "INSERT INTO apex_positions(user_id,mode,mint,name,symbol,entry_ts,entry_sol,"
                "entry_tokens,entry_price,entry_mcap,decimals,tokens_left,profile,peak_ratio,"
                "clock_start,buy_tx,curve_slope,dev_holding_pct,entry_rank_on_curve) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (uid, mode, mint, name, symbol, now, sol, tokens, entry_price, mcap, dec,
                 tokens, profile, 1.0, now, sig, journal.get("curve_slope"),
                 journal.get("dev_holding_pct"), journal.get("entry_rank_on_curve")))
            pid = cur.lastrowid
            await db.commit()
        if buyers_task is not None:
            _spawn(_attach_buyers(pid, buyers_task))
        _stats["entries"] += 1
        prof = PROFILES[profile]
        tag = "🔴 LIVE"
        if profile == "S":
            plan = ("Stop       <code>-10%</code> hard · 90s verdict: above <code>+8%</code> or out\n"
                    "Partials   40% at <code>+50%</code> · 30% at <code>+150%</code> · rest trails 35%\n")
        else:
            plan = (f"Stop       <code>-{(1 - prof.get('stop', HARD_STOP_RATIO)) * 100:.0f}%</code> hard · "
                    f"trailing after +{(prof.get('trail_activate', TRAIL_ACTIVATE_RATIO) - 1) * 100:.0f}%\n")
        await _notify(uid, (
            f"🎯 <b>Position opened</b>  ·  {tag}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<b>{html.escape(name)}</b> <code>${html.escape(symbol)}</code>\n"
            f"<code>{mint}</code>\n\n"
            f"Entry      <code>{entry_price:.3e} SOL</code>\n"
            f"Size       <code>{sol:.4f} SOL</code> (≈ ${sol * px:,.2f})\n"
            f"Mkt cap    <code>${mcap:,.0f}</code>\n"
            f"Exit plan  {prof['icon']} {prof['name']}\n"
            f"{plan}\n"
            f"<a href=\"https://dexscreener.com/solana/{mint}\">Chart</a> · "
            f"<a href=\"https://solscan.io/token/{mint}\">Solscan</a>"))
    except Exception as e:
        logger.error(f"enter_one {uid} {mint[:8]}: {e}", exc_info=True)


async def _close_or_reduce(pos: Dict, tokens: float, reason: str, tier: int,
                           full: bool, slippage_bps: int = EXIT_SLIPPAGE_BPS) -> bool:
    ok, sol_out, tx = await _sell(pos, tokens, slippage_bps)
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
        elif reason == "verdict_90s":
            head = "⏱ <b>90-second verdict — no impulse, out</b>"
        elif reason == "emergency_sell":
            head = "🚨 <b>Emergency exit</b>"
        else:
            head = "✅ <b>Position closed — target hit</b>"
        sign = "+" if pnl >= 0 else ""
        await _notify(uid, (
            f"{head}\n━━━━━━━━━━━━━━━\n{nm}\n\n"
            f"Entry     <code>{pos['entry_price']:.3e}</code>\n"
            f"Returned  <code>{realized:.4f} SOL</code>\n"
            f"Net       <code>{sign}{pnl:.4f} SOL</code> (≈ {sign}${pnl * px:,.2f})\n"
            f"Reason    <code>{reason}</code>"))
        if pnl > 0 and _win_hook["fn"]:
            asyncio.create_task(_win_hook["fn"](uid, pnl, pos["name"] or ""))
        if pos["profile"] == "S":
            await _scalp_breaker(uid)
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


_PROC_START = time.time()
_session_start: Dict[int, float] = {}


def session_start(uid: int) -> float:
    """When this user's current session began: the last time they pressed START (or, if the bot
    restarted mid-session, the restart). Never reaches back into earlier trade history."""
    return _session_start.setdefault(uid, _PROC_START)


async def _scalp_breaker(uid: int):
    """Stop the sniper once this session has produced 20 losing scalp trades.
    Only trades closed since the session began are counted — earlier history is ignored."""
    since = session_start(uid)
    rows = [p for p in await positions(uid, "closed", 500)
            if p["profile"] == "S" and (p["exit_ts"] or 0) >= since]
    losses = [p for p in rows if p["realized_sol"] < p["entry_sol"]]
    if len(losses) < SCALP_SESSION_LOSSES:
        return
    net = sum(p["realized_sol"] - p["entry_sol"] for p in rows)
    await set_setting(uid, paused=1)
    await _notify(uid, "🛑 <b>Session circuit breaker — sniper stopped</b>\n" + RULE +
                  f"\nThis session: <b>{len(losses)} losing</b> scalp trades out of {len(rows)} "
                  f"(net <b>{net:+.4f} SOL</b>). Apex stopped itself so a bad stretch can't keep "
                  "draining the wallet. Open positions are still protected.\n\n"
                  "Press ▶ START for a fresh session whenever you're ready, or switch strategy.")


async def _manage(pos: Dict):
    lock = _pos_locks.setdefault(pos["id"], asyncio.Lock())
    if lock.locked():
        return
    async with lock:
        tokens = pos["tokens_left"]
        if tokens <= 0:
            return
        q = await jup_quote(pos["mint"], WSOL, int(tokens * 10 ** pos["decimals"]),
                            EXIT_SLIPPAGE_BPS, lane="exit")
        if not q:
            fails = pos["fails"] + 1
            if fails >= (50 if pos["profile"] == "S" else DEAD_AFTER_FAILS):
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
        hit = pos["tiers_hit"] | d.get("flags", 0)
        await _update_pos(pos["id"], peak_ratio=d["peak"], clock_start=d["clock_start"],
                          last_ratio=ratio, fails=0, tiers_hit=hit)
        cur = dict(pos, tokens_left=tokens, tiers_hit=hit)
        of_original = PROFILES[pos["profile"]].get("tier_basis") == "original"
        for kind, frac, reason, tier in d["actions"]:
            if kind == "all":
                amt = cur["tokens_left"]
            elif of_original:
                amt = min(cur["tokens_left"], pos["entry_tokens"] * frac)
            else:
                amt = cur["tokens_left"] * frac
            full = kind == "all" or amt >= cur["tokens_left"] * 0.999
            if not await _close_or_reduce(cur, amt, reason, tier, full):
                fails = _exit_fails.get(pos["id"], 0) + 1 if full else 0
                _exit_fails[pos["id"]] = fails
                if full and fails >= EXIT_FAILS_TO_EMERGENCY:
                    _exit_fails.pop(pos["id"], None)
                    raise ExitFailed(reason)          # the worker escalates to an emergency sell
                if time.time() - _exit_warned.get(pos["id"], 0) > 300:
                    _exit_warned[pos["id"]] = time.time()
                    await _notify(pos["user_id"],
                                  f"⚠️ <b>Exit didn't go through</b> — "
                                  f"{html.escape(pos['name'] or pos['mint'][:6])}\n"
                                  f"Trigger: <code>{reason}</code>. The sell failed (usually slippage "
                                  "or a congested network). Apex retries on the next check"
                                  + (f"; after {EXIT_FAILS_TO_EMERGENCY} failed tries it sells at "
                                     "any slippage." if full else "."))
                break
            _exit_fails.pop(pos["id"], None)
            cur = await get_position(pos["id"])
            if not cur or cur["status"] != "open":
                break


# ══════════════════════════════════════════════════════════════════════════════
# EXIT WORKERS — one async worker per open position, watched by a supervisor.
# A worker that dies (crashes, hangs, or can't get a triggered exit through) hands its position
# to an emergency market sell at any slippage. No position can block another.
# ══════════════════════════════════════════════════════════════════════════════
class ExitFailed(Exception):
    """A triggered full exit failed EXIT_FAILS_TO_EMERGENCY times in a row."""


_exit_fails: Dict[int, int] = {}
_workers: Dict[int, asyncio.Task] = {}
_beats: Dict[int, float] = {}
_emergency: Dict[int, asyncio.Task] = {}


async def get_position(pid: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM apex_positions WHERE id=?", (pid,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def _token_balance_strict(pubkey: str, mint: str) -> Optional[float]:
    """The wallet's real token balance, or None if the RPC couldn't say (never a guessed 0)."""
    res = await _h("getTokenAccountsByOwner", [pubkey, {"mint": mint}, {"encoding": "jsonParsed"}])
    if res is None or "value" not in res:
        return None
    try:
        return sum(float(a["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"] or 0)
                   for a in res["value"])
    except Exception:
        return None


def _interval(pos: Dict) -> float:
    return SCALP_FAST_INTERVAL if pos["profile"] == "S" else MONITOR_INTERVAL


async def _position_worker(pid: int):
    """Checks one position on its own clock until it closes. Each step has a hard timeout."""
    while True:
        _beats[pid] = time.time()
        pos = await get_position(pid)
        if not pos or pos["status"] != "open" or pos["tokens_left"] <= 0:
            return
        await asyncio.wait_for(_manage(pos), timeout=WORKER_STEP_TIMEOUT)
        _beats[pid] = time.time()
        await asyncio.sleep(_interval(pos))


async def _emergency_sell(pid: int, why: str) -> bool:
    """Sell everything still held, at any slippage. Uses the wallet's real balance, so a sell that
    landed after its worker died is never repeated."""
    pos = await get_position(pid)
    if not pos or pos["status"] != "open" or pos["tokens_left"] <= 0:
        return True
    lock = _pos_locks.setdefault(pid, asyncio.Lock())
    try:
        await asyncio.wait_for(lock.acquire(), timeout=15)
        held = True
    except asyncio.TimeoutError:
        held = False                                   # a hung step still holds it: go anyway
    try:
        pos = await get_position(pid)
        if not pos or pos["status"] != "open" or pos["tokens_left"] <= 0:
            return True
        name = html.escape(pos["name"] or pos["mint"][:6])
        tokens = pos["tokens_left"]
        if pos["mode"] == "live":
            w = await _wallet(pos["user_id"])
            bal = await _token_balance_strict(w["pubkey"], pos["mint"]) if w else None
            if bal is not None and bal <= 0:
                await _update_pos(pid, status="closed", tokens_left=0, exit_ts=time.time(),
                                  exit_reason="emergency_sell")
                logger.warning(f"emergency {pid}: wallet already holds no {pos['mint'][:8]} — closed")
                return True
            if bal is not None:
                tokens = min(tokens, bal)
        for attempt in range(1, EMERGENCY_ATTEMPTS + 1):
            if await _close_or_reduce(pos, tokens, "emergency_sell", -1, True, EMERGENCY_SLIPPAGE_BPS):
                logger.warning(f"emergency sell {pid} {pos['mint'][:8]} done ({why}, attempt {attempt})")
                return True
            if attempt < EMERGENCY_ATTEMPTS:
                await asyncio.sleep(EMERGENCY_RETRY_SECS)
        logger.error(f"emergency sell {pid} {pos['mint'][:8]} failed {EMERGENCY_ATTEMPTS}x ({why})")
        await _notify(pos["user_id"],
                      f"🚨 <b>Emergency exit failed</b> — {name}\n"
                      f"Trigger: <code>{html.escape(why)}</code>. No sell went through at any slippage. "
                      "Apex keeps watching this position and will try again.")
        return False
    finally:
        if held:
            lock.release()


async def _run_emergency(pid: int, why: str):
    try:
        await _emergency_sell(pid, why)
    except Exception as e:
        logger.error(f"emergency {pid}: {e}", exc_info=True)


def _worker_died(pid: int, task: asyncio.Task) -> Optional[str]:
    """Why a finished worker ended badly, or None if it finished normally."""
    if task.cancelled():
        return "worker cancelled"
    exc = task.exception()
    if exc is None:
        return None
    if isinstance(exc, ExitFailed):
        return f"exit {exc} failed {EXIT_FAILS_TO_EMERGENCY}x"
    if isinstance(exc, asyncio.TimeoutError):
        return f"check took over {WORKER_STEP_TIMEOUT}s"
    return f"worker crashed: {type(exc).__name__}: {exc}"


async def supervise_once(now: Optional[float] = None):
    """One supervisor pass: start missing workers, and hand dead or stalled ones to an emergency sell."""
    now = now or time.time()
    open_ids = {p["id"] for p in await positions(None, "open", 1000)}
    for pid in list(_workers):
        t = _workers[pid]
        if pid not in open_ids:
            if not t.done():
                t.cancel()
            _workers.pop(pid, None)
            _beats.pop(pid, None)
            continue
        why = None
        if t.done():
            why = _worker_died(pid, t)
        elif now - _beats.get(pid, now) > WORKER_STALL_SECS:
            t.cancel()
            why = f"no heartbeat for {int(now - _beats[pid])}s"
        if t.done() or why:
            _workers.pop(pid, None)
            _beats.pop(pid, None)
            if why and not (pid in _emergency and not _emergency[pid].done()):
                logger.error(f"exit worker {pid} died — {why}; emergency sell")
                _emergency[pid] = asyncio.create_task(_run_emergency(pid, why))
    for pid in open_ids:
        if pid in _workers or (pid in _emergency and not _emergency[pid].done()):
            continue
        _beats[pid] = now
        _workers[pid] = asyncio.create_task(_position_worker(pid))


async def monitor_loop():
    logger.info("Apex exit supervisor started (one worker per open position)")
    while True:
        try:
            await supervise_once()
        except Exception as e:
            logger.error(f"supervisor: {e}", exc_info=True)
        await asyncio.sleep(SUPERVISOR_INTERVAL)


async def housekeeping_loop():
    """Credit flush, webhook-silence watchdog, seen-set pruning."""
    while True:
        await asyncio.sleep(30)
        try:
            await _flush_credits()
            _act = await _active_users()
            flow_tune(time.time(), any(u.get("entry") == "flow" for u in _act))
            scalp_tune(time.time(), any(u.get("entry") == "scalp" for u in _act))
            global _last_summary
            if time.time() - _last_summary > 300:
                _last_summary = time.time()
                today, month = await credits_used()
                top = sorted(_reject_reasons.items(), key=lambda x: -x[1])[:3]
                logger.info(f"APEX flow: threshold={_flow['min_score']:.2f} "
                            f"signals_last_hour={len([t for t in _flow['signals'] if time.time() - t < 3600])}"
                            f" | scalp: threshold={_scalp['min_score']:.2f} "
                            f"signals_last_hour={len([t for t in _scalp['signals'] if time.time() - t < 3600])}")
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
                    msg = ("🧮 <b>Credit guard engaged</b>\nApex has used ~95% of this "
                           "month's Helius credit ceiling. Screening is paused to protect "
                           "your budget; exits keep running.") if can_export(u["user_id"]) else (
                           "⏸ <b>Screening paused</b>\nNew entries are paused for now. "
                           "Open positions are still protected and exits keep running.")
                    await _notify(u["user_id"], msg)
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

# ── DexScreener (free, no Helius credits) ────────────────────────────────────
_dex_sem = asyncio.Semaphore(2)
_dex_cache: Dict[str, Tuple[float, Optional[Dict]]] = {}


async def dex_metrics(mint: str) -> Optional[Dict]:
    """5-minute buys/sells, volume and price change from DexScreener. None if not indexed yet."""
    hit = _dex_cache.get(mint)
    if hit and time.time() - hit[0] < 20:
        return hit[1]
    out = None
    try:
        async with _dex_sem:
            async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0",
                                                      "Accept": "application/json"}) as sess:
                async with sess.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
                                    timeout=aiohttp.ClientTimeout(total=6)) as r:
                    if r.status == 200:
                        pairs = [p for p in (await r.json()).get("pairs") or []
                                 if p.get("chainId") == "solana"]
                        if pairs:
                            p = pairs[0]
                            m5 = (p.get("txns") or {}).get("m5") or {}
                            out = {"buys5": int(m5.get("buys") or 0), "sells5": int(m5.get("sells") or 0),
                                   "vol5": float((p.get("volume") or {}).get("m5") or 0),
                                   "chg5": float((p.get("priceChange") or {}).get("m5") or 0),
                                   "mcap": float(p.get("marketCap") or 0)}
    except Exception as e:
        logger.debug(f"dexscreener: {e}")
    _dex_cache[mint] = (time.time(), out)
    return out


def cluster_size(amounts: List[float], tol: float = 0.015) -> int:
    """Largest group of wallets whose balances are within `tol` of each other.
    Bundled launches spread supply over many fresh wallets holding near-identical amounts."""
    a = sorted(x for x in amounts if x > 0)
    best, j = 0, 0
    for i in range(len(a)):
        while a[i] > a[j] * (1 + tol):
            j += 1
        best = max(best, i - j + 1)
    return best


def flow_score(buyers: int, top10: float, real_sol: float, age_s: float,
               dex: Optional[Dict]) -> float:
    """0..1 momentum score. DexScreener terms are dropped (and weights renormalised) if unavailable."""
    parts = [(0.25, min(1.0, buyers / 12)),
             (0.10, max(0.0, 1 - top10 / FLOW_TOP10)),
             (0.20, min(1.0, (real_sol / max(age_s / 60, 0.5)) / 3.0))]     # SOL raised per minute
    if dex:
        n = dex["buys5"] + dex["sells5"]
        pressure = dex["buys5"] / n if n else 0.5
        chg = dex["chg5"]
        sweet = (1.0 if 0 <= chg <= 150 else 0.5 if 150 < chg <= 400
                 else 0.3 if -20 <= chg < 0 else 0.0)                      # trending, not already parabolic
        parts += [(0.20, min(1.0, max(0.0, (pressure - 0.45) / 0.30))),    # buy pressure
                  (0.10, min(1.0, n / 40)),                                # activity
                  (0.15, sweet)]
    w = sum(x for x, _ in parts)
    return sum(x * v for x, v in parts) / w


_flow = {"min_score": FLOW_SCORE_START, "signals": [], "active_since": 0.0, "last_tune": 0.0}


def flow_evaluate(pool: Pool, holders, amounts, real_sol: float, age_s: float,
                  dex: Optional[Dict]) -> Tuple[bool, str, float]:
    """(ok, reason, score). Reasons other than 'score_low' are final for this coin."""
    if pool.dev_pct0 > FLOW_MAX_DEV:
        return False, "dev_over_8", 0.0
    if holders and holders[0][1] > MAX_SINGLE_PCT:
        return False, "whale_over_15", 0.0
    top10 = sum(p for _, p in holders[:10])
    if top10 > FLOW_TOP10:
        return False, "top10_over_45", 0.0
    if pool.dev_pct0 >= 1.0 and not any(o == pool.dev for o, _ in holders):
        return False, "dev_sold", 0.0
    if cluster_size(amounts) >= FLOW_MAX_CLUSTER:
        return False, "bundled", 0.0
    buyers = sum(1 for o, _ in holders if o != pool.dev)
    if buyers < FLOW_MIN_BUYERS:
        return False, "few_buyers", 0.0
    if real_sol < FLOW_MIN_SOL:
        return False, "curve_idle", 0.0
    sc = flow_score(buyers, top10, real_sol, age_s, dex)
    if sc < _flow["min_score"]:
        return False, "score_low", sc
    return True, "", sc


def _tune(state: Dict, target: int, floor: float, cap: float, now: float, active: bool):
    """Steer a score threshold toward `target` signals an hour: loosen when short, tighten when flooded.
    Warm-up first, then at most one step every 5 minutes."""
    if not active:
        state["active_since"] = 0.0
        return
    if not state["active_since"]:
        state["active_since"] = now
        return
    state["signals"] = [t for t in state["signals"] if now - t < 3600]
    ran = min(3600.0, now - state["active_since"])
    if ran < FLOW_TUNE_WARMUP or now - state["last_tune"] < FLOW_TUNE_EVERY:
        return
    state["last_tune"] = now
    expected = target * ran / 3600
    n = len(state["signals"])
    if n < expected * 0.9:
        state["min_score"] = max(floor, round(state["min_score"] - FLOW_TUNE_STEP, 3))
    elif n > expected * 1.8:
        state["min_score"] = min(cap, round(state["min_score"] + FLOW_TUNE_STEP, 3))


def flow_tune(now: float, active: bool):
    _tune(_flow, FLOW_TARGET_PER_HOUR, FLOW_SCORE_FLOOR, FLOW_SCORE_CAP, now, active)


_scalp = {"min_score": SCALP_SCORE_START, "signals": [], "active_since": 0.0, "last_tune": 0.0}


def scalp_tune(now: float, active: bool):
    _tune(_scalp, SCALP_TARGET_PER_HOUR, SCALP_SCORE_FLOOR, SCALP_SCORE_CAP, now, active)


def scalp_evaluate(pool: Pool, holders, amounts, real_sol: float, age_s: float,
                   dex: Optional[Dict], prev_sol: Optional[float]) -> Tuple[bool, str, float]:
    """Loosest gates + a 'is it moving right now' test. Reasons other than the retryable ones are final."""
    if pool.dev_pct0 > SCALP_MAX_DEV:
        return False, "dev_over_15", 0.0
    if holders and holders[0][1] > SCALP_MAX_SINGLE:
        return False, "whale_over_25", 0.0
    top10 = sum(p for _, p in holders[:10])
    if top10 > SCALP_TOP10:
        return False, "top10_over_60", 0.0
    if pool.dev_pct0 >= 1.0 and not any(o == pool.dev for o, _ in holders):
        return False, "dev_sold", 0.0                      # the dumper has already left
    if cluster_size(amounts) >= SCALP_MAX_CLUSTER:
        return False, "bundled", 0.0
    if dex and dex["sells5"] > dex["buys5"] * 1.5 and dex["sells5"] >= 6:
        return False, "dumping", 0.0                       # sells already outrunning buys
    if dex and dex["chg5"] > 600:
        return False, "parabolic", 0.0                     # we'd be buying the top
    buyers = sum(1 for o, _ in holders if o != pool.dev)
    if buyers < SCALP_MIN_BUYERS:
        return False, "few_buyers", 0.0
    if real_sol < SCALP_MIN_SOL:
        return False, "curve_idle", 0.0
    # movement: money is arriving on the curve right now
    speed = real_sol / max(age_s / 60, 0.5)
    moving = speed >= SCALP_MOVE_SPEED or (prev_sol is not None and real_sol - prev_sol >= SCALP_MOVE_DELTA)
    if dex:
        n = dex["buys5"] + dex["sells5"]
        moving = moving and n >= 5 and dex["buys5"] / n >= 0.5
    if not moving:
        return False, "no_movement", 0.0
    sc = flow_score(buyers, min(top10, FLOW_TOP10), real_sol, age_s, dex)
    if sc < _scalp["min_score"]:
        return False, "score_low", sc
    return True, "", sc


_bg_tasks: set = set()


def _spawn(coro) -> "asyncio.Task":
    """create_task that keeps a strong reference until the task finishes (asyncio only keeps a
    weak one, so a long-waiting task could otherwise be garbage-collected mid-flight)."""
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t


def curve_price_ratio(sol_a: float, sol_b: float) -> float:
    """Bonding-curve price at b relative to a, from SOL raised. Price = vSOL / vTokens and
    vTokens = K / vSOL, so price = vSOL² / K."""
    va, vb = PUMP_START_SOL + sol_a, PUMP_START_SOL + sol_b
    return (vb / va) ** 2 if va > 0 else 1.0


async def _curve_read(pool: Pool, curve_owner: str) -> Optional[Tuple[float, float, list]]:
    """(timestamp, SOL raised, holders) for a pump.fun coin, or None if unreadable."""
    async with _pipeline_sem:
        hv = await _holder_view(pool.mint, PUMP_SUPPLY, curve_owner)
    if hv is None:
        return None
    holders, curve_ui, _ = hv
    return time.time(), pump_real_sol(curve_ui), holders


async def scalp_acceleration_gate(pool: Pool, curve_owner: str, sol0: float, t0: float,
                                  prev_sol: Optional[float], prev_t: Optional[float]
                                  ) -> Tuple[bool, str, Dict]:
    """
    Rule 4: enter only if the bonding curve shows UPWARD ACCELERATION in the 30 seconds right
    before the fill. Readings at t0 (the passing check), t0+20 and t0+30.
        recent speed  = SOL added in the 30s window / 30s
        prior speed   = SOL added between the previous check and t0 (or since launch)
        pass          = recent speed > 0 and recent speed > prior speed
    Also returns the journal fields measured at the fill: curve_slope (% price change over the
    last 10s), dev_holding_pct, entry_rank_on_curve (% of the way to graduation), and
    price_move (curve price at the fill vs t0, used to update the market cap).
    """
    await asyncio.sleep(max(0.0, t0 + SCALP_ACCEL_WINDOW - SCALP_SLOPE_WINDOW - time.time()))
    r20 = await _curve_read(pool, curve_owner)
    await asyncio.sleep(max(0.0, t0 + SCALP_ACCEL_WINDOW - time.time()))
    r30 = await _curve_read(pool, curve_owner)
    if r20 is None or r30 is None:
        return False, "accel_unreadable", {}
    t20, sol20, _ = r20
    t30, sol30, holders = r30
    recent = (sol30 - sol0) / max(t30 - t0, 1.0)
    if prev_sol is not None and prev_t is not None and t0 - prev_t > 0:
        prior = (sol0 - prev_sol) / (t0 - prev_t)
    else:
        prior = sol0 / max(t0 - pool.ts, 1.0)
    if recent <= 0:
        return False, "accel_flat", {}
    if recent <= prior:
        return False, "accel_slowing", {}
    return True, "", {
        "curve_slope": (curve_price_ratio(sol20, sol30) - 1.0) * 100.0,
        "dev_holding_pct": next((p for o, p in holders if o == pool.dev), 0.0),
        "entry_rank_on_curve": min(100.0, sol30 / PUMP_GRADUATION_SOL * 100.0),
        "price_move": curve_price_ratio(sol0, sol30),
    }


async def _buyers_before(mint: str, curve_addr: str, t_end: float,
                         window: float = SCALP_ACCEL_WINDOW) -> Optional[int]:
    """Journal: unique wallets that BOUGHT on the bonding curve in the `window` seconds before
    t_end. A buyer is a fee payer whose balance of this token went up in that transaction."""
    sigs = await _h("getSignaturesForAddress", [curve_addr, {"limit": 100, "commitment": "confirmed"}])
    if sigs is None:
        return None
    win = [s["signature"] for s in sigs
           if not s.get("err") and s.get("blockTime") and t_end - window <= s["blockTime"] < t_end][:60]
    txs = await asyncio.gather(*[_h("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed",
                                                             "maxSupportedTransactionVersion": 0}])
                                 for sig in win])
    buyers = set()
    for tx in txs:
        try:
            keys = tx["transaction"]["message"]["accountKeys"]
            payer = keys[0]["pubkey"] if isinstance(keys[0], dict) else keys[0]
            meta = tx.get("meta") or {}

            def held(side: str) -> float:
                return sum(float(b["uiTokenAmount"]["uiAmount"] or 0) for b in meta.get(side) or []
                           if b.get("mint") == mint and b.get("owner") == payer)

            if held("postTokenBalances") > held("preTokenBalances"):
                buyers.add(payer)
        except Exception:
            continue
    return len(buyers)


async def _attach_buyers(pid: int, task: "asyncio.Task"):
    try:
        n = await task
    except Exception:
        n = None
    if n is not None:
        await _update_pos(pid, buyers_30s_before_entry=n)


async def _finish_entry(pool: Pool, dec: int, mcap: float, entry: str = "apex",
                        journal: Optional[Dict] = None):
    t_decide = time.time()
    buyers_task = (_spawn(_buyers_before(pool.mint, curve_pda(pool.mint), t_decide))
                   if pool.kind == "pump" else None)
    meta = await _das("getAsset", {"id": pool.mint}) or {}
    md = (meta.get("content") or {}).get("metadata") or {}
    name = md.get("name") or pool.mint[:6]
    symbol = md.get("symbol") or "?"
    if entry == "flow":
        _flow["signals"].append(time.time())
    elif entry == "scalp":
        _scalp["signals"].append(time.time())
    for u in await _active_users():
        if u.get("entry", "apex") == entry:
            _spawn(_enter_one(u, pool.mint, name, symbol, dec, mcap, journal, buyers_task))
    logger.info(f"APEX PASS [{entry}/{pool.kind}] {name} ({pool.mint[:8]}) mcap=${mcap:,.0f}"
                + (f" journal={ {k: round(v, 2) for k, v in journal.items()} }" if journal else ""))


async def pump_pipeline(pool: Pool):
    """
    pump.fun launches, for whichever entry strategies have a running user. One holder lookup per
    check is shared by all of them, so running several costs no more credits.

    PRECISION  free dev<=5% -> T+60 holders (5+ unique buyers, whale/top10/dev gates)
               -> wait for curve liquidity >= $3,000 (re-checks out to 10 min) -> final safety.
    FLOW       free dev<=8% -> T+60 / T+120 holders + DexScreener momentum, bundle and dev-sold
               detection, scored; the threshold self-tunes toward ~20 signals an hour.
    SCALP      free dev<=15% -> loosest gates + a live 'is it moving' test at T+60 / T+120,
               dump / parabolic filters; the threshold self-tunes toward ~50 signals an hour.
    """
    global _pump_watchers
    users = await _active_users()
    ent = {u.get("entry", "apex") for u in users}
    want_apex = "apex" in ent and pool.dev_pct0 <= MAX_DEV_PCT
    want_flow = "flow" in ent and pool.dev_pct0 <= FLOW_MAX_DEV
    want_scalp = "scalp" in ent and pool.dev_pct0 <= SCALP_MAX_DEV
    if not (want_apex or want_flow or want_scalp):
        _count_reject("dev_over_" + ("15" if "scalp" in ent else "8" if "flow" in ent else "5"))
        return
    if _pump_watchers >= PUMP_MAX_WATCHERS:
        _count_reject("capacity")
        return
    _pump_watchers += 1
    first_pass = True
    prev_sol: Optional[float] = None
    prev_t: Optional[float] = None
    final: Optional[Verdict] = None
    curve_owner = curve_pda(pool.mint)
    try:
        for i, age in enumerate(PUMP_CHECK_AGES):
            wait = pool.ts + age - time.time()
            if wait > 0:
                await asyncio.sleep(wait)
            users = await _active_users()
            ent = {u.get("entry", "apex") for u in users}
            want_apex = want_apex and "apex" in ent
            want_flow = want_flow and i < FLOW_CHECKS and "flow" in ent
            want_scalp = want_scalp and i < SCALP_CHECKS and "scalp" in ent
            if not (want_apex or want_flow or want_scalp) or not await _budget_ok():
                return
            last = i == len(PUMP_CHECK_AGES) - 1
            async with _pipeline_sem:
                hv = await _holder_view(pool.mint, PUMP_SUPPLY, curve_owner)
            if hv is None:
                if last:
                    _count_reject("holders_unreadable")
                continue
            holders, curve_ui, amounts = hv
            t_read = time.time()
            px = await sol_usd()
            real_sol = pump_real_sol(curve_ui)
            age_s = max(1.0, t_read - pool.ts)
            journal_now = {"dev_holding_pct": next((p for o, p in holders if o == pool.dev), 0.0),
                           "entry_rank_on_curve": min(100.0, real_sol / PUMP_GRADUATION_SOL * 100.0)}
            dex = await dex_metrics(pool.mint) if (want_flow or want_scalp) else None

            # ---------------- SCALP
            if want_scalp:
                ok, why, sc = scalp_evaluate(pool, holders, amounts, real_sol, age_s, dex, prev_sol)
                retry = why in ("score_low", "few_buyers", "curve_idle", "no_movement")
                if ok:
                    want_scalp = False
                    if final is None:
                        async with _pipeline_sem:
                            final = await pump_final(pool)
                    if final.ok:
                        # Rule 4 watches the curve for 30s before the fill; run it on its own so
                        # the other strategies' checks on this coin aren't held up.
                        _spawn(_scalp_confirm_and_enter(
                            pool, curve_owner, final.data["decimals"], final.data["mcap"],
                            real_sol, t_read, prev_sol, prev_t))
                    else:
                        _count_reject("scalp_" + final.reason)
                        if final.permanent:
                            await blacklist_add(pool.mint, final.reason)
                            return
                elif not retry or i == SCALP_CHECKS - 1:
                    want_scalp = False
                    _count_reject("scalp_" + why)

            # ---------------- FLOW
            if want_flow:
                ok, why, sc = flow_evaluate(pool, holders, amounts, real_sol, age_s, dex)
                if ok:
                    want_flow = False
                    if final is None:
                        async with _pipeline_sem:
                            final = await pump_final(pool)
                    if final.ok:
                        _stats["flow_signals"] = _stats.get("flow_signals", 0) + 1
                        await _finish_entry(pool, final.data["decimals"], final.data["mcap"], "flow",
                                            journal_now)
                    else:
                        _count_reject("flow_" + final.reason)
                        if final.permanent:
                            await blacklist_add(pool.mint, final.reason)
                            return
                elif why not in ("score_low", "few_buyers", "curve_idle"):
                    want_flow = False
                    _count_reject("flow_" + why)
                elif i == FLOW_CHECKS - 1:                  # last look: still too quiet / weak
                    want_flow = False
                    _count_reject("flow_" + why)

            # ---------------- PRECISION
            if want_apex:
                top10 = sum(p for _, p in holders[:10])
                buyers = sum(1 for o, _ in holders if o != pool.dev)
                reason = ""
                if holders and holders[0][1] > MAX_SINGLE_PCT:
                    reason = "whale_over_15"
                elif top10 > MAX_TOP10_PCT:
                    reason = "top10_over_40"
                elif next((p for o, p in holders if o == pool.dev), 0.0) > MAX_DEV_PCT:
                    reason = "dev_over_5"
                elif buyers < MIN_UNIQUE_BUYERS:
                    reason = "few_buyers"      # 5 unique buyers in the first 60s, or it's out
                if reason:
                    _count_reject(reason)
                    want_apex = False
                else:
                    if first_pass:
                        first_pass = False
                        _stats["stage1_pass"] += 1
                    if real_sol * px >= MIN_POOL_USD:
                        want_apex = False
                        if final is None:
                            async with _pipeline_sem:
                                final = await pump_final(pool)
                        if final.ok:
                            _stats["confirmed"] += 1
                            await _finish_entry(pool, final.data["decimals"], final.data["mcap"], "apex",
                                                journal_now)
                        else:
                            _count_reject(final.reason)
                            if final.permanent:
                                await blacklist_add(pool.mint, final.reason)
                                return
                    elif last:
                        _count_reject("liquidity_low")
                        want_apex = False

            prev_sol, prev_t = real_sol, t_read
            if not (want_apex or want_flow or want_scalp):
                return
    finally:
        _pump_watchers -= 1


async def _scalp_confirm_and_enter(pool: Pool, curve_owner: str, dec: int, mcap: float,
                                   sol0: float, t0: float, prev_sol: Optional[float],
                                   prev_t: Optional[float]):
    try:
        ok, why, j = await scalp_acceleration_gate(pool, curve_owner, sol0, t0, prev_sol, prev_t)
        if not ok:
            _count_reject("scalp_" + why)
            return
        if not await _budget_ok() or not any(u.get("entry") == "scalp" for u in await _active_users()):
            return
        _stats["scalp_signals"] = _stats.get("scalp_signals", 0) + 1
        journal = {k: j[k] for k in ("curve_slope", "dev_holding_pct", "entry_rank_on_curve")}
        await _finish_entry(pool, dec, mcap * j["price_move"], "scalp", journal)
    except Exception as e:
        logger.error(f"scalp confirm {pool.mint[:8]}: {e}", exc_info=True)


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
        f"<code>Entry    </code>{ENTRIES[s.get('entry', 'apex')]['icon']} {ENTRIES[s.get('entry', 'apex')]['name']}\n"
        f"<code>Exit     </code>{'🗡 Scalp (automatic)' if s.get('entry') == 'scalp' else prof['icon'] + ' ' + prof['name']}\n"
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
        [("👛 Wallet", "ax:wallet"), ("💎 Fund Collections", "vlt:home")],
        [("📡 Status", "ax:stat")],
        [("🚫 Blacklist", "ax:bl"), ("📖 How it works", "ax:how")],
    ] + ([[("🛡 Admin", "adm:home")]] if _access["admin"] and uid == _access["admin"] else []))
    return text, kb


WELCOME = (
    "<b>◆ CLEX APEX</b>\n<i>Precision entries on fresh Solana launches.</i>\n" + RULE + "\n"
    "Apex watches every new launch, throws out the junk, and only moves when a coin "
    "clears <b>every</b> safety gate. Then it manages the exit for you — targets, "
    "stops, all of it.\n\n"
    "It trades <b>real SOL</b> from a dedicated Phantom wallet, but only while you "
    "have it <b>started</b>. Nothing happens until you press ▶ START.\n\n"
    "<b>Three quick steps:</b> exit strategy, trade size, wallet.")


ENTRIES = {
    "apex": {"icon": "🎯", "name": "Apex Precision",
             "blurb": "Strict gauntlet. Few trades, all conditions must clear, entries once a coin "
                      "has real liquidity."},
    "flow": {"icon": "🌊", "name": "Apex Flow",
             "blurb": "Looser, faster. Aims for ~20 entries an hour and picks the strongest "
                      "momentum, filtering out bundled launches, dumping devs and whales."},
    "scalp": {"icon": "🗡", "name": "Apex Scalp",
              "blurb": "Loose gates, then the curve must be accelerating in the 30s before the buy. "
                       "−10% stop, a 90-second verdict (above +8% or out), 40% sold at +50%, "
                       "30% at +150%, and a runner that trails 35% below its peak with no time "
                       "limit. A circuit breaker stops it after a bad streak."},
}


async def _strategy_page(uid: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    cur = "apex"
    if uid:
        s, _ = await get_settings(uid)
        cur = (s or {}).get("entry", "apex")
    lines = [f"<b>🧭 Strategy</b>\n{RULE}\n<b>1 · Entry — what to buy</b>"]
    for k, e in ENTRIES.items():
        lines.append(f"\n{'✅' if k == cur else '▫️'} <b>{e['icon']} {e['name']}</b>\n<i>{e['blurb']}</i>")
    lines.append(f"\n{RULE}\n<b>2 · Exit — how to take profit</b> <i>(applies to every new trade; "
                 "🗡 Scalp always uses its own exit)</i>")
    for k, p in PROFILES.items():
        if p.get("hidden"):
            continue
        lines.append(f"\n<b>{k} · {p['icon']} {p['name']}</b>\n<i>{p['blurb']}</i>\n" +
                     "\n".join(f"  • {r}" for r in p["rows"]))
    lines.append("\n\n<b>Always on:</b> −45% hard stop · 72h time-stop if flat.")
    kb = _kb([[(("✅ " if k == cur else "") + f"{e['icon']} {e['name']}", f"ax:ent:{k}")]
              for k, e in ENTRIES.items()] +
             [[(f"{k} · {p['name']}", f"ax:prof:{k}")] for k, p in PROFILES.items()
              if not p.get("hidden")] +
             [HOME_BTN])
    return "\n".join(lines), kb


@router.callback_query(F.data.startswith("ax:ent:"))
async def cb_ent(q: CallbackQuery):
    k = q.data.split(":")[2]
    if k not in ENTRIES:
        await q.answer("Unknown strategy")
        return
    await get_settings(q.from_user.id, create=True)
    await set_setting(q.from_user.id, entry=k)
    t, kb = await _strategy_page(q.from_user.id)
    await _send(q, f"✅ Entry strategy set to <b>{ENTRIES[k]['name']}</b>.\n\n" + t, kb)


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
        t, kb = await _strategy_page(m.from_user.id)
        await m.answer(WELCOME, parse_mode=ParseMode.HTML)
        await m.answer("<b>Step 1 of 2</b> — " + t, parse_mode=ParseMode.HTML, reply_markup=kb)
        return
    t, kb = await _home(m.from_user.id)
    await _send(m, t, kb)


@router.message(Command("settings"))
async def cmd_settings(m: Message):
    t, kb = await _strategy_page(m.from_user.id)
    await _send(m, t, kb)


@router.callback_query(F.data == "ax:home")
async def cb_home(q: CallbackQuery):
    _await.pop(q.from_user.id, None)
    t, kb = await _home(q.from_user.id)
    await _send(q, t, kb)


@router.callback_query(F.data == "ax:strat")
async def cb_strat(q: CallbackQuery):
    t, kb = await _strategy_page(q.from_user.id)
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
    need = size_sol + 0.004 + GAS_RESERVE_SOL
    if bal < need:
        await _send(target, f"<b>💸 Fund your wallet first</b>\n{RULE}\n"
                            f"Balance <b>{bal:.4f} SOL</b> · needed about <b>{need:.4f} SOL</b>\n"
                            f"(one trade + fees + the {GAS_RESERVE_SOL:g} SOL gas reserve, which is never "
                            "spent on trades).\n\n"
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
    _session_start[uid] = time.time()                 # fresh session: the loss counter starts at zero
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
            floor = f"stop −{(1 - PROFILES[p['profile']].get('stop', HARD_STOP_RATIO)) * 100:.0f}%"
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
                "\n\nKeep your balance you choose trade in here. Apex halts new entries "
                "below $4 so exits can always pay gas.")
        return text, _kb([[("📥 Deposit", "ax:dep"), ("📤 Send funds", "ax:send")],
                          [("🔀 My wallets", "ax:wsel")],
                          ([("🔑 Export key", "ax:wexp"), ("🗑 Disconnect", "ax:wdisc")] if can_export(uid)
                           else [("🗑 Disconnect", "ax:wdisc")]),
                          [("↻ Refresh", "ax:wallet")], HOME_BTN])
    if await wl_list(uid):
        return await _wallets_page(uid)
    text = (f"<b>👛 Connect your wallet</b>\n{RULE}\n"
            "Apex trades from one Solana wallet that the bot signs for. Choose how to start:\n\n"
            "<b>👻 Connect my Phantom</b>\n"
            "Use your preferred Phantom account! This will fund all trades. "
            "CLEX trades SOL only.\n\n"
            "<b>✨ Create a new wallet</b> 🔒\n"
            "Apex generates a fresh wallet. Unlocks once you've connected your own Phantom.\n\n"
            "⚠️ Either way, wallets are (AES-encrypted).")
    return text, _kb([[("👻 Connect my Phantom", "ax:wconnect")],
                      [("✨ Create a new wallet", "ax:wcreate")], HOME_BTN])


async def _wallets_page(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    ws = await wl_list(uid)
    act = await _wallet(uid)
    rows, lines = [], []
    for i, r in enumerate(ws, 1):
        on = bool(act and act["pubkey"] == r["pubkey"])
        bal = await sn.get_sol_balance(r["pubkey"])
        kind = "👻 Phantom" if r["kind"] == "phantom" else "✨ Apex"
        lines.append(f"{'🟢' if on else '⚪'} <b>{kind}</b> <code>{_short(r['pubkey'])}</code> · {bal:.4f} SOL"
                     + ("  <i>(trading)</i>" if on else ""))
        if not on:
            rows.append([(f"Trade with {kind} {_short(r['pubkey'])}", f"ax:wuse:{r['pubkey']}")])
    text = (f"<b>🔀 My wallets</b>\n{RULE}\n" + ("\n".join(lines) or "No wallets yet.") +
            "\n\nApex trades with the 🟢 wallet. Switch only while the sniper is stopped and nothing is open.")
    rows.append([("👻 Connect another Phantom", "ax:wconnect"), ("✨ Create Apex wallet", "ax:wcreate")])
    rows.append([("‹ Wallet", "ax:wallet")] if act else HOME_BTN)
    return text, _kb(rows)


@router.callback_query(F.data == "ax:wsel")
async def cb_wsel(q: CallbackQuery):
    t, kb = await _wallets_page(q.from_user.id)
    await _send(q, t, kb)


@router.callback_query(F.data.startswith("ax:wuse:"))
async def cb_wuse(q: CallbackQuery):
    uid = q.from_user.id
    ok, why = await wl_activate(uid, q.data.split(":", 2)[2])
    if not ok:
        await q.answer(why, show_alert=True)
        return
    await q.answer("Switched")
    t, kb = await _wallets_page(uid)
    await _send(q, t, kb)


@router.callback_query(F.data == "ax:wallet")
async def cb_wallet(q: CallbackQuery):
    t, kb = await _wallet_page(q.from_user.id)
    await _send(q, t, kb)


@router.callback_query(F.data == "ax:wconnect")
async def cb_wconnect(q: CallbackQuery):
    _await[q.from_user.id] = "wallet"
    await _send(q, "<b>🔐 Send your private key</b>\n" + RULE +
                "\nIn Phantom: tap the menu ☰ → your account → <b>Show Private Key</b> → "
                "choose <b>Solana</b> → copy it.\n"
                "(The recovery phrase won't work — it must be the private key.)\n\n"
                "Paste it in your next message. Keys are (AES-encrypted) and wiped after every session.",
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
    if not await wl_has_phantom(uid):
        await _send(q, f"<b>🔒 Connect your Phantom first</b>\n{RULE}\n"
                       "Apex wallets unlock after you connect a Phantom wallet of your own. "
                       "Once it's connected you can create an Apex wallet and choose which "
                       "one to trade with.",
                    _kb([[("👻 Connect my Phantom", "ax:wconnect")], HOME_BTN]))
        return
    from solders.keypair import Keypair
    kp = Keypair()
    pub = str(kp.pubkey())
    raw = base58.b58encode(bytes(kp)).decode()
    await wl_add(uid, raw, pub, "apex")
    if not await _wallet(uid):
        await wl_activate(uid, pub, force=True)
    else:
        await _send(q, f"<b>✨ Apex wallet created</b>\n{RULE}\n<code>{pub}</code>\n\n"
                       "It's in your list. Choose it under <b>My wallets</b> when you want to "
                       "trade with it (stop the sniper first).",
                    _kb([[("🔀 My wallets", "ax:wsel")], HOME_BTN]))
        return
    await _send(q, (f"<b>✨ Wallet created</b>\n{RULE}\n<code>{pub}</code>\n\n"
                    "Next:\n" + ("1. <b>Export your key now</b> and save it in a password manager.\n"
                                 if can_export(uid) else
                                 "1. Your key is stored encrypted by Apex. Use <b>Send funds</b> any time "
                                 "to move your SOL out.\n") +
                    "2. <b>Deposit SOL</b> to this address (Solana network only).\n"
                    "3. Press ▶ START SNIPER."),
                _kb([([("🔑 Export my key", "ax:wexp")] if can_export(uid) else []) + [("📥 Deposit", "ax:dep")],
                     HOME_BTN]))


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
    if not can_export(q.from_user.id):
        await q.answer("Key export isn't available.", show_alert=True)
        return
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
    if not can_export(uid):
        await q.answer("Key export isn't available.", show_alert=True)
        return
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
    w = await _wallet(q.from_user.id)
    if w:
        await wl_remove(q.from_user.id, w["pubkey"])
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
        status, err = await _confirm_tx(sig, 40)
        if status == "failed":
            return False, sig, friendly_err(err)
        if status == "timeout":
            return False, sig, "not confirmed in time — check Solscan before retrying"
        return True, sig, ""
    except Exception as e:
        logger.error(f"send_sol: {e}", exc_info=True)
        return False, "", "unexpected error"


async def _status_page(uid: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    owner = can_export(uid)
    today, month = await credits_used() if owner else (0, 0)
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
            f"<b>Apex Flow</b>\n"
            f"Signals (last hour)  <code>{len([t for t in _flow['signals'] if time.time() - t < 3600])}</code>"
            f" / target <code>{FLOW_TARGET_PER_HOUR}</code>\n"
            f"Score threshold      <code>{_flow['min_score']:.2f}</code> <i>(self-tunes)</i>\n\n"
            f"<b>Apex Scalp</b>\n"
            f"Signals (last hour)  <code>{len([t for t in _scalp['signals'] if time.time() - t < 3600])}</code>"
            f" / target <code>{SCALP_TARGET_PER_HOUR}</code>\n"
            f"Score threshold      <code>{_scalp['min_score']:.2f}</code> <i>(self-tunes)</i>\n\n"
            f"<b>Top rejections</b>\n{rtxt}"
            + (f"\n\n<b>Helius credits (est.)</b>\n"
               f"Today <code>{today:,}</code> · Month <code>{month:,}</code> / "
               f"<code>{MONTHLY_CREDIT_CEILING:,}</code>" if owner else ""))
    return text, _kb([[("↻ Refresh", "ax:stat")], HOME_BTN])


@router.callback_query(F.data == "ax:stat")
async def cb_stat(q: CallbackQuery):
    t, kb = await _status_page(q.from_user.id)
    await _send(q, t, kb)


@router.message(Command("status"))
async def cmd_status(m: Message):
    t, kb = await _status_page(m.from_user.id)
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
        "<b>🗡 Scalp mode</b> takes coins that are moving and whose curve is accelerating in the 30s "
        "before the buy. −10% hard stop; at 90 seconds it must be above +8% or it's sold; 40% sells "
        "at +50%, another 30% at +150%, and the rest trails 35% below its peak with no time limit. "
        "A circuit breaker stops it after a bad streak.\n\n"
        "<b>🌊 Flow mode</b> trades the same safety basics but scores momentum — buy pressure, "
        "curve speed, holder spread — and drops bundled launches and dumping devs. It tunes "
        "itself toward ~20 entries an hour.\n\n"
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
        "/balance — wallet\n/status — engine status\n/blacklist — banned tokens"),
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
        _await.pop(uid, None)
        await wl_add(uid, raw, pub, "phantom")
        if await _wallet(uid):                            # already trading with another wallet
            t, kb = await _wallets_page(uid)
            await m.answer(f"✅ <b>Phantom added</b> · <code>{_short(pub)}</code>\n"
                           "Pick it below when you want to trade with it.\n\n" + t,
                           parse_mode=ParseMode.HTML, reply_markup=kb)
            return
        await wl_activate(uid, pub, force=True)           # legacy sniper stays disabled
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
    ("balance", "Wallet balance"), ("status", "Engine status"),
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
