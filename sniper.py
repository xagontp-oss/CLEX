"""
CLEX Sniper Module v3 — dynamic sizing, intelligent exit engine, live PnL,
rug detection, wallet fingerprinting, first-buyer blacklist, graduated token
detection, partial sell command, cross-position risk cap, performance tracker,
custom trade amount per user.
"""
import asyncio
import aiosqlite
import aiohttp
import logging
import os
import time
import base64
import base58
import json as jsonlib
from typing import Optional, Dict, Tuple, List
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── ENV ───────────────────────────────────────────────────────────────────────
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
MASTER_KEY     = os.getenv("MASTER_ENCRYPTION_KEY", "")
DB_PATH        = "clex.db"

RPC_ENDPOINTS = [
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}",
    "https://api.mainnet-beta.solana.com",
]

PUMPPORTAL_URL  = "https://pumpportal.fun/api/trade-local"
JUPITER_QUOTE   = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP    = "https://quote-api.jup.ag/v6/swap"
WSOL_MINT       = "So11111111111111111111111111111111111111112"

POSITION_CHECK_INTERVAL = 15
MIN_BUY_INTERVAL        = 30
LIVE_UPDATE_INTERVAL    = 60
RUG_DROP_THRESHOLD      = 0.15
MAX_OPEN_EXPOSURE_PCT   = 0.30
BLACKLIST_THRESHOLD     = 3

# ── RISK PROFILES ─────────────────────────────────────────────────────────────
RISK_PROFILES = {
    "low": {
        "label":             "🛡 Low Risk",
        "buy_pct":           0.05,
        "min_buy":           0.02,
        "max_buy":           0.10,
        "slippage":          10,
        "priority_fee":      0.001,
        "skip_preflight":    False,
        "trailing_stop_pct": 0.25,
        "tiered_t1_mult":    2.0,
        "tiered_t2_mult":    4.0,
        "tiered_trail_pct":  0.20,
        "fixed_tp":          1.8,
        "fixed_sl":          0.75,
        "time_decay_mins":   10,
        "desc": "5% of balance · trail 25% · tiered exits · hair trigger rug",
    },
    "moderate": {
        "label":             "⚡ Moderate",
        "buy_pct":           0.10,
        "min_buy":           0.05,
        "max_buy":           0.25,
        "slippage":          15,
        "priority_fee":      0.003,
        "skip_preflight":    True,
        "trailing_stop_pct": 0.30,
        "tiered_t1_mult":    2.5,
        "tiered_t2_mult":    6.0,
        "tiered_trail_pct":  0.25,
        "fixed_tp":          2.2,
        "fixed_sl":          0.60,
        "time_decay_mins":   8,
        "desc": "10% of balance · trail 30% · tiered exits · hair trigger rug",
    },
    "psycho": {
        "label":             "🤑 Psycho",
        "buy_pct":           0.20,
        "min_buy":           0.10,
        "max_buy":           0.50,
        "slippage":          25,
        "priority_fee":      0.005,
        "skip_preflight":    True,
        "trailing_stop_pct": 0.35,
        "tiered_t1_mult":    3.0,
        "tiered_t2_mult":    10.0,
        "tiered_trail_pct":  0.30,
        "fixed_tp":          2.5,
        "fixed_sl":          0.40,
        "time_decay_mins":   6,
        "desc": "20% of balance · trail 35% · max aggression · hair trigger rug",
    },
}

# ── EXIT MODE CLASSIFIER ──────────────────────────────────────────────────────
def classify_exit_mode(momentum: Dict) -> str:
    cv = momentum.get("curve_velocity", 0)
    hv = momentum.get("holder_velocity", 0)
    tx = momentum.get("tx_count", 0)
    if cv >= 1.5 and hv >= 5 and tx >= 25:
        return "momentum"
    elif cv >= 0.4 and hv >= 1:
        return "steady"
    return "weak"

# ── DYNAMIC BUY SIZE ──────────────────────────────────────────────────────────
def calc_buy_size(balance: float, profile: Dict,
                  custom_amount: Optional[float] = None) -> float:
    """
    If custom_amount is set, use it directly (capped to usable balance).
    Otherwise use profile buy_pct of balance.
    """
    fee     = profile["priority_fee"]
    usable  = balance - fee - 0.005
    if usable <= 0:
        return 0.0
    if custom_amount and custom_amount > 0:
        return round(min(custom_amount, usable), 4)
    raw = balance * profile["buy_pct"]
    raw = max(raw, profile["min_buy"])
    raw = min(raw, profile["max_buy"])
    return round(min(raw, usable), 4)

# ── ENCRYPTION ────────────────────────────────────────────────────────────────
def _fernet():
    from cryptography.fernet import Fernet
    if not MASTER_KEY:
        raise RuntimeError("MASTER_ENCRYPTION_KEY not set")
    return Fernet(MASTER_KEY.encode())

def encrypt_key(k: str) -> str: return _fernet().encrypt(k.encode()).decode()
def decrypt_key(k: str) -> str: return _fernet().decrypt(k.encode()).decode()

# ── DB ────────────────────────────────────────────────────────────────────────
async def init_sniper_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sniper_users (
                user_id         INTEGER PRIMARY KEY,
                encrypted_key   TEXT NOT NULL,
                pubkey          TEXT NOT NULL,
                risk_profile    TEXT NOT NULL DEFAULT 'low',
                sniper_enabled  INTEGER NOT NULL DEFAULT 0,
                setup_at        REAL NOT NULL,
                last_buy_at     REAL DEFAULT NULL,
                custom_amount   REAL DEFAULT NULL
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sniper_positions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER NOT NULL,
                mint            TEXT NOT NULL,
                name            TEXT,
                symbol          TEXT,
                sol_spent       REAL NOT NULL,
                token_amount    REAL DEFAULT 0,
                buy_tx          TEXT,
                bought_at       REAL NOT NULL,
                status          TEXT NOT NULL DEFAULT 'open',
                sell_tx         TEXT DEFAULT NULL,
                pnl_sol         REAL DEFAULT NULL,
                pnl_pct         REAL DEFAULT NULL,
                exit_mode       TEXT NOT NULL DEFAULT 'steady',
                peak_value_sol  REAL DEFAULT NULL,
                trailing_stop   REAL DEFAULT NULL,
                tier1_sold      INTEGER DEFAULT 0,
                tier2_sold      INTEGER DEFAULT 0,
                last_update_at  REAL DEFAULT NULL
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sniper_setup_state (
                user_id         INTEGER PRIMARY KEY,
                step            TEXT NOT NULL,
                risk_profile    TEXT DEFAULT NULL
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sniper_blacklist (
                wallet          TEXT PRIMARY KEY,
                snipe_count     INTEGER NOT NULL DEFAULT 1,
                first_seen      REAL NOT NULL,
                last_seen       REAL NOT NULL
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sniper_performance (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER NOT NULL,
                mint            TEXT NOT NULL,
                name            TEXT,
                symbol          TEXT,
                sol_spent       REAL NOT NULL,
                sol_received    REAL NOT NULL,
                pnl_sol         REAL NOT NULL,
                pnl_pct         REAL NOT NULL,
                exit_reason     TEXT NOT NULL,
                exit_mode       TEXT NOT NULL,
                hold_secs       REAL NOT NULL,
                closed_at       REAL NOT NULL
            )""")
        # safe migrations
        for col, defn in [
            ("exit_mode",      "TEXT DEFAULT 'steady'"),
            ("peak_value_sol", "REAL DEFAULT NULL"),
            ("trailing_stop",  "REAL DEFAULT NULL"),
            ("tier1_sold",     "INTEGER DEFAULT 0"),
            ("tier2_sold",     "INTEGER DEFAULT 0"),
            ("last_update_at", "REAL DEFAULT NULL"),
            ("custom_amount",  "REAL DEFAULT NULL"),
        ]:
            try:
                await db.execute(
                    f"ALTER TABLE sniper_users ADD COLUMN {col} {defn}")
            except: pass
        for col, defn in [
            ("exit_mode",      "TEXT DEFAULT 'steady'"),
            ("peak_value_sol", "REAL DEFAULT NULL"),
            ("trailing_stop",  "REAL DEFAULT NULL"),
            ("tier1_sold",     "INTEGER DEFAULT 0"),
            ("tier2_sold",     "INTEGER DEFAULT 0"),
            ("last_update_at", "REAL DEFAULT NULL"),
        ]:
            try:
                await db.execute(
                    f"ALTER TABLE sniper_positions ADD COLUMN {col} {defn}")
            except: pass
        await db.commit()

# ── USER QUERIES ──────────────────────────────────────────────────────────────
async def get_sniper_user(user_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id,pubkey,risk_profile,sniper_enabled,last_buy_at "
            "FROM sniper_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row: return None
        return {"user_id":row[0],"pubkey":row[1],"risk_profile":row[2],
                "sniper_enabled":bool(row[3]),"last_buy_at":row[4]}

async def save_sniper_user(user_id: int, private_key_b58: str,
                           pubkey: str, risk_profile: str):
    enc = encrypt_key(private_key_b58)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO sniper_users "
            "(user_id,encrypted_key,pubkey,risk_profile,sniper_enabled,setup_at) "
            "VALUES (?,?,?,?,0,?)",
            (user_id, enc, pubkey, risk_profile, time.time()))
        await db.commit()

async def delete_sniper_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM sniper_users WHERE user_id=?", (user_id,))
        await db.execute(
            "UPDATE sniper_positions SET status='wallet_deleted' "
            "WHERE user_id=? AND status='open'", (user_id,))
        await db.commit()

async def toggle_sniper(user_id: int, enabled: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sniper_users SET sniper_enabled=? WHERE user_id=?",
                         (1 if enabled else 0, user_id))
        await db.commit()

async def update_risk_profile(user_id: int, profile: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sniper_users SET risk_profile=? WHERE user_id=?",
                         (profile, user_id))
        await db.commit()

async def set_custom_amount(user_id: int, amount: Optional[float]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sniper_users SET custom_amount=? WHERE user_id=?",
                         (amount, user_id))
        await db.commit()

async def get_custom_amount(user_id: int) -> Optional[float]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT custom_amount FROM sniper_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else None

async def get_enabled_snipers() -> List[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id,encrypted_key,pubkey,risk_profile,last_buy_at "
            "FROM sniper_users WHERE sniper_enabled=1")
        rows = await cur.fetchall()
        return [{"user_id":r[0],"encrypted_key":r[1],"pubkey":r[2],
                 "risk_profile":r[3],"last_buy_at":r[4]} for r in rows]

async def set_last_buy(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sniper_users SET last_buy_at=? WHERE user_id=?",
                         (time.time(), user_id))
        await db.commit()

async def _get_encrypted_key(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT encrypted_key FROM sniper_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else None

# ── SETUP STATE ───────────────────────────────────────────────────────────────
async def set_setup_state(user_id: int, step: str, risk: Optional[str] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO sniper_setup_state VALUES (?,?,?)",
                         (user_id, step, risk))
        await db.commit()

async def get_setup_state(user_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT step,risk_profile FROM sniper_setup_state WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return {"step":row[0],"risk":row[1]} if row else None

async def clear_setup_state(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM sniper_setup_state WHERE user_id=?", (user_id,))
        await db.commit()

# ── BLACKLIST ─────────────────────────────────────────────────────────────────
async def record_first_buyer(wallet: str):
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO sniper_blacklist (wallet,snipe_count,first_seen,last_seen)
            VALUES (?,1,?,?)
            ON CONFLICT(wallet) DO UPDATE SET
                snipe_count = snipe_count + 1,
                last_seen   = excluded.last_seen
        """, (wallet, now, now))
        await db.commit()

async def is_blacklisted(wallet: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT snipe_count FROM sniper_blacklist WHERE wallet=?", (wallet,))
        row = await cur.fetchone()
        return row is not None and row[0] >= BLACKLIST_THRESHOLD

async def get_blacklist_count() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM sniper_blacklist WHERE snipe_count>=?",
            (BLACKLIST_THRESHOLD,))
        return (await cur.fetchone())[0]

# ── POSITIONS ─────────────────────────────────────────────────────────────────
async def save_position(user_id: int, mint: str, name: str, symbol: str,
                        sol_spent: float, token_amount: float, buy_tx: str,
                        exit_mode: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO sniper_positions "
            "(user_id,mint,name,symbol,sol_spent,token_amount,buy_tx,bought_at,"
            "exit_mode,peak_value_sol,trailing_stop,tier1_sold,tier2_sold,last_update_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,0,0,?)",
            (user_id, mint, name, symbol, sol_spent, token_amount,
             buy_tx, time.time(), exit_mode, sol_spent, None, time.time()))
        await db.commit()

async def get_open_positions(user_id: Optional[int] = None) -> List[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        if user_id:
            cur = await db.execute(
                "SELECT id,user_id,mint,name,symbol,sol_spent,token_amount,bought_at,"
                "exit_mode,peak_value_sol,trailing_stop,tier1_sold,tier2_sold,last_update_at "
                "FROM sniper_positions WHERE status='open' AND user_id=?", (user_id,))
        else:
            cur = await db.execute(
                "SELECT id,user_id,mint,name,symbol,sol_spent,token_amount,bought_at,"
                "exit_mode,peak_value_sol,trailing_stop,tier1_sold,tier2_sold,last_update_at "
                "FROM sniper_positions WHERE status='open'")
        rows = await cur.fetchall()
        return [{"id":r[0],"user_id":r[1],"mint":r[2],"name":r[3],"symbol":r[4],
                 "sol_spent":r[5],"token_amount":r[6],"bought_at":r[7],
                 "exit_mode":r[8],"peak_value_sol":r[9] or r[5],
                 "trailing_stop":r[10],"tier1_sold":bool(r[11]),
                 "tier2_sold":bool(r[12]),"last_update_at":r[13]}
                for r in rows]

async def get_total_open_exposure(user_id: int) -> float:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT SUM(sol_spent) FROM sniper_positions "
            "WHERE status='open' AND user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] or 0.0

async def update_position_peak(pos_id: int, peak: float, trail_stop: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE sniper_positions SET peak_value_sol=?,trailing_stop=? WHERE id=?",
            (peak, trail_stop, pos_id))
        await db.commit()

async def mark_tier_sold(pos_id: int, tier: int, new_token_amount: float):
    col = "tier1_sold" if tier == 1 else "tier2_sold"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE sniper_positions SET {col}=1,token_amount=? WHERE id=?",
            (new_token_amount, pos_id))
        await db.commit()

async def update_last_notified(pos_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sniper_positions SET last_update_at=? WHERE id=?",
                         (time.time(), pos_id))
        await db.commit()

async def close_position(pos_id: int, status: str, sell_tx: str,
                         pnl_sol: float, pnl_pct: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE sniper_positions "
            "SET status=?,sell_tx=?,pnl_sol=?,pnl_pct=? WHERE id=?",
            (status, sell_tx, pnl_sol, pnl_pct, pos_id))
        await db.commit()

# ── PERFORMANCE TRACKER ───────────────────────────────────────────────────────
async def record_performance(pos: Dict, sol_received: float,
                             pnl_sol: float, pnl_pct: float, exit_reason: str):
    hold_secs = time.time() - pos["bought_at"]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO sniper_performance "
            "(user_id,mint,name,symbol,sol_spent,sol_received,pnl_sol,pnl_pct,"
            "exit_reason,exit_mode,hold_secs,closed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (pos["user_id"], pos["mint"], pos["name"], pos["symbol"],
             pos["sol_spent"], sol_received, pnl_sol, pnl_pct,
             exit_reason, pos["exit_mode"], hold_secs, time.time()))
        await db.commit()

async def get_performance_stats(user_id: int) -> Dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT pnl_sol,pnl_pct,exit_reason,hold_secs,name,symbol,closed_at "
            "FROM sniper_performance WHERE user_id=? ORDER BY closed_at DESC", (user_id,))
        rows = await cur.fetchall()
    if not rows: return {}
    total     = len(rows)
    wins      = [r for r in rows if r[0] > 0]
    losses    = [r for r in rows if r[0] <= 0]
    total_pnl = sum(r[0] for r in rows)
    avg_win   = sum(r[1] for r in wins) / len(wins) if wins else 0
    avg_loss  = sum(r[1] for r in losses) / len(losses) if losses else 0
    best      = max(rows, key=lambda r: r[0])
    worst     = min(rows, key=lambda r: r[0])
    avg_hold  = sum(r[3] for r in rows) / total
    return {
        "total": total, "wins": len(wins), "losses": len(losses),
        "win_rate": round(len(wins)/total*100, 1),
        "total_pnl": round(total_pnl, 4),
        "avg_win_pct": round(avg_win, 1),
        "avg_loss_pct": round(avg_loss, 1),
        "best":  {"name":best[4],"symbol":best[5],"pnl":best[0],"pct":best[1]},
        "worst": {"name":worst[4],"symbol":worst[5],"pnl":worst[0],"pct":worst[1]},
        "avg_hold_mins": round(avg_hold/60, 1),
        "recent": [{"name":r[4],"symbol":r[5],"pnl":r[0],"pct":r[1],"reason":r[2]}
                   for r in rows[:5]],
    }

# ── WALLET UTILS ──────────────────────────────────────────────────────────────
def validate_private_key(key_b58: str) -> Tuple[bool, str, str]:
    try:
        from solders.keypair import Keypair
        raw = base58.b58decode(key_b58.strip())
        kp  = Keypair.from_bytes(raw)
        return True, str(kp.pubkey()), ""
    except Exception as e:
        return False, "", str(e)

def load_keypair(encrypted_key: str):
    from solders.keypair import Keypair
    raw = base58.b58decode(decrypt_key(encrypted_key))
    return Keypair.from_bytes(raw)

async def get_sol_balance(pubkey: str) -> float:
    try:
        rpc_url = f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY','')}"
        async with aiohttp.ClientSession() as s:
            async with s.post(rpc_url,
                json={"jsonrpc":"2.0","id":1,"method":"getBalance","params":[pubkey]},
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                return round(data.get("result",{}).get("value",0)/1e9, 4)
    except: return 0.0

async def get_token_balance(pubkey: str, mint: str) -> float:
    try:
        rpc_url = f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY','')}"
        async with aiohttp.ClientSession() as s:
            async with s.post(rpc_url,
                json={"jsonrpc":"2.0","id":1,"method":"getTokenAccountsByOwner",
                      "params":[pubkey,{"mint":mint},{"encoding":"jsonParsed"}]},
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                data  = await r.json()
                accts = data.get("result",{}).get("value",[])
                if accts:
                    return float(accts[0]["account"]["data"]["parsed"]["info"]
                                 ["tokenAmount"]["uiAmount"] or 0)
    except: pass
    return 0.0

# ── WALLET FINGERPRINTING ─────────────────────────────────────────────────────
async def fingerprint_dev_wallet(dev_wallet: str) -> Dict:
    result = {"vol_7d_sol": 0.0, "single_funder": False,
              "funder_address": None, "flag": None}
    try:
        sigs = await _rpc("getSignaturesForAddress",
                          [dev_wallet, {"limit": 100, "commitment": "confirmed"}])
        if not sigs: return result
        cutoff = time.time() - 7 * 86400
        recent = [s for s in sigs if (s.get("blockTime") or 0) >= cutoff]
        if not recent: return result
        inflow_sources: Dict[str, float] = {}
        for sig_info in recent[:30]:
            try:
                tx_data = await _rpc("getTransaction",
                    [sig_info["signature"],
                     {"encoding":"jsonParsed","maxSupportedTransactionVersion":0}])
                if not tx_data: continue
                meta      = tx_data.get("meta", {})
                pre_b     = meta.get("preBalances", [])
                post_b    = meta.get("postBalances", [])
                acct_keys = (tx_data.get("transaction",{})
                             .get("message",{}).get("accountKeys",[]))
                for i, key in enumerate(acct_keys):
                    addr = key if isinstance(key, str) else key.get("pubkey","")
                    if addr == dev_wallet and i < len(pre_b) and i < len(post_b):
                        delta = (post_b[i] - pre_b[i]) / 1e9
                        if delta > 0.01:
                            for j, k2 in enumerate(acct_keys):
                                a2 = k2 if isinstance(k2, str) else k2.get("pubkey","")
                                if a2 != dev_wallet and j < len(pre_b) and j < len(post_b):
                                    d2 = (pre_b[j] - post_b[j]) / 1e9
                                    if d2 > 0:
                                        inflow_sources[a2] = inflow_sources.get(a2, 0) + d2
                        result["vol_7d_sol"] += max(delta, 0)
                await asyncio.sleep(0.05)
            except: continue
        if inflow_sources:
            top_src, top_amt = max(inflow_sources.items(), key=lambda x: x[1])
            total_inflow     = sum(inflow_sources.values())
            if total_inflow > 0 and top_amt / total_inflow >= 0.80:
                result["single_funder"]  = True
                result["funder_address"] = top_src
                result["flag"]           = "🔴 Funded 80%+ from single source"
    except Exception as e:
        logger.debug(f"Fingerprint error {dev_wallet[:20]}: {e}")
    return result

# ── GRADUATION DETECTION ──────────────────────────────────────────────────────
async def is_graduated(mint: str) -> bool:
    try:
        result   = await _rpc("getTokenLargestAccounts", [mint, {"commitment":"confirmed"}])
        if not result: return False
        accounts = result.get("value", [])
        if not accounts: return True
        largest  = float(accounts[0].get("uiAmount") or 0)
        return (largest / 1_000_000_000 * 100) < 5.0
    except: return False

# ── RPC ───────────────────────────────────────────────────────────────────────
async def _rpc(method: str, params: list) -> Optional[Dict]:
    try:
        rpc_url = f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY','')}"
        async with aiohttp.ClientSession() as s:
            async with s.post(rpc_url,
                json={"jsonrpc":"2.0","id":1,"method":method,"params":params},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    return (await r.json()).get("result")
    except: pass
    return None

async def _send_to_rpc(url: str, tx_b64: str, skip: bool) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url,
                json={"jsonrpc":"2.0","id":1,"method":"sendTransaction",
                      "params":[tx_b64,{"encoding":"base64","skipPreflight":skip,
                                        "preflightCommitment":"processed","maxRetries":3}]},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                data = await r.json()
                if "result" in data: return data["result"]
    except: pass
    return None

async def send_transaction_fast(tx_bytes: bytes, keypair,
                                skip: bool = True) -> Optional[str]:
    from solders.transaction import VersionedTransaction
    tx     = VersionedTransaction.from_bytes(tx_bytes)
    signed = VersionedTransaction(tx.message, [keypair])
    tx_b64 = base64.b64encode(bytes(signed)).decode()
    rpc_url = f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY','')}"
    endpoints = [rpc_url, "https://api.mainnet-beta.solana.com"]
    results = await asyncio.gather(
        *[_send_to_rpc(u, tx_b64, skip) for u in endpoints],
        return_exceptions=True)
    for r in results:
        if isinstance(r, str) and r: return r
    return None

async def confirm_transaction(sig: str, timeout_s: int = 25) -> bool:
    deadline = time.time() + timeout_s
    rpc_url  = f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY','')}"
    async with aiohttp.ClientSession() as s:
        while time.time() < deadline:
            try:
                async with s.post(rpc_url,
                    json={"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses",
                          "params":[[sig],{"searchTransactionHistory":True}]},
                    timeout=aiohttp.ClientTimeout(total=4)) as r:
                    data = await r.json()
                    val  = (data.get("result",{}).get("value") or [None])[0]
                    if val and val.get("confirmationStatus") in ("confirmed","finalized"):
                        return True
            except: pass
            await asyncio.sleep(1.5)
    return False

# ── CURRENT VALUE ─────────────────────────────────────────────────────────────
async def get_position_value_sol(mint: str, token_amount: float) -> float:
    try:
        data        = await _rpc("getTokenSupply", [mint])
        decimals    = (data or {}).get("value",{}).get("decimals",6)
        token_lamps = int(token_amount * (10**decimals))
        if token_lamps <= 0: return 0.0
        async with aiohttp.ClientSession() as s:
            async with s.get(JUPITER_QUOTE,
                params={"inputMint":mint,"outputMint":WSOL_MINT,
                        "amount":token_lamps,"slippageBps":500},
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status != 200: return 0.0
                q = await r.json()
                return int(q.get("outAmount",0)) / 1e9
    except: return 0.0

# ── BUY ENGINE ────────────────────────────────────────────────────────────────
async def _buy_pumpfun(mint: str, pubkey: str, keypair,
                       profile: Dict, buy_sol: float) -> Tuple[bool, str]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(PUMPPORTAL_URL,
                json={"publicKey":pubkey,"action":"buy","mint":mint,
                      "amount":buy_sol,"denominatedInSol":"true",
                      "slippage":profile["slippage"],
                      "priorityFee":profile["priority_fee"],"pool":"pump"},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200: return False, f"pumpportal {r.status}"
                tx_bytes = await r.read()
        sig = await send_transaction_fast(tx_bytes, keypair, profile["skip_preflight"])
        if not sig: return False, "send failed"
        ok = await confirm_transaction(sig)
        return (True, sig) if ok else (False, f"unconfirmed:{sig}")
    except Exception as e: return False, str(e)

async def _buy_jupiter(mint: str, pubkey: str, keypair,
                       profile: Dict, buy_sol: float) -> Tuple[bool, str]:
    try:
        lamports   = int(buy_sol * 1_000_000_000)
        slip_bps   = profile["slippage"] * 100
        prio_lamps = int(profile["priority_fee"] * 1_000_000_000)
        async with aiohttp.ClientSession() as s:
            async with s.get(JUPITER_QUOTE,
                params={"inputMint":WSOL_MINT,"outputMint":mint,
                        "amount":lamports,"slippageBps":slip_bps},
                timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status != 200: return False, f"jup quote {r.status}"
                quote = await r.json()
            if "error" in quote: return False, f"jup: {quote['error']}"
            async with s.post(JUPITER_SWAP,
                json={"quoteResponse":quote,"userPublicKey":pubkey,
                      "wrapAndUnwrapSol":True,"dynamicComputeUnitLimit":True,
                      "prioritizationFeeLamports":prio_lamps},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200: return False, f"jup swap {r.status}"
                swap = await r.json()
        tx_bytes = base64.b64decode(swap.get("swapTransaction",""))
        if not tx_bytes: return False, "no swapTransaction"
        sig = await send_transaction_fast(tx_bytes, keypair, profile["skip_preflight"])
        if not sig: return False, "send failed"
        ok = await confirm_transaction(sig)
        return (True, sig) if ok else (False, f"unconfirmed:{sig}")
    except Exception as e: return False, str(e)

async def execute_user_buy(user_id: int, mint: str, name: str,
                           symbol: str, momentum: Dict,
                           top_holders: Optional[List[str]] = None
                           ) -> Tuple[bool, str, str, float, str]:
    user = await get_sniper_user(user_id)
    if not user or not user["sniper_enabled"]:
        return False, "sniper off", "none", 0, ""
    lba = user.get("last_buy_at") or 0
    if time.time() - lba < MIN_BUY_INTERVAL:
        return False, "rate limited", "none", 0, ""

    profile   = RISK_PROFILES[user["risk_profile"]]
    pubkey    = user["pubkey"]
    exit_mode = classify_exit_mode(momentum)

    if top_holders:
        for holder in top_holders[:5]:
            if await is_blacklisted(holder):
                return False, "blacklisted holder in top 5", "none", 0, ""

    bal          = await get_sol_balance(pubkey)
    exposure     = await get_total_open_exposure(user_id)
    max_exposure = bal * MAX_OPEN_EXPOSURE_PCT
    if exposure >= max_exposure:
        return False, f"exposure cap hit ({exposure:.3f}/{max_exposure:.3f} SOL)", "none", 0, ""

    # ── SIZING: custom amount or profile % ────────────────────────────────
    custom_amt = await get_custom_amount(user_id)
    buy_sol    = calc_buy_size(bal, profile, custom_amt)

    if buy_sol <= 0:
        fee = profile["priority_fee"] + 0.005
        return False, f"balance too low for fees (have {bal:.4f} SOL, need >{fee:.3f} SOL)", "none", 0, ""

    # Low balance warning — log it but don't block
    fee_needed = profile["priority_fee"] + 0.005
    if bal < buy_sol + fee_needed:
        logger.warning(f"User {user_id} low balance {bal:.4f} SOL for {buy_sol} SOL trade")

    enc_key   = await _get_encrypted_key(user_id)
    kp        = load_keypair(enc_key)
    graduated = await is_graduated(mint)

    if not graduated:
        ok, sig = await _buy_pumpfun(mint, pubkey, kp, profile, buy_sol)
        method  = "pump.fun"
        if not ok:
            logger.warning(f"pump.fun failed ({sig}), trying Jupiter...")
            ok, sig = await _buy_jupiter(mint, pubkey, kp, profile, buy_sol)
            method  = "jupiter"
    else:
        ok, sig = await _buy_jupiter(mint, pubkey, kp, profile, buy_sol)
        method  = "jupiter(graduated)"

    if not ok:
        return False, sig, "failed", 0, ""

    token_amt = await get_token_balance(pubkey, mint)
    await save_position(user_id=user_id, mint=mint, name=name, symbol=symbol,
                        sol_spent=buy_sol, token_amount=token_amt,
                        buy_tx=sig, exit_mode=exit_mode)
    await set_last_buy(user_id)
    logger.info(f"BUY user={user_id} {name} {buy_sol}SOL via {method} mode={exit_mode}")
    return True, sig, method, buy_sol, exit_mode

# ── SELL ENGINE ───────────────────────────────────────────────────────────────
async def _sell_via_jupiter(mint: str, token_amount: float, pubkey: str,
                            keypair, profile: Dict) -> Tuple[bool, str, float]:
    try:
        data        = await _rpc("getTokenSupply", [mint])
        decimals    = (data or {}).get("value",{}).get("decimals", 6)
        token_lamps = int(token_amount * (10**decimals))
        slip_bps    = profile["slippage"] * 100
        prio_lamps  = int(profile["priority_fee"] * 1_000_000_000)
        async with aiohttp.ClientSession() as s:
            async with s.get(JUPITER_QUOTE,
                params={"inputMint":mint,"outputMint":WSOL_MINT,
                        "amount":token_lamps,"slippageBps":slip_bps},
                timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status != 200: return False, f"sell quote {r.status}", 0
                quote = await r.json()
            if "error" in quote: return False, quote["error"], 0
            sol_out = int(quote.get("outAmount",0)) / 1e9
            async with s.post(JUPITER_SWAP,
                json={"quoteResponse":quote,"userPublicKey":pubkey,
                      "wrapAndUnwrapSol":True,"dynamicComputeUnitLimit":True,
                      "prioritizationFeeLamports":prio_lamps},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200: return False, f"sell swap {r.status}", 0
                swap = await r.json()
        tx_bytes = base64.b64decode(swap.get("swapTransaction",""))
        if not tx_bytes: return False, "no swapTransaction", 0
        sig = await send_transaction_fast(tx_bytes, keypair, profile["skip_preflight"])
        if not sig: return False, "send failed", 0
        ok = await confirm_transaction(sig)
        return (True, sig, sol_out) if ok else (False, f"unconfirmed:{sig}", 0)
    except Exception as e: return False, str(e), 0

async def execute_full_sell(pos: Dict, reason: str) -> Tuple[bool, str, float, float]:
    enc_key = await _get_encrypted_key(pos["user_id"])
    if not enc_key: return False, "no key", 0, 0
    user    = await get_sniper_user(pos["user_id"])
    profile = RISK_PROFILES[user["risk_profile"]]
    kp      = load_keypair(enc_key)
    pubkey  = user["pubkey"]
    token_amt = await get_token_balance(pubkey, pos["mint"])
    if token_amt <= 0:
        await close_position(pos["id"], "sold_"+reason, "no_balance", 0, 0)
        return False, "no balance", 0, 0
    ok, sig, sol_out = await _sell_via_jupiter(pos["mint"], token_amt, pubkey, kp, profile)
    if not ok: return False, sig, 0, 0
    pnl_sol = round(sol_out - pos["sol_spent"], 4)
    pnl_pct = round((sol_out / pos["sol_spent"] - 1) * 100, 1)
    status  = {"tp":"sold_tp","sl":"sold_sl","rug":"sold_rug",
               "time_decay":"sold_decay","manual":"sold_manual"}.get(reason,"sold")
    await close_position(pos["id"], status, sig, pnl_sol, pnl_pct)
    await record_performance(pos, sol_out, pnl_sol, pnl_pct, reason)
    return True, sig, pnl_sol, pnl_pct

async def execute_partial_sell(pos: Dict, pct: float,
                               tier: int) -> Tuple[bool, str, float]:
    enc_key = await _get_encrypted_key(pos["user_id"])
    if not enc_key: return False, "no key", 0
    user    = await get_sniper_user(pos["user_id"])
    profile = RISK_PROFILES[user["risk_profile"]]
    kp      = load_keypair(enc_key)
    pubkey  = user["pubkey"]
    token_amt      = await get_token_balance(pubkey, pos["mint"])
    tokens_to_sell = token_amt * pct
    if tokens_to_sell <= 0: return False, "no tokens", 0
    ok, sig, sol_out = await _sell_via_jupiter(
        pos["mint"], tokens_to_sell, pubkey, kp, profile)
    if not ok: return False, sig, 0
    await mark_tier_sold(pos["id"], tier, token_amt - tokens_to_sell)
    return True, sig, sol_out

async def execute_manual_sell(user_id: int, pos_id: int) -> Tuple[bool, str, float, float]:
    positions = await get_open_positions(user_id)
    pos = next((p for p in positions if p["id"] == pos_id), None)
    if not pos: return False, "position not found", 0, 0
    return await execute_full_sell(pos, "manual")

# ── ALERT CALLBACK ────────────────────────────────────────────────────────────
_alert_callback = None
_prev_values: Dict[int, float] = {}

def set_alert_callback(fn): global _alert_callback; _alert_callback = fn

async def _notify(user_id: int, text: str):
    if _alert_callback:
        await _alert_callback(user_id, text)

def _pnl_emoji(pct: float) -> str:
    if pct >= 100: return "🚀"
    if pct >= 50:  return "🟢"
    if pct >= 10:  return "📈"
    if pct >= 0:   return "🟡"
    if pct >= -20: return "🟠"
    return "🔴"

# ── POSITION MONITOR ──────────────────────────────────────────────────────────
async def position_monitor_loop():
    logger.info("Position monitor started")
    await asyncio.sleep(10)
    while True:
        try:
            positions = await get_open_positions()
            for pos in positions:
                user = await get_sniper_user(pos["user_id"])
                if not user: continue
                profile   = RISK_PROFILES[user["risk_profile"]]
                uid       = pos["user_id"]
                mint      = pos["mint"]
                name      = pos["name"] or "?"
                symbol    = pos["symbol"] or "?"
                sol_spent = pos["sol_spent"]
                exit_mode = pos["exit_mode"]

                current_val = await get_position_value_sol(mint, pos["token_amount"])
                if current_val <= 0: continue

                ratio    = current_val / sol_spent
                pnl_pct  = round((ratio - 1) * 100, 1)
                pnl_sol  = round(current_val - sol_spent, 4)
                age_mins = round((time.time() - pos["bought_at"]) / 60, 1)

                peak = pos["peak_value_sol"] or sol_spent
                if current_val > peak:
                    peak       = current_val
                    trail_stop = peak * (1 - profile["trailing_stop_pct"])
                    await update_position_peak(pos["id"], peak, trail_stop)
                    pos["peak_value_sol"] = peak
                    pos["trailing_stop"]  = trail_stop
                peak_ratio = peak / sol_spent
                trail_stop = pos["trailing_stop"] or (sol_spent * (1 - profile["trailing_stop_pct"]))

                # Hair trigger rug
                prev_val = _prev_values.get(pos["id"], current_val)
                if prev_val > 0:
                    drop_pct = (prev_val - current_val) / prev_val
                    if drop_pct >= RUG_DROP_THRESHOLD:
                        logger.warning(f"RUG DETECTED {name}: -{drop_pct*100:.1f}%")
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "rug")
                        if ok:
                            await _notify(uid,
                                f"🚨 *RUG DETECTED — Emergency Exit*\n"
                                f"*{name}* (${symbol})\n"
                                f"Dropped {drop_pct*100:.1f}% in {POSITION_CHECK_INTERVAL}s\n"
                                f"PnL: {'+' if pnl>=0 else ''}{pnl:.4f} SOL ({pct_:.1f}%)\n"
                                f"`{sig[:20]}...`")
                        _prev_values.pop(pos["id"], None); continue
                _prev_values[pos["id"]] = current_val

                if exit_mode == "momentum":
                    if not pos["tier1_sold"] and ratio >= profile["tiered_t1_mult"]:
                        ok, sig, sol_out = await execute_partial_sell(pos, 0.40, 1)
                        if ok:
                            await _notify(uid,
                                f"🟢 *Tier 1 Exit — {profile['tiered_t1_mult']}x*\n"
                                f"*{name}* (${symbol})\n"
                                f"Sold 40% → +{sol_out:.4f} SOL\n"
                                f"60% still running · `{sig[:20]}...`")
                        continue
                    if pos["tier1_sold"] and not pos["tier2_sold"] and ratio >= profile["tiered_t2_mult"]:
                        ok, sig, sol_out = await execute_partial_sell(pos, 0.583, 2)
                        if ok:
                            await _notify(uid,
                                f"🚀 *Tier 2 Exit — {profile['tiered_t2_mult']}x*\n"
                                f"*{name}* (${symbol})\n"
                                f"Sold 35% more → +{sol_out:.4f} SOL\n"
                                f"Last 25% trailing · `{sig[:20]}...`")
                        continue
                    if pos["tier2_sold"] and current_val <= trail_stop:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "tp")
                        if ok:
                            await _notify(uid,
                                f"🏁 *Trailing Stop — Full Exit*\n"
                                f"*{name}* (${symbol})\n"
                                f"Peak {peak_ratio:.2f}x\n"
                                f"PnL: +{pnl:.4f} SOL (+{pct_:.1f}%)\n`{sig[:20]}...`")
                        _prev_values.pop(pos["id"], None); continue
                    if not pos["tier1_sold"] and current_val <= trail_stop:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "sl")
                        if ok:
                            await _notify(uid,
                                f"🔴 *Stop Loss*\n*{name}* (${symbol})\n"
                                f"PnL: {pnl:.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        _prev_values.pop(pos["id"], None); continue

                elif exit_mode == "steady":
                    if current_val <= trail_stop and peak_ratio >= 1.0:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "tp")
                        if ok:
                            await _notify(uid,
                                f"{'🟢' if pnl>=0 else '🔴'} *Trailing Stop Exit*\n"
                                f"*{name}* (${symbol})\n"
                                f"Peak {peak_ratio:.2f}x · Exit {ratio:.2f}x\n"
                                f"PnL: {'+' if pnl>=0 else ''}{pnl:.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        _prev_values.pop(pos["id"], None); continue
                    if ratio <= (1 - profile["trailing_stop_pct"] - 0.05):
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "sl")
                        if ok:
                            await _notify(uid,
                                f"🔴 *Stop Loss*\n*{name}* (${symbol})\n"
                                f"PnL: {pnl:.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        _prev_values.pop(pos["id"], None); continue

                elif exit_mode == "weak":
                    if ratio >= profile["fixed_tp"]:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "tp")
                        if ok:
                            await _notify(uid,
                                f"🟢 *Take Profit*\n*{name}* (${symbol})\n"
                                f"PnL: +{pnl:.4f} SOL (+{pct_:.1f}%)\n`{sig[:20]}...`")
                        _prev_values.pop(pos["id"], None); continue
                    if ratio <= profile["fixed_sl"]:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "sl")
                        if ok:
                            await _notify(uid,
                                f"🔴 *Stop Loss*\n*{name}* (${symbol})\n"
                                f"PnL: {pnl:.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        _prev_values.pop(pos["id"], None); continue

                if age_mins >= profile["time_decay_mins"] and ratio < 1.2:
                    ok, sig, pnl, pct_ = await execute_full_sell(pos, "time_decay")
                    if ok:
                        await _notify(uid,
                            f"⏱ *Time Decay Exit*\n*{name}* (${symbol})\n"
                            f"Held {age_mins}m with no momentum\n"
                            f"PnL: {'+' if pnl>=0 else ''}{pnl:.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                    _prev_values.pop(pos["id"], None); continue

                last_upd = pos.get("last_update_at") or 0
                if time.time() - last_upd >= LIVE_UPDATE_INTERVAL:
                    emoji      = _pnl_emoji(pnl_pct)
                    mode_label = {"momentum":"🔥 Momentum","steady":"📈 Steady",
                                  "weak":"💤 Weak"}.get(exit_mode, exit_mode)
                    await _notify(uid,
                        f"{emoji} *Live Update — {name}* (${symbol})\n"
                        f"Value: {current_val:.4f} SOL "
                        f"({'+' if pnl_pct>=0 else ''}{pnl_pct:.1f}%)\n"
                        f"Peak: {peak_ratio:.2f}x · Age: {age_mins}m\n"
                        f"Mode: {mode_label} · Trail: {(trail_stop/sol_spent):.2f}x")
                    await update_last_notified(pos["id"])

        except Exception as e:
            logger.error(f"Position monitor error: {e}")
        await asyncio.sleep(POSITION_CHECK_INTERVAL)
