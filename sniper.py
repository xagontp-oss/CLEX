"""
CLEX Sniper Module — QuickNode RPC, dynamic sizing, intelligent exits,
rug detection, blacklist, graduated token support, performance tracker.
"""
import asyncio
import aiosqlite
import aiohttp
import logging
import os
import time
import base64
import base58
from typing import Optional, Dict, Tuple, List
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

MASTER_KEY = os.getenv("MASTER_ENCRYPTION_KEY", "")
DB_PATH    = "clex.db"

def _rpc_url() -> str:
    qn  = os.getenv("QUICKNODE_RPC", "")
    hel = os.getenv("HELIUS_API_KEY", "")
    if qn:  return qn
    if hel: return f"https://mainnet.helius-rpc.com/?api-key={hel}"
    return "https://api.mainnet-beta.solana.com"

PUMPPORTAL_URL = "https://pumpportal.fun/api/trade-local"
JUPITER_QUOTE  = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP   = "https://quote-api.jup.ag/v6/swap"
WSOL_MINT      = "So11111111111111111111111111111111111111112"

POSITION_CHECK_INTERVAL = 15
MIN_BUY_INTERVAL        = 30
LIVE_UPDATE_INTERVAL    = 60
RUG_DROP_THRESHOLD      = 0.15
MAX_OPEN_EXPOSURE_PCT   = 0.30
BLACKLIST_THRESHOLD     = 3

# ── RISK PROFILES ─────────────────────────────────────────────────────────────
RISK_PROFILES = {
    "low": {
        "label": "🛡 Low Risk", "buy_pct": 0.05, "max_buy": 0.10,
        "slippage": 10, "priority_fee": 0.001, "skip_preflight": False,
        "trailing_stop_pct": 0.25, "tiered_t1_mult": 2.0, "tiered_t2_mult": 4.0,
        "fixed_tp": 1.8, "fixed_sl": 0.75, "time_decay_mins": 10,
        "desc": "5% of balance · trail 25% · tiered exits",
    },
    "moderate": {
        "label": "⚡ Moderate", "buy_pct": 0.10, "max_buy": 0.25,
        "slippage": 15, "priority_fee": 0.003, "skip_preflight": True,
        "trailing_stop_pct": 0.30, "tiered_t1_mult": 2.5, "tiered_t2_mult": 6.0,
        "fixed_tp": 2.2, "fixed_sl": 0.60, "time_decay_mins": 8,
        "desc": "10% of balance · trail 30% · tiered exits",
    },
    "psycho": {
        "label": "🤑 Psycho", "buy_pct": 0.20, "max_buy": 0.50,
        "slippage": 25, "priority_fee": 0.005, "skip_preflight": True,
        "trailing_stop_pct": 0.35, "tiered_t1_mult": 3.0, "tiered_t2_mult": 10.0,
        "fixed_tp": 2.5, "fixed_sl": 0.40, "time_decay_mins": 6,
        "desc": "20% of balance · trail 35% · max aggression",
    },
}

def classify_exit_mode(momentum: Dict) -> str:
    cv = momentum.get("curve_velocity", 0)
    hv = momentum.get("holder_velocity", 0)
    tx = momentum.get("tx_count", 0)
    if cv >= 1.5 and hv >= 5 and tx >= 25: return "momentum"
    if cv >= 0.4 and hv >= 1:              return "steady"
    return "weak"

def calc_buy_size(balance: float, profile: Dict, custom: Optional[float] = None) -> float:
    fee    = profile["priority_fee"]
    usable = balance - fee - 0.005
    if usable <= 0: return 0.0
    if custom and custom > 0: return round(min(custom, usable), 4)
    return round(min(balance * profile["buy_pct"], profile["max_buy"], usable), 4)

# ── ENCRYPTION ────────────────────────────────────────────────────────────────
def _fernet():
    from cryptography.fernet import Fernet
    if not MASTER_KEY: raise RuntimeError("MASTER_ENCRYPTION_KEY not set")
    return Fernet(MASTER_KEY.encode())

def encrypt_key(k: str) -> str: return _fernet().encrypt(k.encode()).decode()
def decrypt_key(k: str) -> str: return _fernet().decrypt(k.encode()).decode()

# ── DB INIT ───────────────────────────────────────────────────────────────────
async def init_sniper_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS sniper_users (
                user_id        INTEGER PRIMARY KEY,
                encrypted_key  TEXT NOT NULL,
                pubkey         TEXT NOT NULL,
                risk_profile   TEXT NOT NULL DEFAULT 'low',
                sniper_enabled INTEGER NOT NULL DEFAULT 0,
                setup_at       REAL NOT NULL,
                last_buy_at    REAL DEFAULT NULL,
                custom_amount  REAL DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS sniper_positions (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id        INTEGER NOT NULL,
                mint           TEXT NOT NULL,
                name           TEXT,
                symbol         TEXT,
                sol_spent      REAL NOT NULL,
                token_amount   REAL DEFAULT 0,
                buy_tx         TEXT,
                bought_at      REAL NOT NULL,
                status         TEXT NOT NULL DEFAULT 'open',
                sell_tx        TEXT DEFAULT NULL,
                pnl_sol        REAL DEFAULT NULL,
                pnl_pct        REAL DEFAULT NULL,
                exit_mode      TEXT NOT NULL DEFAULT 'steady',
                peak_value_sol REAL DEFAULT NULL,
                trailing_stop  REAL DEFAULT NULL,
                tier1_sold     INTEGER DEFAULT 0,
                tier2_sold     INTEGER DEFAULT 0,
                last_update_at REAL DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS sniper_setup_state (
                user_id      INTEGER PRIMARY KEY,
                step         TEXT NOT NULL,
                risk_profile TEXT DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS sniper_blacklist (
                wallet      TEXT PRIMARY KEY,
                snipe_count INTEGER NOT NULL DEFAULT 1,
                first_seen  REAL NOT NULL,
                last_seen   REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sniper_performance (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                mint        TEXT NOT NULL,
                name        TEXT,
                symbol      TEXT,
                sol_spent   REAL NOT NULL,
                sol_received REAL NOT NULL,
                pnl_sol     REAL NOT NULL,
                pnl_pct     REAL NOT NULL,
                exit_reason TEXT NOT NULL,
                exit_mode   TEXT NOT NULL,
                hold_secs   REAL NOT NULL,
                closed_at   REAL NOT NULL
            );
        """)
        for col, defn in [
            ("custom_amount", "REAL DEFAULT NULL"),
        ]:
            try:
                await db.execute(f"ALTER TABLE sniper_users ADD COLUMN {col} {defn}")
            except Exception:
                pass
        for col, defn in [
            ("exit_mode",      "TEXT DEFAULT 'steady'"),
            ("peak_value_sol", "REAL DEFAULT NULL"),
            ("trailing_stop",  "REAL DEFAULT NULL"),
            ("tier1_sold",     "INTEGER DEFAULT 0"),
            ("tier2_sold",     "INTEGER DEFAULT 0"),
            ("last_update_at", "REAL DEFAULT NULL"),
        ]:
            try:
                await db.execute(f"ALTER TABLE sniper_positions ADD COLUMN {col} {defn}")
            except Exception:
                pass
        await db.commit()

# ── USER CRUD ─────────────────────────────────────────────────────────────────
async def get_sniper_user(user_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id,pubkey,risk_profile,sniper_enabled,last_buy_at "
            "FROM sniper_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row: return None
        return {"user_id": row[0], "pubkey": row[1], "risk_profile": row[2],
                "sniper_enabled": bool(row[3]), "last_buy_at": row[4]}

async def save_sniper_user(user_id: int, pk_b58: str, pubkey: str, risk: str):
    enc = encrypt_key(pk_b58)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO sniper_users "
            "(user_id,encrypted_key,pubkey,risk_profile,sniper_enabled,setup_at) "
            "VALUES (?,?,?,?,0,?)", (user_id, enc, pubkey, risk, time.time()))
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
        return [{"user_id": r[0], "encrypted_key": r[1], "pubkey": r[2],
                 "risk_profile": r[3], "last_buy_at": r[4]} for r in rows]

async def _set_last_buy(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sniper_users SET last_buy_at=? WHERE user_id=?",
                         (time.time(), user_id))
        await db.commit()

async def _get_enc_key(user_id: int) -> Optional[str]:
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
        return {"step": row[0], "risk": row[1]} if row else None

async def clear_setup_state(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM sniper_setup_state WHERE user_id=?", (user_id,))
        await db.commit()

# ── BLACKLIST ─────────────────────────────────────────────────────────────────
async def record_first_buyer(wallet: str):
    now = time.time()
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                """INSERT INTO sniper_blacklist (wallet,snipe_count,first_seen,last_seen)
                   VALUES (?,1,?,?)
                   ON CONFLICT(wallet) DO UPDATE SET
                       snipe_count = snipe_count + 1,
                       last_seen   = excluded.last_seen""",
                (wallet, now, now))
            await db.commit()
    except Exception:
        pass

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
                        sol_spent: float, token_amount: float,
                        buy_tx: str, exit_mode: str):
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO sniper_positions "
            "(user_id,mint,name,symbol,sol_spent,token_amount,buy_tx,bought_at,"
            "exit_mode,peak_value_sol,tier1_sold,tier2_sold,last_update_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,0,0,?)",
            (user_id, mint, name, symbol, sol_spent, token_amount,
             buy_tx, now, exit_mode, sol_spent, now))
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
    return [{"id": r[0], "user_id": r[1], "mint": r[2], "name": r[3], "symbol": r[4],
             "sol_spent": r[5], "token_amount": r[6], "bought_at": r[7],
             "exit_mode": r[8], "peak_value_sol": r[9] or r[5],
             "trailing_stop": r[10], "tier1_sold": bool(r[11]),
             "tier2_sold": bool(r[12]), "last_update_at": r[13]}
            for r in rows]

async def get_total_open_exposure(user_id: int) -> float:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT SUM(sol_spent) FROM sniper_positions "
            "WHERE status='open' AND user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] or 0.0

async def _update_peak(pos_id: int, peak: float, trail: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE sniper_positions SET peak_value_sol=?,trailing_stop=? WHERE id=?",
            (peak, trail, pos_id))
        await db.commit()

async def _mark_tier(pos_id: int, tier: int, new_tokens: float):
    col = "tier1_sold" if tier == 1 else "tier2_sold"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE sniper_positions SET {col}=1,token_amount=? WHERE id=?",
            (new_tokens, pos_id))
        await db.commit()

async def update_last_notified(pos_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE sniper_positions SET last_update_at=? WHERE id=?",
            (time.time(), pos_id))
        await db.commit()

async def close_position(pos_id: int, status: str, sell_tx: str,
                         pnl_sol: float, pnl_pct: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE sniper_positions SET status=?,sell_tx=?,pnl_sol=?,pnl_pct=? WHERE id=?",
            (status, sell_tx, pnl_sol, pnl_pct, pos_id))
        await db.commit()

# ── PERFORMANCE ───────────────────────────────────────────────────────────────
async def record_performance(pos: Dict, sol_received: float,
                             pnl_sol: float, pnl_pct: float, reason: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO sniper_performance "
            "(user_id,mint,name,symbol,sol_spent,sol_received,pnl_sol,pnl_pct,"
            "exit_reason,exit_mode,hold_secs,closed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (pos["user_id"], pos["mint"], pos["name"], pos["symbol"],
             pos["sol_spent"], sol_received, pnl_sol, pnl_pct,
             reason, pos["exit_mode"], time.time() - pos["bought_at"], time.time()))
        await db.commit()

async def get_performance_stats(user_id: int) -> Dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT pnl_sol,pnl_pct,exit_reason,hold_secs,name,symbol "
            "FROM sniper_performance WHERE user_id=? ORDER BY closed_at DESC", (user_id,))
        rows = await cur.fetchall()
    if not rows: return {}
    total = len(rows)
    wins  = [r for r in rows if r[0] > 0]
    losses= [r for r in rows if r[0] <= 0]
    return {
        "total": total, "wins": len(wins), "losses": len(losses),
        "win_rate": round(len(wins)/total*100, 1),
        "total_pnl": round(sum(r[0] for r in rows), 4),
        "avg_win_pct": round(sum(r[1] for r in wins)/len(wins), 1) if wins else 0,
        "avg_loss_pct": round(sum(r[1] for r in losses)/len(losses), 1) if losses else 0,
        "best":  {"name": max(rows, key=lambda r: r[0])[4],
                  "pnl":  max(rows, key=lambda r: r[0])[0]},
        "worst": {"name": min(rows, key=lambda r: r[0])[4],
                  "pnl":  min(rows, key=lambda r: r[0])[0]},
        "avg_hold_mins": round(sum(r[3] for r in rows)/total/60, 1),
        "recent": [{"name": r[4], "symbol": r[5], "pnl": r[0],
                    "pct": r[1], "reason": r[2]} for r in rows[:5]],
    }

# ── WALLET UTILS ──────────────────────────────────────────────────────────────
def validate_private_key(key_b58: str) -> Tuple[bool, str, str]:
    try:
        from solders.keypair import Keypair
        kp = Keypair.from_bytes(base58.b58decode(key_b58.strip()))
        return True, str(kp.pubkey()), ""
    except Exception as e:
        return False, "", str(e)

def load_keypair(enc_key: str):
    from solders.keypair import Keypair
    return Keypair.from_bytes(base58.b58decode(decrypt_key(enc_key)))

async def get_sol_balance(pubkey: str) -> float:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(_rpc_url(),
                json={"jsonrpc": "2.0", "id": 1,
                      "method": "getBalance", "params": [pubkey]},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                data = await r.json()
                return round(data.get("result", {}).get("value", 0) / 1e9, 4)
    except Exception:
        return 0.0

async def get_token_balance(pubkey: str, mint: str) -> float:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(_rpc_url(),
                json={"jsonrpc": "2.0", "id": 1,
                      "method": "getTokenAccountsByOwner",
                      "params": [pubkey, {"mint": mint}, {"encoding": "jsonParsed"}]},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                data  = await r.json()
                accts = data.get("result", {}).get("value", [])
                if accts:
                    return float(accts[0]["account"]["data"]["parsed"]
                                 ["info"]["tokenAmount"]["uiAmount"] or 0)
    except Exception:
        pass
    return 0.0

# ── RPC ───────────────────────────────────────────────────────────────────────
async def _rpc(method: str, params: list) -> Optional[Dict]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(_rpc_url(),
                json={"jsonrpc": "2.0", "id": 1,
                      "method": method, "params": params},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    return (await r.json()).get("result")
    except Exception:
        pass
    return None

async def _send_tx(url: str, tx_b64: str, skip: bool) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url,
                json={"jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
                      "params": [tx_b64, {"encoding": "base64",
                                          "skipPreflight": skip,
                                          "preflightCommitment": "processed",
                                          "maxRetries": 3}]},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                data = await r.json()
                return data.get("result")
    except Exception:
        return None

async def send_transaction_fast(tx_bytes: bytes, keypair, skip: bool = True) -> Optional[str]:
    from solders.transaction import VersionedTransaction
    tx     = VersionedTransaction.from_bytes(tx_bytes)
    signed = VersionedTransaction(tx.message, [keypair])
    tx_b64 = base64.b64encode(bytes(signed)).decode()
    results = await asyncio.gather(
        _send_tx(_rpc_url(), tx_b64, skip),
        _send_tx("https://api.mainnet-beta.solana.com", tx_b64, skip),
        return_exceptions=True)
    for r in results:
        if isinstance(r, str) and r:
            return r
    return None

async def confirm_transaction(sig: str, timeout_s: int = 25) -> bool:
    deadline = time.time() + timeout_s
    async with aiohttp.ClientSession() as s:
        while time.time() < deadline:
            try:
                async with s.post(_rpc_url(),
                    json={"jsonrpc": "2.0", "id": 1,
                          "method": "getSignatureStatuses",
                          "params": [[sig], {"searchTransactionHistory": True}]},
                    timeout=aiohttp.ClientTimeout(total=4)) as r:
                    data = await r.json()
                    val  = ((data.get("result") or {}).get("value") or [None])[0]
                    if val and val.get("confirmationStatus") in ("confirmed", "finalized"):
                        return True
            except Exception:
                pass
            await asyncio.sleep(1.5)
    return False

async def get_position_value_sol(mint: str, token_amount: float) -> float:
    try:
        data     = await _rpc("getTokenSupply", [mint])
        decimals = (data or {}).get("value", {}).get("decimals", 6)
        lamps    = int(token_amount * (10 ** decimals))
        if lamps <= 0: return 0.0
        async with aiohttp.ClientSession() as s:
            async with s.get(JUPITER_QUOTE,
                params={"inputMint": mint, "outputMint": WSOL_MINT,
                        "amount": lamps, "slippageBps": 500},
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status != 200: return 0.0
                return int((await r.json()).get("outAmount", 0)) / 1e9
    except Exception:
        return 0.0

async def is_graduated(mint: str) -> bool:
    try:
        data     = await _rpc("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
        accounts = (data or {}).get("value", [])
        if not accounts: return True
        return (float(accounts[0].get("uiAmount") or 0) / 1_000_000_000 * 100) < 5.0
    except Exception:
        return False

async def fingerprint_dev_wallet(dev_wallet: str) -> Dict:
    result = {"single_funder": False, "flag": None}
    try:
        sigs = await _rpc("getSignaturesForAddress",
                          [dev_wallet, {"limit": 50, "commitment": "confirmed"}])
        if not sigs: return result
        cutoff = time.time() - 7 * 86400
        recent = [s for s in sigs if (s.get("blockTime") or 0) >= cutoff]
        if not recent: return result
        inflows: Dict[str, float] = {}
        for sig_info in recent[:20]:
            try:
                tx = await _rpc("getTransaction",
                    [sig_info["signature"],
                     {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
                if not tx: continue
                meta = tx.get("meta", {})
                pre  = meta.get("preBalances", [])
                post = meta.get("postBalances", [])
                keys = (tx.get("transaction", {}).get("message", {})
                          .get("accountKeys", []))
                for i, key in enumerate(keys):
                    addr = key if isinstance(key, str) else key.get("pubkey", "")
                    if addr == dev_wallet and i < len(pre) and i < len(post):
                        delta = (post[i] - pre[i]) / 1e9
                        if delta > 0.01:
                            for j, k2 in enumerate(keys):
                                a2 = k2 if isinstance(k2, str) else k2.get("pubkey", "")
                                if a2 != dev_wallet and j < len(pre) and j < len(post):
                                    d2 = (pre[j] - post[j]) / 1e9
                                    if d2 > 0:
                                        inflows[a2] = inflows.get(a2, 0) + d2
                await asyncio.sleep(0.05)
            except Exception:
                continue
        if inflows:
            top_amt = max(inflows.values())
            total   = sum(inflows.values())
            if total > 0 and top_amt / total >= 0.80:
                result["single_funder"] = True
                result["flag"] = "🔴 Funded 80%+ from single source"
    except Exception:
        pass
    return result

# ── BUY ENGINE ────────────────────────────────────────────────────────────────
async def _buy_pumpfun(mint: str, pubkey: str, kp, profile: Dict,
                       sol: float) -> Tuple[bool, str]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(PUMPPORTAL_URL,
                json={"publicKey": pubkey, "action": "buy", "mint": mint,
                      "amount": sol, "denominatedInSol": "true",
                      "slippage": profile["slippage"],
                      "priorityFee": profile["priority_fee"], "pool": "pump"},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200: return False, f"pumpportal {r.status}"
                tx_bytes = await r.read()
        sig = await send_transaction_fast(tx_bytes, kp, profile["skip_preflight"])
        if not sig: return False, "send failed"
        return (True, sig) if await confirm_transaction(sig) else (False, f"unconfirmed:{sig}")
    except Exception as e:
        return False, str(e)

async def _buy_jupiter(mint: str, pubkey: str, kp, profile: Dict,
                       sol: float) -> Tuple[bool, str]:
    try:
        slip_bps   = profile["slippage"] * 100
        prio_lamps = int(profile["priority_fee"] * 1e9)
        async with aiohttp.ClientSession() as s:
            async with s.get(JUPITER_QUOTE,
                params={"inputMint": WSOL_MINT, "outputMint": mint,
                        "amount": int(sol * 1e9), "slippageBps": slip_bps},
                timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status != 200: return False, f"jup quote {r.status}"
                quote = await r.json()
            if "error" in quote: return False, quote["error"]
            async with s.post(JUPITER_SWAP,
                json={"quoteResponse": quote, "userPublicKey": pubkey,
                      "wrapAndUnwrapSol": True, "dynamicComputeUnitLimit": True,
                      "prioritizationFeeLamports": prio_lamps},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200: return False, f"jup swap {r.status}"
                swap = await r.json()
        tx_bytes = base64.b64decode(swap.get("swapTransaction", ""))
        if not tx_bytes: return False, "no swapTransaction"
        sig = await send_transaction_fast(tx_bytes, kp, profile["skip_preflight"])
        if not sig: return False, "send failed"
        return (True, sig) if await confirm_transaction(sig) else (False, f"unconfirmed:{sig}")
    except Exception as e:
        return False, str(e)

async def execute_user_buy(user_id: int, mint: str, name: str,
                           symbol: str, momentum: Dict,
                           top_holders: Optional[List[str]] = None
                           ) -> Tuple[bool, str, str, float, str]:
    user = await get_sniper_user(user_id)
    if not user or not user["sniper_enabled"]:
        return False, "sniper off", "none", 0, ""
    if time.time() - (user.get("last_buy_at") or 0) < MIN_BUY_INTERVAL:
        return False, "rate limited", "none", 0, ""

    profile   = RISK_PROFILES[user["risk_profile"]]
    exit_mode = classify_exit_mode(momentum)
    pubkey    = user["pubkey"]

    if top_holders:
        for h in top_holders[:5]:
            if await is_blacklisted(h):
                return False, "blacklisted holder", "none", 0, ""

    bal      = await get_sol_balance(pubkey)
    exposure = await get_total_open_exposure(user_id)
    if exposure >= bal * MAX_OPEN_EXPOSURE_PCT:
        return False, "exposure cap hit", "none", 0, ""

    buy_sol = calc_buy_size(bal, profile, await get_custom_amount(user_id))
    if buy_sol <= 0:
        return False, f"balance too low (have {bal:.4f} SOL)", "none", 0, ""

    enc_key   = await _get_enc_key(user_id)
    kp        = load_keypair(enc_key)
    graduated = await is_graduated(mint)

    if not graduated:
        ok, sig = await _buy_pumpfun(mint, pubkey, kp, profile, buy_sol)
        method  = "pump.fun"
        if not ok:
            ok, sig = await _buy_jupiter(mint, pubkey, kp, profile, buy_sol)
            method  = "jupiter"
    else:
        ok, sig = await _buy_jupiter(mint, pubkey, kp, profile, buy_sol)
        method  = "jupiter(graduated)"

    if not ok:
        return False, sig, "failed", 0, ""

    token_amt = await get_token_balance(pubkey, mint)
    await save_position(user_id, mint, name, symbol, buy_sol, token_amt, sig, exit_mode)
    await _set_last_buy(user_id)
    logger.info(f"BUY uid={user_id} {name} {buy_sol}SOL via {method} mode={exit_mode}")
    return True, sig, method, buy_sol, exit_mode

# ── SELL ENGINE ───────────────────────────────────────────────────────────────
async def _sell_jupiter(mint: str, tokens: float, pubkey: str, kp,
                        profile: Dict) -> Tuple[bool, str, float]:
    try:
        data     = await _rpc("getTokenSupply", [mint])
        decimals = (data or {}).get("value", {}).get("decimals", 6)
        lamps    = int(tokens * (10 ** decimals))
        slip_bps = profile["slippage"] * 100
        prio     = int(profile["priority_fee"] * 1e9)
        async with aiohttp.ClientSession() as s:
            async with s.get(JUPITER_QUOTE,
                params={"inputMint": mint, "outputMint": WSOL_MINT,
                        "amount": lamps, "slippageBps": slip_bps},
                timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status != 200: return False, f"quote {r.status}", 0.0
                quote = await r.json()
            if "error" in quote: return False, quote["error"], 0.0
            sol_out = int(quote.get("outAmount", 0)) / 1e9
            async with s.post(JUPITER_SWAP,
                json={"quoteResponse": quote, "userPublicKey": pubkey,
                      "wrapAndUnwrapSol": True, "dynamicComputeUnitLimit": True,
                      "prioritizationFeeLamports": prio},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200: return False, f"swap {r.status}", 0.0
                swap = await r.json()
        tx_bytes = base64.b64decode(swap.get("swapTransaction", ""))
        if not tx_bytes: return False, "no swapTransaction", 0.0
        sig = await send_transaction_fast(tx_bytes, kp, profile["skip_preflight"])
        if not sig: return False, "send failed", 0.0
        ok = await confirm_transaction(sig)
        return (True, sig, sol_out) if ok else (False, f"unconfirmed:{sig}", 0.0)
    except Exception as e:
        return False, str(e), 0.0

async def execute_full_sell(pos: Dict, reason: str) -> Tuple[bool, str, float, float]:
    enc = await _get_enc_key(pos["user_id"])
    if not enc: return False, "no key", 0.0, 0.0
    user    = await get_sniper_user(pos["user_id"])
    profile = RISK_PROFILES[user["risk_profile"]]
    kp      = load_keypair(enc)
    pubkey  = user["pubkey"]
    tokens  = await get_token_balance(pubkey, pos["mint"])
    if tokens <= 0:
        await close_position(pos["id"], f"sold_{reason}", "no_balance", 0, 0)
        return False, "no balance", 0.0, 0.0
    ok, sig, sol_out = await _sell_jupiter(pos["mint"], tokens, pubkey, kp, profile)
    if not ok: return False, sig, 0.0, 0.0
    pnl_sol = round(sol_out - pos["sol_spent"], 4)
    pnl_pct = round((sol_out / pos["sol_spent"] - 1) * 100, 1)
    status  = {"tp": "sold_tp", "sl": "sold_sl", "rug": "sold_rug",
               "time_decay": "sold_decay", "manual": "sold_manual"}.get(reason, "sold")
    await close_position(pos["id"], status, sig, pnl_sol, pnl_pct)
    await record_performance(pos, sol_out, pnl_sol, pnl_pct, reason)
    return True, sig, pnl_sol, pnl_pct

async def execute_partial_sell(pos: Dict, pct: float, tier: int) -> Tuple[bool, str, float]:
    enc = await _get_enc_key(pos["user_id"])
    if not enc: return False, "no key", 0.0
    user    = await get_sniper_user(pos["user_id"])
    profile = RISK_PROFILES[user["risk_profile"]]
    kp      = load_keypair(enc)
    pubkey  = user["pubkey"]
    tokens  = await get_token_balance(pubkey, pos["mint"])
    to_sell = tokens * pct
    if to_sell <= 0: return False, "no tokens", 0.0
    ok, sig, sol_out = await _sell_jupiter(pos["mint"], to_sell, pubkey, kp, profile)
    if not ok: return False, sig, 0.0
    await _mark_tier(pos["id"], tier, tokens - to_sell)
    return True, sig, sol_out

async def execute_manual_sell(user_id: int, pos_id: int) -> Tuple[bool, str, float, float]:
    positions = await get_open_positions(user_id)
    pos = next((p for p in positions if p["id"] == pos_id), None)
    if not pos: return False, "not found", 0.0, 0.0
    return await execute_full_sell(pos, "manual")

# ── ALERT CALLBACK + POSITION MONITOR ─────────────────────────────────────────
_alert_callback = None
_prev_values: Dict[int, float] = {}

def set_alert_callback(fn):
    global _alert_callback
    _alert_callback = fn

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

async def position_monitor_loop():
    logger.info("Position monitor started")
    await asyncio.sleep(10)
    while True:
        try:
            for pos in await get_open_positions():
                user = await get_sniper_user(pos["user_id"])
                if not user: continue
                profile   = RISK_PROFILES[user["risk_profile"]]
                uid       = pos["user_id"]
                name      = pos["name"] or "?"
                symbol    = pos["symbol"] or "?"
                sol_spent = pos["sol_spent"]
                exit_mode = pos["exit_mode"]

                cur_val = await get_position_value_sol(pos["mint"], pos["token_amount"])
                if cur_val <= 0: continue

                ratio    = cur_val / sol_spent
                pnl_pct  = round((ratio - 1) * 100, 1)
                age_mins = round((time.time() - pos["bought_at"]) / 60, 1)

                # Update peak / trailing stop
                peak = pos["peak_value_sol"] or sol_spent
                if cur_val > peak:
                    peak  = cur_val
                    trail = peak * (1 - profile["trailing_stop_pct"])
                    await _update_peak(pos["id"], peak, trail)
                    pos["peak_value_sol"] = peak
                    pos["trailing_stop"]  = trail
                peak_ratio = peak / sol_spent
                trail_stop = pos["trailing_stop"] or (sol_spent * (1 - profile["trailing_stop_pct"]))

                # Hair-trigger rug detection
                prev = _prev_values.get(pos["id"], cur_val)
                if prev > 0 and (prev - cur_val) / prev >= RUG_DROP_THRESHOLD:
                    ok, sig, pnl, pct_ = await execute_full_sell(pos, "rug")
                    if ok:
                        await _notify(uid,
                            f"🚨 *RUG — Emergency Exit*\n*{name}* (${symbol})\n"
                            f"PnL: {pnl:+.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                    _prev_values.pop(pos["id"], None)
                    continue
                _prev_values[pos["id"]] = cur_val

                # Exit logic
                exited = False
                if exit_mode == "momentum":
                    if not pos["tier1_sold"] and ratio >= profile["tiered_t1_mult"]:
                        ok, sig, out = await execute_partial_sell(pos, 0.40, 1)
                        if ok:
                            await _notify(uid,
                                f"🟢 *Tier 1 — {profile['tiered_t1_mult']}x*\n"
                                f"*{name}* sold 40% → +{out:.4f} SOL\n`{sig[:20]}...`")
                        exited = True
                    elif pos["tier1_sold"] and not pos["tier2_sold"] and ratio >= profile["tiered_t2_mult"]:
                        ok, sig, out = await execute_partial_sell(pos, 0.583, 2)
                        if ok:
                            await _notify(uid,
                                f"🚀 *Tier 2 — {profile['tiered_t2_mult']}x*\n"
                                f"*{name}* sold 35% more → +{out:.4f} SOL\n`{sig[:20]}...`")
                        exited = True
                    elif pos["tier2_sold"] and cur_val <= trail_stop:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "tp")
                        if ok:
                            await _notify(uid,
                                f"🏁 *Trail Exit*\n*{name}* peak {peak_ratio:.2f}x\n"
                                f"PnL: {pnl:+.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        exited = True
                    elif not pos["tier1_sold"] and cur_val <= trail_stop:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "sl")
                        if ok:
                            await _notify(uid,
                                f"🔴 *Stop Loss*\n*{name}*\n"
                                f"PnL: {pnl:.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        exited = True

                elif exit_mode == "steady":
                    if cur_val <= trail_stop and peak_ratio >= 1.0:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "tp")
                        if ok:
                            await _notify(uid,
                                f"{'🟢' if pnl>=0 else '🔴'} *Trail Exit*\n"
                                f"*{name}* peak {peak_ratio:.2f}x → {ratio:.2f}x\n"
                                f"PnL: {pnl:+.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        exited = True
                    elif ratio <= (1 - profile["trailing_stop_pct"] - 0.05):
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "sl")
                        if ok:
                            await _notify(uid,
                                f"🔴 *Stop Loss*\n*{name}*\n"
                                f"PnL: {pnl:.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        exited = True

                elif exit_mode == "weak":
                    if ratio >= profile["fixed_tp"]:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "tp")
                        if ok:
                            await _notify(uid,
                                f"🟢 *Take Profit*\n*{name}*\n"
                                f"PnL: {pnl:+.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        exited = True
                    elif ratio <= profile["fixed_sl"]:
                        ok, sig, pnl, pct_ = await execute_full_sell(pos, "sl")
                        if ok:
                            await _notify(uid,
                                f"🔴 *Stop Loss*\n*{name}*\n"
                                f"PnL: {pnl:.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                        exited = True

                if not exited and age_mins >= profile["time_decay_mins"] and ratio < 1.2:
                    ok, sig, pnl, pct_ = await execute_full_sell(pos, "time_decay")
                    if ok:
                        await _notify(uid,
                            f"⏱ *Time Decay Exit*\n*{name}* held {age_mins}m\n"
                            f"PnL: {pnl:+.4f} SOL ({pct_:.1f}%)\n`{sig[:20]}...`")
                    exited = True

                if exited:
                    _prev_values.pop(pos["id"], None)
                    continue

                # Live PnL update
                last_upd = pos.get("last_update_at") or 0
                if time.time() - last_upd >= LIVE_UPDATE_INTERVAL:
                    mode_label = {"momentum": "🔥 Momentum", "steady": "📈 Steady",
                                  "weak": "💤 Weak"}.get(exit_mode, exit_mode)
                    await _notify(uid,
                        f"{_pnl_emoji(pnl_pct)} *{name}* (${symbol})\n"
                        f"Value: {cur_val:.4f} SOL ({pnl_pct:+.1f}%)\n"
                        f"Peak: {peak_ratio:.2f}x · Age: {age_mins}m · {mode_label}")
                    await update_last_notified(pos["id"])

        except Exception as e:
            logger.error(f"Position monitor error: {e}")
        await asyncio.sleep(POSITION_CHECK_INTERVAL)
