"""
CLEX Sniper Module — per-user wallet, risk profiles, auto TP/SL, position monitor.
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
HELIUS_API_KEY  = os.getenv("HELIUS_API_KEY", "")
HELIUS_RPC      = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
MASTER_KEY      = os.getenv("MASTER_ENCRYPTION_KEY", "")  # Fernet key
DB_PATH         = "clex.db"

# Additional RPC endpoints for speed (race submissions)
RPC_ENDPOINTS = [
    HELIUS_RPC,
    "https://api.mainnet-beta.solana.com",
]

PUMPPORTAL_URL = "https://pumpportal.fun/api/trade-local"
JUPITER_QUOTE  = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP   = "https://quote-api.jup.ag/v6/swap"
WSOL_MINT      = "So11111111111111111111111111111111111111112"

POSITION_CHECK_INTERVAL = 20   # seconds between TP/SL checks
MIN_BUY_INTERVAL        = 30   # seconds between buys for same user

# ── RISK PROFILES ─────────────────────────────────────────────────────────────
RISK_PROFILES = {
    "low": {
        "label":        "🛡 Low Risk",
        "buy_sol":      0.05,
        "take_profit":  2.0,     # sell at 2x entry value
        "stop_loss":    0.70,    # sell if value drops to 70% of entry (−30%)
        "slippage":     10,      # %
        "priority_fee": 0.001,   # SOL
        "skip_preflight": False,
        "desc":         "0.05 SOL · 2x TP · −30% SL · safe slippage",
    },
    "moderate": {
        "label":        "⚡ Moderate",
        "buy_sol":      0.1,
        "take_profit":  3.0,
        "stop_loss":    0.50,
        "slippage":     15,
        "priority_fee": 0.003,
        "skip_preflight": True,
        "desc":         "0.1 SOL · 3x TP · −50% SL · fast execution",
    },
    "psycho": {
        "label":        "🤑 Psycho",
        "buy_sol":      0.25,
        "take_profit":  10.0,
        "stop_loss":    0.20,    # only exit at −80%
        "slippage":     25,
        "priority_fee": 0.005,
        "skip_preflight": True,
        "desc":         "0.25 SOL · 10x TP · −80% SL · max aggression",
    },
}

# ── ENCRYPTION ────────────────────────────────────────────────────────────────
def _fernet():
    from cryptography.fernet import Fernet
    if not MASTER_KEY:
        raise RuntimeError("MASTER_ENCRYPTION_KEY not set")
    return Fernet(MASTER_KEY.encode())

def encrypt_key(private_key_b58: str) -> str:
    return _fernet().encrypt(private_key_b58.encode()).decode()

def decrypt_key(encrypted: str) -> str:
    return _fernet().decrypt(encrypted.encode()).decode()

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
                last_buy_at     REAL DEFAULT NULL
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
                tp_target       REAL NOT NULL,
                sl_target       REAL NOT NULL
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sniper_setup_state (
                user_id         INTEGER PRIMARY KEY,
                step            TEXT NOT NULL,
                risk_profile    TEXT DEFAULT NULL
            )""")
        await db.commit()

# ── USER QUERIES ──────────────────────────────────────────────────────────────
async def get_sniper_user(user_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id, pubkey, risk_profile, sniper_enabled, last_buy_at "
            "FROM sniper_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return None
        return {"user_id": row[0], "pubkey": row[1], "risk_profile": row[2],
                "sniper_enabled": bool(row[3]), "last_buy_at": row[4]}

async def save_sniper_user(user_id: int, private_key_b58: str,
                           pubkey: str, risk_profile: str):
    enc = encrypt_key(private_key_b58)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO sniper_users "
            "(user_id, encrypted_key, pubkey, risk_profile, sniper_enabled, setup_at) "
            "VALUES (?,?,?,?,0,?)",
            (user_id, enc, pubkey, risk_profile, time.time()))
        await db.commit()

async def delete_sniper_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM sniper_users WHERE user_id=?", (user_id,))
        await db.execute("UPDATE sniper_positions SET status='wallet_deleted' WHERE user_id=? AND status='open'",
                         (user_id,))
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

async def get_enabled_snipers() -> List[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id, encrypted_key, pubkey, risk_profile, last_buy_at "
            "FROM sniper_users WHERE sniper_enabled=1")
        rows = await cur.fetchall()
        return [{"user_id": r[0], "encrypted_key": r[1], "pubkey": r[2],
                 "risk_profile": r[3], "last_buy_at": r[4]} for r in rows]

async def set_last_buy(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sniper_users SET last_buy_at=? WHERE user_id=?",
                         (time.time(), user_id))
        await db.commit()

# ── SETUP STATE ───────────────────────────────────────────────────────────────
async def set_setup_state(user_id: int, step: str, risk: Optional[str] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO sniper_setup_state VALUES (?,?,?)",
            (user_id, step, risk))
        await db.commit()

async def get_setup_state(user_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT step, risk_profile FROM sniper_setup_state WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return {"step": row[0], "risk": row[1]} if row else None

async def clear_setup_state(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM sniper_setup_state WHERE user_id=?", (user_id,))
        await db.commit()

# ── POSITIONS ─────────────────────────────────────────────────────────────────
async def save_position(user_id: int, mint: str, name: str, symbol: str,
                        sol_spent: float, token_amount: float, buy_tx: str,
                        tp: float, sl: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO sniper_positions "
            "(user_id,mint,name,symbol,sol_spent,token_amount,buy_tx,bought_at,tp_target,sl_target) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (user_id, mint, name, symbol, sol_spent, token_amount,
             buy_tx, time.time(), tp, sl))
        await db.commit()

async def get_open_positions(user_id: Optional[int] = None) -> List[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        if user_id:
            cur = await db.execute(
                "SELECT id,user_id,mint,name,symbol,sol_spent,token_amount,"
                "bought_at,tp_target,sl_target FROM sniper_positions "
                "WHERE status='open' AND user_id=?", (user_id,))
        else:
            cur = await db.execute(
                "SELECT id,user_id,mint,name,symbol,sol_spent,token_amount,"
                "bought_at,tp_target,sl_target FROM sniper_positions WHERE status='open'")
        rows = await cur.fetchall()
        return [{"id":r[0],"user_id":r[1],"mint":r[2],"name":r[3],"symbol":r[4],
                 "sol_spent":r[5],"token_amount":r[6],"bought_at":r[7],
                 "tp_target":r[8],"sl_target":r[9]} for r in rows]

async def close_position(pos_id: int, status: str, sell_tx: str,
                         pnl_sol: float, pnl_pct: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE sniper_positions SET status=?,sell_tx=?,pnl_sol=?,pnl_pct=? WHERE id=?",
            (status, sell_tx, pnl_sol, pnl_pct, pos_id))
        await db.commit()

# ── WALLET UTILS ──────────────────────────────────────────────────────────────
def validate_private_key(key_b58: str) -> Tuple[bool, str, str]:
    """Returns (valid, pubkey, error_msg)."""
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
        async with aiohttp.ClientSession() as s:
            async with s.post(HELIUS_RPC,
                json={"jsonrpc":"2.0","id":1,"method":"getBalance","params":[pubkey]},
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                return round(data.get("result",{}).get("value",0) / 1e9, 4)
    except:
        return 0.0

async def get_token_balance(pubkey: str, mint: str) -> float:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(HELIUS_RPC,
                json={"jsonrpc":"2.0","id":1,"method":"getTokenAccountsByOwner",
                      "params":[pubkey,{"mint":mint},{"encoding":"jsonParsed"}]},
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                accts = data.get("result",{}).get("value",[])
                if accts:
                    return float(accts[0]["account"]["data"]["parsed"]["info"]
                                 ["tokenAmount"]["uiAmount"] or 0)
    except:
        pass
    return 0.0

# ── TRANSACTION ENGINE ────────────────────────────────────────────────────────
async def _send_to_rpc(rpc_url: str, tx_b64: str, skip_preflight: bool) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(rpc_url,
                json={"jsonrpc":"2.0","id":1,"method":"sendTransaction",
                      "params":[tx_b64,{
                          "encoding":              "base64",
                          "skipPreflight":         skip_preflight,
                          "preflightCommitment":   "processed",
                          "maxRetries":            3,
                      }]},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                data = await r.json()
                if "result" in data:
                    return data["result"]
    except:
        pass
    return None

async def send_transaction_fast(tx_bytes: bytes, keypair,
                                skip_preflight: bool = True) -> Optional[str]:
    """Sign once, race across all RPC endpoints simultaneously."""
    from solders.transaction import VersionedTransaction
    tx     = VersionedTransaction.from_bytes(tx_bytes)
    signed = VersionedTransaction(tx.message, [keypair])
    tx_b64 = base64.b64encode(bytes(signed)).decode()

    tasks = [_send_to_rpc(url, tx_b64, skip_preflight) for url in RPC_ENDPOINTS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, str) and r:
            return r
    return None

async def confirm_transaction(sig: str, timeout_s: int = 25) -> bool:
    deadline = time.time() + timeout_s
    async with aiohttp.ClientSession() as s:
        while time.time() < deadline:
            try:
                async with s.post(HELIUS_RPC,
                    json={"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses",
                          "params":[[sig],{"searchTransactionHistory":True}]},
                    timeout=aiohttp.ClientTimeout(total=4)) as r:
                    data = await r.json()
                    val  = (data.get("result",{}).get("value") or [None])[0]
                    if val and val.get("confirmationStatus") in ("confirmed","finalized"):
                        return True
            except:
                pass
            await asyncio.sleep(1.5)
    return False

# ── BUY ENGINE ────────────────────────────────────────────────────────────────
async def _buy_pumpfun(mint: str, pubkey: str, keypair,
                       profile: Dict) -> Tuple[bool, str]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(PUMPPORTAL_URL,
                json={"publicKey":        pubkey,
                      "action":           "buy",
                      "mint":             mint,
                      "amount":           profile["buy_sol"],
                      "denominatedInSol": "true",
                      "slippage":         profile["slippage"],
                      "priorityFee":      profile["priority_fee"],
                      "pool":             "pump"},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    return False, f"pumpportal {r.status}"
                tx_bytes = await r.read()

        sig = await send_transaction_fast(tx_bytes, keypair, profile["skip_preflight"])
        if not sig:
            return False, "send failed"
        ok = await confirm_transaction(sig)
        return (True, sig) if ok else (False, f"unconfirmed:{sig}")
    except Exception as e:
        return False, str(e)

async def _buy_jupiter(mint: str, pubkey: str, keypair,
                       profile: Dict) -> Tuple[bool, str]:
    try:
        lamports   = int(profile["buy_sol"] * 1_000_000_000)
        slip_bps   = profile["slippage"] * 100
        prio_lamps = int(profile["priority_fee"] * 1_000_000_000)

        async with aiohttp.ClientSession() as s:
            async with s.get(JUPITER_QUOTE,
                params={"inputMint": WSOL_MINT, "outputMint": mint,
                        "amount": lamports, "slippageBps": slip_bps},
                timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status != 200:
                    return False, f"jup quote {r.status}"
                quote = await r.json()
            if "error" in quote:
                return False, f"jup error: {quote['error']}"

            async with s.post(JUPITER_SWAP,
                json={"quoteResponse": quote, "userPublicKey": pubkey,
                      "wrapAndUnwrapSol": True,
                      "dynamicComputeUnitLimit": True,
                      "prioritizationFeeLamports": prio_lamps},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    return False, f"jup swap {r.status}"
                swap = await r.json()

        tx_bytes = base64.b64decode(swap.get("swapTransaction", ""))
        if not tx_bytes:
            return False, "no swapTransaction"
        sig = await send_transaction_fast(tx_bytes, keypair, profile["skip_preflight"])
        if not sig:
            return False, "send failed"
        ok = await confirm_transaction(sig)
        return (True, sig) if ok else (False, f"unconfirmed:{sig}")
    except Exception as e:
        return False, str(e)

async def execute_user_buy(user_id: int, mint: str, name: str,
                           symbol: str, entry_curve: float) -> Tuple[bool, str, str]:
    """
    Full buy flow for a single user.
    Returns (success, tx_sig_or_err, method).
    """
    user = await get_sniper_user(user_id)
    if not user or not user["sniper_enabled"]:
        return False, "sniper off", "none"

    # Rate limit per user
    lba = user.get("last_buy_at") or 0
    if time.time() - lba < MIN_BUY_INTERVAL:
        return False, "rate limited", "none"

    profile = RISK_PROFILES[user["risk_profile"]]
    kp      = load_keypair(
        (await _get_encrypted_key(user_id)) or "")
    pubkey  = user["pubkey"]

    # Check balance
    bal = await get_sol_balance(pubkey)
    needed = profile["buy_sol"] + profile["priority_fee"] + 0.01  # +0.01 for fees
    if bal < needed:
        return False, f"low balance {bal:.3f} SOL (need {needed:.3f})", "none"

    # Try pump.fun first
    ok, sig = await _buy_pumpfun(mint, pubkey, kp, profile)
    method  = "pump.fun"

    if not ok:
        logger.warning(f"User {user_id} pump.fun failed ({sig}), trying Jupiter...")
        ok, sig = await _buy_jupiter(mint, pubkey, kp, profile)
        method  = "jupiter"

    if not ok:
        return False, sig, "failed"

    # Record position
    token_amt = await get_token_balance(pubkey, mint)
    await save_position(
        user_id=user_id, mint=mint, name=name, symbol=symbol,
        sol_spent=profile["buy_sol"], token_amount=token_amt,
        buy_tx=sig, tp=profile["take_profit"], sl=profile["stop_loss"])
    await set_last_buy(user_id)

    logger.info(f"User {user_id} sniped {name} via {method}: {sig[:20]}...")
    return True, sig, method

async def _get_encrypted_key(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT encrypted_key FROM sniper_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else None

# ── SELL ENGINE ───────────────────────────────────────────────────────────────
async def _sell_jupiter(mint: str, token_amount: float, pubkey: str,
                        keypair, profile: Dict) -> Tuple[bool, str, float]:
    """Sell token_amount → SOL. Returns (ok, sig, sol_received)."""
    try:
        # Get decimals + lamport amount
        async with aiohttp.ClientSession() as s:
            async with s.post(HELIUS_RPC,
                json={"jsonrpc":"2.0","id":1,"method":"getTokenSupply","params":[mint]},
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                decimals = data.get("result",{}).get("value",{}).get("decimals",6)

        token_lamports = int(token_amount * (10 ** decimals))
        slip_bps       = profile["slippage"] * 100
        prio_lamps     = int(profile["priority_fee"] * 1_000_000_000)

        async with aiohttp.ClientSession() as s:
            async with s.get(JUPITER_QUOTE,
                params={"inputMint": mint, "outputMint": WSOL_MINT,
                        "amount": token_lamports, "slippageBps": slip_bps},
                timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status != 200:
                    return False, f"sell quote {r.status}", 0
                quote = await r.json()
            if "error" in quote:
                return False, quote["error"], 0

            sol_out = int(quote.get("outAmount", 0)) / 1e9

            async with s.post(JUPITER_SWAP,
                json={"quoteResponse": quote, "userPublicKey": pubkey,
                      "wrapAndUnwrapSol": True,
                      "dynamicComputeUnitLimit": True,
                      "prioritizationFeeLamports": prio_lamps},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    return False, f"sell swap {r.status}", 0
                swap = await r.json()

        tx_bytes = base64.b64decode(swap.get("swapTransaction",""))
        if not tx_bytes:
            return False, "no swapTransaction", 0
        sig = await send_transaction_fast(tx_bytes, keypair, profile["skip_preflight"])
        if not sig:
            return False, "send failed", 0
        ok = await confirm_transaction(sig)
        return (True, sig, sol_out) if ok else (False, f"unconfirmed:{sig}", 0)
    except Exception as e:
        return False, str(e), 0

async def execute_sell(pos: Dict, reason: str) -> Tuple[bool, str, float, float]:
    """
    Sell a position. Returns (ok, sig, pnl_sol, pnl_pct).
    reason: 'tp' | 'sl' | 'manual'
    """
    user_id = pos["user_id"]
    enc_key = await _get_encrypted_key(user_id)
    if not enc_key:
        return False, "no key", 0, 0
    user    = await get_sniper_user(user_id)
    profile = RISK_PROFILES[user["risk_profile"]]
    kp      = load_keypair(enc_key)
    pubkey  = user["pubkey"]

    # Get actual current token balance (more accurate than stored amount)
    token_amt = await get_token_balance(pubkey, pos["mint"])
    if token_amt <= 0:
        await close_position(pos["id"], "sold_"+reason, "no_balance", 0, 0)
        return False, "no balance", 0, 0

    ok, sig, sol_out = await _sell_jupiter(pos["mint"], token_amt, pubkey, kp, profile)
    if not ok:
        return False, sig, 0, 0

    pnl_sol = round(sol_out - pos["sol_spent"], 4)
    pnl_pct = round((sol_out / pos["sol_spent"] - 1) * 100, 1)
    status  = "sold_tp" if reason == "tp" else ("sold_sl" if reason == "sl" else "sold_manual")
    await close_position(pos["id"], status, sig, pnl_sol, pnl_pct)
    return True, sig, pnl_sol, pnl_pct

# ── GET CURRENT VALUE ─────────────────────────────────────────────────────────
async def get_position_value_sol(mint: str, token_amount: float) -> float:
    """Jupiter quote: how much SOL would we get selling token_amount right now."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(HELIUS_RPC,
                json={"jsonrpc":"2.0","id":1,"method":"getTokenSupply","params":[mint]},
                timeout=aiohttp.ClientTimeout(total=4)) as r:
                data = await r.json()
                decimals = data.get("result",{}).get("value",{}).get("decimals",6)

        token_lamps = int(token_amount * (10**decimals))
        if token_lamps <= 0:
            return 0.0

        async with aiohttp.ClientSession() as s:
            async with s.get(JUPITER_QUOTE,
                params={"inputMint": mint, "outputMint": WSOL_MINT,
                        "amount": token_lamps, "slippageBps": 500},
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status != 200:
                    return 0.0
                q = await r.json()
                return int(q.get("outAmount",0)) / 1e9
    except:
        return 0.0

# ── POSITION MONITOR ──────────────────────────────────────────────────────────
# Injected at startup from main.py
_alert_callback = None

def set_alert_callback(fn):
    global _alert_callback
    _alert_callback = fn

async def position_monitor_loop():
    logger.info("Position monitor started")
    await asyncio.sleep(10)  # warm-up delay
    while True:
        try:
            positions = await get_open_positions()
            for pos in positions:
                user    = await get_sniper_user(pos["user_id"])
                if not user:
                    continue

                current_val = await get_position_value_sol(
                    pos["mint"], pos["token_amount"])
                if current_val <= 0:
                    continue

                ratio = current_val / pos["sol_spent"]

                if ratio >= pos["tp_target"]:
                    logger.info(f"TP hit {pos['name']} {ratio:.2f}x")
                    ok, sig, pnl, pct = await execute_sell(pos, "tp")
                    if ok and _alert_callback:
                        await _alert_callback(
                            pos["user_id"],
                            f"🟢 *Take Profit Hit!*\n"
                            f"{pos['name']} (${pos['symbol']})\n"
                            f"PnL: +{pnl:.4f} SOL (+{pct:.1f}%)\n"
                            f"`{sig[:20]}...`"
                        )

                elif ratio <= pos["sl_target"]:
                    logger.info(f"SL hit {pos['name']} {ratio:.2f}x")
                    ok, sig, pnl, pct = await execute_sell(pos, "sl")
                    if ok and _alert_callback:
                        await _alert_callback(
                            pos["user_id"],
                            f"🔴 *Stop Loss Hit*\n"
                            f"{pos['name']} (${pos['symbol']})\n"
                            f"PnL: {pnl:.4f} SOL ({pct:.1f}%)\n"
                            f"`{sig[:20]}...`"
                        )

        except Exception as e:
            logger.error(f"Position monitor error: {e}")

        await asyncio.sleep(POSITION_CHECK_INTERVAL)