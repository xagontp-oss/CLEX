"""
CLEX risk layer. Every LIVE entry passes through here and every live close is recorded here.

State is derived from the closed-position table, so it survives restarts and cannot drift:
  * Rolling expectancy kill switch: if the mean return of the last RISK_ROLLING_WINDOW closed live
    trades (since the user's last /resume) is below RISK_ROLLING_FLOOR, live entries stop.
  * Daily loss cap: if net realized loss since 00:00 UTC exceeds RISK_DAILY_CAP_SOL, live entries stop.
  * Fixed-fractional sizing with a fee-aware floor.
Paper trades are never counted and never blocked.
"""
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiosqlite

DB_PATH = os.getenv("DB_PATH", "clex.db")

RISK_MAX_BANKROLL_PCT = float(os.getenv("RISK_MAX_BANKROLL_PCT", "0.02"))
SIZE_CAP_MULT = 2.0                  # never more than 2x the 2% target (4% of balance)
MIN_STAKE_SOL = 0.08                 # fee-aware floor
MAX_STAKE_SOL = 0.50
FEE_PER_ROUNDTRIP_SOL = 0.003
MIN_STAKE_FEE_MULT = 20.0            # stake / round-trip fees must be at least this
RISK_ROLLING_WINDOW = int(os.getenv("RISK_ROLLING_WINDOW", "50"))
RISK_ROLLING_FLOOR = float(os.getenv("RISK_ROLLING_FLOOR", "-0.08"))
RISK_DAILY_CAP_SOL = float(os.getenv("RISK_DAILY_CAP_SOL", "2.0"))


def day_start(now: Optional[float] = None) -> float:
    d = datetime.fromtimestamp(now or time.time(), tz=timezone.utc)
    return d.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def evaluate(trades: List[Tuple[float, float, float]], resume_ts: float, now: Optional[float] = None,
             window: int = RISK_ROLLING_WINDOW, floor: float = RISK_ROLLING_FLOOR,
             cap: float = RISK_DAILY_CAP_SOL) -> Dict:
    """Pure. trades: (exit_ts, entry_sol, realized_sol) for CLOSED LIVE positions, any order."""
    now = now or time.time()
    since_day = day_start(now)
    rows = sorted((t for t in trades if t[0] and t[0] > resume_ts), key=lambda t: t[0])[-window:]
    rets = [(r - e) / e for _, e, r in rows if e > 0]
    mean = sum(rets) / len(rets) if rets else 0.0
    kill = len(rets) >= window and mean < floor
    day_net = sum(r - e for ts, e, r in trades if ts and ts >= since_day)
    day_loss = max(0.0, -day_net)
    cap_hit = day_loss >= cap
    return {"kill": kill, "cap_hit": cap_hit, "halted": kill or cap_hit, "n": len(rets), "window": window,
            "rolling_mean": mean, "floor": floor, "day_loss": day_loss, "cap": cap}


async def status(uid: int) -> Dict:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute("SELECT risk_resume_ts FROM apex_settings WHERE user_id=?", (uid,))
        row = await cur.fetchone()
        resume_ts = float(row[0] or 0.0) if row else 0.0
        cur = await db.execute(
            "SELECT exit_ts, entry_sol, realized_sol FROM apex_positions "
            "WHERE user_id=? AND mode='live' AND status='closed' AND exit_ts IS NOT NULL "
            "ORDER BY exit_ts DESC LIMIT 500", (uid,))
        trades = [(float(a), float(b), float(c)) for a, b, c in await cur.fetchall()]
    st = evaluate(trades, resume_ts)
    st["resume_ts"] = resume_ts
    return st


def size_for_user(balance_sol: float, chosen_sol: float, gas_reserve: float) -> Tuple[Optional[float], str]:
    """(stake, note). stake None means "do not enter" and note says why.
    The user's chosen size is only ever reduced, never raised: a $5 choice is blocked below the
    fee floor instead of silently risking more than they picked."""
    if chosen_sol < MIN_STAKE_SOL or chosen_sol / FEE_PER_ROUNDTRIP_SOL < MIN_STAKE_FEE_MULT:
        return None, (f"A {chosen_sol:.3f} SOL trade is below the {MIN_STAKE_SOL:g} SOL fee-safe minimum "
                      f"(about ${MIN_STAKE_SOL * 200:.0f}): network fees would eat over "
                      f"{FEE_PER_ROUNDTRIP_SOL / MIN_STAKE_SOL * 100:.1f}% of it. "
                      "Pick a larger size, or use Paper mode.")
    cap = min(MAX_STAKE_SOL, max(MIN_STAKE_SOL, balance_sol * RISK_MAX_BANKROLL_PCT * SIZE_CAP_MULT))
    stake = min(chosen_sol, cap)
    if balance_sol - stake - gas_reserve < 0:
        return None, "Balance too low for this size once the gas reserve is kept."
    note = (f"size reduced to {stake:.3f} SOL (risk cap {cap:.3f})" if stake < chosen_sol - 1e-9 else "")
    return stake, note
