"""
CLEX LAB: a 24/7 paper-trading research engine. It never touches a wallet or spends SOL.

What it does
  1. JOURNAL   Every new pump.fun launch is tracked on-chain (raw bonding-curve reserves, batched
               reads every ~5s for up to 15 minutes), plus holder snapshots at ~T+60s and ~T+120s.
  2. SIMULATE  Each tracked coin is replayed through a registry of pre-registered strategy VARIANTS.
               Entry and exit fills use the real constant-product curve math (price impact, the
               pump.fun fee, a network fee per leg) at several execution latencies, so we learn how
               much speed is worth.
  3. JUDGE     Only trades made AFTER a variant was registered ("forward" trades) count. A variant
               is promoted only when a strict checklist passes (see judge()). Challenger variants
               are spawned from the best performers, but they start with zero evidence, and the
               multiple-testing correction grows with every variant ever registered.

Honest limits (also shown in the UI)
  * Paper is not live. MEV/sandwiching, failed transactions and queueing are not modelled.
  * Sells on the curve are assumed to land after `latency` seconds. That is a target for the
    execution engine, not a description of today's Jupiter-based exits.
  * Coins that never showed on-chain activity by T+90s are dropped (they could not have been traded
    by any variant). Coins that showed activity and then died are kept, losses included.
"""
import asyncio
import base64
import copy
import csv
import hashlib
import html
import io
import json
import logging
import os
import random
import struct
import time
import zlib
from bisect import bisect_left, bisect_right
from collections import Counter
from typing import Dict, List, Optional, Tuple

import aiosqlite
from aiogram import F, Router
from aiogram.types import BufferedInputFile, CallbackQuery

import apex

logger = logging.getLogger("lab")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
ENABLED = os.getenv("LAB_ENABLED", "1") != "0"
TICK_S = 5.0                      # poll period
HORIZON_S = 900                   # track each coin for at most 15 minutes
MAX_ACTIVE = int(os.getenv("LAB_MAX_ACTIVE", "160"))
DAILY_CALLS = int(os.getenv("LAB_DAILY_CALLS", "120000"))    # Helius-call budget the lab may use per UTC day
MAX_LAUNCH_AGE_S = 60             # only admit launches the webhook delivers within a minute
DEAD_AT_S = 90                    # a coin with < DEAD_INFLOW SOL of net buying by then is dropped
DEAD_INFLOW = 0.25
COLLAPSE_FRAC = 0.15              # settle when price falls below 15% of its peak
HOLDER_AGES = (60.0, 120.0)
HOLDER_MIN_INFLOW = 0.3
BATCH = 100                       # accounts per getMultipleAccounts

PUMP_FEE = 0.0125                 # protocol + creator fee, charged on both sides
LEG_FEE_SOL = 0.0015              # priority fee + tip per transaction (assumption; verify on-chain)
STAKE_SOL = 0.075                 # simulated position size (about the owner's usual stake)
LATENCIES = (2.0, 5.0, 10.0, 20.0)
PRIMARY_LATENCY = 5.0
STRESS_LATENCY = 10.0

MIN_TRADES = 600
CONFIRM_EXTRA = 150               # must still pass after this many MORE trades
MAX_DD_STAKES = 10.0
RETIRE_MIN_N = 300
EVAL_EVERY_S = 900
EVOLVE_EVERY_S = 6 * 3600
DIGEST_EVERY_S = 12 * 3600
MAX_VARIANTS = 60
MAX_SPAWN = 4
EVOLVE_MIN_N = 150
PATH_KEEP_DAYS = 14
BACKTEST_LIMIT = 4000
BOOT = 4000

CURVE_DISC = bytes.fromhex("17b7f83760d8ac60")
VERDICT_BIT = 1 << 6

# ══════════════════════════════════════════════════════════════════════════════
# CURVE ACCOUNT + MATH (pure)
# ══════════════════════════════════════════════════════════════════════════════
Sample = Tuple[float, float, float, float, float]      # (t, vtok, vsol, rtok, rsol), token ui / SOL


def parse_curve(b64: str):
    """(vtok, vsol, rtok, rsol, supply, complete) in token-ui / SOL units, or None."""
    try:
        raw = base64.b64decode(b64)
    except Exception:
        return None
    if len(raw) < 49 or raw[:8] != CURVE_DISC:
        return None
    vtok, vsol, rtok, rsol, supply = struct.unpack_from("<5Q", raw, 8)
    return vtok / 1e6, vsol / 1e9, rtok / 1e6, rsol / 1e9, supply / 1e6, raw[48] == 1


def buy_tokens(vtok: float, vsol: float, sol: float, fee: float = PUMP_FEE) -> float:
    """Tokens received for `sol` spent on a constant-product curve (fee taken from the input)."""
    dx = sol * (1.0 - fee)
    if vtok <= 0 or vsol <= 0 or dx <= 0:
        return 0.0
    return vtok - vtok * vsol / (vsol + dx)


def sell_sol(vtok: float, vsol: float, tok: float, fee: float = PUMP_FEE) -> float:
    """SOL received for selling `tok` tokens (fee taken from the output)."""
    if vtok <= 0 or vsol <= 0 or tok <= 0:
        return 0.0
    nv = vtok + tok
    return (vsol - vtok * vsol / nv) * (1.0 - fee)


# NOTE: on a constant-product curve an instant buy-then-sell loses only the fees (price impact cancels),
# so "impact" is NOT a cost to gate on. What matters is our SHARE of the curve (see `share`) and the
# curve TYPE (v0). Both are recorded as features.


# ══════════════════════════════════════════════════════════════════════════════
# CANDIDATE + CONTEXT SERIES (pure)
# ══════════════════════════════════════════════════════════════════════════════
class Cand:
    """A tracked coin: its price path and holder snapshots. Everything the simulator needs."""
    __slots__ = ("mint", "launch_ts", "dev", "dev_pct0", "path", "holders", "v0", "supply",
                 "end_reason", "ts", "_ctx")

    def __init__(self, mint: str, launch_ts: float, dev: str = "", dev_pct0: float = 0.0):
        self.mint, self.launch_ts, self.dev, self.dev_pct0 = mint, launch_ts, dev, dev_pct0
        self.path: List[Sample] = []
        self.holders: List[Dict] = []
        self.v0 = 0.0
        self.supply = 0.0
        self.end_reason = ""
        self.ts: List[float] = []
        self._ctx: Optional[List[Dict]] = None

    def add(self, t: float, vtok: float, vsol: float, rtok: float, rsol: float, supply: float = 0.0):
        if not self.path:
            self.v0 = round(max(0.0, vsol - rsol), 3)
            self.supply = supply
        self.path.append((t, vtok, vsol, rtok, rsol))
        self.ts.append(t)
        self._ctx = None

    def finalize(self):
        self.ts = [s[0] for s in self.path]
        self._ctx = None


def _lookback(ts: List[float], vals: List[float], t: float, back: float) -> Tuple[float, float]:
    """(value, dt) at the last sample at or before t-back (dt = real elapsed time to t)."""
    j = bisect_right(ts, t - back) - 1
    if j < 0:
        j = 0
    return vals[j], t - ts[j]


def build_ctx(c: Cand) -> List[Dict]:
    """One context dict per sample, using ONLY samples up to and including that one (causal)."""
    if c._ctx is not None:
        return c._ctx
    P = c.path
    ts = [s[0] for s in P]
    vs = [s[2] for s in P]
    pr = [(s[2] / s[1]) if (s[1] > 0 and s[2] > 0) else None for s in P]
    out: List[Dict] = []
    if not P or pr[0] is None:
        c._ctx = out
        return out
    p0 = pr[0]
    peak = 0.0
    hts = [h["t"] for h in c.holders]
    for i in range(len(P)):
        if pr[i] is None:
            break
        t = ts[i]
        peak = max(peak, pr[i])

        def rate(w: float) -> Optional[float]:
            v, dt = _lookback(ts, vs, t, w)
            return (vs[i] - v) / dt * 60.0 if dt >= 0.5 * w else None

        v30, dt30 = _lookback(ts, vs, t, 30.0)
        v60, dt60 = _lookback(ts, vs, t, 60.0)
        prior = ((v30 - v60) / max(dt60 - dt30, 1e-9) * 60.0) if (dt60 - dt30) >= 15.0 else None
        p10, dt10 = _lookback(ts, pr, t, 10.0)
        slope10 = (pr[i] / p10 - 1.0) if (dt10 >= 5.0 and p10) else None

        def pos(m: int) -> Optional[int]:
            if i < m:
                return None
            return sum(1 for k in range(i - m + 1, i + 1) if vs[k] > vs[k - 1])

        hj = bisect_right(hts, t) - 1
        out.append({
            "i": i, "age": t - c.launch_ts, "t": t, "price": pr[i], "mult": pr[i] / p0,
            "inflow": vs[i] - vs[0], "rate10": rate(10.0), "rate20": rate(20.0), "rate30": rate(30.0),
            "rate_prior30": prior, "pos4": pos(4), "pos8": pos(8), "slope10": slope10,
            "dd": pr[i] / peak - 1.0, "peak_mult": peak / p0,
            "share": STAKE_SOL / (max(vs[i] - vs[0], 0.0) + STAKE_SOL),
            "holder": c.holders[hj] if hj >= 0 else None,
        })
    c._ctx = out
    return out


# ══════════════════════════════════════════════════════════════════════════════
# EXIT RULES + SIMULATOR (pure)
# ══════════════════════════════════════════════════════════════════════════════
def exit_decide(x: Dict, ratio: float, tiers_hit: int, peak: float, age: float) -> Dict:
    """Same shape as apex.decide_exit. mode 'verdict' mirrors the live Scalp profile exactly
    (a parity test enforces it); mode 'plain' is a simple stop / trail / time-stop profile.
    Tier fractions are fractions of the ORIGINAL position; frac >= 1 means everything."""
    peak = max(peak, ratio)
    out = {"actions": [], "peak": peak, "flags": 0}
    stop = x.get("stop", 0.90)
    trail_keep = x.get("trail_keep", 0.65)
    if x.get("mode", "verdict") == "verdict":
        vs = x.get("verdict_secs")
        runner = bool(tiers_hit & 1)
        engaged = bool(tiers_hit & VERDICT_BIT) or runner
        if engaged and ratio <= peak * trail_keep:
            out["actions"].append(("all", 1.0, "trailing_stop", -1))
            return out
        if ratio <= stop:
            out["actions"].append(("all", 1.0, "stop_loss", -1))
            return out
        if vs and not engaged and age >= vs:
            if ratio > x.get("verdict_ratio", 1.08):
                out["flags"] |= VERDICT_BIT
            else:
                out["actions"].append(("all", 1.0, "verdict", -1))
                return out
    else:
        trail_on = x.get("trail_on")
        if trail_on and peak >= trail_on and ratio <= peak * trail_keep:
            out["actions"].append(("all", 1.0, "trailing_stop", -1))
            return out
        if ratio <= stop:
            out["actions"].append(("all", 1.0, "stop_loss", -1))
            return out
        ts_secs = x.get("time_stop_secs")
        if ts_secs and age >= ts_secs and ratio <= x.get("time_stop_ratio", 1.0):
            out["actions"].append(("all", 1.0, "time_stop", -1))
            return out
    for i, (thr, frac) in enumerate(x.get("tiers", [])):
        if tiers_hit & (1 << i) or ratio < thr:
            continue
        if frac >= 1.0:
            out["actions"].append(("all", 1.0, "target", i))
            break
        out["actions"].append(("partial", frac, f"target_{int(round((thr - 1) * 100))}pct", i))
    return out


def simulate(c: Cand, d_idx: int, x: Dict, latency: float, stake: float = STAKE_SOL) -> Optional[Dict]:
    """Replay one trade on a coin's recorded path. The decision was made at sample d_idx; the buy
    fills at the first sample at least `latency` seconds later; every exit trigger likewise fills
    `latency` seconds after the tick that tripped it. Returns None if no fill was possible."""
    P, ts = c.path, c.ts
    n = len(P)
    t_dec = ts[d_idx]
    e = bisect_left(ts, t_dec + latency)
    if e >= n:
        return None
    _, vt, vs, rtok, _ = P[e]
    if vs <= 0 or vt <= 0:
        return None
    tokens0 = buy_tokens(vt, vs, stake)
    if tokens0 <= 0 or tokens0 > rtok:
        return None
    price_e = vs / vt
    cost = stake + LEG_FEE_SOL
    left, proceeds, peak, hit = tokens0, 0.0, 1.0, 0
    entry_t = ts[e]
    reason, t_exit, n_sells = "", entry_t, 0
    i = e + 1
    tiny = tokens0 * 1e-9
    while i < n and left > tiny:
        _, vt_i, vs_i, _, _ = P[i]
        if vs_i <= 0 or vt_i <= 0:
            break
        ratio = sell_sol(vt_i, vs_i, left) / (stake * left / tokens0)
        dec = exit_decide(x, ratio, hit, peak, ts[i] - entry_t)
        peak = dec["peak"]
        hit |= dec["flags"]
        if not dec["actions"]:
            i += 1
            continue
        j = min(bisect_left(ts, ts[i] + latency, i), n - 1)
        vt_j, vs_j = P[j][1], P[j][2]
        if vs_j <= 0 or vt_j <= 0:
            break
        for kind, frac, why, tier in dec["actions"]:
            tok = left if kind == "all" else min(left, frac * tokens0)
            if tok <= 0:
                continue
            out = sell_sol(vt_j, vs_j, tok)
            proceeds += out - LEG_FEE_SOL
            n_sells += 1
            nv = vt_j + tok                                   # our own sale moves the curve
            vs_j, vt_j = vt_j * vs_j / nv, nv
            left -= tok
            if tier >= 0:
                hit |= 1 << tier
            reason, t_exit = why, ts[j]
            if left <= tiny:
                break
        i = j + 1
    if left > tiny:                                           # path ended: close at the last live sample
        k = n - 1
        while k > e and (P[k][2] <= 0 or P[k][1] <= 0):
            k -= 1
        proceeds += sell_sol(P[k][1], P[k][2], left) - LEG_FEE_SOL
        n_sells += 1
        reason = reason if (reason and left <= tiny) else (c.end_reason or "horizon")
        t_exit = ts[k]
    pnl = proceeds - cost
    return {"ret": pnl / stake, "pnl": pnl, "reason": reason, "hold": t_exit - entry_t,
            "peak": peak, "entry_idx": e, "mult_entry": price_e / (P[0][2] / P[0][1]),
            "sells": n_sells}


# ══════════════════════════════════════════════════════════════════════════════
# VARIANTS: entry rules (pure)
# ══════════════════════════════════════════════════════════════════════════════
def _common_ok(e: Dict, c: Dict, cand: Cand) -> bool:
    if cand.v0 < e.get("v0_min", 0.0) or cand.v0 > e.get("v0_max", 1e9):
        return False                                          # curve type filter (classic ~30 vs low-start ~0.5)
    if c["inflow"] < e.get("min_inflow", 0.5):
        return False
    if c["mult"] > e.get("max_mult", 6.0) or c["mult"] < e.get("min_mult", 0.0):
        return False
    mx = e.get("max_share")
    if mx is not None and c["share"] > mx:
        return False
    return True


def _scalp_ok(e: Dict, cand: Cand, c: Dict) -> bool:
    """Replica of the live Scalp gates, using the same thresholds as apex.py."""
    h = c["holder"]
    if h is None:
        return False
    if cand.dev_pct0 > apex.SCALP_MAX_DEV or h["top1"] > apex.SCALP_MAX_SINGLE or h["top10"] > apex.SCALP_TOP10:
        return False
    if cand.dev_pct0 >= 1.0 and h["dev_now"] <= 0.0:
        return False                                          # the dumper already left
    if h["cluster"] >= apex.SCALP_MAX_CLUSTER or h["buyers"] < apex.SCALP_MIN_BUYERS:
        return False
    r30, rp = c["rate30"], c["rate_prior30"]
    if r30 is None or r30 < e.get("min_rate", apex.SCALP_MOVE_SPEED):
        return False
    return rp is not None and r30 > rp                        # acceleration, as in Rule 4


def _momo_ok(e: Dict, cand: Cand, c: Dict) -> bool:
    key = "pos8" if e.get("w", 4) >= 8 else "pos4"
    p = c[key]
    r = c["rate20"] if key == "pos4" else c["rate30"]
    if p is None or r is None or p < e.get("k", 4) or r < e.get("min_rate", 0.8):
        return False
    s = c["slope10"]
    return s is not None and s <= e.get("max_slope10", 0.25)


def _early_ok(e: Dict, cand: Cand, c: Dict) -> bool:
    p, r, s = c["pos4"], c["rate20"], c["slope10"]
    if p is None or r is None or s is None:
        return False
    return p >= e.get("pos_k", 3) and r >= e.get("min_rate", 0.5) and s <= e.get("max_slope10", 0.10)


def find_decision(entry: Dict, cand: Cand, ctxs: Optional[List[Dict]] = None, key: str = "") -> Optional[int]:
    """First sample index at which the variant's entry rule fires, or None. Causal: each decision
    sees only the samples up to that point."""
    ctxs = build_ctx(cand) if ctxs is None else ctxs
    fam = entry["family"]
    target = 0.0
    if fam == "random":
        u = int(hashlib.sha1((cand.mint + "|" + key).encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
        target = entry["min_age"] + u * (entry["max_age"] - entry["min_age"])
    for c in ctxs:
        if c["age"] < entry["min_age"]:
            continue
        if c["age"] > entry["max_age"]:
            break
        if not _common_ok(entry, c, cand):
            continue
        if fam == "random":
            ok = c["age"] >= target
        elif fam == "all":
            ok = True
        elif fam == "scalp":
            ok = _scalp_ok(entry, cand, c)
        elif fam == "momo":
            ok = _momo_ok(entry, cand, c)
        elif fam == "early":
            ok = _early_ok(entry, cand, c)
        else:
            ok = False
        if ok:
            return c["i"]
    return None


def run_variant(params: Dict, cand: Cand, key: str = "") -> Optional[Dict]:
    """Decide + simulate at every latency. Returns the trade record (primary latency drives stats)."""
    ctxs = build_ctx(cand)
    d = find_decision(params["entry"], cand, ctxs, key)
    if d is None:
        return None
    stake = params.get("stake", STAKE_SOL)
    res = {L: simulate(cand, d, params["exit"], L, stake) for L in LATENCIES}
    prim = res[PRIMARY_LATENCY]
    if prim is None:
        return None
    ctx = ctxs[d]
    return {"decision_ts": cand.ts[d], "entry_age": ctx["age"], "ret": prim["ret"], "pnl": prim["pnl"],
            "reason": prim["reason"], "hold": prim["hold"], "peak": prim["peak"], "share": ctx["share"],
            "v0": cand.v0, "mult_entry": prim["mult_entry"],
            "ret_l2": res[2.0]["ret"] if res[2.0] else None,
            "ret_l10": res[10.0]["ret"] if res[10.0] else None,
            "ret_l20": res[20.0]["ret"] if res[20.0] else None}


# ══════════════════════════════════════════════════════════════════════════════
# STATISTICS + PROMOTION (pure)
# ══════════════════════════════════════════════════════════════════════════════
def compute_stats(rows: List[Dict], k_total: int, seed: int = 1) -> Dict:
    """rows: forward trades in time order, each with ret / ret_l2 / ret_l10 / ret_l20 / reason / v0."""
    rets = [r["ret"] for r in rows]
    n = len(rets)
    s: Dict = {"n": n, "k_total": k_total}
    if n == 0:
        return s
    mean = sum(rets) / n
    wins = [x for x in rets if x > 0]
    losses = [x for x in rets if x <= 0]
    s.update(mean=mean, median=sorted(rets)[n // 2], win=len(wins) / n,
             avg_win=(sum(wins) / len(wins)) if wins else 0.0,
             avg_loss=(sum(losses) / len(losses)) if losses else 0.0,
             pf=(sum(wins) / -sum(losses)) if losses and sum(losses) < 0 else None,
             total=sum(rets))
    rng = random.Random(seed)
    means = sorted(sum(rng.choices(rets, k=n)) / n for _ in range(BOOT))
    a = max(2, int(0.05 / max(k_total, 1) * BOOT))
    s.update(lo_adj=means[a], lo90=means[int(0.05 * BOOT)], lo95=means[int(0.025 * BOOT)],
             hi95=means[int(0.975 * BOOT)])
    cum = peak = dd = 0.0
    for x in rets:
        cum += x
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
    s["maxdd"] = dd
    rest = sorted(rets, reverse=True)[3:]
    s["top3_mean"] = (sum(rest) / len(rest)) if rest else None
    h = n // 2
    s["h1"] = (sum(rets[:h]) / h) if h else None
    s["h2"] = (sum(rets[h:]) / (n - h)) if n - h else None
    for L in ("l2", "l10", "l20"):
        v = [r[f"ret_{L}"] for r in rows if r.get(f"ret_{L}") is not None]
        s[L] = (sum(v) / len(v)) if v else None
    ex: Dict[str, List[float]] = {}
    for r in rows:
        ex.setdefault(r.get("reason") or "?", []).append(r["ret"])
    s["exits"] = {k: [len(v), sum(v) / len(v)] for k, v in ex.items()}
    buckets: Dict[str, List[float]] = {}
    for r in rows:
        buckets.setdefault("classic (v0>=10)" if (r.get("v0") or 0) >= 10 else "low-start (v0<10)", []).append(r["ret"])
    s["curve"] = {k: [len(v), sum(v) / len(v)] for k, v in buckets.items()}
    return s


def is_baseline(name: str) -> bool:
    return name.startswith("CTRL") or name == "ALL_T60"


def criteria(s: Dict) -> List[Tuple[str, bool]]:
    return [
        (f"at least {MIN_TRADES} forward trades", s.get("n", 0) >= MIN_TRADES),
        ("lower confidence bound > 0 (corrected for all variants tested)", (s.get("lo_adj") or -1) > 0),
        ("still > 0 after removing the 3 best trades", (s.get("top3_mean") if s.get("top3_mean") is not None else -1) > 0),
        ("both halves of the sample > 0", (s.get("h1") or -1) > 0 and (s.get("h2") or -1) > 0),
        (f"max drawdown <= {MAX_DD_STAKES:g} stakes", s.get("maxdd", 99) <= MAX_DD_STAKES),
        (f"mean > 0 even at {STRESS_LATENCY:g}s execution latency", (s.get("l10") or -1) > 0),
    ]


def judge(s: Dict, first_pass_n: Optional[int], keep_alive: bool = False) -> Tuple[str, Optional[int]]:
    """(status, first_pass_n). PASSED needs the whole checklist to hold, and to STILL hold after
    CONFIRM_EXTRA more trades (guards against stopping the moment a lucky streak looks good).
    keep_alive=True (controls and the all-coins baseline) never retires: they must keep collecting."""
    n = s.get("n", 0)
    if n >= RETIRE_MIN_N and s.get("hi95") is not None and s["hi95"] < 0:
        if not keep_alive:
            return "RETIRED", None
        return ("WATCH", None) if n >= MIN_TRADES else ("COLLECTING", None)
    if n < MIN_TRADES:
        return "COLLECTING", None
    if all(ok for _, ok in criteria(s)):
        if first_pass_n is None:
            return "CONFIRMING", n
        return ("PASSED" if n >= first_pass_n + CONFIRM_EXTRA else "CONFIRMING"), first_pass_n
    return "WATCH", None


# ══════════════════════════════════════════════════════════════════════════════
# VARIANT REGISTRY
# ══════════════════════════════════════════════════════════════════════════════
X_S = {"mode": "verdict", "stop": 0.90, "verdict_secs": 90, "verdict_ratio": 1.08,
       "tiers": [[1.5, 0.4], [2.5, 0.3]], "trail_keep": 0.65}


def _x(**kw) -> Dict:
    d = copy.deepcopy(X_S)
    d.update(kw)
    return d


E_RANDOM = {"family": "random", "min_age": 60, "max_age": 240, "min_inflow": 0.5, "max_mult": 6.0}
E_ALL = {"family": "all", "min_age": 60, "max_age": 120, "min_inflow": 0.5, "max_mult": 6.0}
E_SCALP = {"family": "scalp", "min_age": 60, "max_age": 150, "min_inflow": 0.5, "min_rate": 0.8, "max_mult": 8.0}
E_MOMO20 = {"family": "momo", "min_age": 60, "max_age": 180, "min_inflow": 0.5, "w": 4, "k": 4,
            "min_rate": 0.8, "max_mult": 6.0, "max_slope10": 0.25}
E_MOMO40 = {"family": "momo", "min_age": 60, "max_age": 180, "min_inflow": 0.5, "w": 8, "k": 7,
            "min_rate": 0.8, "max_mult": 6.0, "max_slope10": 0.25}
E_EARLY = {"family": "early", "min_age": 45, "max_age": 120, "min_inflow": 0.4, "min_mult": 1.05,
           "max_mult": 1.6, "pos_k": 3, "min_rate": 0.5, "max_slope10": 0.10}


def _ty(e: Dict, kind: str) -> Dict:
    """Restrict an entry rule to one curve type: classic (virtual SOL ~30) or low-start (~0.5)."""
    d = copy.deepcopy(e)
    if kind == "classic":
        d["v0_min"] = 10.0
    else:
        d["v0_max"] = 9.999
    return d


SEEDS: List[Tuple[str, Dict]] = [
    ("CTRL_RANDOM", {"entry": E_RANDOM, "exit": _x()}),
    ("CTRL_RANDOM_CLASSIC", {"entry": _ty(E_RANDOM, "classic"), "exit": _x()}),
    ("CTRL_RANDOM_LOW", {"entry": _ty(E_RANDOM, "low"), "exit": _x()}),
    ("ALL_T60", {"entry": E_ALL, "exit": _x()}),
    ("SCALP_REPLICA", {"entry": E_SCALP, "exit": _x()}),
    ("SCALP_CLASSIC", {"entry": _ty(E_SCALP, "classic"), "exit": _x()}),
    ("SCALP_LOW", {"entry": _ty(E_SCALP, "low"), "exit": _x()}),
    ("MOMO20", {"entry": E_MOMO20, "exit": _x()}),
    ("MOMO20_CLASSIC", {"entry": _ty(E_MOMO20, "classic"), "exit": _x()}),
    ("MOMO20_LOW", {"entry": _ty(E_MOMO20, "low"), "exit": _x()}),
    ("MOMO40", {"entry": E_MOMO40, "exit": _x()}),
    ("EARLY_FLAT", {"entry": E_EARLY, "exit": _x()}),
    ("EARLY_CLASSIC", {"entry": _ty(E_EARLY, "classic"), "exit": _x()}),
    ("EARLY_LOW", {"entry": _ty(E_EARLY, "low"), "exit": _x()}),
    ("MOMO20.STOP8", {"entry": E_MOMO20, "exit": _x(stop=0.92)}),
    ("MOMO20.STOP20", {"entry": E_MOMO20, "exit": _x(stop=0.80)}),
    ("MOMO20.FF45", {"entry": E_MOMO20, "exit": _x(verdict_secs=45, verdict_ratio=1.03)}),
    ("MOMO20.TRAIL25", {"entry": E_MOMO20, "exit": _x(trail_keep=0.75)}),
    ("MOMO20.RUNNER", {"entry": E_MOMO20, "exit": _x(tiers=[])}),
    ("MOMO20.TP50", {"entry": E_MOMO20, "exit": {"mode": "plain", "stop": 0.90, "tiers": [[1.5, 1.0]],
                                                  "time_stop_secs": 300, "time_stop_ratio": 1.0}}),
]


def params_hash(params: Dict) -> str:
    return hashlib.sha1(json.dumps(params, sort_keys=True).encode()).hexdigest()[:8]


def nudges(params: Dict) -> List[Tuple[str, Dict]]:
    """Single-parameter neighbours of a variant (for challenger generation)."""
    out: List[Tuple[str, Dict]] = []

    def put(tag: str, sec: str, key: str, val):
        p = copy.deepcopy(params)
        p[sec][key] = val
        out.append((tag, p))

    e, x = params["entry"], params["exit"]
    for key, muls, lo, hi in (("min_rate", (0.75, 1.33), 0.2, 4.0), ("max_mult", (0.8, 1.25), 1.2, 12.0),
                              ("max_slope10", (0.6, 1.5), 0.03, 0.6), ("min_inflow", (0.7, 1.4), 0.2, 3.0)):
        if key in e:
            for m in muls:
                v = round(min(hi, max(lo, e[key] * m)), 3)
                if v != e[key]:
                    put(f"{key}x{m:g}", "entry", key, v)
    for dk, step in (("min_age", 15), ("max_age", 30)):
        for sg in (-1, 1):
            v = e[dk] + sg * step
            if 40 <= v <= 240 and (dk == "min_age" and v < e["max_age"] or dk == "max_age" and v > e["min_age"]):
                put(f"{dk}{v}", "entry", dk, v)
    for key, step, lo, hi in (("stop", 0.03, 0.75, 0.95), ("trail_keep", 0.07, 0.5, 0.85),
                              ("verdict_ratio", 0.03, 1.0, 1.2)):
        if key in x:
            for sg in (-1, 1):
                v = round(min(hi, max(lo, x[key] + sg * step)), 3)
                if v != x[key]:
                    put(f"{key}{v:g}", "exit", key, v)
    if x.get("verdict_secs"):
        for v in (60, 120):
            if v != x["verdict_secs"]:
                put(f"verdict{v}s", "exit", "verdict_secs", v)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def _db():
    return aiosqlite.connect(apex.DB_PATH, timeout=10)


async def init_db():
    async with _db() as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS lab_candidates (
                mint TEXT PRIMARY KEY, launch_ts REAL, dev TEXT, dev_pct0 REAL, v0 REAL, supply REAL,
                n_samples INTEGER, max_inflow REAL, peak_mult REAL, end_mult REAL, end_reason TEXT,
                end_age REAL, path BLOB, holders TEXT, settled_ts REAL);
            CREATE INDEX IF NOT EXISTS ix_lab_cand_ts ON lab_candidates(launch_ts);
            CREATE TABLE IF NOT EXISTS lab_variants (
                rid INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE, name TEXT, family TEXT, params TEXT,
                hash TEXT, registered_ts REAL, retired_ts REAL, parent TEXT, origin TEXT,
                status TEXT DEFAULT 'COLLECTING', first_pass_n INTEGER);
            CREATE TABLE IF NOT EXISTS lab_trades (
                variant_id TEXT, mint TEXT, decision_ts REAL, entry_age REAL, fwd INTEGER, ret REAL, pnl REAL,
                exit_reason TEXT, hold_s REAL, peak_ratio REAL, share REAL, v0 REAL, mult_entry REAL,
                ret_l2 REAL, ret_l10 REAL, ret_l20 REAL, PRIMARY KEY (variant_id, mint));
            CREATE INDEX IF NOT EXISTS ix_lab_tr ON lab_trades(variant_id, fwd, decision_ts);
            CREATE TABLE IF NOT EXISTS lab_eval (variant_id TEXT PRIMARY KEY, ts REAL, stats TEXT, status TEXT);
            CREATE TABLE IF NOT EXISTS lab_meta (key TEXT PRIMARY KEY, val TEXT);
            CREATE TABLE IF NOT EXISTS lab_daily (
                day TEXT PRIMARY KEY, launches INTEGER DEFAULT 0, admitted INTEGER DEFAULT 0,
                dead INTEGER DEFAULT 0, settled INTEGER DEFAULT 0, calls INTEGER DEFAULT 0,
                dropped INTEGER DEFAULT 0);
        """)
        await db.commit()


async def meta_get(key: str, default: str = "") -> str:
    async with _db() as db:
        cur = await db.execute("SELECT val FROM lab_meta WHERE key=?", (key,))
        r = await cur.fetchone()
    return r[0] if r else default


async def meta_set(key: str, val: str):
    async with _db() as db:
        await db.execute("INSERT INTO lab_meta(key,val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                         (key, val))
        await db.commit()


async def register_variant(name: str, params: Dict, parent: str = "", origin: str = "seed") -> Tuple[str, bool]:
    """Register an immutable variant. The id embeds a hash of its parameters, so changing any
    parameter creates a NEW variant with fresh evidence. Returns (id, created)."""
    h = params_hash(params)
    vid = f"{name}.{h}"
    async with _db() as db:
        cur = await db.execute("SELECT 1 FROM lab_variants WHERE id=?", (vid,))
        if await cur.fetchone():
            return vid, False
        await db.execute(
            "INSERT INTO lab_variants(id,name,family,params,hash,registered_ts,parent,origin) VALUES (?,?,?,?,?,?,?,?)",
            (vid, name, params["entry"]["family"], json.dumps(params, sort_keys=True), h, time.time(), parent, origin))
        await db.commit()
    return vid, True


async def ensure_seeds() -> int:
    n = 0
    for name, params in SEEDS:
        _, created = await register_variant(name, params)
        n += created
    return n


async def get_variants(active_only: bool = True) -> List[Dict]:
    async with _db() as db:
        db.row_factory = aiosqlite.Row
        q = "SELECT * FROM lab_variants" + (" WHERE retired_ts IS NULL AND status!='RETIRED'" if active_only else "")
        cur = await db.execute(q + " ORDER BY rid")
        rows = [dict(r) for r in await cur.fetchall()]
    for r in rows:
        r["params"] = json.loads(r["params"])
    return rows


async def variants_ever() -> int:
    async with _db() as db:
        cur = await db.execute("SELECT COUNT(*) FROM lab_variants")
        return (await cur.fetchone())[0]


# ── persistence of candidates ────────────────────────────────────────────────
def pack_path(path: List[Sample], launch_ts: float) -> bytes:
    rows = [[round(t - launch_ts, 2), vt, vs, rt, rs] for t, vt, vs, rt, rs in path]
    return zlib.compress(json.dumps(rows, separators=(",", ":")).encode())


def unpack_path(blob: bytes, launch_ts: float) -> List[Sample]:
    return [(launch_ts + r[0], r[1], r[2], r[3], r[4]) for r in json.loads(zlib.decompress(blob))]


async def save_candidate(c: Cand):
    path = c.path
    vs0 = path[0][2] if path else 0.0
    max_in = max((s[2] - vs0 for s in path), default=0.0)
    pr = [s[2] / s[1] for s in path if s[1] > 0 and s[2] > 0]
    peak_mult = (max(pr) / pr[0]) if pr else 1.0
    end_mult = (pr[-1] / pr[0]) if pr else 1.0
    async with _db() as db:
        await db.execute(
            "INSERT OR REPLACE INTO lab_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (c.mint, c.launch_ts, c.dev, c.dev_pct0, c.v0, c.supply, len(path), max_in, peak_mult, end_mult,
             c.end_reason, (path[-1][0] - c.launch_ts) if path else 0.0, pack_path(path, c.launch_ts),
             json.dumps(c.holders), time.time()))
        await db.commit()


async def insert_trades(rows: List[Tuple]):
    if not rows:
        return
    async with _db() as db:
        await db.executemany(
            "INSERT OR IGNORE INTO lab_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        await db.commit()


def trade_row(vid: str, mint: str, fwd: int, t: Dict) -> Tuple:
    return (vid, mint, t["decision_ts"], t["entry_age"], fwd, t["ret"], t["pnl"], t["reason"], t["hold"], t["peak"],
            t["share"], t["v0"], t["mult_entry"], t["ret_l2"], t["ret_l10"], t["ret_l20"])


async def run_all_variants(c: Cand, variants: Optional[List[Dict]] = None) -> int:
    """Replay a settled coin through every active variant and store the trades."""
    variants = variants if variants is not None else await get_variants()
    rows = []
    for v in variants:
        t = run_variant(v["params"], c, v["id"])
        if t:
            rows.append(trade_row(v["id"], c.mint, int(t["decision_ts"] >= v["registered_ts"]), t))
    await insert_trades(rows)
    return len(rows)


async def backtest_variant(v: Dict, limit: int = BACKTEST_LIMIT) -> int:
    """Replay a NEW variant over stored history. These trades are flagged fwd=0: they are shown for
    context but never count toward promotion."""
    done = 0
    async with _db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM lab_candidates WHERE path IS NOT NULL ORDER BY launch_ts DESC LIMIT ?",
                               (limit,))
        rows = await cur.fetchall()
    batch: List[Tuple] = []
    for r in rows:
        c = Cand(r["mint"], r["launch_ts"], r["dev"], r["dev_pct0"])
        c.path = unpack_path(r["path"], r["launch_ts"])
        c.holders = json.loads(r["holders"] or "[]")
        c.v0, c.supply, c.end_reason = r["v0"], r["supply"], r["end_reason"] or ""
        c.finalize()
        t = run_variant(v["params"], c, v["id"])
        if t:
            batch.append(trade_row(v["id"], c.mint, int(t["decision_ts"] >= v["registered_ts"]), t))
            done += 1
        if len(batch) >= 200:
            await insert_trades(batch)
            batch = []
            await asyncio.sleep(0)
    await insert_trades(batch)
    return done


# ══════════════════════════════════════════════════════════════════════════════
# LIVE TRACKER
# ══════════════════════════════════════════════════════════════════════════════
class Track:
    __slots__ = ("cand", "curve", "next_due", "added", "snaps", "snapping", "max_inflow", "peak_price",
                 "misses", "settling")

    def __init__(self, cand: Cand, curve: str, now: float):
        self.cand, self.curve, self.added = cand, curve, now
        self.next_due = now
        self.snaps: set = set()
        self.snapping = False
        self.max_inflow = 0.0
        self.peak_price = 0.0
        self.misses = 0
        self.settling = False


_active: Dict[str, Track] = {}
_ctr = Counter()                    # in-memory daily counters, flushed to lab_daily
_calls = {"day": "", "n": 0}
_started = 0.0
_tasks: List[asyncio.Task] = []
_snap_sem = asyncio.Semaphore(4)
_settle_lock = asyncio.Lock()


def _today() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _count_calls(k: int = 1):
    d = _today()
    if _calls["day"] != d:
        _calls["day"], _calls["n"] = d, 0
    _calls["n"] += k
    _ctr["calls"] += k


def calls_today() -> int:
    return _calls["n"] if _calls["day"] == _today() else 0


def on_launch(pool) -> bool:
    """Called by apex for every new pump.fun launch (sync, never raises, never blocks)."""
    try:
        if not ENABLED or getattr(pool, "kind", "") != "pump" or pool.mint in _active:
            return False
        _ctr["launches"] += 1
        now = time.time()
        if now - pool.ts > MAX_LAUNCH_AGE_S:
            return False
        if len(_active) >= MAX_ACTIVE or calls_today() >= DAILY_CALLS:
            _ctr["dropped"] += 1
            return False
        curve = apex.curve_pda(pool.mint)
        if not curve:
            return False
        _active[pool.mint] = Track(Cand(pool.mint, pool.ts, pool.dev, pool.dev_pct0), curve, now)
        _ctr["admitted"] += 1
        return True
    except Exception as e:
        logger.debug(f"on_launch: {e}")
        return False


def _interval(age: float) -> float:
    return 5.0 if age < 180 else 10.0 if age < 420 else 20.0


def ingest(tr: Track, now: float, parsed) -> Optional[str]:
    """Feed one curve reading. Returns a settle reason, or None to keep tracking.
    `parsed` is parse_curve(...) output, 'missing', or None (unparseable)."""
    c = tr.cand
    age = now - c.launch_ts
    if parsed is None or parsed == "missing":
        tr.misses += 1
        tr.next_due = now + TICK_S
        return "unreadable" if tr.misses >= (6 if c.path else 4) else None
    vtok, vsol, rtok, rsol, supply, complete = parsed
    if complete or vtok <= 0 or vsol <= 0:
        return "graduated"
    tr.misses = 0
    c.add(now, vtok, vsol, rtok, rsol, supply)
    inflow = vsol - c.path[0][2]
    tr.max_inflow = max(tr.max_inflow, inflow)
    price = vsol / vtok
    tr.peak_price = max(tr.peak_price, price)
    tr.next_due = now + _interval(age)
    if age >= HORIZON_S:
        return "horizon"
    if age >= DEAD_AT_S and tr.max_inflow < DEAD_INFLOW and inflow < DEAD_INFLOW:
        return "dead"
    if age > 45 and tr.max_inflow >= DEAD_INFLOW and price < COLLAPSE_FRAC * tr.peak_price:
        return "collapsed"
    return None


async def _read_curves(addrs: List[str]):
    _count_calls(1)
    res = await apex._h("getMultipleAccounts", [addrs, {"encoding": "base64", "commitment": "confirmed"}])
    if res is None:
        return None
    out = []
    for v in (res.get("value") or []):
        if not v:
            out.append("missing")
            continue
        try:
            out.append(parse_curve(v["data"][0]))
        except Exception:
            out.append(None)
    return out


async def _snapshot(tr: Track):
    c = tr.cand
    try:
        async with _snap_sem:
            _count_calls(2)
            hv = await apex._holder_view(c.mint, c.supply or float(apex.PUMP_SUPPLY), tr.curve)
        if not hv:
            return
        holders, _, amounts = hv
        c.holders.append({
            "t": time.time(), "buyers": sum(1 for o, _ in holders if o != c.dev),
            "top1": holders[0][1] if holders else 0.0, "top10": sum(p for _, p in holders[:10]),
            "dev_now": next((p for o, p in holders if o == c.dev), 0.0),
            "cluster": apex.cluster_size(amounts), "nh": len(holders)})
        c._ctx = None
    except Exception as e:
        logger.debug(f"snapshot {c.mint[:8]}: {e}")
    finally:
        tr.snapping = False


def _maybe_snapshot(tr: Track, now: float):
    c = tr.cand
    age = now - c.launch_ts
    if tr.snapping or not c.path or calls_today() >= DAILY_CALLS:
        return
    inflow = c.path[-1][2] - c.path[0][2]
    for a in HOLDER_AGES:
        if age >= a and a not in tr.snaps and inflow >= HOLDER_MIN_INFLOW:
            tr.snaps.add(a)
            tr.snapping = True
            apex._spawn(_snapshot(tr))
            return


async def settle(tr: Track, why: str):
    """Persist a finished coin and replay it through every active variant."""
    if tr.settling:
        return
    tr.settling = True
    _active.pop(tr.cand.mint, None)
    c = tr.cand
    c.end_reason = why
    c.finalize()
    if why == "dead" or len(c.path) < 3:
        _ctr["dead"] += 1
        return
    try:
        async with _settle_lock:
            await save_candidate(c)
            await run_all_variants(c)
        _ctr["settled"] += 1
    except Exception as e:
        logger.error(f"lab settle {c.mint[:8]}: {e}", exc_info=True)


async def poll_once(now: Optional[float] = None) -> int:
    """One tracker pass: read every due coin in batches and settle whatever is finished."""
    now = time.time() if now is None else now
    due = [t for t in _active.values() if t.next_due <= now + TICK_S / 2 and not t.settling]
    n = 0
    if calls_today() >= DAILY_CALLS * 1.25:                       # hard stop: finish what we have
        for t in due:
            await settle(t, "budget")
        return 0
    if not await apex._budget_ok():
        return 0
    for i in range(0, len(due), BATCH):
        chunk = due[i:i + BATCH]
        vals = await _read_curves([t.curve for t in chunk])
        if vals is None or len(vals) != len(chunk):
            continue
        tnow = time.time()
        for tr, v in zip(chunk, vals):
            why = ingest(tr, tnow, v)
            n += 1
            if why:
                await settle(tr, why)
            else:
                _maybe_snapshot(tr, tnow)
    return n


async def _poll_loop():
    while True:
        try:
            await poll_once()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"lab poll: {e}", exc_info=True)
        await asyncio.sleep(TICK_S)


async def flush_counters():
    if not _ctr:
        return
    snap = {k: _ctr.pop(k) for k in list(_ctr)}
    async with _db() as db:
        await db.execute("INSERT OR IGNORE INTO lab_daily(day) VALUES (?)", (_today(),))
        for k in ("launches", "admitted", "dead", "settled", "calls", "dropped"):
            if snap.get(k):
                await db.execute(f"UPDATE lab_daily SET {k}={k}+? WHERE day=?", (snap[k], _today()))
        await db.commit()


# ══════════════════════════════════════════════════════════════════════════════
# EVALUATION, EVOLUTION, DIGEST
# ══════════════════════════════════════════════════════════════════════════════
async def fwd_rows(vid: str) -> List[Dict]:
    async with _db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT ret, ret_l2, ret_l10, ret_l20, exit_reason AS reason, v0, decision_ts FROM lab_trades "
            "WHERE variant_id=? AND fwd=1 ORDER BY decision_ts", (vid,))
        return [dict(r) for r in await cur.fetchall()]


async def evaluate_all() -> List[Tuple[str, str, str]]:
    """Recompute stats for every active variant. Returns [(variant id, old status, new status)] changes."""
    k = await variants_ever()
    changes = []
    for v in await get_variants(active_only=True):
        rows = await fwd_rows(v["id"])
        s = await asyncio.to_thread(compute_stats, rows, k)
        status, fpn = judge(s, v["first_pass_n"], keep_alive=is_baseline(v["name"]))
        s["status"] = status
        async with _db() as db:
            await db.execute("INSERT INTO lab_eval(variant_id,ts,stats,status) VALUES (?,?,?,?) "
                             "ON CONFLICT(variant_id) DO UPDATE SET ts=excluded.ts, stats=excluded.stats, status=excluded.status",
                             (v["id"], time.time(), json.dumps(s), status))
            await db.execute("UPDATE lab_variants SET status=?, first_pass_n=?, retired_ts=? WHERE id=?",
                             (status, fpn, time.time() if status == "RETIRED" else None, v["id"]))
            await db.commit()
        if status != v["status"]:
            changes.append((v["id"], v["status"], status))
    await meta_set("last_eval", str(time.time()))
    return changes


async def get_eval(vid: str) -> Optional[Dict]:
    async with _db() as db:
        cur = await db.execute("SELECT stats FROM lab_eval WHERE variant_id=?", (vid,))
        r = await cur.fetchone()
    return json.loads(r[0]) if r else None


async def get_variant(vid: str) -> Optional[Dict]:
    for v in await get_variants(active_only=False):
        if v["id"] == vid:
            return v
    return None


async def evolve() -> List[str]:
    """Spawn challengers: single-parameter neighbours of the best performers. They start with
    zero forward evidence, so luck in the parents does not carry over, and every one raises the
    multiple-testing bar for ALL variants. Controls are never mutated. Returns new variant ids."""
    existing = await get_variants(active_only=True)
    budget = min(MAX_SPAWN, MAX_VARIANTS - len(existing))
    if budget <= 0:
        return []
    scored = []
    for v in existing:
        if v["name"].startswith("CTRL") or v["family"] == "random":
            continue
        s = await get_eval(v["id"])
        if s and s.get("n", 0) >= EVOLVE_MIN_N and s.get("lo90") is not None:
            scored.append((s["lo90"], v))
    scored.sort(key=lambda x: -x[0])
    parents = [v for _, v in scored[:3]]
    pools = [nudges(v["params"]) for v in parents]
    for pl in pools:
        random.shuffle(pl)
    seen = {v["hash"] for v in await get_variants(active_only=False)}
    created: List[str] = []
    while budget > 0 and any(pools):
        for pi, parent in enumerate(parents):
            if budget <= 0:
                break
            while pools[pi]:
                tag, p = pools[pi].pop()
                h = params_hash(p)
                if h in seen:
                    continue
                seen.add(h)
                name = f"{parent['name'].split('~')[0]}~{tag}"
                vid, made = await register_variant(name, p, parent=parent["id"], origin="challenger")
                if made:
                    created.append(vid)
                    budget -= 1
                    nv = await get_variant(vid)
                    if nv:
                        apex._spawn(backtest_variant(nv))
                    break
    await meta_set("last_evolve", str(time.time()))
    return created


async def prune():
    cutoff = time.time() - PATH_KEEP_DAYS * 86400
    async with _db() as db:
        await db.execute("UPDATE lab_candidates SET path=NULL, holders=NULL WHERE launch_ts<? AND path IS NOT NULL", (cutoff,))
        await db.commit()


def _pct(x: Optional[float], d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


async def leaderboard(limit: int = 8) -> List[Dict]:
    rows = []
    for v in await get_variants(active_only=False):
        s = await get_eval(v["id"]) or {"n": 0}
        rows.append({"v": v, "s": s})
    rows.sort(key=lambda r: (r["v"]["status"] == "RETIRED", -(r["s"].get("lo90") if r["s"].get("lo90") is not None else -9)))
    return rows[:limit] if limit else rows


ICON = {"COLLECTING": "⏳", "CONFIRMING": "🔎", "PASSED": "✅", "WATCH": "👀", "RETIRED": "🗑"}


async def summary_text() -> str:
    top = await leaderboard(6)
    lines = []
    for r in top:
        v, s = r["v"], r["s"]
        lines.append(f"{ICON.get(v['status'], '·')} <code>{html.escape(v['name'][:22]):22s}</code> "
                     f"n={s.get('n', 0)}/{MIN_TRADES} mean {_pct(s.get('mean'))} lo {_pct(s.get('lo_adj'))}")
    passed = [r for r in top if r["v"]["status"] == "PASSED"]
    verdict = (f"🟢 {len(passed)} variant(s) cleared the paper checklist. That is still not proof. Run tiny live to compare real fills."
               if passed else "🔴 No variant has cleared the checklist. Nothing here justifies real money yet.")
    return "\n".join(lines) + "\n\n" + verdict


async def digest():
    admin = apex._access.get("admin")
    if not admin:
        return
    d = await _daily_totals()
    await apex._notify(admin, "🧪 <b>Lab digest</b>\n" + apex.RULE + "\n"
                       f"Tracking <b>{len(_active)}</b> coins · {d['settled']} settled · {d['dead']} dropped as dead\n"
                       f"Helius calls today <b>{calls_today():,}</b> / {DAILY_CALLS:,}\n\n" + await summary_text())


async def _daily_totals() -> Dict:
    async with _db() as db:
        cur = await db.execute("SELECT COALESCE(SUM(launches),0), COALESCE(SUM(admitted),0), COALESCE(SUM(dead),0), "
                               "COALESCE(SUM(settled),0), COALESCE(SUM(dropped),0) FROM lab_daily")
        a = await cur.fetchone()
        cur = await db.execute("SELECT COUNT(*) FROM lab_trades WHERE fwd=1")
        t = (await cur.fetchone())[0]
    return {"launches": a[0] + _ctr["launches"], "admitted": a[1] + _ctr["admitted"], "dead": a[2] + _ctr["dead"],
            "settled": a[3] + _ctr["settled"], "dropped": a[4] + _ctr["dropped"], "trades": t}


async def _supervise(name: str, fn, period: float, first: float = 0.0):
    await asyncio.sleep(first)
    while True:
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"lab {name}: {e}", exc_info=True)
        await asyncio.sleep(period)


async def _heartbeat():
    """One log line every 5 minutes so the tracker can be verified from the platform logs."""
    d = await _daily_totals()
    logger.info(f"Lab heartbeat: tracking={len(_active)} launches={d['launches']} admitted={d['admitted']} "
                f"settled={d['settled']} dead={d['dead']} dropped={d['dropped']} fwd_trades={d['trades']} "
                f"calls_today={calls_today()}")


async def _eval_job():
    changes = await evaluate_all()
    admin = apex._access.get("admin")
    for vid, old, new in changes:
        if new == "PASSED" and admin:
            await apex._notify(admin, f"✅ <b>Lab: {html.escape(vid)} cleared the full paper checklist</b>\n"
                                      "Paper is not proof. Next step: tiny live size, compare real fills to the paper "
                                      "model. Open 🧪 Lab for the numbers.")


async def _evolve_job():
    made = await evolve()
    if made:
        logger.info(f"lab evolve: {len(made)} challengers {made}")


async def startup(bot=None):
    global _started
    if not ENABLED:
        logger.info("Lab disabled (LAB_ENABLED=0)")
        return
    await init_db()
    created = await ensure_seeds()
    _started = time.time()
    if not await meta_get("started"):
        await meta_set("started", str(_started))
    _tasks.extend([
        asyncio.create_task(_poll_loop()),
        asyncio.create_task(_supervise("flush", flush_counters, 60, 60)),
        asyncio.create_task(_supervise("heartbeat", _heartbeat, 300, 120)),
        asyncio.create_task(_supervise("eval", _eval_job, EVAL_EVERY_S, 300)),
        asyncio.create_task(_supervise("evolve", _evolve_job, EVOLVE_EVERY_S, EVOLVE_EVERY_S)),
        asyncio.create_task(_supervise("digest", digest, DIGEST_EVERY_S, 3600)),
        asyncio.create_task(_supervise("prune", prune, 3600, 600)),
    ])
    logger.info(f"Lab online: {created} new seed variants, tracker started")


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM UI (owner only)
# ══════════════════════════════════════════════════════════════════════════════
router = Router()
RULE = apex.RULE


async def _owner(q: CallbackQuery) -> bool:
    if apex.can_export(q.from_user.id):
        return True
    await q.answer("Owner only", show_alert=True)
    return False


def _up(secs: float) -> str:
    secs = int(secs)
    return f"{secs // 3600}h{(secs % 3600) // 60:02d}m" if secs >= 3600 else f"{secs // 60}m"


async def home_page() -> Tuple[str, "apex.InlineKeyboardMarkup"]:
    d = await _daily_totals()
    run = (f"● RUNNING · up {_up(time.time() - _started)}" if _tasks and ENABLED else "■ OFF")
    text = (f"<b>🧪 CLEX LAB</b>\n<i>24/7 paper research. No wallet, no SOL.</i>\n{RULE}\n"
            f"<code>Status   </code>{run}\n"
            f"<code>Tracking </code>{len(_active)} coins now\n"
            f"<code>Seen     </code>{d['launches']:,} launches · {d['settled']:,} journaled · {d['dead']:,} dead\n"
            f"<code>Trades   </code>{d['trades']:,} forward paper trades\n"
            f"<code>Calls    </code>{calls_today():,} / {DAILY_CALLS:,} Helius today\n{RULE}\n"
            f"<b>Leaderboard</b> <i>(after fees, {PRIMARY_LATENCY:g}s latency; lo = corrected 95% lower bound)</i>\n"
            + await summary_text() + f"\n{RULE}\n"
            f"<i>Promotion needs {MIN_TRADES}+ forward trades and a 6-point checklist.</i>")
    kb = apex._kb([[("🏆 All variants", "lab:vars:0"), ("↻ Refresh", "lab:home")],
                   [("📤 Trades CSV", "lab:csv:trades"), ("📤 Coins CSV", "lab:csv:cands")],
                   [("ℹ️ How it works", "lab:how")], [("‹ Admin", "adm:home")]])
    return text, kb


async def vars_page(page: int) -> Tuple[str, "apex.InlineKeyboardMarkup"]:
    rows = await leaderboard(0)
    per = 8
    chunk = rows[page * per:(page + 1) * per]
    text = f"<b>🏆 Variants</b> · {len(rows)} registered\n{RULE}\n<i>Tap one for the full report.</i>"
    btns = [[(f"{ICON.get(r['v']['status'], '·')} {r['v']['name'][:24]}  n={r['s'].get('n', 0)}  {_pct(r['s'].get('mean'), 0)}",
              f"lab:v:{r['v']['rid']}")] for r in chunk]
    nav = []
    if page > 0:
        nav.append(("‹ Prev", f"lab:vars:{page - 1}"))
    if (page + 1) * per < len(rows):
        nav.append(("Next ›", f"lab:vars:{page + 1}"))
    return text, apex._kb(btns + ([nav] if nav else []) + [[("‹ Lab", "lab:home")]])


async def variant_page(rid: int) -> Tuple[str, "apex.InlineKeyboardMarkup"]:
    v = next((x for x in await get_variants(False) if x["rid"] == rid), None)
    if not v:
        return "Variant not found.", apex._kb([[("‹ Lab", "lab:home")]])
    s = await get_eval(v["id"]) or {"n": 0}
    lines = [f"<b>{ICON.get(v['status'], '')} {html.escape(v['name'])}</b>", f"<code>{html.escape(v['id'])}</code>",
             f"{v['origin']}" + (f" · parent {html.escape(v['parent'])}" if v["parent"] else "") +
             f" · registered {_up(time.time() - v['registered_ts'])} ago", RULE]
    if s.get("n"):
        lines += [f"Trades <b>{s['n']}</b>/{MIN_TRADES} · win <b>{s['win'] * 100:.0f}%</b> · avg win {_pct(s['avg_win'], 0)} / avg loss {_pct(s['avg_loss'], 0)}",
                  f"Mean/trade <b>{_pct(s['mean'])}</b> · median {_pct(s['median'])} · profit factor {s['pf']:.2f}" if s.get("pf") else
                  f"Mean/trade <b>{_pct(s['mean'])}</b> · median {_pct(s['median'])}",
                  f"95% interval {_pct(s['lo95'])} … {_pct(s['hi95'])} · corrected lower {_pct(s['lo_adj'])}",
                  f"Max drawdown {s['maxdd']:.1f} stakes · total {s['total']:+.1f} stakes",
                  RULE, "<b>Execution speed sensitivity</b> (mean per trade)",
                  f"2s {_pct(s.get('l2'))} · <b>5s {_pct(s['mean'])}</b> · 10s {_pct(s.get('l10'))} · 20s {_pct(s.get('l20'))}",
                  RULE, "<b>Exits</b>"]
        for k, (n_, m_) in sorted(s["exits"].items(), key=lambda kv: -kv[1][0])[:6]:
            lines.append(f"<code>{html.escape(k)[:16]:16s}</code> n={n_} {_pct(m_, 0)}")
        lines.append("<b>Curve type</b>")
        for k, (n_, m_) in s["curve"].items():
            lines.append(f"<code>{k:18s}</code> n={n_} {_pct(m_, 0)}")
        lines += [RULE, "<b>Promotion checklist</b>"] + [("✅ " if ok else "❌ ") + t for t, ok in criteria(s)]
    else:
        lines.append("No forward trades yet.")
    kb = apex._kb([[("↻", f"lab:v:{rid}"), ("‹ Variants", "lab:vars:0")], [("‹ Lab", "lab:home")]])
    return "\n".join(lines), kb


HOW = (f"<b>ℹ️ How the Lab works</b>\n{RULE}\n"
       "• Every new pump.fun launch is tracked on-chain for up to 15 minutes (raw curve reserves every ~5s, "
       "holder snapshots at ~1 and ~2 minutes).\n"
       "• Each variant is a frozen rule set (entry + exit). Finished coins are replayed through all of them using "
       "real curve math: price impact, the 1.25% fee, a network fee per leg, and execution latency.\n"
       "• Only trades made after a variant was registered count (forward testing). Backtests on history are "
       "shown for context only.\n"
       "• Challengers are spawned from the best variants every 6 hours. The more variants exist, the harder the "
       "bar gets (multiple-testing correction), so luck cannot pile up.\n"
       f"• <b>Promotion</b>: {MIN_TRADES}+ forward trades, corrected lower bound &gt; 0, still &gt; 0 without the 3 best "
       "trades, both halves positive, drawdown capped, positive even at 10s latency, and it must still hold "
       f"after {CONFIRM_EXTRA} more trades.\n"
       "• Random-entry controls show whether any signal beats noise.\n\n"
       "<b>Limits:</b> paper is not live. MEV, failed transactions and queueing are not modelled, and exits assume "
       "a fast native curve sell, not today's Jupiter path.")


@router.callback_query(F.data.startswith("lab:"))
async def cb_lab(q: CallbackQuery):
    if not await _owner(q):
        return
    parts = q.data.split(":")
    try:
        if parts[1] == "home":
            t, kb = await home_page()
        elif parts[1] == "vars":
            t, kb = await vars_page(int(parts[2]))
        elif parts[1] == "v":
            t, kb = await variant_page(int(parts[2]))
        elif parts[1] == "how":
            t, kb = HOW, apex._kb([[("‹ Lab", "lab:home")]])
        elif parts[1] == "csv":
            await q.answer()
            name, data = await build_csv(parts[2])
            await q.message.answer_document(BufferedInputFile(data, name), caption=f"Lab export {name}")
            return
        else:
            await q.answer()
            return
    except Exception as e:
        logger.error(f"lab ui {q.data}: {e}", exc_info=True)
        await q.answer("Lab page failed, see logs", show_alert=True)
        return
    await apex._send(q, t, kb)


async def build_csv(kind: str) -> Tuple[str, bytes]:
    buf = io.StringIO()
    w = csv.writer(buf)
    async with _db() as db:
        if kind == "trades":
            cur = await db.execute(
                "SELECT v.name, t.variant_id, t.mint, t.decision_ts, t.entry_age, t.fwd, t.ret, t.ret_l2, t.ret_l10, "
                "t.ret_l20, t.exit_reason, t.hold_s, t.peak_ratio, t.share, t.v0, t.mult_entry FROM lab_trades t "
                "JOIN lab_variants v ON v.id=t.variant_id ORDER BY t.decision_ts DESC LIMIT 200000")
            w.writerow(["variant", "variant_id", "mint", "decision_ts", "entry_age_s", "forward", "ret", "ret_l2",
                        "ret_l10", "ret_l20", "exit_reason", "hold_s", "peak_ratio", "curve_share", "curve_v0",
                        "mult_at_entry"])
        else:
            cur = await db.execute(
                "SELECT mint, launch_ts, dev_pct0, v0, supply, n_samples, max_inflow, peak_mult, end_mult, end_reason, "
                "end_age FROM lab_candidates ORDER BY launch_ts DESC LIMIT 200000")
            w.writerow(["mint", "launch_ts", "dev_pct0", "curve_v0", "supply", "samples", "max_inflow_sol",
                        "peak_mult", "end_mult", "end_reason", "tracked_s"])
        w.writerows(await cur.fetchall())
    return f"lab_{kind}_{time.strftime('%Y%m%d_%H%M', time.gmtime())}.csv", buf.getvalue().encode("utf-8")
