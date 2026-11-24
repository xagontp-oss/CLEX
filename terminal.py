"""
CLEX TERMINAL: the owner's read-only research console, served as a Telegram Mini App.

  GET /terminal             the single-page front end (static; holds no data)
  GET /terminal/api/...     JSON for the Lab, the market, positions and the system

Every data call must carry Telegram's signed `initData` in the X-Init-Data header. It is verified
with the bot token (HMAC-SHA256, Telegram's documented scheme), must be fresh, and must belong to
ADMIN_ID. Nothing here can trade, change a setting, or read a private key: the database is opened
read-only and no wallet column is ever selected.
"""
import hashlib
import hmac
import json
import math
import os
import statistics
import time
import urllib.parse
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import aiosqlite
from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import HTMLResponse

import apex
import lab
import risk

router = APIRouter()
INIT_MAX_AGE = 86400
STARTED = time.time()
_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "terminal.html")


# ── auth ─────────────────────────────────────────────────────────────────────
def verify_init_data(init_data: str, token: str, now: Optional[float] = None,
                     max_age: int = INIT_MAX_AGE) -> Optional[Dict]:
    """The Telegram user dict if `init_data` is correctly signed and fresh, else None."""
    try:
        if not init_data or not token:
            return None
        d = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True, strict_parsing=True))
        got = d.pop("hash", None)
        if not got:
            return None
        check = "\n".join(f"{k}={v}" for k, v in sorted(d.items()))
        secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, got):
            return None
        if (now if now is not None else time.time()) - int(d.get("auth_date", "0")) > max_age:
            return None
        return json.loads(d["user"])
    except Exception:
        return None


async def owner(x_init_data: str = Header(default="")) -> int:
    admin = apex._access.get("admin")
    user = verify_init_data(x_init_data, os.getenv("TELEGRAM_TOKEN", ""))
    if not user or not admin or int(user.get("id", 0)) != int(admin):
        raise HTTPException(status_code=401, detail="owner only")
    return int(admin)


def terminal_url() -> str:
    u = os.getenv("TERMINAL_URL", "")
    if u:
        return u
    d = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    return f"https://{d}/terminal" if d else ""


# ── helpers ──────────────────────────────────────────────────────────────────
def clean(o: Any) -> Any:
    """JSON cannot carry NaN/inf: turn them into null, recursively."""
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(v) for v in o]
    return o


_cache: Dict[str, Tuple[float, Any]] = {}


async def cached(key: str, ttl: float, fn: Callable[[], Awaitable[Any]]) -> Any:
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = clean(await fn())
    _cache[key] = (now, val)
    return val


def _ro():
    return aiosqlite.connect(f"file:{apex.DB_PATH}?mode=ro", uri=True, timeout=10)


def downsample(xs: List[float], n: int = 240) -> List[float]:
    if len(xs) <= n:
        return xs
    step = len(xs) / n
    return [xs[min(len(xs) - 1, int((i + 1) * step) - 1)] for i in range(n)]


def cumulative(rets: List[float]) -> List[float]:
    out, c = [], 0.0
    for r in rets:
        c += r
        out.append(c)
    return out


def rolling(rets: List[float], w: int = 50) -> List[float]:
    out, s = [], 0.0
    for i, r in enumerate(rets):
        s += r
        if i >= w:
            s -= rets[i - w]
        if i >= w - 1:
            out.append(s / w)
    return out


HIST_LO, HIST_STEP, HIST_BINS = -1.0, 0.125, 24        # -100% .. +200% in 12.5% bins, plus an overflow bin


def histogram(rets: List[float]) -> List[int]:
    h = [0] * (HIST_BINS + 1)
    for r in rets:
        i = int((r - HIST_LO) / HIST_STEP)
        h[max(0, min(HIST_BINS, i))] += 1
    return h


def _summ(pnls: List[float], entries: List[float]) -> Dict:
    n = len(pnls)
    if not n:
        return {"n": 0}
    rets = [p / e for p, e in zip(pnls, entries) if e > 0]
    w = [p for p in pnls if p > 0]
    l = [p for p in pnls if p <= 0]
    return {"n": n, "win": len(w) / n, "pnl": sum(pnls), "mean_ret": (sum(rets) / len(rets)) if rets else None,
            "avg_win": (sum(w) / len(w)) if w else 0.0, "avg_loss": (sum(l) / len(l)) if l else 0.0,
            "pf": (sum(w) / -sum(l)) if l and sum(l) < 0 else None}


# ── data builders ────────────────────────────────────────────────────────────
async def _last_curve(vid: str, n: int = 160) -> List[float]:
    async with lab._db() as db:
        cur = await db.execute("SELECT ret FROM lab_trades WHERE variant_id=? AND fwd=1 "
                               "ORDER BY decision_ts DESC LIMIT ?", (vid, n))
        rows = [r[0] for r in await cur.fetchall()][::-1]
    return downsample(cumulative(rows), 60)


async def build_overview() -> Dict:
    d = await lab._daily_totals()
    k = await lab.variants_ever()
    board = []
    for r in await lab.leaderboard(0):
        v, s = r["v"], r["s"]
        crit = lab.criteria(s) if s.get("n") else []
        board.append({
            "id": v["id"], "name": v["name"], "family": v["family"], "status": v["status"], "origin": v["origin"],
            "parent": v["parent"], "age_h": (time.time() - v["registered_ts"]) / 3600.0,
            "n": s.get("n", 0), "mean": s.get("mean"), "median": s.get("median"), "win": s.get("win"),
            "avg_win": s.get("avg_win"), "avg_loss": s.get("avg_loss"), "pf": s.get("pf"),
            "lo_adj": s.get("lo_adj"), "lo90": s.get("lo90"), "hi95": s.get("hi95"), "maxdd": s.get("maxdd"),
            "top3": s.get("top3_mean"), "h1": s.get("h1"), "h2": s.get("h2"),
            "l2": s.get("l2"), "l10": s.get("l10"), "l20": s.get("l20"),
            "ok": sum(1 for _, ok in crit if ok), "of": len(crit) or 6,
            "spark": await _last_curve(v["id"]) if s.get("n") else []})
    last_eval = float(await lab.meta_get("last_eval", "0") or 0)
    sol = 0.0
    try:
        sol = float(await apex.sol_usd() or 0.0)
    except Exception:
        pass
    today, month = await apex.credits_used()
    rej = sorted(apex._reject_reasons.items(), key=lambda kv: -kv[1])[:14]
    admin = apex._access.get("admin") or 0
    return {
        "now": time.time(), "uptime": time.time() - STARTED,
        "lab": {"enabled": lab.ENABLED, "tracking": len(lab._active), "max_active": lab.MAX_ACTIVE,
                "launches": d["launches"], "admitted": d["admitted"], "settled": d["settled"], "dead": d["dead"],
                "dropped": d["dropped"], "fwd_trades": d["trades"], "calls_today": lab.calls_today(),
                "daily_calls": lab.DAILY_CALLS, "variants_ever": k, "variants_active": sum(1 for b in board if b["status"] != "RETIRED"),
                "last_eval": last_eval, "min_trades": lab.MIN_TRADES,
                "passed": [b["name"] for b in board if b["status"] == "PASSED"],
                "confirming": [b["name"] for b in board if b["status"] == "CONFIRMING"]},
        "board": board,
        "apex": {"sol_usd": sol, "credits_today": today, "credits_month": month,
                 "credit_ceiling": apex.MONTHLY_CREDIT_CEILING, "credit_halt": apex.CREDIT_HALT_AT,
                 "events": apex._stats.get("events", 0), "idle_skips": apex._stats.get("idle_skips", 0),
                 "entries": apex._stats.get("entries", 0), "webhook_age": time.time() - apex._last_webhook,
                 "halted": bool(apex._halted.get("on")), "rejects": rej,
                 "flow_score": apex._flow["min_score"], "scalp_score": apex._scalp["min_score"]},
        "risk": (await risk.status(admin)) if admin else None,
        "flags": {"native_tx": apex.NATIVE_TX_ENABLED, "auto_tuner": apex.AUTO_TUNER_ENABLED, "lab": lab.ENABLED,
                  "commit": (os.getenv("RAILWAY_GIT_COMMIT_SHA", "") or "")[:7]},
    }


async def build_variant(vid: str) -> Dict:
    v = await lab.get_variant(vid)
    if not v:
        raise HTTPException(status_code=404, detail="no such variant")
    s = await lab.get_eval(vid) or {"n": 0}
    rows = (await lab.fwd_rows(vid))[-30000:]
    rets = [r["ret"] for r in rows]
    crit = [{"t": t, "ok": bool(ok)} for t, ok in lab.criteria(s)] if s.get("n") else []
    async with lab._db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT decision_ts, entry_age, ret, exit_reason, hold_s, peak_ratio, v0 FROM lab_trades "
                               "WHERE variant_id=? AND fwd=1 ORDER BY decision_ts DESC LIMIT 30", (vid,))
        tail = [dict(r) for r in await cur.fetchall()]
    ex = [{"reason": k, "n": a[0], "mean": a[1]} for k, a in (s.get("exits") or {}).items()]
    ex.sort(key=lambda e: -e["n"])
    return {
        "v": {"id": v["id"], "name": v["name"], "family": v["family"], "status": v["status"], "origin": v["origin"],
              "parent": v["parent"], "registered": v["registered_ts"], "params": v["params"]},
        "s": s, "checklist": crit, "min_trades": lab.MIN_TRADES,
        "equity": downsample(cumulative(rets)), "rolling": downsample(rolling(rets)),
        "hist": histogram(rets), "hist_lo": HIST_LO, "hist_step": HIST_STEP,
        "exits": ex, "tail": tail,
        "latency": [{"s": 2, "m": s.get("l2")}, {"s": 5, "m": s.get("mean")}, {"s": 10, "m": s.get("l10")},
                    {"s": 20, "m": s.get("l20")}],
        "curve": [{"k": k, "n": a[0], "m": a[1]} for k, a in (s.get("curve") or {}).items()],
        "quantiles": ({q: sorted(rets)[min(len(rets) - 1, int(q / 100 * len(rets)))] for q in (5, 25, 50, 75, 95)}
                      if rets else {}),
        "stdev": statistics.pstdev(rets) if len(rets) > 1 else None,
    }


async def build_market() -> Dict:
    now = time.time()
    since = now - 72 * 3600
    async with lab._db() as db:
        cur = await db.execute(
            "SELECT CAST(launch_ts/3600 AS INTEGER)*3600 AS h, COUNT(*), AVG(peak_mult), "
            "AVG(CASE WHEN peak_mult>=2 THEN 1.0 ELSE 0.0 END), AVG(CASE WHEN v0>=10 THEN 1.0 ELSE 0.0 END), "
            "AVG(max_inflow) FROM lab_candidates WHERE launch_ts>? GROUP BY h ORDER BY h", (since,))
        hourly = [{"h": r[0], "n": r[1], "peak": r[2], "p2": r[3], "classic": r[4], "inflow": r[5]}
                  for r in await cur.fetchall()]
        cur = await db.execute("SELECT peak_mult FROM lab_candidates WHERE launch_ts>?", (now - 86400,))
        peaks = [r[0] for r in await cur.fetchall() if r[0] is not None]
        cur = await db.execute("SELECT day, launches, admitted, dead, settled, calls, dropped FROM lab_daily "
                               "ORDER BY day DESC LIMIT 14")
        daily = [dict(zip(("day", "launches", "admitted", "dead", "settled", "calls", "dropped"), r))
                 for r in await cur.fetchall()][::-1]
        ctrl: Dict[str, Any] = {}
        for nm in ("CTRL_RANDOM", "ALL_T60"):
            cur = await db.execute("SELECT id FROM lab_variants WHERE name=?", (nm,))
            row = await cur.fetchone()
            if not row:
                continue
            cur = await db.execute(
                "SELECT CAST(decision_ts/3600 AS INTEGER)*3600 AS h, COUNT(*), AVG(ret) FROM lab_trades "
                "WHERE variant_id=? AND fwd=1 AND decision_ts>? GROUP BY h ORDER BY h", (row[0], since))
            by_h = [{"h": r[0], "n": r[1], "m": r[2]} for r in await cur.fetchall()]
            cur = await db.execute(
                "SELECT CAST((decision_ts % 86400)/3600 AS INTEGER) AS hod, COUNT(*), AVG(ret) FROM lab_trades "
                "WHERE variant_id=? AND fwd=1 GROUP BY hod ORDER BY hod", (row[0],))
            tod = [{"hod": r[0], "n": r[1], "m": r[2]} for r in await cur.fetchall()]
            ctrl[nm] = {"hourly": by_h, "tod": tod}
    edges = [(0, 1.1, "<1.1x"), (1.1, 1.5, "1.1-1.5x"), (1.5, 2, "1.5-2x"), (2, 5, "2-5x"), (5, 10, "5-10x"), (10, 1e9, "10x+")]
    dist = [{"k": lbl, "n": sum(1 for p in peaks if lo <= p < hi)} for lo, hi, lbl in edges]
    return {"now": now, "hourly": hourly, "peak_dist": dist, "peaks_n": len(peaks), "daily": daily, "ctrl": ctrl}


async def build_positions() -> Dict:
    async with _ro() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, user_id, mode, name, symbol, profile, entry_ts, entry_sol, tokens_left, entry_tokens, peak_ratio, "
            "last_ratio, curve_type, realized_sol FROM apex_positions WHERE status='open' ORDER BY entry_ts DESC LIMIT 60")
        open_ = [dict(r) for r in await cur.fetchall()]
        cur = await db.execute(
            "SELECT id, user_id, mode, name, symbol, profile, entry_ts, exit_ts, entry_sol, realized_sol, exit_reason, "
            "peak_ratio, curve_type FROM apex_positions WHERE status='closed' AND exit_ts IS NOT NULL "
            "ORDER BY exit_ts DESC LIMIT 400")
        closed = [dict(r) for r in await cur.fetchall()][::-1]
        cur = await db.execute("SELECT side, method, slippage_bps FROM apex_slippage_log ORDER BY id DESC LIMIT 600")
        slip_rows = [tuple(r) for r in await cur.fetchall()]
    for p in open_ + closed:
        p["user"] = str(p.pop("user_id"))[-4:]
    out: Dict[str, Any] = {"open": open_, "now": time.time()}
    out["closed"] = [{**c, "pnl": c["realized_sol"] - c["entry_sol"]} for c in closed][-120:]
    for mode in ("live", "paper"):
        cs = [c for c in closed if c["mode"] == mode]
        pn = [c["realized_sol"] - c["entry_sol"] for c in cs]
        out[mode] = {**_summ(pn, [c["entry_sol"] for c in cs]), "equity": downsample(cumulative(pn), 160),
                     "exits": sorted(({"reason": r, "n": sum(1 for c in cs if c["exit_reason"] == r),
                                       "mean": statistics.mean([(c["realized_sol"] - c["entry_sol"]) / c["entry_sol"]
                                                                for c in cs if c["exit_reason"] == r and c["entry_sol"] > 0] or [0])}
                                      for r in {c["exit_reason"] or "?" for c in cs}), key=lambda e: -e["n"])}
    slip: Dict[str, List[float]] = {}
    for side, method, bps in slip_rows:
        slip.setdefault(f"{side}/{method}", []).append(float(bps))
    out["slippage"] = [{"k": k, "n": len(v), "mean": statistics.mean(v), "median": statistics.median(v),
                        "p10": sorted(v)[int(0.1 * (len(v) - 1))], "p90": sorted(v)[int(0.9 * (len(v) - 1))]}
                       for k, v in sorted(slip.items())]
    admin = apex._access.get("admin") or 0
    out["risk"] = await risk.status(admin) if admin else None
    return out


CONFIG_KEYS = [
    ("ENTRY", ["MIN_POOL_USD", "MAX_MCAP_USD", "PUMP_CHECK_AGES", "MAX_PRICE_IMPACT_PCT", "MAX_PRICE_IMPACT_PCT_SCALP"]),
    ("PRECISION", ["MIN_UNIQUE_BUYERS", "MAX_TOP10_PCT", "MAX_SINGLE_PCT", "MAX_DEV_PCT"]),
    ("FLOW", ["FLOW_MAX_DEV", "FLOW_MIN_BUYERS", "FLOW_TOP10", "FLOW_MAX_CLUSTER", "FLOW_MIN_SOL", "FLOW_SCORE_START"]),
    ("SCALP", ["SCALP_MAX_DEV", "SCALP_MAX_SINGLE", "SCALP_TOP10", "SCALP_MAX_CLUSTER", "SCALP_MIN_BUYERS",
               "SCALP_MIN_SOL", "SCALP_SCORE_START", "SCALP_ENTRY_SLIPPAGE_BPS", "SCALP_FAST_INTERVAL"]),
    ("EXIT", ["HARD_STOP_RATIO", "TRAIL_KEEP", "TIME_STOP_SECS", "TIME_STOP_RATIO", "EXIT_SLIPPAGE_BPS", "FLOW_POLL_SECS"]),
    ("EXEC", ["ENTRY_SLIPPAGE_BPS", "PRIORITY_MAX_LAMPORTS", "GAS_RESERVE_SOL", "MAX_OPEN_POSITIONS", "NATIVE_TX_ENABLED"]),
    ("PAPER", ["PAPER_EXIT_SLIP_DOWN", "PAPER_EXIT_SLIP_UP", "PAPER_FEE_SOL", "PAPER_START_SOL"]),
]
RISK_KEYS = ["RISK_MAX_BANKROLL_PCT", "MIN_STAKE_SOL", "MAX_STAKE_SOL", "RISK_ROLLING_WINDOW", "RISK_ROLLING_FLOOR",
             "RISK_DAILY_CAP_SOL"]
LAB_KEYS = ["STAKE_SOL", "LEG_FEE_SOL", "PUMP_FEE", "LATENCIES", "MIN_TRADES", "CONFIRM_EXTRA", "MAX_DD_STAKES",
            "RETIRE_MIN_N", "MAX_VARIANTS", "MAX_SPAWN", "HORIZON_S", "DEAD_AT_S", "DEAD_INFLOW", "COLLAPSE_FRAC",
            "EVOLVE_EVERY_S", "EVAL_EVERY_S", "PATH_KEEP_DAYS"]


def _val(mod, k):
    v = getattr(mod, k, None)
    return list(v) if isinstance(v, tuple) else v


async def build_system() -> Dict:
    prof = {k: {"name": p["name"], "tiers": [list(t) for t in p["tiers"]], "stop": p.get("stop", apex.HARD_STOP_RATIO),
                "trail": p.get("trail_keep", apex.TRAIL_KEEP) if p["trail"] else None}
            for k, p in apex.PROFILES.items()}
    cfg = [{"group": g, "items": [{"k": k, "v": _val(apex, k)} for k in ks]} for g, ks in CONFIG_KEYS]
    cfg.append({"group": "RISK", "items": [{"k": k, "v": _val(risk, k)} for k in RISK_KEYS]})
    cfg.append({"group": "LAB", "items": [{"k": k, "v": _val(lab, k)} for k in LAB_KEYS]})
    async with _ro() as db:
        cur = await db.execute("SELECT COUNT(*) FROM apex_blacklist")
        bl = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM lab_candidates")
        cands = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM lab_trades")
        trades = (await cur.fetchone())[0]
    try:
        size = os.path.getsize(apex.DB_PATH)
    except OSError:
        size = 0
    return {"config": cfg, "profiles": prof, "blacklist": bl, "lab_candidates": cands, "lab_trades": trades,
            "db_bytes": size, "uptime": time.time() - STARTED, "now": time.time(),
            "env": {"native_tx": apex.NATIVE_TX_ENABLED, "auto_tuner": apex.AUTO_TUNER_ENABLED, "lab": lab.ENABLED,
                    "commit": (os.getenv("RAILWAY_GIT_COMMIT_SHA", "") or "")[:7]}}


# ── routes ───────────────────────────────────────────────────────────────────
@router.get("/terminal", response_class=HTMLResponse)
async def page():
    with open(_HTML_PATH, encoding="utf-8") as f:
        html = f.read()
    return HTMLResponse(html, headers={
        "Cache-Control": "no-store",
        "Content-Security-Policy": ("default-src 'none'; script-src 'self' 'unsafe-inline' https://telegram.org; "
                                    "style-src 'unsafe-inline'; connect-src 'self'; img-src data:; base-uri 'none'; "
                                    "form-action 'none'"),
        "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer"})


@router.get("/terminal/api/overview")
async def api_overview(_: int = Depends(owner)):
    return await cached("overview", 10, build_overview)


@router.get("/terminal/api/variant")
async def api_variant(id: str, _: int = Depends(owner)):
    return await cached("var:" + id, 20, lambda: build_variant(id))


@router.get("/terminal/api/market")
async def api_market(_: int = Depends(owner)):
    return await cached("market", 60, build_market)


@router.get("/terminal/api/positions")
async def api_positions(_: int = Depends(owner)):
    return await cached("positions", 10, build_positions)


@router.get("/terminal/api/system")
async def api_system(_: int = Depends(owner)):
    return await cached("system", 30, build_system)


async def install_menu(bot) -> None:
    """Give the owner a persistent TERMINAL button next to the message box."""
    url, admin = terminal_url(), apex._access.get("admin")
    if not url or not admin:
        return
    from aiogram.types import MenuButtonWebApp, WebAppInfo
    await bot.set_chat_menu_button(chat_id=admin, menu_button=MenuButtonWebApp(text="TERMINAL", web_app=WebAppInfo(url=url)))
