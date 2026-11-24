import os, sys, asyncio, time, tempfile, random, base64, struct, json, copy, math
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "lab.db")
os.environ["MASTER_ENCRYPTION_KEY"] = "x" * 43 + "="
sys.path.insert(0, sys.argv[1])
import apex
import lab
from solders.keypair import Keypair as KP

random.seed(12345)
NOW = time.time()


# ── helpers ─────────────────────────────────────────────────────────────────
def mk(flows, mint="M", v0=30.0, launch=None, dt=5.0, dev_pct0=2.0, holders=None, end="horizon", first=3.0):
    """Synthetic constant-product coin. flows[i] = net SOL added at sample i (negative = net selling)."""
    launch = NOW + 5000 if launch is None else launch
    c = lab.Cand(mint, launch, "DEV", dev_pct0)
    vtok0 = 1.073e9
    k = vtok0 * v0
    vsol, t = v0, launch + first
    for dx in flows:
        vsol = max(vsol + dx, 1e-6)
        vtok = k / vsol
        c.add(t, vtok, vsol, 793.1e6 - (vtok0 - vtok), vsol - v0, 1e9)
        t += dt
    c.holders = holders or []
    c.end_reason = end
    c.finalize()
    return c


def snap(t, buyers=6, top1=5.0, top10=30.0, dev_now=2.0, cluster=1):
    return {"t": t, "buyers": buyers, "top1": top1, "top10": top10, "dev_now": dev_now, "cluster": cluster, "nh": 12}


def curve_b64(vtok, vsol, rtok, rsol, supply=1e9, complete=False):
    raw = bytearray(lab.CURVE_DISC)
    raw += struct.pack("<5Q", int(vtok * 1e6), int(vsol * 1e9), int(rtok * 1e6), int(rsol * 1e9), int(supply * 1e6))
    raw += bytes([1 if complete else 0]) + bytes(166 - 49)
    return base64.b64encode(bytes(raw)).decode()


async def main():
    await apex.init_db(); await apex.sn.init_sniper_db(); await lab.init_db()

    # ── 1. account parsing ──────────────────────────────────────────────────
    p = lab.parse_curve(curve_b64(1_066_806_059, 0.548, 786_906_059, 0.009))
    assert p and abs(p[0] - 1_066_806_059) < 1 and abs(p[1] - 0.548) < 1e-9 and not p[5], p
    assert lab.parse_curve(curve_b64(0, 0, 0, 0, complete=True))[5] is True
    assert lab.parse_curve(base64.b64encode(b"\x00" * 166).decode()) is None, "wrong discriminator must not parse"
    assert lab.parse_curve("not base64!!") is None and lab.parse_curve(base64.b64encode(b"short").decode()) is None
    print("curve account parsing OK (layout, completion flag, bad discriminator/short/garbage)")

    # ── 2. curve math ───────────────────────────────────────────────────────
    vt, vs = 1.0e9, 30.0
    tok = lab.buy_tokens(vt, vs, 1.0)
    back = lab.sell_sol(vt - tok, vs + 1.0 * (1 - lab.PUMP_FEE), tok)
    assert abs(back - 1.0 * (1 - lab.PUMP_FEE) ** 2) < 1e-9, back          # instant round trip = fees only
    assert lab.buy_tokens(vt, vs, 2.0) > lab.buy_tokens(vt, vs, 1.0) > 0
    assert lab.buy_tokens(vt, vs, 2.0) < 2 * lab.buy_tokens(vt, vs, 1.0), "bigger buys must get worse prices"
    assert lab.buy_tokens(0, vs, 1) == 0 and lab.sell_sol(vt, 0, 5) == 0 and lab.sell_sol(vt, vs, 0) == 0
    print("curve math OK (round trip costs exactly the fees; impact monotonic; guards)")

    # ── 3. exit-rule PARITY with the live Scalp profile (fuzz) ──────────────
    rng = random.Random(7)
    X = lab.X_S
    bad = 0
    for _ in range(20000):
        ratio = rng.choice([rng.uniform(0.4, 4.5), rng.choice([0.9, 1.08, 1.5, 2.5, 0.65 * 2.0])])
        peak = max(ratio, rng.uniform(0.9, 5.0)) if rng.random() < 0.9 else ratio
        hit = rng.choice([0, 1, 2, 3, 64, 65, 66, 67])
        age = rng.choice([rng.uniform(0, 300), 89.99, 90.0, 90.01])
        a = apex.decide_exit("S", ratio, hit, peak, 1000.0, 1000.0 + age)
        b = lab.exit_decide(X, ratio, hit, peak, age)
        norm = lambda acts: [(k_, f_, (r_ if not r_.startswith("target") and not r_.startswith("verdict") else r_.split("_")[0]), t_) for k_, f_, r_, t_ in acts]
        if norm(a["actions"]) != norm(b["actions"]) or a["flags"] != b["flags"] or abs(a["peak"] - b["peak"]) > 1e-12:
            bad += 1
            if bad < 4:
                print("MISMATCH", ratio, peak, hit, age, a, b)
    assert bad == 0, f"{bad} parity mismatches"
    print("exit parity OK (20,000 fuzzed cases identical to apex.decide_exit('S'))")

    # ── 4. simulator scenarios ──────────────────────────────────────────────
    flat = mk([0.0] * 60)
    r = lab.simulate(flat, 2, lab.X_S, 5.0)
    assert r and r["reason"] == "verdict" and -0.10 < r["ret"] < -0.03, r      # fees only: ~ -2.5% - 4% leg fees
    assert 85 <= r["hold"] <= 100, r["hold"]

    up = mk([0.0] * 6 + [1.0] * 40 + [-3.0] * 20)
    r = lab.simulate(up, 2, lab.X_S, 5.0)
    assert r["ret"] > 0.4 and r["sells"] >= 3 and r["reason"] == "trailing_stop", r
    assert r["peak"] > 2.5, r

    rug = mk([0.0] * 6 + [0.5] * 10 + [-8.0] + [-2.0] * 12)
    r2, r20 = lab.simulate(rug, 2, lab.X_S, 2.0), lab.simulate(rug, 2, lab.X_S, 20.0)
    assert r2["reason"] == "stop_loss" and r20["reason"] == "stop_loss", (r2, r20)
    assert r20["ret"] < r2["ret"] - 0.05, "slower execution must fill a falling stop worse"
    assert lab.simulate(flat, len(flat.path) - 1, lab.X_S, 5.0) is None, "no sample left to fill on"
    # plain mode: take-profit 50% then time-stop
    tp = mk([0.0] * 6 + [1.0] * 14 + [0.0] * 30)
    r = lab.simulate(tp, 2, {"mode": "plain", "stop": 0.9, "tiers": [[1.5, 1.0]], "time_stop_secs": 300, "time_stop_ratio": 1.0}, 5.0)
    assert r["reason"] == "target" and r["ret"] > 0.3, r
    stale = mk([0.0] * 120)
    r = lab.simulate(stale, 2, {"mode": "plain", "stop": 0.9, "tiers": [[1.5, 1.0]], "time_stop_secs": 120, "time_stop_ratio": 1.0}, 5.0)
    assert r["reason"] == "time_stop", r
    runner = lab.simulate(up, 2, lab.exit_decide.__globals__["_x"](tiers=[]), 5.0)
    assert runner["sells"] == 1 and runner["reason"] == "trailing_stop", runner     # no partials: single trailing exit
    horizon = mk([0.0] * 6 + [0.3] * 5, end="horizon")                              # path ends while still open
    r = lab.simulate(horizon, 1, {"mode": "plain", "stop": 0.5, "tiers": []}, 2.0)
    assert r["reason"] == "horizon", r
    print("simulator OK (verdict, partials+trail, stop vs latency, plain TP/time-stop, runner, horizon close, no-fill)")

    # ── 5. context series + entry rules ─────────────────────────────────────
    steady = mk([0.0] + [0.4] * 40, holders=[snap(NOW + 5000 + 62)])
    ctx = lab.build_ctx(steady)
    c20 = next(c for c in ctx if c["age"] > 70)
    assert abs(c20["rate30"] - 0.4 / 5 * 60) < 0.05, c20["rate30"]                  # 0.08 SOL/s = 4.8 SOL/min... see below
    assert c20["pos4"] == 4 and c20["inflow"] > 0.5 and c20["mult"] > 1.0
    assert lab.build_ctx(steady) is lab.build_ctx(steady), "ctx cached"
    d = lab.find_decision(lab.E_MOMO20, steady)
    assert d is not None and ctx[d]["age"] >= 60
    # a holder snapshot taken AFTER the decision must be invisible to it
    assert ctx[d]["holder"] is None or ctx[d]["holder"]["t"] <= ctx[d]["t"]
    early_ctx = [c for c in ctx if c["t"] < NOW + 5000 + 62]
    assert all(c["holder"] is None for c in early_ctx), "future holder snapshot leaked backwards"
    d_s = lab.find_decision(lab.E_SCALP, steady)
    print(f"context OK (rates, positive windows, no-lookahead holders); momo fires at index {d}, scalp at {d_s}")

    # curve type filters
    low = mk([0.0] + [0.05] * 40, v0=0.55)
    assert low.v0 == 0.55 and steady.v0 == 30.0
    ent_c, ent_l = lab._ty(lab.E_ALL, "classic"), lab._ty(lab.E_ALL, "low")
    big = mk([0.0] + [0.05] * 40, v0=0.55)
    assert lab.find_decision(ent_c, big) is None and lab.find_decision(ent_l, big) is not None
    assert lab.find_decision(ent_c, steady) is not None and lab.find_decision(ent_l, steady) is None
    print("curve-type filters OK (classic vs low-start)")

    # ── 6. CAUSALITY property test: the future can never change a decision ──
    n_checked = 0
    for trial in range(400):
        v0 = rng.choice([30.0, 0.55, 5.0])
        scale = v0 / 30.0 + 0.2
        flows = [0.0] + [rng.gauss(0.35 * scale, 0.5 * scale) for _ in range(rng.randint(30, 90))]
        hs = [snap(NOW + 5000 + rng.uniform(55, 120), buyers=rng.randint(1, 9), dev_now=rng.choice([0.0, 2.0]))]
        full = mk(flows, v0=v0, holders=hs, mint=f"C{trial}")
        for name, entry in (("momo", lab.E_MOMO20), ("momo40", lab.E_MOMO40), ("early", lab.E_EARLY),
                            ("scalp", lab.E_SCALP), ("random", lab.E_RANDOM), ("all", lab.E_ALL)):
            e = copy.deepcopy(entry); e["min_inflow"] = 0.1
            d = lab.find_decision(e, full, key="k")
            if d is None:
                continue
            n_checked += 1
            cut = lab.Cand(full.mint, full.launch_ts, "DEV", full.dev_pct0)
            cut.path = list(full.path[:d + 1]); cut.holders = list(hs); cut.v0 = full.v0; cut.finalize()
            assert lab.find_decision(e, cut, key="k") == d, (name, trial, d)
            alt = lab.Cand(full.mint, full.launch_ts, "DEV", full.dev_pct0)
            alt.path = list(full.path[:d + 1]) + [(s[0], s[1] * 3, s[2] * 0.2 + 0.01, s[3], s[4]) for s in full.path[d + 1:]]
            alt.holders = list(hs); alt.v0 = full.v0; alt.finalize()
            assert lab.find_decision(e, alt, key="k") == d, ("future-mod", name, trial, d)
    assert n_checked > 300, n_checked
    print(f"causality OK ({n_checked} decisions: identical when the future is truncated or rewritten)")

    # ── 7. statistics + promotion logic ─────────────────────────────────────
    def rows_of(rets, l10=None):
        return [{"ret": x, "ret_l2": x, "ret_l10": (x if l10 is None else l10), "ret_l20": x, "reason": "r", "v0": 30.0} for x in rets]

    good = lab.compute_stats(rows_of([0.05 + rng.gauss(0, 0.3) for _ in range(800)]), 20)
    assert good["n"] == 800 and good["lo95"] < good["mean"] < good["hi95"] and good["lo_adj"] <= good["lo95"]
    zero = lab.compute_stats(rows_of([rng.gauss(0, 0.5) for _ in range(800)]), 20)
    assert zero["lo_adj"] < 0 and not all(ok for _, ok in lab.criteria(zero))
    st, fpn = lab.judge(zero, None); assert st == "WATCH" and fpn is None
    sure = lab.compute_stats(rows_of([0.1 + 0.02 * ((i % 7) - 3) / 3 for i in range(700)]), 20)
    assert all(ok for _, ok in lab.criteria(sure)), lab.criteria(sure)
    st1, f1 = lab.judge(sure, None); assert st1 == "CONFIRMING" and f1 == 700
    st2, f2 = lab.judge({**sure, "n": 700 + 100}, f1); assert st2 == "CONFIRMING"
    st3, f3 = lab.judge({**sure, "n": 700 + lab.CONFIRM_EXTRA}, f1); assert st3 == "PASSED"
    assert lab.judge({**sure, "n": 100}, None)[0] == "COLLECTING"
    losing = lab.compute_stats(rows_of([-0.12 + rng.gauss(0, 0.2) for _ in range(400)]), 20)
    assert lab.judge(losing, None)[0] == "RETIRED"
    assert lab.judge(losing, None, keep_alive=True)[0] == "COLLECTING", "controls/baseline are never retired"
    assert lab.is_baseline("CTRL_RANDOM_LOW") and lab.is_baseline("ALL_T60") and not lab.is_baseline("MOMO20")
    # two lottery tickets carrying a negative book must NOT pass
    lottery = [-0.06] * 650 + [25.0, 25.0, 0.0] * 1
    ls = lab.compute_stats(rows_of(lottery), 20)
    assert ls["mean"] > 0 and (ls["top3_mean"] or 0) <= 0 and not all(ok for _, ok in lab.criteria(ls))
    # drawdown cap
    dd = lab.compute_stats(rows_of([0.2] * 300 + [-1.0] * 40 + [0.2] * 400), 20)
    assert dd["maxdd"] >= 40 and not dict(lab.criteria(dd))[f"max drawdown <= {lab.MAX_DD_STAKES:g} stakes"]
    # multiple-testing: same data, more variants => a lower (more conservative) bound
    a, b = lab.compute_stats(rows_of([0.05 + rng.gauss(0, 0.3) for _ in range(800)]), 1), None
    b = lab.compute_stats(rows_of([x["ret"] for x in rows_of([0.05 + rng.gauss(0, 0.3) for _ in range(800)])]), 60)
    s1 = lab.compute_stats(rows_of([0.05 + (i % 11 - 5) * 0.05 for i in range(800)]), 1)
    s60 = lab.compute_stats(rows_of([0.05 + (i % 11 - 5) * 0.05 for i in range(800)]), 60)
    assert s60["lo_adj"] < s1["lo_adj"], (s60["lo_adj"], s1["lo_adj"])
    assert lab.compute_stats([], 5) == {"n": 0, "k_total": 5}
    print("statistics OK (CI ordering, multiple-testing penalty, confirm-after-more-trades, retire, lottery & drawdown guards)")

    # ── 8. registry: immutable, idempotent, hashed ──────────────────────────
    n1 = await lab.ensure_seeds(); n2 = await lab.ensure_seeds()
    assert n1 == len(lab.SEEDS) == 20 and n2 == 0
    vs = await lab.get_variants(False)
    assert len({v["id"] for v in vs}) == 20 and all(v["id"].endswith(v["hash"]) for v in vs)
    p0 = copy.deepcopy(lab.SEEDS[7][1]); p0["entry"]["min_rate"] = 1.0
    vid, made = await lab.register_variant("MOMO20", p0)
    assert made and vid != next(v["id"] for v in vs if v["name"] == "MOMO20"), "a changed parameter must be a NEW variant"
    print("registry OK (20 seeds, idempotent, parameter change = new variant)")

    # ── 9. settle -> trades (forward flag) + backtest flag ──────────────────
    t0 = NOW + 6000
    pump = mk([0.0] + [0.45] * 20 + [0.0] * 6 + [-1.5] * 12, mint="PUMPMINT", launch=t0,
              holders=[snap(t0 + 62), snap(t0 + 122)])
    n = await lab.run_all_variants(pump)
    assert n >= 5, n
    async with lab._db() as db:
        cur = await db.execute("SELECT COUNT(*), SUM(fwd) FROM lab_trades"); tot, fw = await cur.fetchone()
        cur = await db.execute("SELECT variant_id, mint, ret, ret_l2, ret_l10, ret_l20 FROM lab_trades LIMIT 1"); row = await cur.fetchone()
    assert tot == fw == n and None not in row, (tot, fw, row)
    again = await lab.run_all_variants(pump)               # idempotent: PRIMARY KEY (variant, mint)
    async with lab._db() as db:
        cur = await db.execute("SELECT COUNT(*) FROM lab_trades"); assert (await cur.fetchone())[0] == n
    # an OLD coin replayed through a NEW variant is a backtest row, not forward evidence
    old = mk([0.0] + [0.45] * 20 + [0.0] * 6, mint="OLDMINT", launch=NOW - 4000, holders=[snap(NOW - 4000 + 62)])
    old.finalize()
    await lab.save_candidate(old)
    nv_params = copy.deepcopy(lab.SEEDS[7][1]); nv_params["entry"]["min_rate"] = 0.9
    nvid, _ = await lab.register_variant("MOMO20~test", nv_params, origin="challenger")
    nv = await lab.get_variant(nvid)
    done = await lab.backtest_variant(nv)
    async with lab._db() as db:
        cur = await db.execute("SELECT fwd FROM lab_trades WHERE variant_id=? AND mint='OLDMINT'", (nvid,)); r = await cur.fetchone()
    assert done >= 1 and r and r[0] == 0, (done, r)
    # path round trip through the database
    async with lab._db() as db:
        db.row_factory = apex.aiosqlite.Row
        cur = await db.execute("SELECT path FROM lab_candidates WHERE mint='OLDMINT'"); blob = (await cur.fetchone())["path"]
    back = lab.unpack_path(blob, old.launch_ts)
    assert len(back) == len(old.path) and all(abs(a[0] - b[0]) < 0.01 and abs(a[2] - b[2]) < 1e-9 for a, b in zip(back, old.path))
    print(f"settle/replay OK ({n} forward trades; idempotent; history replay flagged fwd=0; path persistence round-trips)")

    # ── 10. tracker: admission, ingestion, settle rules ─────────────────────
    class FP:                                              # a Pool lookalike with a REAL pubkey as mint
        def __init__(self, mint, ts, kind="pump", dev="DEV", dev_pct0=1.0):
            self.mint, self.ts, self.kind, self.dev, self.dev_pct0 = mint, ts, kind, dev, dev_pct0
    m1 = str(KP().pubkey())
    assert lab.on_launch(FP(m1, time.time() - 5)) and m1 in lab._active
    assert not lab.on_launch(FP(m1, time.time())), "duplicate launch ignored"
    assert not lab.on_launch(FP(str(KP().pubkey()), time.time() - 500)), "stale webhook ignored"
    assert not lab.on_launch(FP(str(KP().pubkey()), time.time(), kind="raydium")), "only pump.fun"
    old_max = lab.MAX_ACTIVE; lab.MAX_ACTIVE = len(lab._active)
    assert not lab.on_launch(FP(str(KP().pubkey()), time.time())) and lab._ctr["dropped"] >= 1, "capacity cap"
    lab.MAX_ACTIVE = old_max
    lab._calls.update(day=lab._today(), n=lab.DAILY_CALLS)
    assert not lab.on_launch(FP(str(KP().pubkey()), time.time())), "daily call budget stops admissions"
    lab._calls["n"] = 0
    lab._active.clear()

    def tr_for(mint="T", launch=None):
        return lab.Track(lab.Cand(mint, launch or 1000.0), "CURVE", 1000.0)

    # dead coin: no net buying by T+90 -> dropped
    t = tr_for(); L = 1000.0
    assert lab.ingest(t, L + 10, (1.07e9, 30.0, 793e6, 0.0, 1e9, False)) is None
    assert lab.ingest(t, L + 60, (1.07e9, 30.1, 793e6, 0.1, 1e9, False)) is None
    assert lab.ingest(t, L + 95, (1.07e9, 30.1, 793e6, 0.1, 1e9, False)) == "dead"
    # active-then-rugged coin is NOT dropped as dead (survivorship guard): it settles as collapsed with its losses
    t = tr_for(); lab.ingest(t, L + 10, (1.0e9, 30.0, 793e6, 0.0, 1e9, False))
    lab.ingest(t, L + 40, (0.9e9, 33.0, 700e6, 3.0, 1e9, False))
    assert lab.ingest(t, L + 95, (1.06e9, 30.2, 790e6, 0.2, 1e9, False)) is None, "had inflow>=0.25 once: not dead"
    assert lab.ingest(t, L + 100, (2.6e9, 12.0, 900e6, 0.0, 1e9, False)) == "collapsed"
    # graduation, horizon, unreadable
    t = tr_for(); lab.ingest(t, L + 10, (1.0e9, 30.0, 793e6, 0.0, 1e9, False))
    assert lab.ingest(t, L + 20, (0, 0, 0, 0, 1e9, True)) == "graduated"
    t = tr_for(); lab.ingest(t, L + 10, (1.0e9, 30.0, 793e6, 0.0, 1e9, False))
    lab.ingest(t, L + 400, (0.9e9, 33.0, 700e6, 3.0, 1e9, False))
    assert lab.ingest(t, L + lab.HORIZON_S + 1, (0.9e9, 33.5, 700e6, 3.5, 1e9, False)) == "horizon"
    t = tr_for()
    for i in range(3): assert lab.ingest(t, L + 10 + i, "missing") is None
    assert lab.ingest(t, L + 14, "missing") == "unreadable"
    assert lab._interval(10) == 5 and lab._interval(200) == 10 and lab._interval(500) == 20
    print("tracker rules OK (admission caps/budget/staleness, dead vs rugged-survivorship, graduation, horizon, unreadable)")

    # ── 11. poll_once end-to-end with a scripted RPC ─────────────────────────
    lab._active.clear()
    pm = str(KP().pubkey()); curve = apex.curve_pda(pm)
    state = {"vsol": 30.0, "calls": 0}

    async def fake_h(method, params):
        if method == "getMultipleAccounts":
            state["calls"] += 1
            return {"value": [{"data": [curve_b64(1.073e9 * 30 / state["vsol"], state["vsol"], 793e6, state["vsol"] - 30), "base64"]} for _ in params[0]]}
        return None
    apex._h = fake_h
    async def fbudget(): return True
    apex._budget_ok = fbudget
    lab.on_launch(FP(pm, time.time() - 1, dev_pct0=1.0))
    tr = lab._active[pm]
    for k_ in range(3):
        state["vsol"] += 0.4
        assert await lab.poll_once(time.time() + 100 * k_ * 0 + 0) >= 0
        tr.next_due = 0.0
    assert len(tr.cand.path) == 3 and tr.cand.v0 == 30.0 and state["calls"] == 3
    # batching: 250 due coins -> 3 calls of <=100
    lab._active.clear(); state["calls"] = 0
    for i in range(250):
        mm = str(KP().pubkey()); lab.on_launch(FP(mm, time.time() - 1))
    assert len(lab._active) == 160 or len(lab._active) == min(250, lab.MAX_ACTIVE)
    nact = len(lab._active)
    await lab.poll_once()
    assert state["calls"] == math.ceil(nact / lab.BATCH), (state["calls"], nact)
    # hard budget: settle everything instead of polling
    lab._calls.update(day=lab._today(), n=int(lab.DAILY_CALLS * 1.3))
    for t_ in lab._active.values(): t_.next_due = 0
    lab._active.clear(); lab._calls["n"] = 0
    print("poll_once OK (scripted RPC, 3 samples, 100-account batching)")

    # settle persists and replays; dead ones are only counted
    c1 = mk([0.0] + [0.45] * 20 + [0.0] * 4, mint="SETTLEME", launch=NOW + 7000, holders=[snap(NOW + 7062)])
    trk = lab.Track(c1, "x", time.time()); lab._active[c1.mint] = trk
    c1.end_reason = ""
    before = lab._ctr["settled"]
    await lab.settle(trk, "horizon")
    assert lab._ctr["settled"] == before + 1 and "SETTLEME" not in lab._active
    async with lab._db() as db:
        cur = await db.execute("SELECT end_reason, n_samples FROM lab_candidates WHERE mint='SETTLEME'"); r = await cur.fetchone()
    assert r == ("horizon", len(c1.path)), r
    c2 = mk([0.0, 0.01, 0.0, 0.0], mint="DEADCOIN", launch=NOW + 7100)
    trk2 = lab.Track(c2, "x", time.time()); lab._active[c2.mint] = trk2
    await lab.settle(trk2, "dead")
    async with lab._db() as db:
        cur = await db.execute("SELECT COUNT(*) FROM lab_candidates WHERE mint='DEADCOIN'"); assert (await cur.fetchone())[0] == 0
    await lab.settle(trk2, "dead")                      # double settle is harmless
    print("settle OK (persist + replay; dead coins counted not stored; idempotent)")

    # ── 12. holder snapshots ─────────────────────────────────────────────────
    async def fhv(mint, supply, owner):
        return ([("DEV", 3.0), ("w1", 6.0), ("w2", 4.0)], 500.0, [30e6, 20e6, 10e6])
    apex._holder_view = fhv
    c3 = mk([0.0] + [0.45] * 20, mint="SNAPCOIN", launch=time.time() - 70)
    trk3 = lab.Track(c3, "x", time.time())
    lab._maybe_snapshot(trk3, time.time()); await asyncio.sleep(0.05)
    assert len(c3.holders) == 1 and c3.holders[0]["buyers"] == 2 and abs(c3.holders[0]["dev_now"] - 3.0) < 1e-9
    lab._maybe_snapshot(trk3, time.time()); await asyncio.sleep(0.05)
    assert len(c3.holders) == 1, "same age slot is not snapshotted twice"
    c4 = mk([0.0, 0.01, 0.0], mint="QUIETCOIN", launch=time.time() - 70)
    trk4 = lab.Track(c4, "x", time.time()); lab._maybe_snapshot(trk4, time.time()); await asyncio.sleep(0.05)
    assert not c4.holders, "no holder reads (credits) for coins with no activity"
    print("holder snapshots OK (once per age slot, skipped for quiet coins)")

    # ── 13. evolution ────────────────────────────────────────────────────────
    async with lab._db() as db:
        vs_ = await lab.get_variants(True)
        for i, v in enumerate(vs_):
            s = {"n": 200, "lo90": 0.01 * i, "mean": 0.02}
            await db.execute("INSERT OR REPLACE INTO lab_eval VALUES (?,?,?,?)", (v["id"], time.time(), json.dumps(s), "COLLECTING"))
        await db.commit()
    before = {v["id"] for v in await lab.get_variants(False)}
    made = await lab.evolve()
    assert 1 <= len(made) <= lab.MAX_SPAWN, made
    allv = await lab.get_variants(False)
    kids = [v for v in allv if v["id"] in made]
    assert all(k["origin"] == "challenger" and k["parent"] and not k["parent"].startswith("CTRL") for k in kids), kids
    assert len({v["hash"] for v in allv}) == len(allv), "no duplicate parameter sets"
    made2 = await lab.evolve()
    assert not set(made2) & set(made)
    lab.MAX_VARIANTS = len(await lab.get_variants(True))
    assert await lab.evolve() == [], "cap respected"
    lab.MAX_VARIANTS = 60
    ks = await lab.variants_ever(); assert ks == len(await lab.get_variants(False))
    print(f"evolution OK ({len(made)} challengers from top parents; no controls mutated; no duplicates; cap)")

    # ── 14. evaluate_all + UI ────────────────────────────────────────────────
    ch = await lab.evaluate_all()
    assert isinstance(ch, list)
    ev = await lab.get_eval(vs_[0]["id"]); assert ev is not None and "status" in ev
    apex._access["admin"] = 111
    shown, docs = [], []
    async def fs(target, text, kb): shown.append((text, kb))
    apex._send = fs

    class Msg:
        async def answer_document(self, f, caption=None): docs.append((f.filename, f.data if hasattr(f, "data") else None))
    class FQ:
        def __init__(self, data, uid=111): self.data = data; self.from_user = type("U", (), {"id": uid})(); self.message = Msg(); self.alerts = []
        async def answer(self, *a, **k): self.alerts.append((a, k))
    for data in ("lab:home", "lab:vars:0", "lab:vars:1", "lab:how"):
        await lab.cb_lab(FQ(data)); text, kb = shown[-1]
        assert len(text) < 4096 and kb.inline_keyboard, data
    rid = (await lab.get_variants(False))[0]["rid"]
    await lab.cb_lab(FQ(f"lab:v:{rid}")); assert "Promotion checklist" in shown[-1][0] or "No forward trades" in shown[-1][0]
    rid2 = next(r for r in [v["rid"] for v in await lab.get_variants(False)] if True)
    for v in await lab.get_variants(False):
        txt, _ = await lab.variant_page(v["rid"]); assert len(txt) < 4096
    await lab.cb_lab(FQ("lab:csv:trades")); await lab.cb_lab(FQ("lab:csv:cands"))
    assert len(docs) == 2 and docs[0][0].startswith("lab_trades_") and docs[1][0].startswith("lab_cands_")
    n_before = len(shown)
    q = FQ("lab:home", uid=999); await lab.cb_lab(q)
    assert len(shown) == n_before and q.alerts and q.alerts[0][1].get("show_alert"), "non-owners are refused"
    name, data = await lab.build_csv("trades"); assert data.startswith(b"variant,")
    print("evaluate + Telegram pages OK (home, variants, detail, how-it-works, CSV; non-owner refused)")

    # ── 15. apex hook runs even when nobody is trading ───────────────────────
    lab._active.clear()
    hm = str(KP().pubkey())
    real_extract = apex.extract_pump
    apex.extract_pump = lambda tx: apex.Pool(mint=hm, dev="D", ts=time.time(), sig="s", kind="pump", dev_pct0=1.0)
    apex._seen.pop(hm, None)
    assert not await apex._active_users(), "precondition: no running users"
    await apex.on_pool_event({"type": "CREATE", "source": "PUMP_FUN", "timestamp": time.time()})
    assert hm in lab._active, "launch must reach the lab before the live idle-skip"
    apex.extract_pump = real_extract
    print("apex hook OK (lab receives launches even with zero active users)")

    # a lab failure can never break the live pipeline
    real_on = lab.on_launch
    lab.on_launch = lambda pool: (_ for _ in ()).throw(RuntimeError("boom"))
    hm2 = str(KP().pubkey()); apex._seen.pop(hm2, None)
    apex.extract_pump = lambda tx: apex.Pool(mint=hm2, dev="D", ts=time.time(), sig="s", kind="pump")
    await apex.on_pool_event({"type": "CREATE", "source": "PUMP_FUN", "timestamp": time.time()})
    lab.on_launch = real_on; apex.extract_pump = real_extract
    print("fault isolation OK (a crashing lab hook does not break on_pool_event)")

    await lab.flush_counters()
    async with lab._db() as db:
        cur = await db.execute("SELECT launches, admitted FROM lab_daily"); r = await cur.fetchone()
    assert r and r[0] >= 1
    print("LAB TESTS PASSED")


asyncio.run(main())
