"""Calibration: does the lab find an edge that is really there, and stay quiet when there is none?"""
import os, sys, random, time, tempfile, statistics as st
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "w.db")
os.environ["MASTER_ENCRYPTION_KEY"] = "x" * 43 + "="
sys.path.insert(0, sys.argv[1])
import apex, lab

NOW = time.time() + 9000
P = {n: p for n, p in lab.SEEDS}


def coin(i, flows, rng, v0=30.0):
    c = lab.Cand(f"W{i}", NOW + i * 7, "DEV", 2.0)
    vtok0, k = 1.073e9, 1.073e9 * v0
    vsol, t = v0, c.launch_ts + 3
    for dx in flows:
        vsol = max(vsol + dx, 0.5)
        vtok = k / vsol
        c.add(t, vtok, vsol, 793.1e6 - (vtok0 - vtok), vsol - v0, 1e9)
        t += 5.0
    c.holders = [{"t": c.launch_ts + 62, "buyers": 6, "top1": 5.0, "top10": 30.0, "dev_now": 2.0, "cluster": 1, "nh": 12},
                 {"t": c.launch_ts + 122, "buyers": 7, "top1": 5.0, "top10": 30.0, "dev_now": 2.0, "cluster": 1, "nh": 12}]
    c.end_reason = "horizon"
    c.finalize()
    return c


def world(kind, n, rng):
    out = []
    for i in range(n):
        warm = [0.0] + [0.45 + rng.gauss(0, 0.05) for _ in range(13)]          # everything looks alike until ~T+68s
        if kind == "edge":                                                    # 45% run, 55% fade
            runner = rng.random() < 0.45
        elif kind == "anti":                                                  # 12% run, 88% fade
            runner = rng.random() < 0.04
        if kind in ("edge", "anti"):
            tail = ([(0.7 if kind == "edge" else 0.3) + rng.gauss(0, 0.2) for _ in range(40)] if runner
                    else [-0.55 + rng.gauss(0, 0.15) for _ in range(40)])
            flows = warm + tail
        else:                                                                 # pure noise: symmetric random walk
            flows = [0.0] + [rng.gauss(0.0, 0.7) for _ in range(70)]
        out.append(coin(i, flows, rng))
    return out


def evaluate(names, cands, k_total=20):
    res = {}
    for nm in names:
        rows = []
        for c in cands:
            t = lab.run_variant(P[nm], c, nm)
            if t:
                rows.append({"ret": t["ret"], "ret_l2": t["ret_l2"], "ret_l10": t["ret_l10"], "ret_l20": t["ret_l20"],
                             "reason": t["reason"], "v0": c.v0})
        res[nm] = lab.compute_stats(rows, k_total)
    return res


rng = random.Random(2024)
names = ["ALL_T60", "MOMO20", "CTRL_RANDOM"]

good = evaluate(names, world("edge", 700, rng))
for nm in names:
    s = good[nm]
    print(f"EDGE   {nm:12s} n={s['n']:4d} mean={s.get('mean', 0) * 100:+6.1f}% lo_adj={s.get('lo_adj', 0) * 100:+6.1f}% lat10={((s.get('l10') or 0) * 100):+6.1f}%")
assert good["ALL_T60"]["n"] >= 600 and good["ALL_T60"]["mean"] > 0.1 and good["ALL_T60"]["lo_adj"] > 0, "planted edge not detected"
assert good["MOMO20"]["mean"] > 0 and good["MOMO20"]["lo_adj"] > 0

bad = evaluate(names, world("anti", 700, rng))
for nm in names:
    s = bad[nm]
    print(f"ANTI   {nm:12s} n={s['n']:4d} mean={s.get('mean', 0) * 100:+6.1f}% hi95={s.get('hi95', 0) * 100:+6.1f}%")
assert bad["ALL_T60"]["mean"] < 0 and bad["ALL_T60"]["hi95"] < 0, "a losing world must look losing"
st_, _ = lab.judge(bad["ALL_T60"], None); assert st_ == "RETIRED"

# pure noise, many independent worlds: how often does a variant clear the lower-bound test by luck?
false_pos = trials = 0
means = []
for w in range(14):
    cands = world("noise", 450, rng)
    res = evaluate(["ALL_T60", "MOMO20", "CTRL_RANDOM", "EARLY_FLAT"], cands, k_total=20)
    for nm, s in res.items():
        if s["n"] < 150:
            continue
        trials += 1
        means.append(s["mean"])
        if s["lo_adj"] > 0:
            false_pos += 1
print(f"NOISE  {trials} variant-worlds: mean return/trade {st.mean(means) * 100:+.1f}% (fees make noise lose); false positives {false_pos}")
assert trials >= 20 and false_pos <= 1, f"too many false positives: {false_pos}/{trials}"
assert st.mean(means) < 0, "noise should lose to fees and slippage, not win"
print("CALIBRATION PASSED (finds a planted edge, rejects an anti-edge, and does not discover edges in noise)")
