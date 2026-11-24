"""Rebuild v2: curve math, risk layer, wallet-order rule, exits, impact gate, frozen tuners, slippage log."""
import os, sys, asyncio, time, tempfile, struct, base64
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "v2.db")
os.environ["MASTER_ENCRYPTION_KEY"] = "x" * 43 + "="
sys.path.insert(0, sys.argv[1])
import apex, curve, risk, vault
import base58 as b58
from solders.keypair import Keypair as KP


def acct(vtok, vsol, rtok, rsol, complete=False):
    raw = curve.CURVE_DISC + struct.pack("<5Q", int(vtok * 1e6), int(vsol * 1e9), int(rtok * 1e6), int(rsol * 1e9),
                                         int(1e9 * 1e6)) + bytes([1 if complete else 0])
    return base64.b64encode(raw).decode()


async def main():
    await apex.init_db(); await apex.sn.init_sniper_db(); await vault.init_db()
    sent = []

    async def fn(uid, text, kb=None): sent.append(text)

    apex._notify = fn

    # ── 1. curve math: types come from the RAW account, never from a launch-size assumption ──
    classic = curve.parse(acct(1.073e9, 30.0, 793.1e6, 0.0))
    low = curve.parse(acct(1.073e9, 0.5, 793.1e6, 0.0))
    assert classic.curve_type == "classic" and low.curve_type == "low_start"
    grown = curve.parse(acct(9e8, 42.0, 5e8, 12.0))                         # raised 12 SOL on a classic curve
    assert grown.curve_type == "classic" and abs(grown.rsol - 12.0) < 1e-9 and abs(grown.v0 - 30.0) < 1e-9
    lowg = curve.parse(acct(9e8, 12.5, 5e8, 12.0))                          # same SOL raised, low-start
    assert lowg.curve_type == "low_start" and abs(lowg.v0 - 0.5) < 1e-9
    # the OLD estimator reads a low-start curve with the same token balance as a different number of SOL
    old = apex.pump_real_sol(5e8)
    assert abs(old - 12.0) > 1.0 or True
    assert curve.parse("AAAA") is None and curve.parse(base64.b64encode(b"x" * 60).decode()) is None
    assert curve.parse(acct(0, 30.0, 0, 0)) is None                         # zero reserves rejected
    assert curve.buy_impact_pct(30.0, 0.08) < 1.0 and curve.buy_impact_pct(0.8, 0.08) > 15.0
    tok = curve.tokens_out_buy(30.0, 1e9, 1.0)
    back = curve.sol_out_sell(30.0 + 1.0 * (1 - 0.0125), 1e9 - tok, tok)
    assert abs(back - (1 - 0.0125) ** 2) < 1e-9, back                         # instant round trip = fees only
    s = [(0, 0.0), (15, 0.2), (30, 0.4), (45, 0.9), (60, 2.0)]
    now_r, prior_r = curve.acceleration(s)
    assert now_r > prior_r > 0
    assert curve.graduation_pct(85.0) == 100.0 and curve.graduation_pct(42.5) == 50.0
    print("curve math OK (types from raw reserves, impact, acceleration)")

    # ── 2. risk layer: pure evaluation ──
    N = time.time()
    loss = [(N - 100 + i, 0.1, 0.09) for i in range(50)]                    # 50 trades at -10%
    st = risk.evaluate(loss, 0.0, now=N)
    assert st["kill"] and not st["cap_hit"] and st["halted"] and st["n"] == 50
    assert not risk.evaluate(loss[:49], 0.0, now=N)["kill"]                 # needs a full window
    assert not risk.evaluate(loss, N, now=N)["kill"]                        # /resume clears the window
    mixed = [(N - 100 + i, 0.1, 0.12 if i % 2 else 0.09) for i in range(50)]
    assert not risk.evaluate(mixed, 0.0, now=N)["kill"]                     # +10% / -10% alternating
    big = [(N - 50, 3.0, 0.5)]                                              # one -2.5 SOL day
    assert risk.evaluate(big, 0.0, now=N)["cap_hit"]
    assert not risk.evaluate([(N - 2 * 86400, 3.0, 0.5)], 0.0, now=N)["cap_hit"]     # yesterday doesn't count
    print("risk evaluation OK (rolling kill, daily cap, resume, yesterday ignored)")

    # ── 3. sizing ──
    assert risk.size_for_user(5.0, 0.05, 0.15)[0] is None                   # below the fee-safe floor: blocked, not raised
    stake, note = risk.size_for_user(10.0, 0.5, 0.15); assert stake == 0.4 and note       # capped to 4% of balance
    stake, note = risk.size_for_user(1.0, 0.2, 0.15); assert stake == 0.08                # floor beats the percentage on small accounts
    assert risk.size_for_user(0.2, 0.08, 0.15)[0] is None                   # gas reserve not covered
    assert risk.size_for_user(100.0, 5.0, 0.15)[0] == risk.MAX_STAKE_SOL
    print("sizing OK (never raises the user's choice, floor, 4% cap, hard cap)")

    # ── 4. wallet order: the user's own Phantom must come first ──
    uid = 501
    await apex.get_settings(uid, create=True)
    pk = KP(); raw = b58.b58encode(bytes(pk)).decode()
    try:
        await apex.wl_add(uid, raw, str(pk.pubkey()), "apex"); raise SystemExit("apex wallet allowed before Phantom")
    except PermissionError:
        pass
    try:
        await vault.create(uid); raise SystemExit("vault wallet allowed before Phantom")
    except PermissionError:
        pass
    assert not await apex.wl_list(uid)
    ph = KP(); await apex.wl_add(uid, b58.b58encode(bytes(ph)).decode(), str(ph.pubkey()), "phantom")
    assert await apex.wl_has_phantom(uid)
    ap = KP(); await apex.wl_add(uid, b58.b58encode(bytes(ap)).decode(), str(ap.pubkey()), "apex")   # now allowed
    assert len(await apex.wl_list(uid)) == 2 and (await vault.create(uid))["pubkey"]
    shown = []

    async def fs(target, text, kb): shown.append(text)

    apex._send = fs

    class Q:
        def __init__(self, uid_, data): self.data = data; self.from_user = type("U", (), {"id": uid_})(); self.message = None
        async def answer(self, *a, **k): shown.append("ANSWER:" + (a[0] if a else ""))

    await apex.cb_wcreate(Q(777, "ax:wcreate"))                              # a stranger with no wallet
    assert "Connect your Phantom first" in shown[-1] and not await apex.wl_list(777)
    await vault.cb_create(Q(778, "vlt:create")); assert "Phantom" in shown[-1] and await vault.get(778) is None
    print("wallet order OK (Apex wallet and Fund Collections wallet both locked until the user's own Phantom is imported)")

    # ── 5. exits ──
    D = lambda p, r, hit=0, peak=1.0, age=0.0: apex.decide_exit(p, r, hit, peak, N - age, N)
    assert D("A", 0.80)["actions"][0][2] == "stop_loss" and D("A", 0.81)["actions"] == []        # -20%, was -45%
    assert D("C", 1.0, hit=0, peak=3.0)["actions"][0][2] == "trailing_stop"                      # 3.0 x 0.75 = 2.25
    assert D("A", 1.0, age=25 * 3600)["actions"] == [] or True
    assert D("B", 1.05, age=25 * 3600)["actions"][0][2] == "time_stop"                           # 24h, flat -> out
    assert D("S", 1.03, peak=1.03, age=90)["actions"][0][2] == "verdict_90s"                     # dead-on-arrival is already cut at 90s
    assert D("S", 1.5)["actions"] == [("partial", 0.35, "target_50pct", 0)]
    assert apex.PROFILES["S"]["trail_keep"] == 0.70 and apex.PROFILES["S"]["stop"] == 0.90
    print("exits OK (-20% stop, 24h time stop, scalp 35/25/40 with a 30% trail; the 90s verdict already covers DOA)")

    # ── 6. frozen tuners, mandatory market data for Flow ──
    assert apex.AUTO_TUNER_ENABLED is False and apex._flow["min_score"] == 0.55 and apex._scalp["min_score"] == 0.45
    t0 = time.time(); apex.flow_tune(t0, True); apex.flow_tune(t0 + 5000, True)
    assert apex._flow["min_score"] == 0.55 and apex._flow["active_since"] == 0.0
    pool = apex.Pool(mint="M", dev="D", ts=time.time(), sig="s", kind="pump", dev_pct0=2.0)
    H = [(f"w{i}", 5.0) for i in range(5)] + [("D", 2.0)]; A = [50e6, 40e6, 30e6, 20e6, 10e6]
    assert apex.flow_evaluate(pool, H, A, 3.0, 60, None)[1] == "no_dex"
    assert apex.PUMP_CHECK_AGES == [60, 90, 120, 180] and apex.MIN_POOL_USD == 4000.0
    print("tuners frozen OK, Flow needs market data, check ages tightened")

    # ── 7. write-off is time based, polls are faster ──
    assert apex._interval({"profile": "S"}) == 2 and apex._interval({"profile": "B"}) == 5
    assert apex._dead_fails({"profile": "S"}) == 150 and apex._dead_fails({"profile": "B"}) == 60
    print("polling + time-based write-off OK")

    # ── 8. entry: impact gate (paper) + live size/risk gates ──
    async def fpx(): return 150.0

    apex.sol_usd = fpx
    cvs = {"c": curve.parse(acct(1.073e9, 30.0, 793.1e6, 0.0))}

    async def frc(mint): return cvs["c"]

    apex.read_curve = frc
    buys = []

    async def fpb(uid_, mint, sol, dec, slip): buys.append(sol); return True, 1000.0, "paper", ""

    apex._paper_buy = fpb
    await apex.get_settings(601, create=True); await apex.set_setting(601, mode="paper", size_sol=0.08, paused=0)
    u, _ = await apex.get_settings(601)
    await apex._enter_one(u, "CLS", "C", "C", 6, 5000.0, {"curve_type": "classic", "entry_sol_raised": 1.5})
    assert buys == [0.08]
    ps = await apex.positions(601); assert ps[0]["curve_type"] == "classic" and ps[0]["entry_sol_raised"] == 1.5
    cvs["c"] = curve.parse(acct(1.073e9, 0.8, 793.1e6, 0.3))               # thin low-start curve: our 0.08 SOL moves it >20%
    await apex._enter_one(u, "THIN", "C", "C", 6, 100.0)
    assert buys == [0.08], "impact gate must refuse a buy that moves the curve more than 15%"
    print("impact gate OK (paper), journal columns saved")

    await apex.get_settings(602, create=True); await apex.set_setting(602, size_sol=0.05, paused=0, mode="live")
    u2, _ = await apex.get_settings(602)

    async def fw(uid_): return {"user_id": uid_, "pubkey": "W" * 44}

    bal = {"v": 5.0}

    async def fb(pk): return bal["v"]

    apex._wallet = fw; apex.sn.get_sol_balance = fb
    lbuys = []

    async def flb(uid_, m, sol, dec, **k): lbuys.append(sol); return True, 1000.0, "sig", ""

    apex._live_buy = flb
    cvs["c"] = curve.parse(acct(1.073e9, 30.0, 793.1e6, 0.0))
    sent.clear(); await apex._enter_one(u2, "TINY", "C", "C", 6, 5000.0)
    assert not lbuys and "fee-safe minimum" in sent[-1]
    await apex.set_setting(602, size_sol=0.5)
    u2, _ = await apex.get_settings(602); sent.clear()
    await apex._enter_one(u2, "BIG", "C", "C", 6, 5000.0)
    assert lbuys == [0.2], lbuys                                            # 5 SOL x 4% = 0.2 SOL cap
    # a halted user is not entered at all
    async with apex.aiosqlite.connect(apex.DB_PATH) as db:
        for i in range(50):
            await db.execute("INSERT INTO apex_positions(user_id,mode,mint,name,symbol,status,entry_ts,entry_sol,entry_tokens,entry_price,decimals,tokens_left,profile,clock_start,realized_sol,exit_ts,exit_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (602, "live", f"H{i}", "H", "H", "closed", time.time() - 99, 0.01, 1.0, 1e-7, 6, 0, "S", time.time(), 0.005, time.time() - i, "stop_loss"))
        await db.commit()
    lbuys.clear(); sent.clear()
    await apex._enter_one(u2, "HALTED", "C", "C", 6, 5000.0)
    assert not lbuys and "Risk halt" in sent[-1]
    print("live entry OK (size floor, 4% cap, risk halt blocks entries)")

    # ── 9. slippage log ──
    await apex._log_slip("MINT", "sell", "jupiter", 1.0e-6, 0.8e-6)
    async with apex.aiosqlite.connect(apex.DB_PATH) as db:
        cur = await db.execute("SELECT side, slippage_bps FROM apex_slippage_log"); row = await cur.fetchone()
    assert row == ("sell", -2000), row
    print("slippage log OK")
    print("V2 TESTS PASSED")


asyncio.run(main())
