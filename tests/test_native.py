"""Native pump.fun sell: layout, eligibility, pre-signing, and the Jupiter fallback."""
import os, sys, asyncio, time, tempfile, struct, base64, random
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "n.db")
os.environ["MASTER_ENCRYPTION_KEY"] = "x" * 43 + "="
os.environ["NATIVE_TX_ENABLED"] = "1"
sys.path.insert(0, sys.argv[1])
import apex, curve, pumpnative as pn
from solders.keypair import Keypair as KP
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

CREATOR = KP().pubkey()


def acct(vsol=30.0, rsol=1.5, complete=False, mayhem=False, cashback=False, quote=pn.ZERO, creator=CREATOR):
    raw = (curve.CURVE_DISC + struct.pack("<5Q", int(1.0e9 * 1e6), int(vsol * 1e9), int(7e8 * 1e6), int(rsol * 1e9), int(1e15))
           + bytes([1 if complete else 0]) + bytes(creator) + bytes([1 if mayhem else 0, 1 if cashback else 0]) + bytes(quote))
    return base64.b64encode(raw).decode()


def global_acct(fr, rs, bb):
    raw = bytearray(1100)
    for i, k in enumerate(fr): raw[162 + 32 * i: 194 + 32 * i] = bytes(k)
    for i, k in enumerate(rs): raw[516 + 32 * i: 548 + 32 * i] = bytes(k)
    for i, k in enumerate(bb): raw[741 + 32 * i: 773 + 32 * i] = bytes(k)
    return base64.b64encode(bytes(raw)).decode()


FR = [KP().pubkey() for _ in range(7)]; RS = [KP().pubkey() for _ in range(7)]; BB = [KP().pubkey() for _ in range(8)]
MINT = KP().pubkey()


async def main():
    # ── 1. parsing + eligibility ──
    i = pn.parse_info(acct()); assert i and i.eligible and i.creator == CREATOR and i.cv.rsol == 1.5
    assert not pn.parse_info(acct(complete=True)).eligible
    assert not pn.parse_info(acct(mayhem=True)).eligible and not pn.parse_info(acct(cashback=True)).eligible
    assert not pn.parse_info(acct(quote=KP().pubkey())).eligible               # not a SOL-quoted curve
    assert pn.parse_info(base64.b64encode(base64.b64decode(acct())[:100]).decode()) is None     # short layout: do not guess
    g = pn.parse_global(global_acct(FR, RS, BB)); assert g and g.fee_recipients == FR and g.reserved == RS and g.buyback == BB
    assert pn.parse_global(base64.b64encode(b"x" * 200).decode()) is None
    print("parsing + eligibility OK (cashback, mayhem, graduated, odd quote mint, short layout all refused)")

    # ── 2. the instruction ──
    user = KP().pubkey()
    ix = pn.sell_ix(user, MINT, pn.TOKEN_2022, i, g, 123456789, 42, rng=random.Random(1))
    A = ix.accounts
    assert len(A) == 16 and ix.program_id == pn.PUMP
    assert ix.data == pn.SELL_DISC + struct.pack("<QQ", 123456789, 42) and len(ix.data) == 24
    assert A[0].pubkey == pn.GLOBAL and A[1].pubkey in FR and A[1].is_writable
    assert A[2].pubkey == MINT and A[3].pubkey == pn.curve_pda(MINT) and A[3].is_writable
    assert A[4].pubkey == pn.ata(A[3].pubkey, MINT, pn.TOKEN_2022) and A[5].pubkey == pn.ata(user, MINT, pn.TOKEN_2022)
    assert A[6].pubkey == user and A[6].is_signer and A[6].is_writable and not any(a.is_signer for a in A[:6] + A[7:])
    assert A[8].pubkey == pn.creator_vault_pda(CREATOR) and A[9].pubkey == pn.TOKEN_2022
    assert A[12].pubkey == pn.FEE_CONFIG and A[13].pubkey == pn.FEE_PROGRAM
    assert A[14].pubkey == pn.curve_v2_pda(MINT) and not A[14].is_writable and A[15].pubkey in BB and A[15].is_writable
    my = pn.parse_info(acct(mayhem=True)); ix2 = pn.sell_ix(user, MINT, pn.TOKEN_PROGRAM, my, g, 1, 1)
    assert ix2.accounts[1].pubkey in RS and ix2.accounts[5].pubkey == pn.ata(user, MINT, pn.TOKEN_PROGRAM)
    # known real mainnet values (a live sell on CQaCF...pump): the derivations must reproduce them exactly
    rm = Pubkey.from_string("CQaCFnJo71mAT7LwW9qNrg5PSUWVc8EW75sFnHYHpump")
    assert str(pn.curve_pda(rm)) == "3ZJFcNHzHVshhkzvjkMbuEEp4K2h6vuj5t6sTSAU83W8"
    assert str(pn.curve_v2_pda(rm)) == "HSSYFzfRqyeArbQQGJy3eqkCNzd1rNLfgHXDPZhq4Wts"
    assert str(pn.FEE_CONFIG) == "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt" and str(pn.EVENT_AUTH) == "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"
    assert str(pn.GLOBAL) == "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"
    print("instruction OK (16 accounts in the live order, flags, data; PDAs equal real mainnet addresses)")

    # ── 3. transactions ──
    bh = "11111111111111111111111111111111"
    kp = KP(); ixk = pn.sell_ix(kp.pubkey(), MINT, pn.TOKEN_2022, i, g, 5, 1)
    signed = VersionedTransaction.from_bytes(pn.build_sell_tx(kp.pubkey(), ixk, bh, 1_000_000, signer=kp))
    assert len(signed.signatures) == 1 and str(signed.signatures[0]) != "1" * 64
    unsigned = VersionedTransaction.from_bytes(pn.build_sell_tx(kp.pubkey(), ixk, bh, 1_000_000))
    assert str(unsigned.signatures[0]) == "1" * 64
    assert pn.min_out_for(i, 1000.0, 10_000) == 1
    est = pn.estimate_out_sol(i, 1000.0); assert abs(pn.min_out_for(i, 1000.0, 2000) / 1e9 - est * 0.8) < 1e-6
    print("transactions OK (signed / unsigned, stop min-out = anything, tier min-out = estimate less slippage)")

    # ── 4. wired into the position manager (fake chain) ──
    await apex.init_db(); await apex.sn.init_sniper_db()
    wallet = KP(); sent_msgs = []
    mint_s = str(MINT)
    state = {"info": acct(), "sim_ok": True, "send_ok": True, "confirm": "ok", "bal": 1000.0, "sends": [], "sims": 0}

    async def fh(method, params):
        if method == "getAccountInfo":
            a = params[0]
            if a == str(pn.curve_pda(MINT)): return {"value": {"data": [state["info"], "base64"]}}
            if a == str(pn.GLOBAL): return {"value": {"data": [global_acct(FR, RS, BB), "base64"]}}
            if a == mint_s: return {"value": {"owner": str(pn.TOKEN_2022), "data": ["", "base64"]}}
        if method == "getLatestBlockhash": return {"value": {"blockhash": bh}}
        if method == "simulateTransaction":
            state["sims"] += 1
            return {"value": {"err": None if state["sim_ok"] else "InstructionError", "logs": ["x"], "unitsConsumed": 55000}}
        if method == "getTransaction":
            return None
        return None

    async def fw(uid): return {"user_id": uid, "pubkey": str(wallet.pubkey())}
    async def fenc(uid): return "enc"
    async def fpx(): return 150.0
    async def fsend(tx, skip=True):
        state["sends"].append(tx); return "SIG" + str(len(state["sends"])) if state["send_ok"] else None
    async def fconf(sig, timeout_s=30): return (state["confirm"], "")
    async def fbal(pk, m): return state["bal"]
    async def fdelta(sig, pk): return 0.0123
    async def fn(uid, text, kb=None): sent_msgs.append(text)

    apex._h = fh; apex._wallet = fw; apex.sol_usd = fpx; apex._notify = fn
    apex.sn._get_enc_key = fenc; apex.sn.load_keypair = lambda enc: wallet
    apex.sn.send_signed_fast = fsend; apex._confirm_tx = fconf; apex.sn.get_token_balance = fbal
    apex._tx_sol_delta = fdelta
    apex._access["admin"] = 999
    assert apex.NATIVE_TX_ENABLED is True

    uid = 9
    await apex.get_settings(uid, create=True)
    async with apex.aiosqlite.connect(apex.DB_PATH) as db:
        cur = await db.execute("INSERT INTO apex_positions(user_id,mode,mint,name,symbol,entry_ts,entry_sol,entry_tokens,entry_price,decimals,tokens_left,profile,clock_start) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                               (uid, "live", mint_s, "N", "N", time.time(), 0.1, 1000.0, 1e-4, 6, 1000.0, "S", time.time()))
        pid = cur.lastrowid; await db.commit()
    pos = await apex.get_position(pid)

    # eligibility is decided by a simulation, once
    st = await apex._native_state(pos); assert st and st["elig"] and state["sims"] == 1
    await apex._native_state(pos); assert state["sims"] == 1
    assert sent_msgs and "Native sell verified" in sent_msgs[-1]
    v = await apex._native_value(pos, 1000.0); assert abs(v - pn.estimate_out_sol(pn.parse_info(acct()), 1000.0)) < 1e-12
    # a failing simulation keeps the coin on Jupiter
    pos2 = dict(pos, id=pid + 1); state["sim_ok"] = False
    assert await apex._native_state(pos2) is None and apex._native[pid + 1]["elig"] is False
    state["sim_ok"] = True
    # a cashback coin is never used natively
    cb = dict(pos, id=pid + 2); state["info"] = acct(cashback=True)
    assert await apex._native_state(cb) is None
    state["info"] = acct()
    print("eligibility OK (simulate once; failed simulation or cashback coin -> Jupiter)")

    # pre-signed exit: built on refresh, used by an urgent sell, single use
    await apex._native_refresh(pid); assert apex._native[pid]["pre"]
    pre_tx = apex._native[pid]["pre"][0]
    ok, out, sig = await apex._sell(pos, 1000.0, apex.EXIT_SLIPPAGE_BPS, "stop_loss", trig_price=1e-4)
    assert ok and sig == "SIG1" and out == 0.0123 and state["sends"][0] == pre_tx and apex._native[pid]["pre"] is None
    # a tier sell is built fresh with slippage protection (min_out > 1)
    ok, out, sig = await apex._sell(pos, 400.0, apex.EXIT_SLIPPAGE_BPS, "target_50pct")
    ix_data = VersionedTransaction.from_bytes(state["sends"][1]).message.instructions[2].data
    assert ok and struct.unpack_from("<QQ", ix_data, 8) == (400_000_000, pn.min_out_for(pn.parse_info(acct()), 400.0, apex.EXIT_SLIPPAGE_BPS))
    assert struct.unpack_from("<Q", ix_data, 16)[0] > 1
    print("pre-signed + fresh sells OK (stop reuses the signed tx once; tier sells carry min-out)")

    # fallbacks: never lose the exit
    jup = {"n": 0}
    async def fq(i_, o, amt, bps, lane="scan"):
        jup["n"] += 1; return {"outAmount": str(int(0.05e9))}
    async def fswap(q, pk): return None                      # Jupiter build fails: we only care that it was TRIED
    apex.jup_quote = fq; apex.jup_swap_tx = fswap
    state["send_ok"] = False
    r = await apex._sell(pos, 1000.0, apex.EXIT_SLIPPAGE_BPS, "stop_loss"); assert r == (False, 0.0, "") and jup["n"] == 1
    state["send_ok"] = True; state["confirm"] = "failed"; jup["n"] = 0
    await apex._sell(pos, 1000.0, apex.EXIT_SLIPPAGE_BPS, "stop_loss"); assert jup["n"] == 1      # failed on chain -> Jupiter
    state["confirm"] = "timeout"; state["bal"] = 1000.0; jup["n"] = 0
    await apex._sell(pos, 1000.0, apex.EXIT_SLIPPAGE_BPS, "stop_loss"); assert jup["n"] == 1      # never landed -> Jupiter
    state["bal"] = 10.0; state["sends"].clear(); jup["n"] = 0
    r = await apex._sell(pos, 1000.0, apex.EXIT_SLIPPAGE_BPS, "stop_loss")
    assert r[0] is True and jup["n"] == 0                                                         # timeout but tokens are gone: do NOT sell twice
    state["confirm"] = "ok"; state["info"] = acct(complete=True); jup["n"] = 0
    await apex._sell(pos, 1000.0, apex.EXIT_SLIPPAGE_BPS, "stop_loss"); assert jup["n"] == 1      # graduated mid-trade -> Jupiter
    print("fallbacks OK (no send, failed on chain, never landed, graduated -> Jupiter; landed-late is not sold twice)")

    # paper positions and the disabled flag never touch the native path
    paper = dict(pos, mode="paper"); assert await apex._native_state(paper) is None
    apex.NATIVE_TX_ENABLED = False; state["info"] = acct(); state["sends"].clear(); jup["n"] = 0
    await apex._sell(pos, 1000.0, apex.EXIT_SLIPPAGE_BPS, "stop_loss"); assert not state["sends"] and jup["n"] == 1
    print("flag off / paper OK")

    # the manager prices the bag from the curve when native is on, and from Jupiter otherwise
    apex.NATIVE_TX_ENABLED = True; jup["n"] = 0
    await apex._manage(await apex.get_position(pid))
    assert jup["n"] == 0
    print("manager prices from the curve OK (no quote API call)")
    print("NATIVE TESTS PASSED")


asyncio.run(main())
