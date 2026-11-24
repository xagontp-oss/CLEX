"""Shared by the test and the visual demo: a database that looks like a week of Lab + trading."""
import asyncio, hashlib, hmac, json, os, random, sys, time, urllib.parse

BOT_TOKEN = "123456789:AAFakeTokenForTestsOnlyNotARealTelegramBotTokenXX"
ADMIN = 7702415898


def sign(user_id=ADMIN, token=BOT_TOKEN, auth_date=None, tamper=False):
    user = json.dumps({"id": user_id, "first_name": "Owner"}, separators=(",", ":"))
    d = {"auth_date": str(int(auth_date if auth_date is not None else time.time())), "query_id": "AAH", "user": user}
    check = "\n".join(f"{k}={v}" for k, v in sorted(d.items()))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    d["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    if tamper:
        d["user"] = d["user"].replace(str(user_id), "1")
    return urllib.parse.urlencode(d)


async def seed(apex, lab, n_per=420, seed_=11):
    rng = random.Random(seed_)
    await apex.init_db(); await apex.sn.init_sniper_db(); await lab.init_db(); await lab.ensure_seeds()
    now = time.time()
    variants = await lab.get_variants(active_only=False)
    exits = ["stop_loss", "verdict", "trailing_stop", "target_50pct", "horizon", "collapse"]
    async with lab._db() as db:
        # candidates over 72h with a day/night rhythm
        for i in range(2600):
            t = now - rng.random() * 72 * 3600
            hr = (t / 3600) % 24
            peak = 1 + abs(rng.gauss(0, 0.5)) ** 1.3 * (1.4 if 13 < hr < 21 else 0.9) + (rng.random() < 0.03) * rng.uniform(2, 14)
            v0 = 30.0 if rng.random() < 0.7 else 0.5
            await db.execute("INSERT OR IGNORE INTO lab_candidates(mint,launch_ts,dev,dev_pct0,v0,supply,n_samples,max_inflow,peak_mult,end_mult,end_reason,end_age,settled_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (f"C{i}", t, "D", 2.0, v0, 1e9, 60, rng.uniform(0.1, 6), peak, peak * rng.uniform(0.1, 0.9), "horizon", 600, t + 900))
        edge = {"MOMO20": 0.035, "SCALP_V2": 0.02, "EARLY_FLAT": 0.012, "MOMO20.STOP8": 0.03}
        for vi, v in enumerate(variants):
            mu = edge.get(v["name"], -0.04 + (vi % 5) * 0.004)
            n = n_per + rng.randint(-90, 120) if not v["name"].startswith("MOMO20.RUNNER") else 60
            for j in range(n):
                t = now - (n - j) * (6 * 86400 / n) + rng.random() * 100
                big = rng.random() < 0.07
                r = rng.uniform(1.0, 4.0) if big else max(-1.0, rng.gauss(mu - 0.04, 0.2) - (0.25 if rng.random() < 0.45 else 0))
                reason = "target_50pct" if r > 0.4 else rng.choice(exits[:3] + exits[4:])
                await db.execute("INSERT OR IGNORE INTO lab_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                 (v["id"], f"T{vi}_{j}", t, rng.uniform(60, 150), 1, r, r * 0.075, reason, rng.uniform(15, 400),
                                  1 + max(0, r) * rng.uniform(0.8, 1.3), 0.03, 30.0 if rng.random() < 0.7 else 0.5, 1.1,
                                  r + 0.02, r - 0.03, r - 0.07))
        today = time.strftime("%Y-%m-%d", time.gmtime())
        for k in range(8):
            d = time.strftime("%Y-%m-%d", time.gmtime(now - k * 86400))
            await db.execute("INSERT OR REPLACE INTO lab_daily VALUES (?,?,?,?,?,?,?)",
                             (d, 28000 - k * 900, 8400 - k * 300, 4900 - k * 150, 3500 - k * 150, 52000 + k * 400, 0))
        await db.commit()
    await lab.evaluate_all()
    # apex positions + fills (paper and live) and a risk history
    async with apex.aiosqlite.connect(apex.DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO apex_settings(user_id,paused,mode) VALUES (?,1,'live')", (ADMIN,))
        for i in range(160):
            mode = "paper" if i % 3 else "live"
            e = 0.08
            r = rng.choice([-0.5, -0.45, -0.1, -0.1, 0.08, 0.5, 1.3, 2.4, -0.35]) if rng.random() > 0.2 else rng.uniform(-0.2, 0.3)
            t = now - (160 - i) * 2200
            await db.execute("INSERT INTO apex_positions(user_id,mode,mint,name,symbol,status,entry_ts,entry_sol,entry_tokens,entry_price,decimals,tokens_left,profile,clock_start,realized_sol,exit_ts,exit_reason,peak_ratio,curve_type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (ADMIN, mode, f"P{i}", f"Coin{i}", f"C{i}", "closed", t, e, 1000, 1e-4, 6, 0, "S", t, e * (1 + r), t + 300,
                              rng.choice(["stop_loss", "verdict_90s", "trailing_stop", "target_50pct"]), 1 + max(r, 0) * 1.2, "classic"))
        for i in range(3):
            await db.execute("INSERT INTO apex_positions(user_id,mode,mint,name,symbol,status,entry_ts,entry_sol,entry_tokens,entry_price,decimals,tokens_left,profile,clock_start,last_ratio,peak_ratio,curve_type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (ADMIN, "paper", f"O{i}", f"Open{i}", "O", "open", now - 70 * (i + 1), 0.08, 1000, 1e-4, 6, 900, "S", now, 0.9 + i * 0.15, 1.2 + i * .1, "low_start"))
        for i in range(90):
            await db.execute("INSERT INTO apex_slippage_log(ts,mint,side,method,decision_price,fill_price,slippage_bps) VALUES (?,?,?,?,?,?,?)",
                             (now - i * 500, "M", rng.choice(["buy", "sell"]), "jupiter", 1e-4, 1e-4, int(rng.gauss(-350, 400))))
        await db.commit()
    apex._access["admin"] = ADMIN
