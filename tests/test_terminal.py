"""CLEX TERMINAL: authentication, every endpoint, and the front end's safety properties."""
import os, sys, asyncio, time, tempfile, re, json
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "t.db")
os.environ["MASTER_ENCRYPTION_KEY"] = "x" * 43 + "="
sys.path.insert(0, sys.argv[1]); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import seed_terminal as sd
os.environ["TELEGRAM_TOKEN"] = sd.BOT_TOKEN
import apex, lab, terminal
import httpx
from fastapi import FastAPI

app = FastAPI(); app.include_router(terminal.router)


async def main():
    await sd.seed(apex, lab, n_per=140)
    # ── 1. signature verification ──
    ok = sd.sign()
    assert terminal.verify_init_data(ok, sd.BOT_TOKEN)["id"] == sd.ADMIN
    assert terminal.verify_init_data(sd.sign(tamper=True), sd.BOT_TOKEN) is None            # edited payload
    assert terminal.verify_init_data(ok, "999999:wrongtoken") is None                        # another bot's signature
    assert terminal.verify_init_data(sd.sign(auth_date=time.time() - 3 * 86400), sd.BOT_TOKEN) is None   # stale
    assert terminal.verify_init_data("", sd.BOT_TOKEN) is None and terminal.verify_init_data("a=b", sd.BOT_TOKEN) is None
    assert terminal.verify_init_data("hash=zz&user=%7B%7D", sd.BOT_TOKEN) is None
    print("initData verification OK (valid, tampered, wrong bot, stale, empty, malformed)")

    # ── 2. endpoints ──
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        H = {"X-Init-Data": ok}
        # the page is public and holds no data; the API refuses everyone but the owner
        r = await c.get("/terminal"); assert r.status_code == 200 and "CLEX TERMINAL" in r.text
        assert "default-src 'none'" in r.headers["content-security-policy"] and r.headers["cache-control"] == "no-store"
        for path in ("overview", "market", "positions", "system", "variant?id=x"):
            assert (await c.get(f"/terminal/api/{path}")).status_code == 401, path
            assert (await c.get(f"/terminal/api/{path}", headers={"X-Init-Data": sd.sign(user_id=42)})).status_code == 401, path
            assert (await c.get(f"/terminal/api/{path}", headers={"X-Init-Data": sd.sign(tamper=True)})).status_code == 401, path
        print("access control OK (no header, a different Telegram user, a forged payload: all refused on every route)")

        ov = (await c.get("/terminal/api/overview", headers=H)).json()
        assert len(ov["board"]) == 21 and ov["lab"]["min_trades"] == 600 and "passed" in ov["lab"]
        b = {x["name"]: x for x in ov["board"]}
        assert b["MOMO20"]["n"] > 100 and b["MOMO20"]["of"] == 6 and len(b["MOMO20"]["spark"]) > 10
        assert ov["apex"]["credit_ceiling"] == 10_000_000 and ov["flags"]["native_tx"] is False and ov["risk"] is not None
        json.dumps(ov, allow_nan=False)                                   # strict JSON: no NaN / Infinity anywhere
        vid = b["MOMO20"]["id"]
        v = (await c.get(f"/terminal/api/variant?id={vid}", headers=H)).json()
        assert v["v"]["name"] == "MOMO20" and len(v["equity"]) <= 240 and len(v["hist"]) == 25 and sum(v["hist"]) == v["s"]["n"]
        assert len(v["checklist"]) == 6 and v["latency"][0]["s"] == 2 and v["tail"] and v["quantiles"]
        assert isinstance(v["v"]["params"]["entry"], dict)
        assert (await c.get("/terminal/api/variant?id=nope", headers=H)).status_code == 404
        mk = (await c.get("/terminal/api/market", headers=H)).json()
        assert mk["hourly"] and sum(x["n"] for x in mk["peak_dist"]) == mk["peaks_n"] and "CTRL_RANDOM" in mk["ctrl"]
        assert len(mk["ctrl"]["CTRL_RANDOM"]["tod"]) > 5 and mk["daily"]
        ps = (await c.get("/terminal/api/positions", headers=H)).json()
        assert ps["open"] and ps["live"]["n"] > 5 and ps["paper"]["n"] > 5 and ps["slippage"] and ps["closed"]
        assert all(len(p["user"]) <= 4 for p in ps["open"] + ps["closed"]), "user ids must be shortened"
        sy = (await c.get("/terminal/api/system", headers=H)).json()
        keys = {i["k"] for g in sy["config"] for i in g["items"]}
        assert {"HARD_STOP_RATIO", "SCALP_MIN_SOL", "RISK_DAILY_CAP_SOL", "MIN_TRADES"} <= keys and sy["profiles"]["S"]["stop"] == 0.9
        print("endpoints OK (overview, variant, market, positions, system: shapes, strict JSON, 404, shortened user ids)")

        # nothing secret can leak: no key material in any payload
        blob = json.dumps([ov, v, mk, ps, sy])
        assert "encrypted_key" not in blob and "MASTER_ENCRYPTION" not in blob and sd.BOT_TOKEN not in blob
        print("no secrets in any payload OK")

    # ── 3. the front end ──
    html = open(os.path.join(sys.argv[1], "terminal.html"), encoding="utf-8").read()
    assert len(html) < 120_000
    srcs = re.findall(r'<script[^>]+src="([^"]+)"', html)
    assert srcs == ["https://telegram.org/js/telegram-web-app.js"], srcs
    assert "http://" not in html and "eval(" not in html and "document.write" not in html
    assert "innerHTML" in html and html.count("esc(") > 30                   # every dynamic string goes through esc()
    assert "box-shadow" not in html and "text-shadow" not in html and "gradient" not in html and "filter:" not in html
    print("front end OK (one external script, no eval, escaped output, flat style: no glow, shadow or gradient)")
    print("TERMINAL TESTS PASSED")


asyncio.run(main())
