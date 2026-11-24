"""PRIVATE MODE: with OWNER_ONLY on (the default) nobody but ADMIN_ID can use anything."""
import os, sys, asyncio, time, tempfile
tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(tmp, "p.db")
os.environ["MASTER_ENCRYPTION_KEY"] = "x" * 43 + "="
os.environ["ADMIN_ID"] = "1000"
os.environ.pop("OWNER_ONLY", None)                     # the default must be private
os.environ["TELEGRAM_TOKEN"] = "123456789:AAFakeTokenForTestsOnlyNotARealTelegramBotTokenXX"
sys.path.insert(0, sys.argv[1]); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import apex, access, lab, terminal
import seed_terminal as sd
import httpx
from fastapi import FastAPI

assert access.OWNER_ONLY is True, "private mode must be ON by default"


class FU:
    def __init__(self, uid): self.id, self.first_name, self.last_name, self.username = uid, "X", "", ""


class FMsg:
    def __init__(self, uid, text): self.from_user = FU(uid); self.chat = type("C", (), {"type": "private"})(); self.text = text; self.out = []
    async def answer(self, text, **k): self.out.append(text)


class FCb:
    def __init__(self, uid, data): self.from_user = FU(uid); self.data = data; self.message = FMsg(uid, ""); self.alerts = []
    async def answer(self, text=None, show_alert=False, **k): self.alerts.append(text)


async def run_mw(ev):
    hits = []
    async def handler(e, d): hits.append(e); return "ok"
    await access.AccessMiddleware()(handler, ev, {})
    return bool(hits)


async def main():
    await apex.init_db(); await apex.sn.init_sniper_db()
    access.store.path = os.path.join(tmp, "access.json")
    # a user who joined earlier and was fully "allowed" before private mode
    access.store.users["2000"] = {"status": "allowed", "name": "Old Client", "username": "", "joined": time.time(), "via": "invite"}
    await apex.get_settings(2000, create=True); await apex.set_setting(2000, paused=0)
    await apex.get_settings(1000, create=True); await apex.set_setting(1000, paused=0)
    class Bot:
        async def get_me(self): return type("M", (), {"username": "clex_cbot"})()
        async def send_message(self, *a, **k): pass
    await access.startup(Bot())

    assert access.is_allowed(1000) and not access.is_allowed(2000) and not access.is_allowed(3000)
    assert await run_mw(FMsg(1000, "/start")) and await run_mw(FCb(1000, "lab:home"))
    for uid in (2000, 3000):
        for ev in (FMsg(uid, "/start"), FMsg(uid, "/admin"), FMsg(uid, "/start inv_abc"), FCb(uid, "lab:home"), FCb(uid, "adm:home"),
                   FCb(uid, "ax:start"), FCb(uid, "vlt:home")):
            access._denied_at.clear()
            assert not await run_mw(ev), (uid, getattr(ev, "text", None) or ev.data)
    m = FMsg(3000, "/start"); access._denied_at.clear(); await run_mw(m)
    assert m.out and "private" in m.out[0].lower() and "invite" not in m.out[0].lower(), m.out      # no hint of how to get in
    print("gate OK (owner passes; earlier members and strangers are refused on every entry point, no invite hint)")

    # invites cannot be created through the panel or redeemed
    token, _ = access.create_invite()
    assert access.redeem(token, 3000, "N", "n") is False and not access.is_allowed(3000)
    print("invites dead OK (a valid token still cannot be redeemed)")

    # the trading engine: only the owner is active; others get no new entries (open positions stay managed)
    users = {u["user_id"] for u in await apex._active_users()}
    assert users == {1000}, users
    assert apex._access["deny"](2000) and not apex._access["deny"](1000)
    print("engine OK (only the owner is an active user; client entries stopped)")

    # Terminal and Lab data: only the owner
    await apex.init_db(); await lab.init_db()
    app = FastAPI(); app.include_router(terminal.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        for who in (None, sd.sign(user_id=2000), sd.sign(user_id=3000)):
            h = {"X-Init-Data": who} if who else {}
            for p in ("overview", "market", "positions", "system"):
                assert (await c.get(f"/terminal/api/{p}", headers=h)).status_code == 401
        assert (await c.get("/terminal/api/overview", headers={"X-Init-Data": sd.sign(user_id=1000)})).status_code == 200
    q = FCb(2000, "lab:home"); await lab.cb_lab(q)
    assert q.alerts, "the Lab must refuse non-owners even if the gate were bypassed"
    print("terminal + lab OK (owner only, independent of the gate)")

    # releasing is a deliberate switch
    src = open(os.path.join(sys.argv[1], "access.py"), encoding="utf-8").read()
    assert 'os.getenv("OWNER_ONLY", "1") != "0"' in src
    print("PRIVATE MODE TESTS PASSED")


asyncio.run(main())
