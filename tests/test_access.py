import os, sys, asyncio, time, tempfile, json
tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(tmp, "a.db")
os.environ["MASTER_ENCRYPTION_KEY"] = "x" * 43 + "="
os.environ["ADMIN_ID"] = "1000"
os.environ["OWNER_ONLY"] = "0"            # these tests exercise the invite flow
sys.path.insert(0, sys.argv[1])
import apex, access
from datetime import datetime


class FU:
    def __init__(self, uid, first="Al", last="", username=""): self.id, self.first_name, self.last_name, self.username = uid, first, last, username


class FChat:
    def __init__(self, t="private"): self.type = t


class FMsg:
    def __init__(self, uid, text, chat="private", **u):
        self.from_user = FU(uid, **u); self.chat = FChat(chat); self.text = text; self.out = []
    async def answer(self, text, **k): self.out.append(text); return self


class FCbMsg:
    def __init__(self, chat="private"): self.chat = FChat(chat); self.out = []
    async def edit_text(self, text, **k): self.out.append((text, k.get("reply_markup")))
    async def answer(self, text, **k): self.out.append((text, k.get("reply_markup")))


class FCb:
    def __init__(self, uid, data, chat="private"):
        self.from_user = FU(uid); self.data = data; self.message = FCbMsg(chat); self.alerts = []
    async def answer(self, text=None, show_alert=False, **k): self.alerts.append(text)


async def run_mw(event):
    hits = []
    async def handler(ev, data): hits.append(ev); return "handled"
    r = await access.AccessMiddleware()(handler, event, {})
    return bool(hits), r


async def fake_send(target, text, kb):
    (target.message.out if hasattr(target, "message") else target.out).append((text, kb))


orig_send = apex._send


async def main():
    apex._send = fake_send
    await apex.init_db(); await apex.sn.init_sniper_db()
    access.store.path = os.path.join(tmp, "access.json")
    notes = []
    class Bot:
        async def get_me(self): return type("M", (), {"username": "clex_cbot"})()
        async def send_message(self, uid, text, **k): notes.append((uid, text))
    # existing users are kept on first run (nobody gets locked out)
    await apex.get_settings(77, create=True); await apex.get_settings(78, create=True)
    await access.startup(Bot())
    assert access.is_allowed(77) and access.is_allowed(78) and access.is_admin(1000) and access.is_allowed(1000)
    assert access._bot_username == "clex_cbot"
    print("startup: existing users grandfathered, admin recognised")

    # strangers are locked out
    m = FMsg(5, "/start"); h, _ = await run_mw(m); assert not h and "invite-only" in m.out[0]
    m = FMsg(5, "hello"); await run_mw(m); assert not m.out                         # rate-limited reply (no spam)
    access._denied_at.clear()
    c = FCb(5, "ax:home"); h, _ = await run_mw(c); assert not h and c.alerts == ["Invite only"]
    m = FMsg(5, "/start", chat="group"); h, _ = await run_mw(m); assert not h and not m.out      # groups ignored
    m = FMsg(5, "/myid"); h, _ = await run_mw(m); assert not h and "5" in m.out[0]               # anyone can look up their ID
    m = FMsg(77, "/start"); h, _ = await run_mw(m); assert h                                      # existing user passes
    m = FMsg(1000, "/admin"); h, _ = await run_mw(m); assert h                                    # owner passes
    print("gate OK (strangers denied, /myid open, groups ignored, members + owner pass)")

    # invite lifecycle
    tok, exp = access.create_invite()
    assert abs(exp - time.time() - 1200) < 5 and access.invite_state(tok) == "valid"
    assert access._link(tok) == f"https://t.me/clex_cbot?start=inv_{tok}"
    access._denied_at.clear()
    m = FMsg(6, f"/start inv_{tok}", first="Bo", username="bobby"); h, _ = await run_mw(m)
    assert h and access.is_allowed(6) and access.invite_state(tok) == "used"
    assert any("Bo" in t and "joined" in t for _, t in notes) and notes[-1][0] == 1000               # owner is told
    access._denied_at.clear()
    m = FMsg(7, f"/start inv_{tok}"); h, _ = await run_mw(m); assert not h and "expired or was already used" in m.out[0] and not access.is_allowed(7)
    tok2, _e = access.create_invite(); access.store.invites[tok2]["expires"] = time.time() - 1; access._denied_at.clear()
    m = FMsg(8, f"/start inv_{tok2}"); h, _ = await run_mw(m); assert not h and not access.is_allowed(8)           # expired
    access._denied_at.clear()
    m = FMsg(9, "/start inv_totallyfake"); h, _ = await run_mw(m); assert not h and not access.is_allowed(9)       # guessed
    # an existing member clicking someone else's link does not burn it
    tok3, _e = access.create_invite(); m = FMsg(6, f"/start inv_{tok3}"); h, _ = await run_mw(m)
    assert h and access.invite_state(tok3) == "valid"
    # revoked invite is dead
    assert access.revoke_invite(tok3[:10]) and access.invite_state(tok3) == "unknown"
    # invites expire at 20 minutes on the clock
    tok4, e4 = access.create_invite(); real = time.time
    time.time = lambda: real() + 1201
    try: assert access.invite_state(tok4) == "expired"
    finally: time.time = real
    print("invite links OK (20 min, single use, revoke, fake/expired/used rejected)")

    # persistence: a restart keeps the list and the pending invites
    tok5, _e = access.create_invite()
    fresh = access.Store(access.store.path); assert fresh.load() and "6" in fresh.users and tok5 in fresh.invites
    print("persistence OK")

    # block / unblock / remove through the panel
    cb = FCb(1000, "adm:block:6"); await access.cb_block(cb)
    assert access.is_blocked(6) and not access.is_allowed(6) and any(u == 6 for u, _ in notes)
    access._denied_at.clear(); m = FMsg(6, "/start"); h, _ = await run_mw(m); assert not h and "Access removed" in m.out[0]
    c = FCb(6, "ax:home"); access._denied_at.clear(); h, _ = await run_mw(c); assert not h
    access._denied_at.clear(); tokb, _e = access.create_invite(); m = FMsg(6, f"/start inv_{tokb}"); h, _ = await run_mw(m)
    assert not h and access.is_blocked(6)                                                         # can't use a new invite to get back in
    await apex.get_settings(6, create=True); await apex.set_setting(6, paused=0)
    await apex.get_settings(77, create=True); await apex.set_setting(77, paused=0)
    active = [u["user_id"] for u in await apex._active_users()]
    assert 6 not in active and 77 in active, active                                               # blocked users get no new trades
    await access.cb_unblock(FCb(1000, "adm:unblock:6")); assert access.is_allowed(6)
    assert 6 in [u["user_id"] for u in await apex._active_users()]
    await access.cb_removego(FCb(1000, "adm:removego:6")); assert access._rec(6) is None and not access.is_allowed(6)
    print("block / unblock / remove OK (blocked users locked out and excluded from new trades)")

    # only the owner can use the panel
    stranger = FCb(77, "adm:home"); await access.cb_home(stranger)
    assert stranger.alerts == ["Not allowed"] and not stranger.message.out
    m = FMsg(77, "/admin"); await access.cmd_admin(m); assert not m.out
    owner = FCb(1000, "adm:home"); await access.cb_home(owner); assert "Admin" in owner.message.out[0][0]
    print("owner-only OK")

    # panel pages render with real stats; read-only DB access cannot write
    async with access._ro() as db:
        try:
            await db.execute("INSERT INTO apex_blacklist VALUES ('x','x',0)"); await db.commit(); raise SystemExit("WRITE SUCCEEDED")
        except Exception as e:
            assert "readonly" in str(e).lower(), e
    async with apex.aiosqlite.connect(apex.DB_PATH) as db:
        await db.execute("INSERT INTO apex_positions(user_id,mode,mint,name,symbol,status,entry_ts,entry_sol,entry_tokens,entry_price,decimals,tokens_left,profile,clock_start,realized_sol,exit_ts,exit_reason) VALUES (77,'live','m','n','s','closed',1,0.1,1,1e-7,6,0,'S',1,0.13,2,'target')")
        await db.execute("INSERT INTO apex_positions(user_id,mode,mint,name,symbol,status,entry_ts,entry_sol,entry_tokens,entry_price,decimals,tokens_left,profile,clock_start) VALUES (77,'live','m2','n','s','open',1,0.1,1,1e-7,6,1,'S',1)")
        await db.commit()
    await apex.set_setting(77, entry="scalp")
    access.store.users["77"].update(name="Ann <b>", username="ann")
    cb = FCb(1000, "adm:users:0"); await access.cb_users(cb); page = cb.message.out[0][0]
    assert "Ann &lt;b&gt;" in page and "1 open" in page and "+0.030 SOL" in page, page
    cb = FCb(1000, "adm:u:77"); await access.cb_user(cb); detail = cb.message.out[0][0]
    assert "Scalp" in detail and "1 open · 1 closed" in detail and "+0.0300 SOL" in detail and "running" in detail, detail
    cb = FCb(1000, "adm:inv"); await access.cb_inv(cb); assert "20 minutes" in cb.message.out[0][0] and "t.me/clex_cbot?start=inv_" in cb.message.out[0][0]
    cb = FCb(1000, "adm:invs"); await access.cb_invs(cb); assert "Pending invites" in cb.message.out[0][0]
    t, kb = await apex._home(1000); assert any("Admin" in b.text for r in kb.inline_keyboard for b in r)
    t, kb = await apex._home(77); assert not any("Admin" in b.text for r in kb.inline_keyboard for b in r)
    print("admin pages OK (stats read-only, names escaped, admin button owner-only)")

    # pagination
    for i in range(20): access.store.users[str(5000 + i)] = {"status": "allowed", "name": f"U{i}", "username": "", "joined": 1000 + i, "via": "invite"}
    cb = FCb(1000, "adm:users:1"); await access.cb_users(cb)
    assert any("Prev" in b.text for r in cb.message.out[0][1].inline_keyboard for b in r)
    print("pagination OK")

    # the real aiogram dispatcher: the gate wraps every router
    apex._send = orig_send
    try:
        from aiogram import Bot as ABot, Dispatcher
        from aiogram.client.session.base import BaseSession
        from aiogram.types import Update, Message as AM, Chat, User
        class Cap(BaseSession):
            def __init__(self): super().__init__(); self.calls = []
            async def close(self): pass
            async def make_request(self, bot, method, timeout=None):
                self.calls.append(method)
                return AM(message_id=1, date=datetime.now(), chat=Chat(id=1, type="private"))
            async def stream_content(self, *a, **k):
                if False: yield b""
        sess = Cap(); bot = ABot("123456:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", session=sess)
        dp = Dispatcher(); access.install(dp); dp.include_router(access.router); dp.include_router(apex.router)
        def upd(uid, text, i): return Update(update_id=i, message=AM(message_id=i, date=datetime.now(), chat=Chat(id=uid, type="private"),
                                              from_user=User(id=uid, is_bot=False, first_name="X"), text=text))
        access._denied_at.clear()
        await dp.feed_update(bot, upd(555, "/start", 1))
        sent = [c for c in sess.calls if c.__class__.__name__ == "SendMessage"]
        assert sent and "invite-only" in sent[-1].text and sent[-1].chat_id == 555, [getattr(c, "text", c) for c in sess.calls]
        assert not any("CLEX APEX" in getattr(c, "text", "") for c in sess.calls)              # no Apex page leaked
        sess.calls.clear(); await dp.feed_update(bot, upd(1000, "/admin", 2))
        assert any("Admin" in getattr(c, "text", "") for c in sess.calls)
        sess.calls.clear(); access._denied_at.clear(); await dp.feed_update(bot, upd(556, "/admin", 3))
        assert not any("🛡" in getattr(c, "text", "") for c in sess.calls)
        print("real dispatcher OK (stranger blocked before any router; owner reaches /admin; stranger can't)")
    except ImportError as e:
        print("dispatcher test skipped:", e)
    print("ACCESS TESTS PASSED")

asyncio.run(main())
