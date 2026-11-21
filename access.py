"""
CLEX Apex — private access, invite links and the owner's admin panel.

Self-contained on purpose:
  * It does NOT touch the database schema or any encryption code.
  * Who is allowed in, and the pending invite links, live in a small JSON file next to the
    database (the Railway volume), written atomically. To move this into your own database later,
    replace the load()/save() methods of `Store` — nothing else needs to change.
  * User statistics for the panel are read from the Apex tables strictly READ-ONLY
    (SQLite is opened with mode=ro, so a write would raise).

Rules
  * The owner is the Telegram ID in the ADMIN_ID environment variable. Only the owner sees the panel.
  * Everyone else must arrive through an invite link: single use, expires after 20 minutes.
  * Blocked users are locked out of the bot and the sniper stops opening new trades for them;
    their already-open positions keep being managed so nothing is stranded.
"""
import asyncio
import html
import json
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiosqlite
from aiogram import BaseMiddleware, F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

import apex

logger = logging.getLogger("access")

DB_PATH = os.getenv("DB_PATH", "clex.db")
ACCESS_FILE = os.getenv("ACCESS_FILE") or str(Path(DB_PATH).resolve().parent / "access.json")
INVITE_TTL = 20 * 60
PAGE_SIZE = 8
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or 0)
except ValueError:
    ADMIN_ID = 0

RULE = apex.RULE
_bot = None
_bot_username = ""
_denied_at: Dict[int, float] = {}


# ══════════════════════════════════════════════════════════════════════════════
# STORE (swap load/save for your own database whenever you like)
# ══════════════════════════════════════════════════════════════════════════════
class Store:
    def __init__(self, path: str):
        self.path = path
        self.users: Dict[str, Dict] = {}      # uid -> {status, name, username, joined, via}
        self.invites: Dict[str, Dict] = {}    # token -> {created, expires, used_by}

    def load(self) -> bool:
        """True if a saved file existed."""
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.users = data.get("users", {})
            self.invites = data.get("invites", {})
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.error(f"access file unreadable ({e}) — starting empty and failing closed")
            return True

    def save(self):
        tmp = self.path + ".tmp"
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"users": self.users, "invites": self.invites}, f)
            os.replace(tmp, self.path)
        except Exception as e:
            logger.error(f"access file not saved: {e}")


store = Store(ACCESS_FILE)


def is_admin(uid: int) -> bool:
    return bool(ADMIN_ID) and uid == ADMIN_ID


def _rec(uid: int) -> Optional[Dict]:
    return store.users.get(str(uid))


def is_allowed(uid: int) -> bool:
    if is_admin(uid):
        return True
    r = _rec(uid)
    return bool(r and r.get("status") == "allowed")


def is_blocked(uid: int) -> bool:
    r = _rec(uid)
    return bool(r and r.get("status") == "blocked")


def _prune():
    now = time.time()
    dead = [t for t, v in store.invites.items()
            if v["expires"] < now - 3600 or (v.get("used_by") and v["expires"] < now)]
    for t in dead:
        store.invites.pop(t, None)
    if dead:
        store.save()


def create_invite() -> Tuple[str, float]:
    _prune()
    token = secrets.token_urlsafe(12)
    exp = time.time() + INVITE_TTL
    store.invites[token] = {"created": time.time(), "expires": exp, "used_by": None}
    store.save()
    return token, exp


def invite_state(token: str) -> str:
    v = store.invites.get(token)
    if not v:
        return "unknown"
    if v.get("used_by"):
        return "used"
    if v["expires"] < time.time():
        return "expired"
    return "valid"


def redeem(token: str, uid: int, name: str, username: str) -> bool:
    """Consume a valid, unused, unexpired invite and grant access."""
    if invite_state(token) != "valid" or is_blocked(uid):
        return False
    store.invites[token]["used_by"] = uid
    store.users[str(uid)] = {"status": "allowed", "name": name, "username": username,
                             "joined": time.time(), "via": "invite"}
    store.save()
    return True


def revoke_invite(prefix: str) -> bool:
    for t in list(store.invites):
        if t.startswith(prefix):
            store.invites.pop(t, None)
            store.save()
            return True
    return False


def pending_invites() -> List[Tuple[str, float]]:
    now = time.time()
    return sorted(((t, v["expires"]) for t, v in store.invites.items()
                   if not v.get("used_by") and v["expires"] > now), key=lambda x: x[1])


def _touch(uid: int, user) -> None:
    r = _rec(uid)
    if not r:
        return
    name = " ".join(x for x in (getattr(user, "first_name", "") or "", getattr(user, "last_name", "") or "") if x)
    uname = getattr(user, "username", "") or ""
    if (name and name != r.get("name")) or (uname != r.get("username", "")):
        r["name"], r["username"] = name or r.get("name", ""), uname
        store.save()


# ══════════════════════════════════════════════════════════════════════════════
# GATE — every message and button press goes through this first
# ══════════════════════════════════════════════════════════════════════════════
class AccessMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        is_cb = hasattr(event, "data") and hasattr(event, "message")
        chat = getattr(getattr(event, "message", None), "chat", None) if is_cb else getattr(event, "chat", None)
        if user is None or (chat is not None and getattr(chat, "type", "private") != "private"):
            return None                                   # ignore groups, channels, anonymous senders
        uid = user.id
        text = "" if is_cb else (getattr(event, "text", None) or "")

        if text.split("@")[0].strip() == "/myid":          # lets the owner find their ID for ADMIN_ID
            await event.answer(f"Your Telegram ID: <code>{uid}</code>", parse_mode=ParseMode.HTML)
            return None

        if is_allowed(uid):
            _touch(uid, user)
            return await handler(event, data)

        if is_blocked(uid):
            await _deny(event, is_cb, "🚫 <b>Access removed.</b>\nThis account no longer has access to CLEX Apex.")
            return None

        if not is_cb and text.startswith("/start inv_"):
            token = text.split(" ", 1)[1][4:].strip()
            name = " ".join(x for x in (user.first_name or "", getattr(user, "last_name", "") or "") if x)
            if redeem(token, uid, name, user.username or ""):
                await _tell_admin(f"✅ <b>{html.escape(name or str(uid))}</b>"
                                  f"{(' @' + user.username) if user.username else ''} joined with an invite "
                                  f"(<code>{uid}</code>).")
                return await handler(event, data)
            await _deny(event, is_cb, "⌛ <b>That invite link has expired or was already used.</b>\n"
                                      "Links last 20 minutes and work once. Ask the owner for a new one.")
            return None

        await _deny(event, is_cb, "🔒 <b>CLEX Apex is invite-only.</b>\nAsk the owner for an invite link — "
                                  "they expire after 20 minutes.")
        return None


async def _deny(event, is_cb: bool, text: str):
    uid = event.from_user.id
    now = time.time()
    if now - _denied_at.get(uid, 0) < 30:                 # don't let strangers make the bot chatter
        if is_cb:
            await event.answer()
        return
    _denied_at[uid] = now
    if len(_denied_at) > 2000:
        _denied_at.clear()
    try:
        if is_cb:
            await event.answer("Invite only", show_alert=True)
        else:
            await event.answer(text, parse_mode=ParseMode.HTML)
    except TelegramAPIError:
        pass


async def _tell_admin(text: str):
    if _bot and ADMIN_ID:
        try:
            await _bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.HTML)
        except TelegramAPIError as e:
            logger.warning(f"admin notify failed: {e}")


def install(dp):
    mw = AccessMiddleware()
    dp.message.outer_middleware(mw)
    dp.callback_query.outer_middleware(mw)


# ══════════════════════════════════════════════════════════════════════════════
# READ-ONLY STATS from the Apex tables
# ══════════════════════════════════════════════════════════════════════════════
def _ro():
    uri = Path(DB_PATH).resolve().as_uri() + "?mode=ro"
    return aiosqlite.connect(uri, uri=True, timeout=10)


async def _db_user_ids() -> List[int]:
    ids = set()
    try:
        async with _ro() as db:
            for table in ("apex_settings", "sniper_users", "apex_positions"):
                try:
                    cur = await db.execute(f"SELECT DISTINCT user_id FROM {table}")
                    ids.update(r[0] for r in await cur.fetchall())
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"could not read existing users: {e}")
    return sorted(ids)


async def user_stats(uids: List[int]) -> Dict[int, Dict]:
    out = {u: {"open": 0, "closed": 0, "net": 0.0, "entry": "apex", "running": False,
               "wallet": None, "size": "—", "profile": "—"} for u in uids}
    try:
        async with _ro() as db:
            cur = await db.execute(
                "SELECT user_id, paused, entry, size_usd, size_sol, profile FROM apex_settings")
            for uid, paused, entry, usd, sol, prof in await cur.fetchall():
                if uid in out:
                    out[uid].update(running=not paused, entry=entry or "apex",
                                    size=(f"{sol:g} SOL" if sol else f"${usd:g}"), profile=prof)
            cur = await db.execute("SELECT user_id, pubkey FROM sniper_users")
            for uid, pub in await cur.fetchall():
                if uid in out:
                    out[uid]["wallet"] = pub
            cur = await db.execute(
                "SELECT user_id, status, COUNT(*), COALESCE(SUM(realized_sol - entry_sol), 0) "
                "FROM apex_positions GROUP BY user_id, status")
            for uid, status, n, net in await cur.fetchall():
                if uid in out:
                    if status == "open":
                        out[uid]["open"] = n
                    elif status == "closed":
                        out[uid]["closed"], out[uid]["net"] = n, float(net)
    except Exception as e:
        logger.warning(f"stats unavailable: {e}")
    return out


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN PANEL (owner only)
# ══════════════════════════════════════════════════════════════════════════════
router = Router()
_kb = apex._kb
ADMIN_BTN = [("🛡 Admin", "adm:home")]


def _label(uid: str, r: Dict) -> str:
    name = r.get("name") or uid
    return html.escape(name) + (f" @{html.escape(r['username'])}" if r.get("username") else "")


def _members() -> List[Tuple[str, Dict]]:
    return sorted(((u, r) for u, r in store.users.items() if int(u) != ADMIN_ID),
                  key=lambda x: -x[1].get("joined", 0))


async def _guard(target) -> bool:
    uid = target.from_user.id
    if is_admin(uid):
        return True
    if hasattr(target, "data"):                            # a button press: tell them, quietly
        await target.answer("Not allowed", show_alert=True)
    return False


async def _admin_home():
    members = _members()
    stats = await user_stats([int(u) for u, _ in members])
    allowed = sum(1 for _, r in members if r["status"] == "allowed")
    blocked = len(members) - allowed
    running = sum(1 for u, r in members if r["status"] == "allowed" and stats[int(u)]["running"])
    open_pos = sum(s["open"] for s in stats.values())
    text = (f"<b>🛡 Admin</b>\n{RULE}\n"
            f"<code>Users     </code>{len(members)}  (✅ {allowed} · 🚫 {blocked})\n"
            f"<code>Sniping   </code>{running} running now\n"
            f"<code>Open pos. </code>{open_pos}\n"
            f"<code>Invites   </code>{len(pending_invites())} pending\n"
            f"{RULE}\n<i>Only you can see this panel.</i>")
    return text, _kb([[("👥 Users", "adm:users:0"), ("➕ New invite link", "adm:inv")],
                      [("🔗 Pending invites", "adm:invs"), ("🗄 Data", "adm:data")], [("‹ Home", "ax:home")]])


@router.message(Command("admin"))
async def cmd_admin(m: Message):
    if not is_admin(m.from_user.id):
        return                                              # silent: don't reveal the panel exists
    t, kb = await _admin_home()
    await apex._send(m, t, kb)


@router.callback_query(F.data == "adm:home")
async def cb_home(q: CallbackQuery):
    if not await _guard(q):
        return
    t, kb = await _admin_home()
    await apex._send(q, t, kb)


@router.callback_query(F.data.startswith("adm:users:"))
async def cb_users(q: CallbackQuery):
    if not await _guard(q):
        return
    page = max(0, int(q.data.split(":")[2]))
    members = _members()
    chunk = members[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    stats = await user_stats([int(u) for u, _ in chunk])
    if not members:
        await apex._send(q, f"<b>👥 Users</b>\n{RULE}\nNobody has joined yet. Create an invite link.",
                         _kb([[("➕ New invite link", "adm:inv")], [("‹ Admin", "adm:home")]]))
        return
    rows, lines = [], [f"<b>👥 Users</b> · {len(members)}\n{RULE}"]
    for u, r in chunk:
        s = stats[int(u)]
        icon = "🚫" if r["status"] == "blocked" else ("🟢" if s["running"] else "⏸")
        lines.append(f"{icon} {_label(u, r)} — {s['open']} open · {s['net']:+.3f} SOL")
        rows.append([(f"{icon} {(r.get('name') or u)[:22]}", f"adm:u:{u}")])
    nav = []
    if page > 0:
        nav.append(("‹ Prev", f"adm:users:{page - 1}"))
    if (page + 1) * PAGE_SIZE < len(members):
        nav.append(("Next ›", f"adm:users:{page + 1}"))
    await apex._send(q, "\n".join(lines), _kb(rows + ([nav] if nav else []) + [[("‹ Admin", "adm:home")]]))


async def _user_page(uid: str):
    r = _rec(int(uid))
    if not r:
        return "That user no longer exists.", _kb([[("‹ Users", "adm:users:0")]])
    s = (await user_stats([int(uid)]))[int(uid)]
    ent = apex.ENTRIES.get(s["entry"], apex.ENTRIES["apex"])
    joined = datetime.fromtimestamp(r.get("joined", 0), timezone.utc).strftime("%Y-%m-%d")
    status = "🚫 Blocked" if r["status"] == "blocked" else "✅ Allowed"
    text = (f"<b>👤 {_label(uid, r)}</b>\n{RULE}\n"
            f"<code>ID        </code>{uid}\n"
            f"<code>Joined    </code>{joined} ({r.get('via', '?')})\n"
            f"<code>Access    </code>{status}\n"
            f"<code>Sniper    </code>{'● running' if s['running'] else '■ stopped'}\n"
            f"<code>Strategy  </code>{ent['icon']} {ent['name']}\n"
            f"<code>Size      </code>{s['size']}\n"
            f"<code>Wallet    </code>{apex._short(s['wallet']) if s['wallet'] else 'none'}\n"
            f"<code>Positions </code>{s['open']} open · {s['closed']} closed\n"
            f"<code>Net P&amp;L   </code>{s['net']:+.4f} SOL\n{RULE}\n"
            + ("<i>Blocked: no access, no new trades. Open positions are still managed.</i>"
               if r["status"] == "blocked" else ""))
    toggle = ([("✅ Unblock", f"adm:unblock:{uid}")] if r["status"] == "blocked"
              else [("🚫 Block", f"adm:block:{uid}")])
    return text, _kb([toggle + [("🗑 Remove", f"adm:remove:{uid}")],
                      [("↻ Refresh", f"adm:u:{uid}")], [("‹ Users", "adm:users:0")], ADMIN_BTN])


@router.callback_query(F.data.startswith("adm:u:"))
async def cb_user(q: CallbackQuery):
    if not await _guard(q):
        return
    t, kb = await _user_page(q.data.split(":")[2])
    await apex._send(q, t, kb)


@router.callback_query(F.data.startswith("adm:block:"))
async def cb_block(q: CallbackQuery):
    if not await _guard(q):
        return
    uid = q.data.split(":")[2]
    r = _rec(int(uid))
    if r:
        r["status"] = "blocked"
        store.save()
        await _tell_user(int(uid), "🚫 Your access to CLEX Apex was removed by the owner. "
                                   "Your open positions are still being managed.")
    t, kb = await _user_page(uid)
    await apex._send(q, t, kb)


@router.callback_query(F.data.startswith("adm:unblock:"))
async def cb_unblock(q: CallbackQuery):
    if not await _guard(q):
        return
    uid = q.data.split(":")[2]
    r = _rec(int(uid))
    if r:
        r["status"] = "allowed"
        store.save()
        await _tell_user(int(uid), "✅ Your access to CLEX Apex was restored.")
    t, kb = await _user_page(uid)
    await apex._send(q, t, kb)


@router.callback_query(F.data.startswith("adm:remove:"))
async def cb_remove(q: CallbackQuery):
    if not await _guard(q):
        return
    uid = q.data.split(":")[2]
    r = _rec(int(uid)) or {}
    await apex._send(q, f"<b>Remove {_label(uid, r)}?</b>\n{RULE}\nThey lose access and need a brand-new "
                        "invite link to return. Their wallet and open positions are untouched (this "
                        "panel never edits the database).",
                     _kb([[("Yes, remove", f"adm:removego:{uid}"), ("Cancel", f"adm:u:{uid}")]]))


@router.callback_query(F.data.startswith("adm:removego:"))
async def cb_removego(q: CallbackQuery):
    if not await _guard(q):
        return
    store.users.pop(q.data.split(":")[2], None)
    store.save()
    await cb_users_page0(q)


async def cb_users_page0(q: CallbackQuery):
    q.data = "adm:users:0"
    await cb_users(q)


async def _tell_user(uid: int, text: str):
    if _bot:
        try:
            await _bot.send_message(uid, text, parse_mode=ParseMode.HTML)
        except TelegramAPIError:
            pass


def _link(token: str) -> str:
    return f"https://t.me/{_bot_username}?start=inv_{token}" if _bot_username else f"inv_{token}"


@router.callback_query(F.data == "adm:inv")
async def cb_inv(q: CallbackQuery):
    if not await _guard(q):
        return
    token, exp = create_invite()
    until = datetime.fromtimestamp(exp, timezone.utc).strftime("%H:%M UTC")
    await apex._send(q, f"<b>🔗 Invite link</b>\n{RULE}\n<code>{_link(token)}</code>\n\n"
                        f"⏳ Expires in <b>20 minutes</b> (until {until})\n☝️ Works <b>once</b> — the first "
                        "person to open it gets in.\n\nTap the link to copy, then send it to them.",
                     _kb([[("➕ Another", "adm:inv"), ("🔗 Pending", "adm:invs")], [("‹ Admin", "adm:home")]]))


@router.callback_query(F.data == "adm:invs")
async def cb_invs(q: CallbackQuery):
    if not await _guard(q):
        return
    _prune()
    pend = pending_invites()
    if not pend:
        await apex._send(q, f"<b>🔗 Pending invites</b>\n{RULE}\nNone. Links expire after 20 minutes.",
                         _kb([[("➕ New invite link", "adm:inv")], [("‹ Admin", "adm:home")]]))
        return
    rows, lines = [], [f"<b>🔗 Pending invites</b> · {len(pend)}\n{RULE}"]
    for t, exp in pend:
        mins = max(1, int((exp - time.time()) / 60))
        lines.append(f"<code>…{t[-6:]}</code> — {mins} min left")
        rows.append([(f"Revoke …{t[-6:]}", f"adm:irev:{t[:10]}")])
    await apex._send(q, "\n".join(lines), _kb(rows + [[("‹ Admin", "adm:home")]]))


@router.callback_query(F.data.startswith("adm:irev:"))
async def cb_irev(q: CallbackQuery):
    if not await _guard(q):
        return
    revoke_invite(q.data.split(":")[2])
    await cb_invs(q)


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════
async def startup(bot):
    """Call after the Apex database is ready."""
    global _bot, _bot_username
    _bot = bot
    try:
        _bot_username = (await bot.get_me()).username or ""
    except Exception as e:
        logger.warning(f"get_me failed: {e}")
    existed = store.load()
    if not existed:
        # First run only: everyone already using the bot keeps access, so nobody is locked out.
        for uid in await _db_user_ids():
            if uid != ADMIN_ID:
                store.users[str(uid)] = {"status": "allowed", "name": "", "username": "",
                                         "joined": time.time(), "via": "existing"}
        store.save()
        logger.info(f"Access list created — {len(store.users)} existing users kept")
    apex.set_access_hooks(is_blocked=lambda uid: is_blocked(uid) or (not is_allowed(uid)),
                          admin_id=ADMIN_ID)
    if not ADMIN_ID:
        logger.warning("ADMIN_ID is not set: the admin panel is disabled. Send /myid to the bot, "
                       "then set ADMIN_ID in Railway Variables.")
    else:
        logger.info(f"Admin panel enabled for Telegram ID {ADMIN_ID}; {len(store.users)} users on the list")
