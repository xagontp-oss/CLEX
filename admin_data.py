"""
CLEX Apex — owner-only data viewer (read-only).

Shows the contents of the bot's tables inside Telegram, for the ADMIN_ID user only, and exports
them as CSV. Everything here is READ-ONLY (the database is opened with mode=ro).

Private keys: the `encrypted_key` column of `sniper_users` is never selected, so it cannot appear
on screen or in an export — only its length is shown. Wallet addresses (public) are shown.
"""
import csv
import html
import io
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from aiogram import F, Router
from aiogram.exceptions import TelegramAPIError
from aiogram.types import BufferedInputFile, CallbackQuery

import access
import apex

logger = logging.getLogger("admin_data")
router = Router()
RULE = apex.RULE
_kb = apex._kb
PAGE = 6
MAX_CSV_ROWS = 50_000

# Columns we allow out of each table. `encrypted_key` is deliberately absent.
TABLES: Dict[str, Dict] = {
    "positions": {"title": "🎯 Trades", "table": "apex_positions", "order": "id DESC",
                  "cols": "id,user_id,mode,mint,name,symbol,status,profile,entry_ts,exit_ts,entry_sol,"
                          "realized_sol,entry_tokens,entry_price,entry_mcap,tokens_left,peak_ratio,"
                          "last_ratio,exit_reason,buy_tx"},
    "settings": {"title": "⚙️ Settings", "table": "apex_settings", "order": "user_id",
                 "cols": "user_id,entry,profile,size_usd,size_sol,mode,paused,risk_ack"},
    "blacklist": {"title": "🚫 Blacklist", "table": "apex_blacklist", "order": "ts DESC",
                  "cols": "mint,reason,ts"},
    "usage": {"title": "🧮 Credit usage", "table": "apex_usage", "order": "day DESC",
              "cols": "day,credits"},
    "wallets": {"title": "👛 Wallets", "table": "sniper_users", "order": "setup_at DESC",
                "cols": "user_id,pubkey,risk_profile,sniper_enabled,setup_at,last_buy_at,custom_amount"},
}


def _ts(t: Optional[float]) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%m-%d %H:%M") if t else "—"


async def _count(table: str) -> int:
    try:
        async with access._ro() as db:
            cur = await db.execute(f"SELECT COUNT(*) FROM {table}")
            return (await cur.fetchone())[0]
    except Exception:
        return 0


async def _guard(q) -> bool:
    return await access._guard(q)


def _name(uid: int) -> str:
    r = access._rec(uid)
    if r and r.get("name"):
        return html.escape(r["name"])
    return "owner" if access.is_admin(uid) else str(uid)


# ══════════════════════════════════════════════════════════════════════════════
@router.callback_query(F.data == "adm:data")
async def cb_data(q: CallbackQuery):
    if not await _guard(q):
        return
    counts = {k: await _count(v["table"]) for k, v in TABLES.items()}
    text = (f"<b>🗄 Data</b>\n{RULE}\nRead-only view of the bot's database. Only you can see this.\n\n"
            + "\n".join(f"{v['title']}  <code>{counts[k]}</code>" for k, v in TABLES.items())
            + "\n\n<i>Wallet keys are never shown — only their addresses.</i>")
    rows = [[(f"{v['title']} ({counts[k]})", f"adm:d:{k}:new:0")] for k, v in TABLES.items()]
    await apex._send(q, text, _kb(rows + [[("‹ Admin", "adm:home")]]))


def _nav(key: str, sort: str, page: int, total: int, extra: Optional[List] = None):
    nav = []
    if page > 0:
        nav.append(("‹ Prev", f"adm:d:{key}:{sort}:{page - 1}"))
    if (page + 1) * PAGE < total:
        nav.append(("Next ›", f"adm:d:{key}:{sort}:{page + 1}"))
    rows = ([nav] if nav else []) + (extra or [])
    rows.append([("📤 Export CSV", f"adm:csv:{key}"), ("↻", f"adm:d:{key}:{sort}:{page}")])
    rows.append([("‹ Data", "adm:data"), ("🛡 Admin", "adm:home")])
    return _kb(rows)


# ── trades ───────────────────────────────────────────────────────────────────
async def _positions_page(sort: str, page: int):
    where, order = {
        "new": ("WHERE mode='live'", "ORDER BY entry_ts DESC"),
        "best": ("WHERE mode='live' AND status='closed'", "ORDER BY (realized_sol-entry_sol) DESC"),
        "worst": ("WHERE mode='live' AND status='closed'", "ORDER BY (realized_sol-entry_sol) ASC"),
    }.get(sort, ("WHERE mode='live'", "ORDER BY entry_ts DESC"))
    async with access._ro() as db:
        db.row_factory = None
        cur = await db.execute("SELECT COUNT(*), SUM(status='closed'), SUM(status='open'), "
                               "SUM(CASE WHEN status='closed' AND realized_sol>entry_sol THEN 1 ELSE 0 END), "
                               "COALESCE(SUM(CASE WHEN status='closed' THEN realized_sol-entry_sol END),0) "
                               "FROM apex_positions WHERE mode='live'")
        n, closed, opened, wins, net = await cur.fetchone()
        cur = await db.execute(
            "SELECT id,user_id,name,symbol,status,profile,entry_ts,exit_ts,entry_sol,realized_sol,"
            "peak_ratio,last_ratio,exit_reason,entry_mcap,mint FROM apex_positions "
            f"{where} {order} LIMIT ? OFFSET ?", (PAGE, page * PAGE))
        rows = await cur.fetchall()
    closed, opened, wins = closed or 0, opened or 0, wins or 0
    head = (f"<b>🎯 Trades</b> · {n}\n{RULE}\n"
            f"Closed <b>{closed}</b> · open <b>{opened}</b> · wins <b>{wins}</b> "
            f"({(wins / closed * 100 if closed else 0):.0f}%) · net <b>{net:+.4f} SOL</b>\n")
    lines = [head]
    for (pid, uid, name, sym, status, prof, ent, ext, esol, rsol, peak, last, why, mc, mint) in rows:
        if status == "closed":
            pnl = rsol - esol
            ico = "🟢" if pnl > 0 else "🔴"
            body = (f"{esol:.4f} → {rsol:.4f} SOL ({pnl:+.4f}, {pnl / esol * 100:+.0f}%)\n"
                    f"   {_ts(ent)}→{_ts(ext)} UTC · {(why or '').replace('_', ' ')} · peak x{peak or 1:.2f}")
        else:
            ico = "⏳"
            body = f"in {esol:.4f} SOL · open · now x{last or 1:.2f} · peak x{peak or 1:.2f}\n   since {_ts(ent)} UTC"
        lines.append(f"{ico} <b>{html.escape(name or mint[:6])}</b> <code>${html.escape(sym or '?')}</code> "
                     f"· {prof} · {_name(uid)}\n   {body}\n   mcap ${mc or 0:,.0f}")
    if not rows:
        lines.append("No trades yet.")
    sorts = [[("🕒 Newest", "adm:d:positions:new:0"), ("🏆 Best", "adm:d:positions:best:0"),
              ("📉 Worst", "adm:d:positions:worst:0")]]
    total = n if sort == "new" else closed
    return "\n".join(lines)[:3900], _nav("positions", sort, page, total, sorts)


# ── generic small tables ─────────────────────────────────────────────────────
async def _simple_page(key: str, page: int):
    spec = TABLES[key]
    async with access._ro() as db:
        db.row_factory = None
        cur = await db.execute(f"SELECT COUNT(*) FROM {spec['table']}")
        total = (await cur.fetchone())[0]
        cur = await db.execute(f"SELECT {spec['cols']} FROM {spec['table']} ORDER BY {spec['order']} "
                               "LIMIT ? OFFSET ?", (PAGE * (2 if key == 'usage' else 1), page * PAGE))
        rows = await cur.fetchall()
    out = [f"<b>{spec['title']}</b> · {total}\n{RULE}"]
    if key == "settings":
        for uid, entry, prof, usd, sol, mode, paused, ack in rows:
            ent = apex.ENTRIES.get(entry, apex.ENTRIES["apex"])
            size = f"{sol:g} SOL" if sol else f"${usd:g}"
            out.append(f"{'■' if paused else '●'} <b>{_name(uid)}</b> <code>{uid}</code>\n"
                       f"   {ent['icon']} {ent['name']} · exit {prof} · {size} · "
                       f"{'stopped' if paused else 'RUNNING'} · risk-ack {'yes' if ack else 'no'}")
    elif key == "blacklist":
        for mint, reason, ts in rows:
            out.append(f"🚫 <code>{mint}</code>\n   {(reason or '').replace('_', ' ')} · {_ts(ts)} UTC")
    elif key == "usage":
        month = time.strftime("%Y-%m", time.gmtime())
        async with access._ro() as db:
            cur = await db.execute("SELECT COALESCE(SUM(credits),0) FROM apex_usage WHERE day LIKE ?",
                                   (month + "%",))
            msum = (await cur.fetchone())[0]
        out.append(f"This month: <b>{msum:,}</b> / {apex.MONTHLY_CREDIT_CEILING:,} "
                   f"({msum / apex.MONTHLY_CREDIT_CEILING * 100:.1f}%)\n")
        for day, credits in rows:
            out.append(f"<code>{day}</code>  {credits:>10,}")
    elif key == "wallets":
        for uid, pub, risk, en, setup, lastbuy, custom in rows:
            out.append(f"👛 <b>{_name(uid)}</b> <code>{uid}</code>\n   <code>{pub}</code>\n"
                       f"   added {_ts(setup)} UTC · last buy {_ts(lastbuy)} · legacy sniper "
                       f"{'on' if en else 'off'}\n   🔒 key stored encrypted (never shown)")
    if not rows:
        out.append("Empty.")
    step = PAGE * (2 if key == "usage" else 1)
    nav = []
    if page > 0:
        nav.append(("‹ Prev", f"adm:d:{key}:new:{page - 1}"))
    if (page + 1) * step < total:
        nav.append(("Next ›", f"adm:d:{key}:new:{page + 1}"))
    kb = _kb(([nav] if nav else []) + [[("📤 Export CSV", f"adm:csv:{key}"), ("↻", f"adm:d:{key}:new:{page}")],
                                       [("‹ Data", "adm:data"), ("🛡 Admin", "adm:home")]])
    return "\n".join(out)[:3900], kb


@router.callback_query(F.data.startswith("adm:d:"))
async def cb_table(q: CallbackQuery):
    if not await _guard(q):
        return
    _, _, key, sort, page = q.data.split(":")
    if key not in TABLES:
        await q.answer("Unknown table")
        return
    try:
        t, kb = (await _positions_page(sort, int(page))) if key == "positions" else (
            await _simple_page(key, int(page)))
    except Exception as e:
        logger.warning(f"data view {key}: {e}")
        t, kb = (f"<b>{TABLES[key]['title']}</b>\n{RULE}\nThat table isn't available yet.",
                 _kb([[("‹ Data", "adm:data")]]))
    await apex._send(q, t, kb)


# ── CSV export ───────────────────────────────────────────────────────────────
async def build_csv(key: str) -> Tuple[str, bytes]:
    spec = TABLES[key]
    async with access._ro() as db:
        db.row_factory = None
        cur = await db.execute(f"SELECT {spec['cols']} FROM {spec['table']} ORDER BY {spec['order']} "
                               f"LIMIT {MAX_CSV_ROWS}")
        rows = await cur.fetchall()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(spec["cols"].split(","))
    w.writerows(rows)
    name = f"apex_{key}_{time.strftime('%Y%m%d_%H%M', time.gmtime())}.csv"
    return name, buf.getvalue().encode("utf-8")


@router.callback_query(F.data.startswith("adm:csv:"))
async def cb_csv(q: CallbackQuery):
    if not await _guard(q):
        return
    key = q.data.split(":")[2]
    if key not in TABLES:
        await q.answer("Unknown table")
        return
    try:
        name, data = await build_csv(key)
    except Exception as e:
        logger.warning(f"csv {key}: {e}")
        await q.answer("Export failed", show_alert=True)
        return
    await q.answer()
    try:
        await q.message.answer_document(BufferedInputFile(data, name),
                                        caption=f"{TABLES[key]['title']} — {name}\n"
                                                "Contains Telegram IDs and wallet addresses. "
                                                "Delete it when you're done.")
    except TelegramAPIError as e:
        logger.warning(f"csv send: {e}")
