"""
CLEX Apex — Fund Collections: an optional personal profit vault.

A user can switch this on and pick what share of every WINNING trade's profit is moved from their
trading wallet into a separate "Fund Collections" wallet that belongs to them. They can see it,
deposit to it, export its key, send from it to any address, or move it back to the trading wallet.

How it fits the rest of the bot
  * One new table, `apex_vault` (CREATE TABLE IF NOT EXISTS). No existing table is changed.
  * The vault wallet's private key is encrypted with the existing helpers `sn.encrypt_key` /
    `sn.decrypt_key` — the encryption code itself is untouched and reused as is.
  * The share is taken from PROFIT only (realized - entry), when a position fully closes with a gain.
    Your stake is never swept.
  * Tiny wins add up as "pending" and are sent once they reach 0.001 SOL (Solana won't accept less
    into a brand-new account), so it never wastes fees on dust transfers.
"""
import asyncio
import html
import logging
import time
from typing import Dict, Optional, Tuple

import aiosqlite
import base58
from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Filter
from aiogram.types import BufferedInputFile, CallbackQuery, Message

import apex
import sniper as sn

logger = logging.getLogger("vault")
router = Router()
RULE = apex.RULE
_kb = apex._kb
HOME_BTN = apex.HOME_BTN

DB_PATH = apex.DB_PATH
PCT_PRESETS = [5, 10, 25, 50]
DEFAULT_PCT = 10
SWEEP_MIN_LAMPORTS = 1_000_000          # 0.001 SOL: smallest transfer worth sending
_locks: Dict[int, asyncio.Lock] = {}
_staged: Dict[int, Dict] = {}           # staged withdrawals, confirm-then-send
STAGE_TTL = 300


def _lock(uid: int) -> asyncio.Lock:
    return _locks.setdefault(uid, asyncio.Lock())


# ══════════════════════════════════════════════════════════════════════════════
# STORAGE
# ══════════════════════════════════════════════════════════════════════════════
async def init_db():
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS apex_vault (
                user_id       INTEGER PRIMARY KEY,
                enabled       INTEGER NOT NULL DEFAULT 0,
                pct           REAL    NOT NULL DEFAULT 10,
                pubkey        TEXT    NOT NULL,
                encrypted_key TEXT    NOT NULL,
                pending       INTEGER NOT NULL DEFAULT 0,
                total_swept   INTEGER NOT NULL DEFAULT 0,
                created       REAL    NOT NULL
            )""")
        await db.commit()


async def get(uid: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT user_id,enabled,pct,pubkey,pending,total_swept,created "
                               "FROM apex_vault WHERE user_id=?", (uid,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def _update(uid: int, **kv):
    cols = ",".join(f"{k}=?" for k in kv)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(f"UPDATE apex_vault SET {cols} WHERE user_id=?", (*kv.values(), uid))
        await db.commit()


async def create(uid: int) -> Dict:
    existing = await get(uid)
    if existing:
        return existing
    from solders.keypair import Keypair
    kp = Keypair()
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("INSERT INTO apex_vault(user_id,enabled,pct,pubkey,encrypted_key,created) "
                         "VALUES (?,0,?,?,?,?)",
                         (uid, DEFAULT_PCT, str(kp.pubkey()),
                          sn.encrypt_key(base58.b58encode(bytes(kp)).decode()), time.time()))
        await db.commit()
    return await get(uid)


async def _keypair(uid: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute("SELECT encrypted_key FROM apex_vault WHERE user_id=?", (uid,))
        row = await cur.fetchone()
    if not row:
        return None
    from solders.keypair import Keypair
    return Keypair.from_bytes(base58.b58decode(sn.decrypt_key(row[0])))


# ══════════════════════════════════════════════════════════════════════════════
# THE SWEEP — called when a position closes at a profit
# ══════════════════════════════════════════════════════════════════════════════
async def on_win(uid: int, profit_sol: float, name: str = ""):
    """Move the user's chosen share of this win's PROFIT into their vault."""
    try:
        v = await get(uid)
        if not v or not v["enabled"] or profit_sol <= 0:
            return
        async with _lock(uid):
            v = await get(uid)
            share = int(profit_sol * (v["pct"] / 100.0) * 1e9)
            pending = v["pending"] + share
            if pending < SWEEP_MIN_LAMPORTS:
                await _update(uid, pending=pending)                  # too small to send yet
                return
            w = await sn.get_sniper_user(uid)
            if not w:
                await _update(uid, pending=pending)
                return
            px = await apex.sol_usd()
            reserve = int((apex.MIN_WALLET_USD / px) * 1e9) if px else 5_000_000
            bal = int(round(await sn.get_sol_balance(w["pubkey"]) * 1e9))
            if bal - pending - apex.SEND_FEE_LAMPORTS < reserve:
                await _update(uid, pending=pending)                  # keep gas for exits; try again next win
                return
            ok, sig, err = await apex.send_sol(uid, v["pubkey"], pending)
            if ok:
                await _update(uid, pending=0, total_swept=v["total_swept"] + pending)
                await apex._notify(uid, (
                    f"💎 <b>Saved to Fund Collections</b>\n{RULE}\n"
                    f"<b>{pending / 1e9:.4f} SOL</b> ({v['pct']:g}% of recent profit"
                    f"{' — ' + html.escape(name) if name else ''})\n"
                    f"<a href=\"https://solscan.io/tx/{sig}\">View transaction</a>"))
            else:
                await _update(uid, pending=pending)
                logger.warning(f"sweep failed for {uid}: {err}")
                await apex._notify_fail(uid, "Fund Collections transfer failed", err, sig,
                                        "Your profit is safe in the trading wallet; Apex will retry "
                                        "after the next win.")
    except Exception as e:
        logger.error(f"vault on_win {uid}: {e}", exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGES
# ══════════════════════════════════════════════════════════════════════════════
async def _home(uid: int) -> Tuple[str, "InlineKeyboardMarkup"]:
    v = await get(uid)
    if not v:
        text = (f"<b>💎 Fund Collections</b>\n{RULE}\n"
                "A <b>separate wallet that belongs to you</b>, where Apex can set aside a share of "
                "every <b>winning</b> trade — your profit insurance.\n\n"
                "• <b>Optional</b> — off until you switch it on\n"
                "• <b>You choose the share</b> (for example 10% of each win's profit)\n"
                "• <b>Profit only</b> — your stake is never touched\n"
                "• <b>You can send it anywhere,</b> or back to the trading wallet, any time\n\n"
                "Set it up and Apex creates the wallet for you.")
        return text, _kb([[("✨ Create my Fund Collections wallet", "vlt:create")], HOME_BTN])
    px = await apex.sol_usd()
    bal = await sn.get_sol_balance(v["pubkey"])
    usd = f" ≈ ${bal * px:,.2f}" if px else ""
    on = bool(v["enabled"])
    text = (f"<b>💎 Fund Collections</b>\n{RULE}\n"
            f"<code>Status    </code><b>{'🟢 ON' if on else '⚪ OFF'}</b>\n"
            f"<code>Share     </code>{v['pct']:g}% of each win's profit\n"
            f"<code>Balance   </code><b>{bal:.4f} SOL</b>{usd}\n"
            f"<code>Saved     </code>{v['total_swept'] / 1e9:.4f} SOL in total\n"
            + (f"<code>Waiting   </code>{v['pending'] / 1e9:.4f} SOL <i>(sent once it reaches 0.001)</i>\n"
               if v["pending"] else "") +
            f"\n<code>{v['pubkey']}</code>\n<i>(your vault address — tap to copy)</i>\n{RULE}\n"
            + ("When a trade closes in profit, Apex moves this share of that <b>profit</b> here."
               if on else "Switched off: nothing is moved. Turn it on to start saving."))
    kb = _kb([[("⚪ Turn OFF" if on else "🟢 Turn ON", "vlt:off" if on else "vlt:on"),
               (f"% Share: {v['pct']:g}%", "vlt:pct")],
              [("📥 Deposit", "vlt:dep"), ("📤 Send out", "vlt:send")],
              [("↩️ To trading wallet", "vlt:tow")] + ([("🔑 Export key", "vlt:exp")] if apex.can_export(uid) else []),
              [("↻ Refresh", "vlt:home")], HOME_BTN])
    return text, kb


@router.callback_query(F.data == "vlt:home")
async def cb_home(q: CallbackQuery):
    apex._await.pop(q.from_user.id, None)
    t, kb = await _home(q.from_user.id)
    await apex._send(q, t, kb)


@router.callback_query(F.data == "vlt:create")
async def cb_create(q: CallbackQuery):
    await create(q.from_user.id)
    t, kb = await _home(q.from_user.id)
    await apex._send(q, "✅ <b>Your Fund Collections wallet is ready.</b> Choose a share, then turn it "
                        "on.\n\n" + t, kb)


@router.callback_query(F.data.in_({"vlt:on", "vlt:off"}))
async def cb_toggle(q: CallbackQuery):
    if not await get(q.from_user.id):
        await q.answer("Set it up first")
        return
    on = q.data == "vlt:on"
    await _update(q.from_user.id, enabled=1 if on else 0)
    t, kb = await _home(q.from_user.id)
    await apex._send(q, ("🟢 <b>Fund Collections is ON.</b>" if on else "⚪ <b>Fund Collections is OFF.</b>")
                     + "\n\n" + t, kb)


@router.callback_query(F.data == "vlt:pct")
async def cb_pct(q: CallbackQuery):
    v = await get(q.from_user.id)
    if not v:
        await q.answer("Set it up first")
        return
    await apex._send(q, f"<b>% Share of each win</b>\n{RULE}\nCurrently <b>{v['pct']:g}%</b>.\n\n"
                        "On every trade that closes in profit, Apex moves this share of the "
                        "<b>profit</b> (not your stake) into Fund Collections.\n"
                        "Example: a +0.10 SOL win at 25% sets aside 0.025 SOL.",
                     _kb([[(f"{p}%", f"vlt:setpct:{p}") for p in PCT_PRESETS],
                          [("Custom %", "vlt:pctc")], [("‹ Back", "vlt:home")]]))


@router.callback_query(F.data.startswith("vlt:setpct:"))
async def cb_setpct(q: CallbackQuery):
    p = float(q.data.split(":")[2])
    if not await get(q.from_user.id) or not (0 < p <= 100):
        await q.answer("Invalid")
        return
    await _update(q.from_user.id, pct=p)
    t, kb = await _home(q.from_user.id)
    await apex._send(q, f"✅ Share set to <b>{p:g}%</b>.\n\n" + t, kb)


@router.callback_query(F.data == "vlt:pctc")
async def cb_pctc(q: CallbackQuery):
    apex._await[q.from_user.id] = "vlt_pct"
    await apex._send(q, f"<b>Custom share</b>\n{RULE}\nSend a number from 1 to 100, e.g. <code>15</code>.",
                     _kb([[("Cancel", "vlt:home")]]))


@router.callback_query(F.data == "vlt:dep")
async def cb_dep(q: CallbackQuery):
    v = await get(q.from_user.id)
    if not v:
        await q.answer("Set it up first")
        return
    bal = await sn.get_sol_balance(v["pubkey"])
    caption = (f"<b>📥 Fund Collections address</b>\n{RULE}\n<code>{v['pubkey']}</code>\n"
               "<i>(tap to copy)</i>\n\n"
               f"Balance: <b>{bal:.4f} SOL</b>\n\n"
               "Apex sends your saved profit here automatically. You can also send SOL here yourself. "
               "<b>Solana network only.</b>")
    kb = _kb([[("↻ Check balance", "vlt:dep")], [("‹ Fund Collections", "vlt:home")]])
    png = apex._qr_png(v["pubkey"])
    if png:
        try:
            await q.message.answer_photo(BufferedInputFile(png, "vault.png"), caption=caption,
                                         parse_mode=ParseMode.HTML, reply_markup=kb)
            await q.answer()
            return
        except TelegramAPIError as e:
            logger.warning(f"vault photo: {e}")
    await apex._send(q, caption, kb)


# ── export the vault key (one-time, self-deleting) ───────────────────────────
@router.callback_query(F.data == "vlt:exp")
async def cb_exp(q: CallbackQuery):
    if not apex.can_export(q.from_user.id):
        await q.answer("Key export isn't available.", show_alert=True)
        return
    if not await get(q.from_user.id):
        await q.answer("Set it up first")
        return
    await apex._send(q, f"<b>🔑 Export Fund Collections key</b>\n{RULE}\nThe key appears in a message that "
                        "<b>deletes itself after 60 seconds</b>.\n\n"
                        "• Anyone who sees it controls this vault's funds\n"
                        "• Make sure nobody is looking at your screen",
                     _kb([[("Show my key", "vlt:expgo")], [("Cancel", "vlt:home")]]))


@router.callback_query(F.data == "vlt:expgo")
async def cb_expgo(q: CallbackQuery):
    if not apex.can_export(q.from_user.id):
        await q.answer("Key export isn't available.", show_alert=True)
        return
    kp = await _keypair(q.from_user.id)
    if not kp:
        await q.answer("No vault")
        return
    await q.answer()
    msg = await q.message.answer(
        "🔑 <b>Fund Collections key</b> — deleting in 60s\n<tg-spoiler>"
        + html.escape(base58.b58encode(bytes(kp)).decode()) + "</tg-spoiler>",
        parse_mode=ParseMode.HTML, protect_content=True)
    del kp

    async def _wipe():
        await asyncio.sleep(60)
        try:
            await msg.delete()
        except Exception:
            pass
    asyncio.create_task(_wipe())


# ── send out / move back ─────────────────────────────────────────────────────
async def _balance_lamports(v: Dict) -> int:
    return int(round(await sn.get_sol_balance(v["pubkey"]) * 1e9))


@router.callback_query(F.data == "vlt:send")
async def cb_send(q: CallbackQuery):
    v = await get(q.from_user.id)
    if not v:
        await q.answer("Set it up first")
        return
    apex._await[q.from_user.id] = "vlt_addr"
    _staged.pop(q.from_user.id, None)
    await apex._send(q, f"<b>📤 Send from Fund Collections</b>\n{RULE}\nPaste the <b>Solana address</b> "
                        "to send SOL to.\n\n• A Phantom address, or an exchange's <i>Solana</i> deposit "
                        "address\n• Transfers can't be undone\n• Tip: send a small test amount first",
                     _kb([[("Cancel", "vlt:home")]]))


@router.callback_query(F.data == "vlt:tow")
async def cb_tow(q: CallbackQuery):
    uid = q.from_user.id
    w = await sn.get_sniper_user(uid)
    if not await get(uid) or not w:
        await q.answer("Connect your trading wallet first", show_alert=True)
        return
    _staged[uid] = {"dest": w["pubkey"], "label": "your trading wallet"}
    apex._await.pop(uid, None)
    await _stage(q, uid, "max")


async def _stage(target, uid: int, text: str):
    v = await get(uid)
    d = _staged.get(uid)
    if not v or not d:
        return
    bal = await _balance_lamports(v)
    lam, err = apex.parse_amount(text, bal, 0)
    if err:
        await apex._send(target, f"❌ {err}", _kb([[("Send max", "vlt:sendmax")], [("Cancel", "vlt:home")]]))
        return
    d.update(lamports=lam, expires=time.time() + STAGE_TTL)
    apex._await.pop(uid, None)
    px = await apex.sol_usd()
    usd = f" (≈ ${lam / 1e9 * px:,.2f})" if px else ""
    await apex._send(target, (
        f"<b>📤 Confirm transfer</b>\n{RULE}\n"
        f"From   Fund Collections\n"
        f"Amount <b>{lam / 1e9:.6f} SOL</b>{usd}\n"
        f"Fee    ≈ {apex.SEND_FEE_LAMPORTS / 1e9:.6f} SOL\n"
        f"To{' (' + d['label'] + ')' if d.get('label') else ''}:\n<code>{d['dest']}</code>\n\n"
        "Check the <b>whole address</b>. Transfers can't be undone."),
        _kb([[("✅ Confirm & send", "vlt:sendgo")], [("Cancel", "vlt:home")]]))


@router.callback_query(F.data == "vlt:sendmax")
async def cb_sendmax(q: CallbackQuery):
    if q.from_user.id not in _staged:
        await q.answer("Start again")
        return
    await _stage(q, q.from_user.id, "max")


async def send_from_vault(uid: int, dest: str, lamports: int) -> Tuple[bool, str, str]:
    try:
        from solders.hash import Hash
        from solders.message import Message as SMessage
        from solders.pubkey import Pubkey
        from solders.system_program import TransferParams, transfer
        from solders.transaction import Transaction
        kp = await _keypair(uid)
        if not kp:
            return False, "", "no vault"
        bh = await sn._rpc("getLatestBlockhash", [{"commitment": "finalized"}])
        blockhash = Hash.from_string(bh["value"]["blockhash"])
        ix = transfer(TransferParams(from_pubkey=kp.pubkey(), to_pubkey=Pubkey.from_string(dest),
                                     lamports=int(lamports)))
        tx = Transaction([kp], SMessage.new_with_blockhash([ix], kp.pubkey(), blockhash), blockhash)
        sig = await sn.send_transaction_fast(bytes(tx), kp, False)
        if not sig:
            return False, "", "the network rejected the transaction"
        status, err = await apex._confirm_tx(sig, 40)
        if status == "failed":
            return False, sig, apex.friendly_err(err)
        if status == "timeout":
            return False, sig, "not confirmed in time — check Solscan before retrying"
        return True, sig, ""
    except Exception as e:
        logger.error(f"send_from_vault: {e}", exc_info=True)
        return False, "", "unexpected error"


@router.callback_query(F.data == "vlt:sendgo")
async def cb_sendgo(q: CallbackQuery):
    uid = q.from_user.id
    d = _staged.get(uid)
    if not d or not d.get("lamports") or time.time() > d.get("expires", 0):
        _staged.pop(uid, None)
        await apex._send(q, "That transfer expired. Start again from Fund Collections.",
                         _kb([[("‹ Fund Collections", "vlt:home")]]))
        return
    _staged.pop(uid, None)                                   # one shot: a double-tap can't send twice
    async with _lock(uid):
        ok, sig, err = await send_from_vault(uid, d["dest"], d["lamports"])
    if ok:
        await apex._send(q, f"✅ <b>Sent {d['lamports'] / 1e9:.6f} SOL</b>\n{RULE}\n"
                            f"To <code>{apex._short(d['dest'])}</code>\n"
                            f"<a href=\"https://solscan.io/tx/{sig}\">View on Solscan</a>",
                         _kb([[("‹ Fund Collections", "vlt:home")], HOME_BTN]))
    else:
        await apex._send(q, f"❌ <b>Transfer failed</b>\n{html.escape(err)}\nNothing was sent.",
                         _kb([[("‹ Fund Collections", "vlt:home")]]))


# ── typed input (address / amount / custom %) ────────────────────────────────
class _AwaitingVault(Filter):
    async def __call__(self, m: Message) -> bool:
        return bool(m.from_user and apex._await.get(m.from_user.id, "").startswith("vlt_") and m.text
                    and not m.text.startswith("/"))


@router.message(_AwaitingVault())
async def on_text(m: Message):
    uid = m.from_user.id
    kind = apex._await.get(uid)
    raw = (m.text or "").strip()
    if kind == "vlt_pct":
        try:
            p = float(raw.rstrip("%"))
            if not (0 < p <= 100):
                raise ValueError
        except ValueError:
            await m.answer("Send a number from 1 to 100, e.g. <code>15</code>.", parse_mode=ParseMode.HTML)
            return
        await _update(uid, pct=p)
        apex._await.pop(uid, None)
        t, kb = await _home(uid)
        await m.answer(f"✅ Share set to <b>{p:g}%</b>.\n\n" + t, parse_mode=ParseMode.HTML, reply_markup=kb)
        return
    v = await get(uid)
    if kind == "vlt_addr":
        if not apex.valid_address(raw):
            await m.answer("❌ That isn't a valid Solana address. Paste it again, or cancel.",
                           reply_markup=_kb([[("Cancel", "vlt:home")]]))
            return
        if v and raw == v["pubkey"]:
            await m.answer("That's the vault's own address. Paste a different one.")
            return
        _staged[uid] = {"dest": raw}
        apex._await[uid] = "vlt_amt"
        bal = await sn.get_sol_balance(v["pubkey"]) if v else 0.0
        await m.answer(f"<b>How much?</b>\nVault balance: <b>{bal:.4f} SOL</b>\n"
                       "Send an amount like <code>0.25</code>, or tap Send max.",
                       parse_mode=ParseMode.HTML,
                       reply_markup=_kb([[("Send max", "vlt:sendmax")], [("Cancel", "vlt:home")]]))
        return
    if kind == "vlt_amt":
        await _stage(m, uid, raw)


# ── hook into the trade engine ───────────────────────────────────────────────
async def startup():
    await init_db()
    apex.set_win_hook(on_win)
    logger.info("Fund Collections ready")
