"""
CLEX native pump.fun SELL: build the bonding-curve sell instruction ourselves instead of going
quote -> swap-API -> sign -> send through Jupiter.

LAYOUT (verified against real mainnet sells, not only the public IDL: the live program takes 16
accounts, two more than the published `sell`):
   0 global              ro     8 creator_vault      rw   (PDA "creator-vault" + bonding_curve.creator)
   1 fee_recipient       rw     9 token_program      ro   (the mint's owner: Token or Token-2022)
   2 mint                ro    10 event_authority    ro
   3 bonding_curve       rw    11 pump program       ro
   4 assoc_bonding_curve rw    12 fee_config         rw   (PDA under the fee program)
   5 assoc_user          rw    13 fee_program        rw
   6 user                rw+S  14 bonding_curve_v2   ro   (PDA "bonding-curve-v2" + mint; need not exist)
   7 system_program      ro    15 buyback_recipient  rw   (one of Global.buyback_fee_recipients)
Data: 8-byte discriminator + u64 amount + u64 min_sol_output.
fee_recipient is one of Global.fee_recipients (reserved_fee_recipients for mayhem coins).

Only plain coins are handled natively. Cashback and mayhem-mode coins, completed (graduated) curves
and anything unexpected return "not eligible" and the caller uses Jupiter. Nothing here touches the
network on its own: callers pass in an `rpc(method, params)` coroutine.
"""
import base64
import os
import random
import struct
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import VersionedTransaction

import curve

PUMP = Pubkey.from_string(curve.PUMP_PROGRAM_ID)
FEE_PROGRAM = Pubkey.from_string("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
ATA_PROGRAM = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
TOKEN_PROGRAM = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
TOKEN_2022 = Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
SYSTEM = Pubkey.from_string("11111111111111111111111111111111")
WSOL = Pubkey.from_string("So11111111111111111111111111111111111111112")
ZERO = Pubkey.from_string("11111111111111111111111111111111")

SELL_DISC = bytes([51, 230, 133, 164, 1, 127, 131, 173])
FEE_CONFIG_SEED = bytes([1, 86, 224, 246, 147, 102, 90, 207, 68, 219, 21, 104, 191, 23, 91, 170,
                         81, 137, 203, 151, 245, 210, 255, 59, 101, 93, 43, 182, 253, 109, 24, 176])
CU_LIMIT = 120_000                     # real sells consume about 55k

GLOBAL = Pubkey.find_program_address([b"global"], PUMP)[0]
EVENT_AUTH = Pubkey.find_program_address([b"__event_authority"], PUMP)[0]
FEE_CONFIG = Pubkey.find_program_address([b"fee_config", FEE_CONFIG_SEED], FEE_PROGRAM)[0]


def curve_pda(mint: Pubkey) -> Pubkey:
    return Pubkey.find_program_address([b"bonding-curve", bytes(mint)], PUMP)[0]


def curve_v2_pda(mint: Pubkey) -> Pubkey:
    return Pubkey.find_program_address([b"bonding-curve-v2", bytes(mint)], PUMP)[0]


def creator_vault_pda(creator: Pubkey) -> Pubkey:
    return Pubkey.find_program_address([b"creator-vault", bytes(creator)], PUMP)[0]


def ata(owner: Pubkey, mint: Pubkey, token_program: Pubkey) -> Pubkey:
    return Pubkey.find_program_address([bytes(owner), bytes(token_program), bytes(mint)], ATA_PROGRAM)[0]


@dataclass
class Info:
    cv: "curve.Curve"
    creator: Pubkey
    mayhem: bool
    cashback: bool
    quote_mint: Pubkey

    @property
    def eligible(self) -> bool:
        return (not self.cv.complete and not self.mayhem and not self.cashback
                and self.quote_mint in (ZERO, WSOL))


def parse_info(b64: str) -> Optional[Info]:
    """Full BondingCurve account (the part curve.parse ignores). None if it is not the layout we know."""
    cv = curve.parse(b64)
    if cv is None:
        return None
    raw = base64.b64decode(b64)
    if len(raw) < 115:
        return None                                            # older/shorter layout: do not guess
    return Info(cv, Pubkey.from_bytes(raw[49:81]), raw[81] == 1, raw[82] == 1, Pubkey.from_bytes(raw[83:115]))


@dataclass
class GlobalInfo:
    fee_recipients: List[Pubkey]
    reserved: List[Pubkey]
    buyback: List[Pubkey]


def _keys(raw: bytes, off: int, n: int) -> List[Pubkey]:
    out = [Pubkey.from_bytes(raw[off + 32 * i: off + 32 * i + 32]) for i in range(n)]
    return [k for k in out if k != ZERO]


def parse_global(b64: str) -> Optional[GlobalInfo]:
    try:
        raw = base64.b64decode(b64)
        if len(raw) < 997:
            return None
        g = GlobalInfo(_keys(raw, 162, 7), _keys(raw, 516, 7), _keys(raw, 741, 8))
        return g if g.fee_recipients and g.buyback else None
    except Exception:
        return None


def sell_ix(user: Pubkey, mint: Pubkey, token_program: Pubkey, info: Info, glob: GlobalInfo,
            amount_raw: int, min_out_lamports: int, rng=random) -> Instruction:
    bc = curve_pda(mint)
    pool = glob.reserved if (info.mayhem and glob.reserved) else glob.fee_recipients
    metas = [
        AccountMeta(GLOBAL, False, False),
        AccountMeta(rng.choice(pool), False, True),
        AccountMeta(mint, False, False),
        AccountMeta(bc, False, True),
        AccountMeta(ata(bc, mint, token_program), False, True),
        AccountMeta(ata(user, mint, token_program), False, True),
        AccountMeta(user, True, True),
        AccountMeta(SYSTEM, False, False),
        AccountMeta(creator_vault_pda(info.creator), False, True),
        AccountMeta(token_program, False, False),
        AccountMeta(EVENT_AUTH, False, False),
        AccountMeta(PUMP, False, False),
        AccountMeta(FEE_CONFIG, False, True),
        AccountMeta(FEE_PROGRAM, False, True),
        AccountMeta(curve_v2_pda(mint), False, False),
        AccountMeta(rng.choice(glob.buyback), False, True),
    ]
    return Instruction(PUMP, SELL_DISC + struct.pack("<QQ", int(amount_raw), int(min_out_lamports)), metas)


def build_sell_tx(user: Pubkey, ix: Instruction, blockhash: str, priority_lamports: int,
                  signer: Optional[Keypair] = None) -> bytes:
    """Compute-budget + sell, as a legacy message. Signed if `signer` is given, else with an empty
    signature (for simulation, which runs with signature verification off)."""
    price = int(priority_lamports * 1_000_000 / CU_LIMIT)               # micro-lamports per CU
    msg = Message.new_with_blockhash([set_compute_unit_limit(CU_LIMIT), set_compute_unit_price(price), ix],
                                     user, Hash.from_string(blockhash))
    tx = VersionedTransaction(msg, [signer]) if signer else VersionedTransaction.populate(msg, [Signature.default()])
    return bytes(tx)


def estimate_out_sol(info: Info, tokens_ui: float) -> float:
    return curve.sol_out_sell(info.cv.vsol, info.cv.vtok, tokens_ui)


def min_out_for(info: Info, tokens_ui: float, slippage_bps: int) -> int:
    """Lamports we will accept at minimum. slippage_bps >= 10000 means any amount (stops)."""
    if slippage_bps >= 10_000:
        return 1
    return max(1, int(estimate_out_sol(info, tokens_ui) * 1e9 * (1 - slippage_bps / 10_000)))


async def fetch_info(rpc: Callable, mint: str) -> Optional[Info]:
    r = await rpc("getAccountInfo", [str(curve_pda(Pubkey.from_string(mint))), {"encoding": "base64", "commitment": "confirmed"}])
    try:
        return parse_info(r["value"]["data"][0])
    except Exception:
        return None


_glob_cache = {"g": None, "at": 0.0}


async def fetch_global(rpc: Callable, now: float, ttl: float = 600.0) -> Optional[GlobalInfo]:
    if _glob_cache["g"] and now - _glob_cache["at"] < ttl:
        return _glob_cache["g"]
    r = await rpc("getAccountInfo", [str(GLOBAL), {"encoding": "base64", "commitment": "confirmed"}])
    try:
        g = parse_global(r["value"]["data"][0])
    except Exception:
        g = None
    if g:
        _glob_cache.update(g=g, at=now)
    return g or _glob_cache["g"]


async def token_program_of(rpc: Callable, mint: str) -> Optional[Pubkey]:
    r = await rpc("getAccountInfo", [mint, {"encoding": "base64"}])
    try:
        owner = r["value"]["owner"]
    except Exception:
        return None
    p = Pubkey.from_string(owner)
    return p if p in (TOKEN_PROGRAM, TOKEN_2022) else None


async def simulate(rpc: Callable, tx_bytes: bytes) -> Tuple[bool, str, int]:
    """(ok, error/log tail, compute units). Runs the exact transaction against current chain state
    without signing or spending anything."""
    r = await rpc("simulateTransaction", [base64.b64encode(tx_bytes).decode(),
                                          {"encoding": "base64", "sigVerify": False,
                                           "replaceRecentBlockhash": True, "commitment": "processed"}])
    v = (r or {}).get("value") or {}
    if v.get("err"):
        return False, f"{v['err']} | {(v.get('logs') or [''])[-1]}", int(v.get("unitsConsumed") or 0)
    if not v:
        return False, "no simulation result", 0
    return True, "", int(v.get("unitsConsumed") or 0)
