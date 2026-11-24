"""
CLEX curve math: the single source of truth for pump.fun bonding-curve numbers.

Pure functions, no network. Everything is derived from the RAW curve account, never from the old
"30 virtual SOL at launch" assumption (some launches start near 0.5 virtual SOL).

Account layout (little-endian): 8-byte discriminator, then u64 virtual_token_reserves,
virtual_sol_reserves, real_token_reserves, real_sol_reserves, token_total_supply (offsets 8..47),
and a `complete` bool at byte 48. Tokens have 6 decimals, SOL 1e9 lamports.
"""
import base64
import struct
from dataclasses import dataclass
from typing import List, Optional, Tuple

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CURVE_DISC = bytes.fromhex("17b7f83760d8ac60")
GRADUATION_SOL = 85.0
PUMP_FEE = 0.0125
CLASSIC_V0_MIN = 10.0               # virtual SOL at launch: classic ~30, low-start ~0.5


@dataclass
class Curve:
    vtok: float                      # virtual token reserves (token units)
    vsol: float                      # virtual SOL reserves
    rtok: float                      # real token reserves
    rsol: float                      # real SOL reserves = SOL actually raised
    supply: float
    complete: bool

    @property
    def v0(self) -> float:
        """Virtual SOL the curve started with (virtual minus raised)."""
        return self.vsol - self.rsol

    @property
    def curve_type(self) -> str:
        return "classic" if self.v0 >= CLASSIC_V0_MIN else "low_start"

    @property
    def price(self) -> float:
        """Spot price in SOL per token."""
        return self.vsol / self.vtok if self.vtok > 0 else 0.0


def parse(b64: str) -> Optional[Curve]:
    try:
        raw = base64.b64decode(b64)
    except Exception:
        return None
    if len(raw) < 49 or raw[:8] != CURVE_DISC:
        return None
    vtok, vsol, rtok, rsol, supply = struct.unpack_from("<5Q", raw, 8)
    c = Curve(vtok / 1e6, vsol / 1e9, rtok / 1e6, rsol / 1e9, supply / 1e6, raw[48] == 1)
    return c if c.vtok > 0 and c.vsol > 0 else None


def graduation_pct(rsol: float) -> float:
    return min(100.0, max(0.0, rsol / GRADUATION_SOL * 100.0))


def tokens_out_buy(vsol: float, vtok: float, sol_in: float, fee: float = PUMP_FEE) -> float:
    dx = sol_in * (1.0 - fee)
    return vtok * dx / (vsol + dx)


def sol_out_sell(vsol: float, vtok: float, tokens_in: float, fee: float = PUMP_FEE) -> float:
    return tokens_in * vsol / (vtok + tokens_in) * (1.0 - fee)


def buy_impact_pct(vsol: float, sol_in: float, fee: float = PUMP_FEE) -> float:
    """How far OUR buy pushes the spot price (price is vsol^2 / k on a constant-product curve).
    A buy that moves the price a lot means a thin curve: the exit will move it again."""
    if vsol <= 0:
        return 100.0
    dx = sol_in * (1.0 - fee)
    return (((vsol + dx) / vsol) ** 2 - 1.0) * 100.0


def price_ratio(a: Curve, b: Curve) -> float:
    """Spot price at b relative to a (raw reserves, no launch-size assumption)."""
    return b.price / a.price if a.price > 0 else 1.0


def speed_sol_per_min(samples: List[Tuple[float, float]], window: float = 30.0) -> float:
    """samples: [(t, real_sol)] oldest first. SOL/min over the trailing window."""
    if len(samples) < 2:
        return 0.0
    t_end = samples[-1][0]
    recent = [s for s in samples if t_end - s[0] <= window]
    if len(recent) < 2:
        return 0.0
    dt = (recent[-1][0] - recent[0][0]) / 60.0
    return (recent[-1][1] - recent[0][1]) / dt if dt > 0 else 0.0


def acceleration(samples: List[Tuple[float, float]]) -> Tuple[float, float]:
    """(rate over the last 30s, rate over the 30s before that), SOL/min. Prior falls back to the
    recent rate when there is not enough history, which reads as "not accelerating"."""
    if len(samples) < 3:
        return 0.0, 0.0
    t_now = samples[-1][0]
    recent = [s for s in samples if t_now - s[0] <= 30]
    prior = [s for s in samples if 30 < t_now - s[0] <= 60]
    now_r = speed_sol_per_min(recent)
    prior_r = speed_sol_per_min(prior) if len(prior) >= 2 else now_r
    return now_r, prior_r
