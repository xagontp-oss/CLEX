# Rebuild v2 (Phase 1): what changed and why

Source: the v2 rebuild spec, built from the 115-trade CSV. Phase 1 is everything that needs no new infrastructure.

## Changed
- `curve.py` (new): pump.fun curve math from the RAW account (types: classic ~30 virtual SOL, low-start ~0.5). Live SOL-raised, price, slope and impact use it; the old 30-SOL estimate is only a fallback.
- `risk.py` (new): rolling-expectancy kill switch (50 live trades, floor -8%), daily loss cap (2 SOL, 00:00 UTC), fixed-fractional sizing (cap 4% of balance, never raised above the user's choice, 0.08 SOL fee-safe floor, 0.5 SOL hard cap). State is derived from closed positions, so it survives restarts. `/resume` clears the rolling window after confirmation; the daily cap cannot be resumed.
- Exits: A-D hard stop -20% (was -45%), trail 20% below peak, 24h time stop (was 72h). Profiles A 1.8x, B 60% at 2x + rest at 4x, C 25/25/25 with 25% trail, D 1.4x. Scalp: partials 35%/25%, runner trails 30% below peak.
- Polling: scalp 2s (was 6s), others 5s (was 30s). Write-off of an unsellable position is time based (5 min).
- Entry: check ages 60/90/120/180s; Precision pool floor $4,000; Scalp gates dev 10%, top1 20%, top10 50%, 3 buyers, 1 SOL raised, parabolic 500%; fixed scores Flow 0.55, Scalp 0.45; Flow requires market data. Auto-tuners frozen (`AUTO_TUNER_ENABLED=1` re-enables).
- Impact gate: a buy that moves the curve more than 15% (12% scalp) is skipped.
- `apex_slippage_log`: decision price vs real fill for every live buy and sell. Positions record `curve_type` and `entry_sol_raised`.
- Wallet order: Apex-generated wallets and the Fund Collections wallet are refused until the user has imported their own Phantom key (UI and function level).
- Lab: SCALP_REPLICA keeps the ORIGINAL gates (frozen) so its evidence stays valid; new variant SCALP_V2 carries the rebuilt gates and exit.

## Deliberately not done yet (Phase 2+)
Native pump.fun buy/sell, pre-signed sells, Jito, gRPC streaming, MEV/failed-tx model in the Lab, paper haircut recalibration. Entry/exit slippage and priority-fee constants are unchanged because they depend on the native engine.

## Deviations from the spec
- The spec's "DOA cut" (sell at 90s if never above 1.03) is already implied by the 90s verdict (sell unless above 1.08), so no extra rule was added.
- A $5 live size is blocked with a message instead of being raised to 0.08 SOL.
- Write-off is time based instead of 15 failed quotes (30s at a 2s poll would orphan held tokens during a Jupiter 429 burst).
