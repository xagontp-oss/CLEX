# CLEX Lab: 24/7 paper research

Owner-only (Admin -> Lab). It never touches a wallet and never spends SOL.

**What runs:** every new pump.fun launch is tracked on-chain (raw bonding-curve reserves, one batched
`getMultipleAccounts` per ~5s, up to 15 min per coin; holder snapshots at ~T+60s/T+120s for coins with
activity). Finished coins are replayed through frozen strategy variants using real constant-product curve
math (price impact, 1.25% pump fee, a network fee per leg) at execution latencies of 2/5/10/20 s.

**Promotion checklist** (all must hold, then again after 150 more trades): 600+ forward trades; multiple-testing-
corrected lower confidence bound > 0; still > 0 without the 3 best trades; both halves positive; max drawdown
<= 10 stakes; mean > 0 at 10 s latency. Controls (random entry, all-coins baseline) are never retired.

**Challengers** are spawned every 6 h as single-parameter neighbours of the best variants (never of controls).
They start with zero evidence and raise the bar for everyone (Bonferroni over every variant ever registered).

**Config (env):** `LAB_ENABLED=0` turns it off; `LAB_MAX_ACTIVE` (default 300 coins tracked at once);
`LAB_DAILY_CALLS` (default 120000 Helius calls per UTC day).

**Honest limits:** paper is not live (no MEV/sandwich, no failed transactions). Exits assume a fast native
curve sell, not the current Jupiter path. Coins with no net buying by T+90 s are dropped (no variant could trade
them); coins that showed activity and then died are kept, losses included.

**Tests:** `python tests/test_lab.py .` and `python tests/test_lab_world.py .` (the second one plants an edge,
an anti-edge and pure noise, and checks the lab finds, rejects and ignores them respectively).
