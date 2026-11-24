# CLEX TERMINAL

Owner-only research console, a Telegram Mini App. Open it from the TERMINAL button next to the message box
(or Admin -> TERMINAL). It is read-only: it cannot trade, change settings, or read keys.

Views: F1 DASH, F2 LAB (all variants, sortable), F3 VAR (one variant: equity curve, rolling mean, return
distribution, latency sensitivity, exits, checklist, last trades), F4 MKT (what the market offered, regime
by hour), F5 POS (risk, live/paper books, real fills vs decision price), F6 SYS (every constant), F7 HELP.
Command line: `VAR <name>`, `SORT <col>`, `R` refresh, `DASH LAB MKT POS SYS HELP`.

Security: every data call carries Telegram's signed initData, verified with the bot token (HMAC-SHA256),
fresh within 24h, and must belong to ADMIN_ID. The page itself holds no data. The database is opened
read-only. Strict CSP, no third-party code except Telegram's own script.

Config: URL comes from `TERMINAL_URL`, else `RAILWAY_PUBLIC_DOMAIN`. Tests: `python tests/test_terminal.py .`
