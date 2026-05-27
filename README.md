# CLEX - Pump.fun Tracker Bot

Real-time Solana wallet monitoring. Get instant Telegram alerts when tracked wallets buy on Pump.fun.

## Features
✅ Live Pump.fun buy detection  
✅ Instant Telegram alerts (2-5 sec)  
✅ Token CA always displayed  
✅ Interactive UI buttons (Add/Delete/List)  
✅ 24/7 monitoring on Railway  
✅ Free to run  

## Quick Setup

### Requirements
- Telegram bot token (from @BotFather)
- Helius API key (from helius.dev)
- Railway account (free)

### Local Test
```bash
pip install -r requirements_min.txt
cp .env.example .env
# Edit .env with your tokens
python bot.py
```

### Deploy to Railway
See `DEPLOY.md` for step-by-step instructions.

## Usage
1. `/start` - Open menu
2. `➕ Add` - Add wallet to track
3. `📋 List` - View tracked wallets
4. `❌ Delete` - Remove wallet
5. Receive alerts when wallet buys!

## Files
- `bot.py` - Main bot (compressed, 380 lines)
- `requirements_min.txt` - Dependencies
- `.env` - Config (edit with your keys)
- `DEPLOY.md` - Railway deployment guide

## Alert Example
```
🚀 PUMP.FUN BUY

🪙 PUMP - Pump Fun Token

💼 CA: 5R2s3PXG2C6qT8K9jX4n2b5Y7z9w2e4r...
💰 Amount: 1.50 PUMP
💬 Pump.fun buy! 🚀

🔗 [Solscan](https://solscan.io/tx/...)
```

## Speed
⚡ 2-5 second alerts  
✅ Zero spam filtering  
✅ Prevents duplicates  

## Monitoring
- Check bot: https://your-railway-app.railway.app/
- View logs: `railway logs -f`
- Add multiple wallets immediately

## Support
- Stuck? Check `DEPLOY.md`
- Bot not responding? Restart on Railway
- Want features? Check the code

## License
MIT
