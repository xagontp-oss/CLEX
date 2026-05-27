# CLEX Pump.fun Tracker - Deploy to Railway

## 1️⃣ Create Telegram Bot
- Open Telegram, search `@BotFather`
- Send `/newbot`
- Name: `CLEX Pump Tracker`
- Username: `clex_tracker_bot`
- Copy token → save it

## 2️⃣ Get Helius API Key
- Go to https://www.helius.dev
- Sign up (free)
- Create API key
- Copy it

## 3️⃣ GitHub Setup
```bash
git init
git add .
git commit -m "init: clex bot"
git push origin main
```

## 4️⃣ Railway Deploy
1. Go to https://railway.app
2. Sign in with GitHub
3. Create → Deploy from GitHub
4. Select your repo
5. Railway auto-detects Python

## 5️⃣ Set Environment Variables
In Railway dashboard → Variables:
```
TELEGRAM_TOKEN = your_bot_token
HELIUS_API_KEY = your_api_key
WEBHOOK_SECRET = change_me_to_random_string
```

## 6️⃣ Get Railway URL
After deploy, Railway gives you a public URL:
```
https://your-app.railway.app
```

## 7️⃣ Configure Helius Webhook
1. Go to Helius console
2. Create webhook
3. URL: `https://your-app.railway.app/webhook`
4. Secret: (match WEBHOOK_SECRET from Railway)
5. Save & test

## 8️⃣ Test Bot
1. Find your bot on Telegram
2. Send `/start`
3. Click "➕ Add"
4. Send a Solana wallet address
5. Send transaction on that wallet
6. Instant alert! 🚀

## Quick Commands
```bash
# View logs
railway logs -f

# Check status
curl https://your-app.railway.app/

# See what changed
git log --oneline
```

## Troubleshooting

**No alerts?**
- Check webhook URL in Helius (must match Railway URL)
- Check secret matches
- Verify bot token is correct
- Check Railway logs: `railway logs -f`

**Bot not responding?**
- Verify TELEGRAM_TOKEN in Railway
- Check bot still exists on Telegram
- Restart: `railway deploy`

**Slow alerts?**
- First alert ~1s (fetches token metadata)
- Next alerts <500ms (cached)
- Normal behavior

## Cost
- Railway: Free tier usually covers this
- Helius: Free tier includes webhooks
- Telegram: Free

Total cost: $0-5/month

Done! Your bot is live 24/7 🎉
