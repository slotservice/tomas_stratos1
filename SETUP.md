# Stratos1 Trading Bot - Setup Notes

## Prerequisites
- Python 3.12+ installed on Windows
- Bybit Demo account with API key/secret
- Telegram account with API ID/Hash (from https://my.telegram.org)
- Telegram Bot created via @BotFather

## Quick Start

### 1. Install Dependencies
```bash
cd stratos1
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
# Copy example files
copy .env.example .env

# Edit .env with your credentials:
# - TG_API_ID, TG_API_HASH, TG_PHONE
# - TG_BOT_TOKEN
# - TG_NOTIFY_CHANNEL_ID
# - BYBIT_API_KEY, BYBIT_API_SECRET
# - BYBIT_DEMO=true (start with demo!)
```

### 3. Generate Telegram Session
```bash
python generate_session.py
# Follow prompts to enter phone and code
# Copy the session string to .env as TG_SESSION_STRING
```

### 4. Configure Trading Parameters
Edit `config.toml` for all trading settings:
- Wallet size, risk %, initial margin
- Leverage rules
- Break-even trigger and buffer
- Scaling steps
- Trailing stop settings
- Hedge settings
- Re-entry rules
- Duplicate blocking threshold
- Report schedule

### 5. Configure Telegram Groups
Edit `telegram_groups.toml` to add/remove source groups.

### 6. Run the Bot
```bash
python main.py
```

### 7. Build Windows Executable
```bash
build.bat
# Executable: dist/stratos1.exe
```

## Configuration Files

| File | Purpose |
|------|---------|
| `.env` | Secrets (API keys, tokens) |
| `config.toml` | All trading parameters |
| `telegram_groups.toml` | Source Telegram groups |

## Key Trading Parameters (config.toml)

| Setting | Default | Description |
|---------|---------|-------------|
| wallet.bot_wallet | 402.1 | Theoretical wallet (USDT) |
| wallet.risk_pct | 0.02 | 2% risk per trade |
| wallet.initial_margin | 20.0 | 20 USDT per trade |
| breakeven.trigger_pct | 2.3 | Move SL to BE at +2.3% |
| breakeven.buffer_pct | 0.15 | BE + 0.15% buffer |
| trailing_stop.activation_pct | 6.1 | Trailing at 6.1% or highest TP |
| trailing_stop.trailing_distance_pct | 2.5 | 2.5% trailing distance |
| hedge.trigger_pct | -2.0 | Hedge at -2% |
| reentry.max_reentries | 2 | Max 2 re-entries |
| duplicate.threshold_pct | 5.0 | Block within 5% entry diff |

## Testing
```bash
# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/test_leverage.py -v
```

## Logs
- Console output (colored)
- File: `stratos1.log` (JSON structured)
- Database: `stratos1.db` (SQLite)

## Troubleshooting

### Telegram session issues
Re-run `generate_session.py` to create a new session.

### Bybit API errors
- Check API key permissions (trade + read)
- Verify demo mode is enabled: `BYBIT_DEMO=true`
- Check hedge mode is supported for the symbol

### Bot won't start
Check the health check output in Telegram. The bot verifies all connections on startup.
