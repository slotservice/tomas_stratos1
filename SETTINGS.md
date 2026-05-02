# Stratos1 — Operator settings reference

Most settings the operator will ever want to change are in plain text files
in the bot's directory. Edit with any text editor (nano, notepad, etc.),
then restart the bot:

```bash
ssh Tomas "cd /opt/stratos1 && nano <file>"
ssh Tomas "systemctl restart stratos1"
```

Some files (currently only `symbol_notes.toml`) are reloaded automatically
on every signal — no restart needed.

---

## `config.toml` — the main settings file

Bot reads this on startup. Restart required after editing.

### `[wallet]` — risk + position sizing

| Field | Default | Meaning |
|---|---|---|
| `bot_wallet` | `402.1` | Theoretical wallet (USDT) used in the leverage formula. NOT your actual Bybit balance. |
| `risk_pct` | `0.02` | Risk per trade (2 % of `bot_wallet`). |
| `initial_margin` | `20.0` | Initial margin per position (USDT). |

### `[leverage]` — leverage bucket boundaries

| Field | Default | Meaning |
|---|---|---|
| `min_leverage` | `6.0` | Used by **swing** signals (always x6) and as floor for low-formula values. |
| `neutral_zone_split` | `6.75` | Below this raw value -> x6. |
| `neutral_zone_upper` | `7.5` | Floor for **dynamic** signals (raw 6.75-7.5 -> x7.5). |
| `max_entry_leverage` | `25.0` | Cap for any computed leverage. |

### `[hedge]` — hedge model

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Master on/off for hedging. |
| `trigger_pct` | `-1.5` | Hedge fires at this % adverse from original entry. |
| `hard_sl_pct` | `2.0` | Hedge hard SL (% from hedge entry, BACKUP behind trailing). |
| `trailing_pct` | `1.2` | Hedge trailing distance (primary exit). |
| `original_force_close_pct` | `2.0` | Original trade auto-close at this % adverse. |
| `timeout_minutes` | `20` | Close hedge if no meaningful move within this window. |
| `no_move_threshold_pct` | `0.5` | "Meaningful move" definition. |

### `[trailing_stop]` — original trade trailing

| Field | Default | Meaning |
|---|---|---|
| `activation_pct` | `6.1` | Trailing activates at this % favorable. |
| `trailing_distance_pct` | `2.5` | Trailing distance once activated. |
| `trigger_type` | `LastPrice` | Bybit trigger source. |

### `[auto_sl]` — fixed-SL fallback (when signal has no SL)

| Field | Default | Meaning |
|---|---|---|
| `fallback_pct` | `3.0` | Auto-SL distance from entry (%). |
| `fallback_leverage` | `10.0` | Fixed leverage for `signal_type=fixed`. |

### `[duplicate]` / `[stale_signal]` / `[capacity]` / `[reentry]`

Standard signal-management knobs. See inline comments in `config.toml`.

### `[reporting]`

| Field | Default | Meaning |
|---|---|---|
| `daily_report_hour` | `23` | Hour of day for daily report (Stockholm time). |
| `weekly_report_day` | `0` | 0 = Monday. |
| `weekly_report_hour` | `23` | Hour of day for weekly report. |
| `audit_snapshot_every_n_trades` | `10` | Post in-bot audit snapshot every N closes. 0 = disabled. |

---

## `telegram_groups.toml` — channels the bot listens to

One block per channel:

```toml
[[groups]]
id   = -1003287883842
name = "Crypto Mason"
```

To add a channel: append a new `[[groups]]` block with the chat ID and a name.
To remove: delete the block.
Restart required.

---

## `symbol_notes.toml` — operator notes per symbol (NEW)

When the bot rejects a signal because the symbol isn't tradable on Bybit
(e.g. delisted), the rejection notification normally reads:

```
📍 Fel: Kontrolera manuellt
```

If you add an entry for that symbol in `symbol_notes.toml`, the bot will
use your custom text instead. Edits take effect on the **next signal** —
no restart required.

```toml
[symbol_notes]
FETUSDT = "FETUSDT checked 2026-05-01 — delisted on Bybit perpetuals (FET+AGIX+OCEAN merger -> ASI)"
```

Result for the next FETUSDT signal:

```
⚠️ Finns inte på bybit ⚠️
🕒 Tid: 4:23 PM
📢 Från kanal: #CoinAura
📊 Symbol: #FETUSDT
📈 Riktning: SHORT
📍 Fel: FETUSDT checked 2026-05-01 — delisted on Bybit perpetuals (FET+AGIX+OCEAN merger -> ASI)
```

---

## `.env` — secrets (do not commit)

Contains Bybit API keys, Telegram session string, etc. Edit with care.
Restart required after changes.

```
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
BYBIT_DEMO=true
TG_API_ID=...
TG_API_HASH=...
TG_BOT_TOKEN=...
TG_NOTIFICATION_CHANNEL_ID=...
TG_SESSION_STRING=...
```

---

## After editing — restart procedure

```bash
ssh Tomas "systemctl restart stratos1 && sleep 5 && systemctl status stratos1 --no-pager | head -8"
```

The bot will reload all settings on restart. Live positions on Bybit are
preserved (state is rehydrated from DB).

If a new TP order or hedge fires while the bot is stopping, it is processed
on the next startup via `state_recovery`.
