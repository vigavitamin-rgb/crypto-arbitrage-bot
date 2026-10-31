# WebSocket V2 — isolated experimental scanner

This directory is a standalone prototype. It does not replace the production scanner in the repository root.

## Run locally

Requires Python 3.11+.

```sh
cd ws_v2
python -m pip install -r requirements.txt
python -m unittest -v test_offline.py
SCAN_TOKEN=choose-a-secret PORT=10000 python app.py
```

The application uses **six persistent public WebSocket feeds** (OKX, MEXC, Bitget, Gate, Kraken and Coinbase). Market feeds and exchange-specific subscription formats must be validated against live traffic before production use. The offline tests exercise parsers with synthetic payloads only.

## Configuration

- `SCAN_TOKEN`: required for `/results`, `/scan` and `/test-telegram`.
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`: optional for Telegram alerts.
- `ALERT_THRESHOLD_PCT`: gross spread alert threshold, default 0.5.
- `MAX_QUOTE_AGE_SECONDS`: maximum quote age, default 3.
- `ALERT_COOLDOWN_SECONDS`: repeat alert cooldown, default 120.
- `PORT`: HTTP port, default 10000.

Check `GET /` for connection status. Check `GET /results` with header `X-Scan-Token` for fresh quotes and parser/connectivity errors. `POST /scan` is a compatibility endpoint; it does not trigger a REST scan.

## Isolated Render test

Create a **separate** Render web service pointing to branch `ws-test-connection`, with root directory `ws_v2`. Use build command `pip install -r requirements.txt` and start command `python app.py`. Do **not** change the existing production service or deploy this branch to it.

Before running live, verify memory consumption, connections, quote freshness and correctness of bids/asks for all six exchanges. Public bid/ask spreads do not account for fees, slippage, transfer costs or liquidity. This is not an execution bot.

**Caution:** `python app.py` starts the WebSocket background thread. A generic `gunicorn app:app` command will only import the Flask app and will **not** start the feed consumer.
