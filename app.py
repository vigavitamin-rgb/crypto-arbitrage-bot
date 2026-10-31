import asyncio
import json
import os
import threading
import time

from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import ccxt.async_support as ccxt
from flask import Flask, jsonify, request


app = Flask(__name__)

# -----------------------------
# Configuration
# -----------------------------

EXCHANGES = (
    'binance',
    'bybit',
    'okx',
    'mexc',
    'bitget',
    'gate',
    'kraken',
    'coinbase',
)

SYMBOLS = (
    'BTC/USDT',
    'ETH/USDT',
    'SOL/USDT',
)

THRESHOLD = float(
    os.getenv('ALERT_THRESHOLD_PCT', '0.5')
)

SCAN_TOKEN = os.getenv('SCAN_TOKEN', '')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

MIN_GAP = int(
    os.getenv('MIN_SCAN_GAP_SECONDS', '55')
)

state_lock = threading.Lock()

state = {
    'status': 'ready',
    'last_run': None,
    'results': [],
    'errors': {},
    'last_error': None,
    'duration_seconds': None,
}

last_started = 0.0


# -----------------------------
# Authorization
# -----------------------------

def authorized():
    return (
        bool(SCAN_TOKEN)
        and request.headers.get('X-Scan-Token')
        == SCAN_TOKEN
    )


# -----------------------------
# Telegram
# -----------------------------

def safe_telegram_error(exc):
    if isinstance(exc, HTTPError):
        try:
            data = json.loads(
                exc.read().decode('utf-8')
            )

            description = str(
                data.get(
                    'description',
                    'No description'
                )
            )

            return (
                f'Telegram HTTP {exc.code}: '
                f'{description[:200]}'
            )

        except Exception:
            return f'Telegram HTTP {exc.code}'

    if isinstance(exc, URLError):
        return (
            'Telegram connection error: '
            f'{type(exc.reason).__name__}'
        )

    return (
        f'Telegram error: '
        f'{type(exc).__name__}'
    )


def telegram_api(method):
    if not TELEGRAM_TOKEN:
        raise RuntimeError(
            'Telegram token not configured'
        )

    req = Request(
        f'https://api.telegram.org/'
        f'bot{TELEGRAM_TOKEN}/{method}',
        method='GET',
    )

    with urlopen(req, timeout=15) as response:
        return json.load(response)


def telegram_send(message):
    if not (
        TELEGRAM_TOKEN
        and TELEGRAM_CHAT_ID
    ):
        return 'Telegram not configured'

    payload = urlencode({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
    }).encode('utf-8')

    req = Request(
        f'https://api.telegram.org/'
        f'bot{TELEGRAM_TOKEN}/sendMessage',
        data=payload,
        method='POST',
    )

    with urlopen(req, timeout=15) as response:
        return str(response.status)


# -----------------------------
# Exchange scanning
# -----------------------------

async def fetch_exchange(exchange_id):
    exchange = getattr(ccxt, exchange_id)({
        'enableRateLimit': True,
        'timeout': 12000,
    })

    try:
        await exchange.load_markets()

        quotes = {}

        for symbol in SYMBOLS:
            if symbol not in exchange.markets:
                continue

            market = exchange.markets[symbol]

            if not market.get('spot', False):
                continue

            book = await exchange.fetch_order_book(
                symbol,
                limit=5,
            )

            if book.get('asks') and book.get('bids'):
                ask, ask_qty = book['asks'][0][:2]
                bid, bid_qty = book['bids'][0][:2]

                if ask > 0 and bid > 0:
                    quotes[symbol] = {
                        'ask': ask,
                        'ask_qty': ask_qty,
                        'bid': bid,
                        'bid_qty': bid_qty,
                    }

        return exchange_id, quotes, None

    except Exception as exc:
        return (
            exchange_id,
            {},
            f'{type(exc).__name__}: '
            f'{str(exc)[:180]}',
        )

    finally:
        await exchange.close()


async def scan_all():
    data = await asyncio.gather(
        *(
            fetch_exchange(exchange)
            for exchange in EXCHANGES
        )
    )

    errors = {
        exchange: error
        for exchange, _, error in data
        if error
    }

    opportunities = []

    for symbol in SYMBOLS:
        quotes_for_symbol = [
            (exchange, quotes[symbol])
            for exchange, quotes, _ in data
            if symbol in quotes
        ]

        for buy_exchange, buy in quotes_for_symbol:
            for sell_exchange, sell in quotes_for_symbol:
                if buy_exchange == sell_exchange:
                    continue

                spread = (
                    sell['bid'] / buy['ask'] - 1
                ) * 100

                if spread >= THRESHOLD:
                    opportunities.append({
                        'symbol': symbol,
                        'buy_exchange': buy_exchange,
                        'sell_exchange': sell_exchange,
                        'buy_ask': buy['ask'],
                        'sell_bid': sell['bid'],
                        'gross_spread_pct': round(
                            spread, 3
                        ),
                        'max_top_book_base_qty': min(
                            buy['ask_qty'],
                            sell['bid_qty'],
                        ),
                    })

    opportunities.sort(
        key=lambda item: item['gross_spread_pct'],
        reverse=True,
    )

    available = {
        exchange: list(quotes)
        for exchange, quotes, _ in data
    }

    return opportunities[:30], errors, available


# -----------------------------
# Background worker
# -----------------------------

def worker():
    started = time.monotonic()

    try:
        opportunities, errors, available = (
            asyncio.run(scan_all())
        )

        message = None

        if opportunities:
            lines = [
                'Возможные арбитражные спреды '
                '(до комиссий):'
            ]

            for item in opportunities[:8]:
                lines.append(
                    f"{item['symbol']}: "
                    f"{item['buy_exchange']} → "
                    f"{item['sell_exchange']}, "
                    f"{item['gross_spread_pct']:.2f}%"
                )

            message = '\n'.join(lines)

        with state_lock:
            state.update(
                status='ready',
                last_run=datetime.now(
                    timezone.utc
                ).isoformat(),
                results=opportunities,
                errors=errors,
                available_symbols=available,
                last_error=None,
                duration_seconds=round(
                    time.monotonic() - started,
                    2,
                ),
            )

        if message:
            try:
                result = telegram_send(message)

                if result == 'Telegram not configured':
                    with state_lock:
                        state['last_error'] = result

            except Exception as exc:
                error = safe_telegram_error(exc)

                app.logger.error(
                    'Telegram notification failed: %s',
                    error,
                )

                with state_lock:
                    state['last_error'] = error

    except Exception as exc:
        with state_lock:
            state.update(
                status='failed',
                last_error=(
                    f'{type(exc).__name__}: '
                    f'{str(exc)[:250]}'
                ),
                duration_seconds=round(
                    time.monotonic() - started,
                    2,
                ),
            )


# -----------------------------
# HTTP routes
# -----------------------------

@app.get('/')
def home():
    return jsonify({
        'service': 'Crypto Arbitrage Scanner',
        'status': 'online',
        'last_run': state['last_run'],
    })


@app.get('/results')
def results():
    if not authorized():
        return jsonify({
            'error': 'Unauthorized'
        }), 401

    with state_lock:
        return jsonify(dict(state))


@app.post('/scan')
def scan():
    global last_started

    if not authorized():
        return jsonify({
            'error': 'Unauthorized'
        }), 401

    with state_lock:
        if state['status'] == 'running':
            return jsonify({
                'status': 'already_running'
            }), 200

        if (
            time.monotonic() - last_started
            < MIN_GAP
        ):
            return jsonify({
                'status': 'rate_limited'
            }), 200

        last_started = time.monotonic()
        state['status'] = 'running'

    threading.Thread(
        target=worker,
        daemon=True,
    ).start()

    return jsonify({
        'status': 'started'
    }), 202


@app.post('/test-telegram')
def test_telegram():
    if not authorized():
        return jsonify({
            'error': 'Unauthorized'
        }), 401

    try:
        result = telegram_send(
            '✅ Belisarius Arbitrage: '
            'тестовое сообщение. '
            'Связь с Telegram работает!'
        )

        if result == 'Telegram not configured':
            return jsonify({
                'status': 'error',
                'message': result,
            }), 503

        return jsonify({
            'status': 'sent',
            'telegram_http_status': result,
        }), 200

    except Exception as exc:
        error = safe_telegram_error(exc)

        app.logger.error(
            'Telegram test failed: %s',
            error,
        )

        return jsonify({
            'status': 'error',
            'message': error,
        }), 502


# -----------------------------
# Telegram diagnostics
# -----------------------------

@app.post('/telegram-chat-check')
def telegram_chat_check():
    if not authorized():
        return jsonify({
            'error': 'Unauthorized'
        }), 401

    if not TELEGRAM_TOKEN:
        return jsonify({
            'error': 'Telegram token not configured'
        }), 503

    try:
        bot_data = telegram_api('getMe')
        webhook_data = telegram_api('getWebhookInfo')

        bot = bot_data.get('result', {})
        webhook = webhook_data.get('result', {})

        webhook_active = bool(
            webhook.get('url')
        )

        chats = {}
        updates_count = None
        updates_error = None

        try:
            updates_data = telegram_api('getUpdates')

            updates = updates_data.get('result', [])
            updates_count = len(updates)

            for update in updates:
                for field in (
                    'message',
                    'edited_message',
                    'channel_post',
                    'my_chat_member',
                ):
                    entry = update.get(field) or {}
                    chat = entry.get('chat') or {}

                    if 'id' in chat:
                        chats[str(chat['id'])] = {
                            'chat_id': chat['id'],
                            'type': chat.get('type'),
                        }

        except Exception as exc:
            updates_error = safe_telegram_error(exc)

        return jsonify({
            'configured_chat_id': TELEGRAM_CHAT_ID,
            'bot_username': bot.get('username'),
            'bot_id': bot.get('id'),
            'webhook_active': webhook_active,
            'pending_update_count': webhook.get(
                'pending_update_count', 0
            ),
            'chats': list(chats.values()),
            'updates_count': updates_count,
            'updates_error': updates_error,
        })

    except Exception as exc:
        return jsonify({
            'status': 'error',
            'message': safe_telegram_error(exc),
        }), 502


# -----------------------------
# Start server
# -----------------------------

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('PORT', '10000')),
    )
