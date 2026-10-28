import asyncio
import os
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import ccxt.async_support as ccxt
from flask import Flask, jsonify, request

app = Flask(__name__)
EXCHANGES = ('binance', 'bybit', 'okx', 'mexc', 'bitget', 'gate', 'kraken', 'coinbase')
SYMBOLS = ('BTC/USDT', 'ETH/USDT', 'SOL/USDT')
THRESHOLD = float(os.getenv('ALERT_THRESHOLD_PCT', '0.5'))
SCAN_TOKEN = os.getenv('SCAN_TOKEN', '')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
MIN_GAP = int(os.getenv('MIN_SCAN_GAP_SECONDS', '55'))
state_lock = threading.Lock()
state = {'status': 'ready', 'last_run': None, 'results': [], 'errors': {}, 'last_error': None, 'duration_seconds': None}
last_started = 0.0


def telegram_send(message):
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        return 'Telegram not configured'
    payload = urlencode({'chat_id': TELEGRAM_CHAT_ID, 'text': message}).encode()
    req = Request(f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage', data=payload, method='POST')
    with urlopen(req, timeout=15) as response:
        return str(response.status)


async def fetch_exchange(exchange_id):
    exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True, 'timeout': 12000})
    try:
        await exchange.load_markets()
        quotes = {}
        for symbol in SYMBOLS:
            if symbol not in exchange.markets or not exchange.markets[symbol].get('spot', False):
                continue
            book = await exchange.fetch_order_book(symbol, limit=5)
            if book.get('asks') and book.get('bids'):
                ask, ask_qty = book['asks'][0][:2]
                bid, bid_qty = book['bids'][0][:2]
                if ask > 0 and bid > 0:
                    quotes[symbol] = {'ask': ask, 'ask_qty': ask_qty, 'bid': bid, 'bid_qty': bid_qty}
        return exchange_id, quotes, None
    except Exception as exc:
        return exchange_id, {}, f'{type(exc).__name__}: {str(exc)[:180]}'
    finally:
        await exchange.close()


async def scan_all():
    data = await asyncio.gather(*(fetch_exchange(e) for e in EXCHANGES))
    errors = {e: error for e, _, error in data if error}
    opportunities = []
    for symbol in SYMBOLS:
        buyers = [(e, q[symbol]) for e, q, _ in data if symbol in q]
        for buy_exchange, buy in buyers:
            for sell_exchange, sell in buyers:
                if buy_exchange == sell_exchange:
                    continue
                spread = (sell['bid'] / buy['ask'] - 1) * 100
                if spread >= THRESHOLD:
                    opportunities.append({'symbol': symbol, 'buy_exchange': buy_exchange, 'sell_exchange': sell_exchange,
                                          'buy_ask': buy['ask'], 'sell_bid': sell['bid'], 'gross_spread_pct': round(spread, 3),
                                          'max_top_book_base_qty': min(buy['ask_qty'], sell['bid_qty'])})
    opportunities.sort(key=lambda o: o['gross_spread_pct'], reverse=True)
    return opportunities[:30], errors, {e: list(q) for e, q, _ in data}


def worker():
    started = time.monotonic()
    try:
        opportunities, errors, available = asyncio.run(scan_all())
        message = None
        if opportunities:
            lines = ['Возможные арбитражные спреды (до комиссий):']
            for item in opportunities[:8]:
                lines.append(f"{item['symbol']}: {item['buy_exchange']} → {item['sell_exchange']}, {item['gross_spread_pct']:.2f}%")
            message = '\n'.join(lines)
        with state_lock:
            state.update(status='ready', last_run=datetime.now(timezone.utc).isoformat(), results=opportunities,
                         errors=errors, available_symbols=available, last_error=None,
                         duration_seconds=round(time.monotonic() - started, 2))
        if message:
            try:
                telegram_send(message)
            except Exception as exc:
                with state_lock:
                    state['last_error'] = f'Telegram: {type(exc).__name__}: {str(exc)[:150]}'
    except Exception as exc:
        with state_lock:
            state.update(status='failed', last_error=f'{type(exc).__name__}: {str(exc)[:250]}',
                         duration_seconds=round(time.monotonic() - started, 2))


@app.get('/')
def home():
    return jsonify({'service': 'Crypto Arbitrage Scanner', 'status': 'online', 'last_run': state['last_run']})


@app.get('/results')
def results():
    if not SCAN_TOKEN or request.headers.get('X-Scan-Token') != SCAN_TOKEN:
        return jsonify({'error': 'Unauthorized'}), 401
    with state_lock:
        return jsonify(dict(state))


@app.post('/scan')
def scan():
    global last_started
    if not SCAN_TOKEN or request.headers.get('X-Scan-Token') != SCAN_TOKEN:
        return jsonify({'error': 'Unauthorized'}), 401
    with state_lock:
        if state['status'] == 'running':
            return jsonify({'status': 'already_running'}), 409
        if time.monotonic() - last_started < MIN_GAP:
            return jsonify({'status': 'rate_limited'}), 429
        last_started = time.monotonic()
        state['status'] = 'running'
    threading.Thread(target=worker, daemon=True).start()
    return jsonify({'status': 'started'}), 202


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '10000')))
