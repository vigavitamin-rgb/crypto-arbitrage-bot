import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import websockets
from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
SYMBOLS = ('BTC/USDT', 'ETH/USDT', 'SOL/USDT')
EXCHANGES = ('okx', 'mexc', 'bitget', 'gate', 'kraken', 'coinbase')
THRESHOLD = float(os.getenv('ALERT_THRESHOLD_PCT', '0.5'))
SCAN_TOKEN = os.getenv('SCAN_TOKEN', '')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
MAX_QUOTE_AGE = float(os.getenv('MAX_QUOTE_AGE_SECONDS', '3'))
ALERT_COOLDOWN = float(os.getenv('ALERT_COOLDOWN_SECONDS', '120'))
LOCK = threading.RLock()
quotes = {}
errors = {}
connections = {}
last_alert = {}
started_at = datetime.now(timezone.utc).isoformat()


def authorized():
    return bool(SCAN_TOKEN) and request.headers.get('X-Scan-Token') == SCAN_TOKEN


def telegram_send(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError('Telegram credentials missing')
    payload = urlencode({'chat_id': TELEGRAM_CHAT_ID, 'text': message}).encode()
    req = Request('https://api.telegram.org/bot' + TELEGRAM_TOKEN + '/sendMessage', data=payload, method='POST')
    with urlopen(req, timeout=10) as response:
        return response.status


def update_quote(exchange, symbol, bid, ask, bid_qty=None, ask_qty=None):
    try:
        bid, ask = float(bid), float(ask)
        if not (0 < bid <= ask):
            return
        with LOCK:
            quotes[(exchange, symbol)] = {'bid': bid, 'ask': ask, 'bid_qty': bid_qty, 'ask_qty': ask_qty, 'updated': time.monotonic()}
    except (TypeError, ValueError):
        return


def opportunities():
    now = time.monotonic()
    with LOCK:
        snapshot = {k: v.copy() for k, v in quotes.items() if now - v['updated'] <= MAX_QUOTE_AGE}
    results = []
    for symbol in SYMBOLS:
        rows = [(ex, v) for (ex, sym), v in snapshot.items() if sym == symbol]
        for buy_ex, buy in rows:
            for sell_ex, sell in rows:
                if buy_ex == sell_ex:
                    continue
                pct = (sell['bid'] / buy['ask'] - 1) * 100
                if pct >= THRESHOLD:
                    results.append({'symbol': symbol, 'buy_exchange': buy_ex, 'sell_exchange': sell_ex, 'buy_ask': buy['ask'], 'sell_bid': sell['bid'], 'gross_spread_pct': round(pct, 3)})
    return sorted(results, key=lambda x: x['gross_spread_pct'], reverse=True)


async def alert_loop():
    while True:
        for item in opportunities():
            key = (item['symbol'], item['buy_exchange'], item['sell_exchange'])
            now = time.monotonic()
            with LOCK:
                eligible = now - last_alert.get(key, -1e12) >= ALERT_COOLDOWN
                if eligible:
                    last_alert[key] = now
            if eligible:
                msg = ('⚡ Возможный спред (до комиссий)\n'
                       f"{item['symbol']}: {item['buy_exchange']} → {item['sell_exchange']}\n"
                       f"Покупка: {item['buy_ask']} / продажа: {item['sell_bid']}\n"
                       f"Спред: {item['gross_spread_pct']:.3f}%\n"
                       'Проверь комиссии, объём и доступность торгов.')
                try:
                    await asyncio.to_thread(telegram_send, msg)
                except Exception as exc:
                    app.logger.warning('Telegram send failed: %s', type(exc).__name__)
        await asyncio.sleep(0.25)


async def consume(exchange, url, subscribe, parser, ping_interval=20):
    delay = 1
    while True:
        try:
            async with websockets.connect(url, ping_interval=ping_interval, ping_timeout=20, max_size=2**20, max_queue=32) as ws:
                with LOCK:
                    connections[exchange] = 'connected'
                    errors.pop(exchange, None)
                delay = 1
                for message in subscribe:
                    await ws.send(json.dumps(message) if isinstance(message, (dict, list)) else message)
                async for raw in ws:
                    if isinstance(raw, bytes):
                        continue
                    try:
                        data = json.loads(raw)
                        for symbol, bid, ask, bq, aq in parser(data):
                            update_quote(exchange, symbol, bid, ask, bq, aq)
                    except (ValueError, TypeError, KeyError, IndexError):
                        continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            with LOCK:
                connections[exchange] = 'disconnected'
                errors[exchange] = type(exc).__name__ + ': ' + str(exc)[:140]
            app.logger.warning('%s disconnected: %s', exchange, type(exc).__name__)
        with LOCK:
            connections[exchange] = 'disconnected'
            for key in list(quotes):
                if key[0] == exchange:
                    del quotes[key]
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)


def okx_parser(data):
    arg = data.get('arg') or {}
    inst = arg.get('instId', '')
    if arg.get('channel') != 'tickers' or inst not in ('BTC-USDT', 'ETH-USDT', 'SOL-USDT'):
        return []
    out = []
    for row in data.get('data', []):
        if row.get('bidPx') and row.get('askPx'):
            out.append((inst.replace('-', '/'), row['bidPx'], row['askPx'], row.get('bidSz'), row.get('askSz')))
    return out


def mexc_parser(data):
    channel = data.get('c', '')
    if 'bookTicker' not in channel:
        return []
    sym = data.get('s', '')
    if sym not in ('BTCUSDT', 'ETHUSDT', 'SOLUSDT'):
        return []
    row = data.get('d') or {}
    bid = row.get('b') or row.get('bidPrice')
    ask = row.get('a') or row.get('askPrice')
    return [(sym[:-4] + '/USDT', bid, ask, row.get('B'), row.get('A'))] if bid and ask else []


def bitget_parser(data):
    arg = data.get('arg') or {}
    if arg.get('channel') != 'ticker':
        return []
    sym = arg.get('instId', '')
    if sym not in ('BTCUSDT', 'ETHUSDT', 'SOLUSDT'):
        return []
    return [(sym[:-4] + '/USDT', r.get('bidPr'), r.get('askPr'), r.get('bidSz'), r.get('askSz')) for r in data.get('data', [])]


def gate_parser(data):
    if data.get('channel') != 'spot.book_ticker' or data.get('event') != 'update':
        return []
    r = data.get('result') or {}
    sym = r.get('s', '')
    if sym not in ('BTC_USDT', 'ETH_USDT', 'SOL_USDT'):
        return []
    return [(sym.replace('_', '/'), r.get('b'), r.get('a'), r.get('B'), r.get('A'))]


def kraken_parser(data):
    if data.get('channel') != 'ticker' or data.get('type') not in ('snapshot', 'update'):
        return []
    out = []
    for r in data.get('data', []):
        sym = r.get('symbol')
        if sym in SYMBOLS:
            out.append((sym, r.get('bid'), r.get('ask'), r.get('bid_qty'), r.get('ask_qty')))
    return out


def coinbase_parser(data):
    if data.get('channel') != 'ticker':
        return []
    out = []
    for event in data.get('events', []):
        for r in event.get('tickers', []):
            sym = r.get('product_id', '')
            if sym in ('BTC-USDT', 'ETH-USDT', 'SOL-USDT'):
                out.append((sym.replace('-', '/'), r.get('best_bid'), r.get('best_ask'), r.get('best_bid_quantity'), r.get('best_ask_quantity')))
    return out


async def run():
    pairs = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    jobs = [
        consume('okx', 'wss://ws.okx.com:8443/ws/v5/public', [{'op': 'subscribe', 'args': [{'channel': 'tickers', 'instId': s.replace('USDT', '-USDT')} for s in pairs]}], okx_parser),
        consume('mexc', 'wss://wbs-api.mexc.com/ws', [{'method': 'SUBSCRIPTION', 'params': [f'spot@public.bookTicker.v3.api@{s}' for s in pairs]}], mexc_parser),
        consume('bitget', 'wss://ws.bitget.com/v2/ws/public', [{'op': 'subscribe', 'args': [{'instType': 'SPOT', 'channel': 'ticker', 'instId': s} for s in pairs]}], bitget_parser),
        consume('gate', 'wss://api.gateio.ws/ws/v4/', [{'time': int(time.time()), 'channel': 'spot.book_ticker', 'event': 'subscribe', 'payload': [s.replace('USDT', '_USDT') for s in pairs]}], gate_parser),
        consume('kraken', 'wss://ws.kraken.com/v2', [{'method': 'subscribe', 'params': {'channel': 'ticker', 'symbol': list(SYMBOLS), 'event_trigger': 'bbo'}}], kraken_parser),
        consume('coinbase', 'wss://advanced-trade-ws.coinbase.com', [{'type': 'subscribe', 'product_ids': [s.replace('/', '-') for s in SYMBOLS], 'channel': 'ticker'}], coinbase_parser),
        alert_loop(),
    ]
    await asyncio.gather(*jobs)


def background():
    asyncio.run(run())


@app.get('/')
def home():
    with LOCK:
        return jsonify({'service': 'Crypto Arbitrage WS Prototype', 'status': 'online', 'connections': dict(connections)})


@app.get('/results')
def results():
    if not authorized():
        return jsonify({'error': 'Unauthorized'}), 401
    now = time.monotonic()
    with LOCK:
        fresh = {f'{ex}:{sym}': {'bid': v['bid'], 'ask': v['ask'], 'age_seconds': round(now - v['updated'], 2)} for (ex, sym), v in quotes.items() if now - v['updated'] <= MAX_QUOTE_AGE}
        status = dict(connections)
        errs = dict(errors)
    return jsonify({'started_at': started_at, 'connections': status, 'errors': errs, 'fresh_quotes': fresh, 'opportunities': opportunities()[:30]})


@app.post('/scan')
def scan():
    if not authorized():
        return jsonify({'error': 'Unauthorized'}), 401
    return jsonify({'status': 'websocket_mode', 'message': 'Continuous scanning active'}), 200


@app.post('/test-telegram')
def test_telegram():
    if not authorized():
        return jsonify({'error': 'Unauthorized'}), 401
    try:
        return jsonify({'telegram_http_status': telegram_send('Belisarius Arbitrage WS: test')}), 200
    except Exception as exc:
        return jsonify({'error': type(exc).__name__}), 502


if __name__ == '__main__':
    threading.Thread(target=background, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '10000')))
