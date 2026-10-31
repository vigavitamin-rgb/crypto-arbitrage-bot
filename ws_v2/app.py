import asyncio
import json
import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import websockets
from market_overlap import build_report
from flask import Flask, jsonify, request, send_file

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
QUOTE_CURRENCIES = ('USDT', 'USD', 'USDC', 'EUR', 'BTC')
BASE_ASSETS = ('BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'LINK', 'LTC')
ALL_PAIRS_TEST = os.getenv('ALL_PAIRS_TEST', '1') == '1'
MAX_WS_PAIRS_PER_EXCHANGE = max(1, int(os.getenv('MAX_WS_PAIRS_PER_EXCHANGE', '60')))
PAIR_ROTATION_SECONDS = max(60, int(os.getenv('PAIR_ROTATION_SECONDS', '180')))
PAIR_UNIVERSE = ()
# Bounded active set; only pairs supported by >=2 exchanges enter calculations.
SYMBOLS = tuple(f'{base}/{quote}' for quote in QUOTE_CURRENCIES for base in BASE_ASSETS if base != quote)
ACTIVE_SYMBOLS = SYMBOLS
SUPPORTED_BY_EXCHANGE = {}
EXCHANGES = ('okx', 'mexc', 'bitget', 'gate', 'kraken', 'coinbase')
THRESHOLD = float(os.getenv('ALERT_THRESHOLD_PCT', '0.5'))
SCAN_TOKEN = os.getenv('SCAN_TOKEN', '')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
MAX_QUOTE_AGE = float(os.getenv('MAX_QUOTE_AGE_SECONDS', '3'))
ALERT_COOLDOWN = float(os.getenv('ALERT_COOLDOWN_SECONDS', '120'))
TRADE_BUDGET_USDT = float(os.getenv('SIM_TRADE_BUDGET_USDT', '50'))
# Conservative entry-tier taker rates; account/region/pair discounts may differ.
TAKER_FEES_PCT = {'okx': 0.35, 'mexc': 0.05, 'bitget': 0.10,
                  'gate': 0.10, 'kraken': 0.80, 'coinbase': 0.90}
TAKER_FEES_PCT = {ex: float(os.getenv('SIM_TAKER_FEE_' + ex.upper() + '_PCT', rate))
                  for ex, rate in TAKER_FEES_PCT.items()}
BUDGET_RUB_PER_EXCHANGE = float(os.getenv('SIM_BUDGET_RUB_PER_EXCHANGE', '5000'))
RUB_PER_USDT = float(os.getenv('SIM_RUB_PER_USDT', '0'))
# Until a verified conversion rate is configured, keep the previous 50 USDT test size.
TRADE_BUDGET_USDT = (BUDGET_RUB_PER_EXCHANGE / RUB_PER_USDT if RUB_PER_USDT > 0
                     else float(os.getenv('SIM_TRADE_BUDGET_USDT', '50')))
SLIPPAGE_PCT = float(os.getenv('SIM_SLIPPAGE_PCT', '0.05'))
ALERT_CHECK_INTERVAL = float(os.getenv('ALERT_CHECK_INTERVAL_SECONDS', '0.02'))
LOCK = threading.RLock()
quotes = {}
errors = {}
connections = {}
last_alert = {}
started_at = datetime.now(timezone.utc).isoformat()
quote_event = None
latency_stats = {ex: {'messages': 0, 'last_receive': None, 'intervals_ms': [], 'processing_ms': []} for ex in EXCHANGES}
scan_ms = []
market_overlap_report = {'complete': False, 'status': 'pending'}
feed_quarantine = set()
feed_bad_checks = {ex: 0 for ex in EXCHANGES}
feed_good_checks = {ex: 0 for ex in EXCHANGES}
notified_active_pairs = set()
notified_active_exchanges = set()
notification_baseline_ready = False
notification_last_sent = 0.0
notification_pair_checks = {sym: 0 for sym in SYMBOLS}
notification_pair_recovery_checks = {sym: 0 for sym in SYMBOLS}
notification_exchange_checks = {ex: 0 for ex in EXCHANGES}
notification_exchange_recovery_checks = {ex: 0 for ex in EXCHANGES}
source_age_ms = {ex: [] for ex in EXCHANGES}
clock_by_exchange = {}
clock_reference = {'offset_ms': None, 'rtt_ms': None, 'measured_at': None, 'source': 'OKX public server time'}


def summarize_ms(values):
    if not values:
        return {'count': 0, 'p50': None, 'p95': None, 'max': None}
    ordered = sorted(values)
    return {'count': len(values), 'p50': round(ordered[len(ordered)//2], 2),
            'p95': round(ordered[min(len(ordered)-1, math.ceil(len(ordered)*0.95)-1)], 2),
            'max': round(ordered[-1], 2)}


def calibrate_clock():
    """Estimate local wall-clock offset using midpoint and bound uncertainty by RTT/2."""
    url = 'https://www.okx.com/api/v5/public/time'
    try:
        t0 = time.time() * 1000
        with urlopen(Request(url, headers={'User-Agent': 'WS-latency-monitor'}), timeout=4) as response:
            data = json.load(response)
        t1 = time.time() * 1000
        server_ms = float(data['data'][0]['ts'])
        rtt = t1 - t0
        if 0 <= rtt <= 1000:
            with LOCK:
                clock_reference.update({'offset_ms': server_ms - (t0 + t1) / 2,
                                        'rtt_ms': rtt, 'measured_at': time.monotonic()})
    except (OSError, ValueError, KeyError, IndexError, TypeError):
        app.logger.warning('Clock reference calibration unavailable')


async def clock_calibration_loop():
    while True:
        await asyncio.to_thread(calibrate_clock)
        await asyncio.sleep(60)


def clock_snapshot():
    with LOCK:
        age = time.monotonic() - clock_reference['measured_at'] if clock_reference['measured_at'] is not None else None
        valid = age is not None and age <= 180
        return {'reference': clock_reference['source'], 'valid': valid,
                'server_minus_local_ms': round(clock_reference['offset_ms'], 2) if valid else None,
                'rtt_ms': round(clock_reference['rtt_ms'], 2) if valid else None,
                'uncertainty_at_least_ms': round(clock_reference['rtt_ms'] / 2, 2) if valid else None,
                'calibration_age_s': round(age, 1) if age is not None else None,
                'scope': 'OKX clock only; other exchanges are not assumed synchronized'}


def exchange_clock_probe(exchange):
    urls = {'okx': 'https://www.okx.com/api/v5/public/time',
            'mexc': 'https://api.mexc.com/api/v3/time',
            'kraken': 'https://api.kraken.com/0/public/Time'}
    try:
        t0 = time.time() * 1000
        with urlopen(Request(urls[exchange], headers={'User-Agent': 'WS-latency-monitor'}), timeout=4) as response:
            data = json.load(response)
        t1 = time.time() * 1000
        if exchange == 'okx':
            server_ms = float(data['data'][0]['ts'])
        elif exchange == 'mexc':
            server_ms = float(data['serverTime'])
        else:
            server_ms = float(data['result']['unixtime']) * 1000
        rtt = t1 - t0
        if not (0 <= rtt <= 500):
            return
        with LOCK:
            clock_by_exchange[exchange] = {'offset_ms': server_ms - (t0+t1)/2,
                                           'rtt_ms': rtt, 'measured_at': time.monotonic()}
    except (OSError, ValueError, KeyError, IndexError, TypeError):
        app.logger.warning('Clock calibration failed: %s', exchange)


async def exchange_clock_loop():
    while True:
        for exchange in ('okx', 'mexc', 'kraken'):
            await asyncio.to_thread(exchange_clock_probe, exchange)
        await asyncio.sleep(60)


def exchange_clock_snapshot():
    now = time.monotonic()
    with LOCK:
        return {ex: {'valid': now-v['measured_at'] <= 180 and v['rtt_ms'] <= 200,
                     'offset_ms': round(v['offset_ms'], 1),
                     'rtt_ms': round(v['rtt_ms'], 1),
                     'age_s': round(now-v['measured_at'], 1)}
                for ex, v in clock_by_exchange.items()}


def record_source_age(exchange, timestamp, received_ms):
    try:
        if isinstance(timestamp, str) and 'T' in timestamp:
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).timestamp() * 1000
        else:
            timestamp = float(timestamp)
            if timestamp < 1e11:
                timestamp *= 1000
        age = received_ms - timestamp
        if -2000 <= age <= 60000:
            with LOCK:
                source_age_ms[exchange].append(age)
                del source_age_ms[exchange][:-500]
    except (ValueError, TypeError, OverflowError):
        return


def record_latency(exchange, received_at, processed_at):
    with LOCK:
        stat = latency_stats[exchange]
        if stat['last_receive'] is not None:
            delta = (received_at - stat['last_receive']) * 1000
            if delta >= 0:
                stat['intervals_ms'].append(delta)
                del stat['intervals_ms'][:-500]
        stat['last_receive'] = received_at
        stat['messages'] += 1
        stat['processing_ms'].append(max(0.0, (processed_at - received_at) * 1000))
        del stat['processing_ms'][:-500]


def freshness_snapshot():
    now = time.monotonic()
    with LOCK:
        result = {}
        for ex in EXCHANGES:
            pairs = {}
            for sym in ACTIVE_SYMBOLS:
                quote = quotes.get((ex, sym))
                age = round(max(0.0, now - quote['updated']) * 1000, 1) if quote else None
                pairs[sym] = {'age_ms': age, 'fresh': age is not None and age <= MAX_QUOTE_AGE * 1000}
            result[ex] = pairs
        return result


def latency_snapshot():
    with LOCK:
        return {'exchanges': {ex: {'messages_since_start': v['messages'],
                 'message_interval_ms': summarize_ms(v['intervals_ms']),
                 'parse_and_update_ms': summarize_ms(v['processing_ms'])}
                 for ex, v in latency_stats.items()},
                 'arbitrage_scan_ms': summarize_ms(scan_ms),
                 'source_age_ms': {ex: summarize_ms(source_age_ms[ex]) for ex in EXCHANGES},
                 'exchange_clock_calibration': exchange_clock_snapshot(),
                 'clock_calibration': clock_snapshot(),
                 'quote_freshness': freshness_snapshot(),
                 'note': 'Local arrival intervals and processing durations, NOT exchange-to-server network latency.'}



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
        if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid <= ask):
            return
        with LOCK:
            quotes[(exchange, symbol)] = {'bid': bid, 'ask': ask, 'bid_qty': bid_qty, 'ask_qty': ask_qty, 'updated': time.monotonic()}
        if quote_event is not None:
            quote_event.set()
    except (TypeError, ValueError):
        return


def opportunities():
    now = time.monotonic()
    with LOCK:
        snapshot = {k: v.copy() for k, v in quotes.items() if now - v['updated'] <= MAX_QUOTE_AGE and k[0] not in feed_quarantine}
    results = []
    for symbol in ACTIVE_SYMBOLS:
        rows = [(ex, v) for (ex, sym), v in snapshot.items() if sym == symbol]
        for buy_ex, buy in rows:
            for sell_ex, sell in rows:
                if buy_ex == sell_ex:
                    continue
                pct = (sell['bid'] / buy['ask'] - 1) * 100
                if pct >= THRESHOLD:
                    buy_price = buy['ask'] * (1 + SLIPPAGE_PCT / 100)
                    sell_price = sell['bid'] * (1 - SLIPPAGE_PCT / 100)
                    unit_cost = buy_price * (1 + TAKER_FEES_PCT[buy_ex] / 100)
                    unit_revenue = sell_price * (1 - TAKER_FEES_PCT[sell_ex] / 100)
                    # Budget is denominated in the pair's quote currency; no invented FX conversion.
                    quote_currency = symbol.split('/')[1]
                    sim_budget = (float(os.getenv('SIM_TRADE_BUDGET_BTC', '0.001')) if quote_currency == 'BTC'\n                                  else float(os.getenv('SIM_TRADE_BUDGET_' + quote_currency, '50')) if quote_currency != 'USDT'\n                                  else TRADE_BUDGET_USDT)\n                    quantity = max(0.0, sim_budget) / unit_cost
                    liquidity_verified = False
                    try:
                        buy_qty = float(buy['ask_qty'])
                        sell_qty = float(sell['bid_qty'])
                        if all(math.isfinite(q) and q > 0 for q in (buy_qty, sell_qty)):
                            quantity = min(quantity, buy_qty, sell_qty)
                            liquidity_verified = True
                    except (TypeError, ValueError):
                        pass
                    net_profit = quantity * (unit_revenue - unit_cost)
                    if net_profit <= 0:
                        continue
                    results.append({'symbol': symbol, 'buy_exchange': buy_ex, 'sell_exchange': sell_ex,
                                    'buy_ask': buy['ask'], 'sell_bid': sell['bid'],
                                    'gross_spread_pct': round(pct, 3),
                                    'estimated_net_spread_pct': round((unit_revenue / unit_cost - 1) * 100, 3),
                                    'estimated_profit_usdt': round(net_profit, 4) if quote_currency == 'USDT' else None,
                                    'estimated_profit_quote': round(net_profit, 8),
                                    'quote_currency': quote_currency,
                                    'estimated_profit_rub': round(net_profit * RUB_PER_USDT, 2) if RUB_PER_USDT > 0 and quote_currency == 'USDT' else None,
                                    'estimated_trade_cost_usdt': round(quantity * unit_cost, 2) if quote_currency == 'USDT' else None,
                                    'estimated_trade_cost_quote': round(quantity * unit_cost, 8),
                                    'liquidity_verified': liquidity_verified,
                                    'buy_taker_fee_pct': TAKER_FEES_PCT[buy_ex],
                                    'sell_taker_fee_pct': TAKER_FEES_PCT[sell_ex],
                                    'budget_rub_per_exchange': BUDGET_RUB_PER_EXCHANGE,
                                    'rub_per_usdt_configured': RUB_PER_USDT > 0,
                                    'assumed_slippage_pct_per_side': SLIPPAGE_PCT})
    return sorted(results, key=lambda x: x['estimated_net_spread_pct'], reverse=True)


async def alert_loop():
    while True:
        await quote_event.wait()
        quote_event.clear()
        scan_started = time.monotonic()
        found = opportunities()
        with LOCK:
            scan_ms.append((time.monotonic() - scan_started) * 1000)
            del scan_ms[:-500]
        for item in found:
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
                       f"Оценка чистой прибыли: {item['estimated_profit_quote']:.6f} {item['quote_currency']} "
                       f"(оборот {item['estimated_trade_cost_quote']:.2f} {item['quote_currency']})\n"
                       f"Ликвидность подтверждена: {'да' if item['liquidity_verified'] else 'нет'}\n"
                       'Проверь комиссии, объём и доступность торгов.')
                try:
                    await asyncio.to_thread(telegram_send, msg)
                except Exception as exc:
                    app.logger.warning('Telegram send failed: %s', type(exc).__name__)



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
                async def keepalive():
                    if exchange not in ('bitget', 'mexc'):
                        return
                    while True:
                        await asyncio.sleep(20)
                        await ws.send('ping' if exchange == 'bitget' else json.dumps({'method': 'PING'}))

                heartbeat = asyncio.create_task(keepalive())
                try:
                    await read_feed(ws, exchange, parser)
                finally:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
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



async def read_feed(ws, exchange, parser):
    async for raw in ws:
        received_at = time.monotonic()
        received_ms = time.time() * 1000
        parsed = []
        source_ts = None
        if isinstance(raw, bytes):
            if exchange == 'mexc':
                try:
                    parsed = mexc_binary_parser(raw)
                except (ValueError, UnicodeError):
                    pass
        elif raw != 'pong':
            try:
                data = json.loads(raw)
                parsed = parser(data)
                if exchange == 'okx':
                    source_ts = (data.get('data') or [{}])[0].get('ts')
                elif exchange == 'bitget':
                    source_ts = data.get('ts')
                elif exchange == 'gate':
                    source_ts = (data.get('result') or {}).get('t')
                elif exchange == 'kraken':
                    source_ts = (data.get('data') or [{}])[0].get('timestamp')
                elif exchange == 'coinbase':
                    source_ts = data.get('timestamp')
            except (ValueError, TypeError, KeyError, IndexError):
                pass
        if parsed:
            for symbol, bid, ask, bq, aq in parsed:
                update_quote(exchange, symbol, bid, ask, bq, aq)
            record_latency(exchange, received_at, time.monotonic())
            record_source_age(exchange, source_ts, received_ms)


def okx_parser(data):
    arg = data.get('arg') or {}
    inst = arg.get('instId', '')
    if arg.get('channel') != 'tickers' or inst.replace('-', '/') not in ACTIVE_SYMBOLS:
        return []
    out = []
    for row in data.get('data', []):
        if row.get('bidPx') and row.get('askPx'):
            out.append((inst.replace('-', '/'), row['bidPx'], row['askPx'], row.get('bidSz'), row.get('askSz')))
    return out


def protobuf_fields(payload):
    """Read protobuf wire fields needed for MEXC's public best-bid/ask feed."""
    pos = 0
    fields = {}
    while pos < len(payload):
        key = 0
        shift = 0
        while True:
            if pos >= len(payload) or shift > 63:
                raise ValueError('Invalid protobuf key')
            byte = payload[pos]
            pos += 1
            key |= (byte & 127) << shift
            if byte < 128:
                break
            shift += 7
        number, wire = key >> 3, key & 7
        if number == 0:
            raise ValueError('Invalid protobuf field')
        if wire == 2:
            length = 0
            shift = 0
            while True:
                if pos >= len(payload) or shift > 63:
                    raise ValueError('Invalid protobuf length')
                byte = payload[pos]
                pos += 1
                length |= (byte & 127) << shift
                if byte < 128:
                    break
                shift += 7
            if length > len(payload) - pos:
                raise ValueError('Truncated protobuf')
            fields[number] = payload[pos:pos + length]
            pos += length
        elif wire == 0:
            while True:
                if pos >= len(payload):
                    raise ValueError('Truncated protobuf varint')
                byte = payload[pos]
                pos += 1
                if byte < 128:
                    break
        elif wire in (1, 5):
            pos += 8 if wire == 1 else 4
            if pos > len(payload):
                raise ValueError('Truncated protobuf fixed field')
        else:
            raise ValueError('Unsupported protobuf wire type')
    return fields


def mexc_binary_parser(raw):
    wrapper = protobuf_fields(raw)
    symbol = wrapper.get(3, b'').decode('ascii')
    if symbol not in {s.replace('/', '') for s in ACTIVE_SYMBOLS} or 315 not in wrapper:
        return []
    book = protobuf_fields(wrapper[315])
    bid, ask = book.get(1, b''), book.get(3, b'')
    if not bid or not ask:
        return []
    return [(next(s for s in ACTIVE_SYMBOLS if s.replace('/', '') == symbol), bid.decode('ascii'), ask.decode('ascii'),
             book.get(2, b'').decode('ascii'), book.get(4, b'').decode('ascii'))]


def mexc_parser(data):
    return []


def bitget_parser(data):
    arg = data.get('arg') or {}
    if arg.get('channel') != 'ticker':
        return []
    sym = arg.get('instId', '')
    if sym not in {s.replace('/', '') for s in ACTIVE_SYMBOLS}:
        return []
    return [(next(s for s in ACTIVE_SYMBOLS if s.replace('/', '') == sym), r.get('bidPr'), r.get('askPr'), r.get('bidSz'), r.get('askSz')) for r in data.get('data', [])]


def gate_parser(data):
    if data.get('channel') != 'spot.book_ticker' or data.get('event') != 'update':
        return []
    r = data.get('result') or {}
    sym = r.get('s', '')
    if sym.replace('_', '/') not in ACTIVE_SYMBOLS:
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
            if sym.replace('-', '/') in SYMBOLS:
                out.append((sym.replace('-', '/'), r.get('best_bid'), r.get('best_ask'), r.get('best_bid_quantity'), r.get('best_ask_quantity')))
    return out


async def market_overlap_loop():
    global market_overlap_report
    while True:
        try:
            report = await asyncio.to_thread(build_report)
            with LOCK:
                market_overlap_report = report
            app.logger.info('Spot market overlap: %s', {k: report.get(k) for k in ('complete', 'rank_counts', 'usdt_rank_counts', 'exchange_counts', 'errors')})
        except Exception as exc:
            app.logger.warning('Spot market overlap refresh failed: %s', type(exc).__name__)
        await asyncio.sleep(21600)


async def notify_market_changes():
    """Report sustained changes in feed availability; not configuration changes."""
    global notification_baseline_ready, notification_last_sent
    now = time.monotonic()
    with LOCK:
        active_pairs = {sym for (ex, sym), q in quotes.items()
                        if ex not in feed_quarantine and now - q['updated'] <= MAX_QUOTE_AGE}
        active_exchanges = {ex for ex in EXCHANGES
                            if connections.get(ex) == 'connected' and ex not in feed_quarantine}
        if not notification_baseline_ready:
            notified_active_pairs.update(active_pairs)
            notified_active_exchanges.update(active_exchanges)
            notification_baseline_ready = True
            return
        for sym in ACTIVE_SYMBOLS:
            is_active = sym in active_pairs
            notification_pair_checks[sym] = notification_pair_checks[sym] + 1 if not is_active else 0
            notification_pair_recovery_checks[sym] = notification_pair_recovery_checks[sym] + 1 if is_active else 0
        for ex in EXCHANGES:
            is_active = ex in active_exchanges
            notification_exchange_checks[ex] = notification_exchange_checks[ex] + 1 if not is_active else 0
            notification_exchange_recovery_checks[ex] = notification_exchange_recovery_checks[ex] + 1 if is_active else 0
        added_pairs = sorted(s for s in SYMBOLS if s not in notified_active_pairs
                             and notification_pair_recovery_checks[s] >= 5)
        removed_pairs = sorted(s for s in notified_active_pairs
                               if notification_pair_checks[s] >= 5)
        added_exchanges = sorted(ex for ex in EXCHANGES if ex not in notified_active_exchanges
                                 and notification_exchange_recovery_checks[ex] >= 5)
        removed_exchanges = sorted(ex for ex in notified_active_exchanges
                                   if notification_exchange_checks[ex] >= 5)
        if not any((added_pairs, removed_pairs, added_exchanges, removed_exchanges)):
            return
        if now - notification_last_sent < 180:
            return
    parts = ['📡 Изменение доступности потоков котировок']
    if added_pairs:
        parts.append('✅ Котировки восстановлены: ' + ', '.join(added_pairs))
    if removed_pairs:
        parts.append('⚠️ Нет свежих котировок: ' + ', '.join(removed_pairs))
    if added_exchanges:
        parts.append('✅ Соединения восстановлены: ' + ', '.join(x.upper() for x in added_exchanges))
    if removed_exchanges:
        parts.append('⛔ Соединения недоступны или исключены: ' + ', '.join(x.upper() for x in removed_exchanges))
    parts.append('Проверка: 5 последовательных циклов по 30 секунд. Это не изменение конфигурации торговых пар.')
    try:
        await asyncio.to_thread(telegram_send, '\n'.join(parts))
    except Exception as exc:
        app.logger.warning('Market change Telegram notification failed: %s', type(exc).__name__)
        return
    with LOCK:
        notified_active_pairs.difference_update(removed_pairs)
        notified_active_pairs.update(added_pairs)
        notified_active_exchanges.difference_update(removed_exchanges)
        notified_active_exchanges.update(added_exchanges)
        notification_last_sent = now


async def diagnostics_loop():
    while True:
        await asyncio.sleep(30)
        now = time.monotonic()
        with LOCK:
            fresh = {ex: sum(1 for (exchange, _), v in quotes.items() if exchange == ex and now - v['updated'] <= MAX_QUOTE_AGE) for ex in EXCHANGES}
            statuses = dict(connections)
        with LOCK:
            for ex in EXCHANGES:
                ok = statuses.get(ex) == 'connected' and fresh[ex] >= 2
                feed_good_checks[ex] = feed_good_checks[ex] + 1 if ok else 0
                feed_bad_checks[ex] = 0 if ok else feed_bad_checks[ex] + 1
                if feed_bad_checks[ex] >= 5 and ex not in feed_quarantine:
                    feed_quarantine.add(ex)
                    app.logger.warning('Feed excluded: %s', ex)
                if feed_good_checks[ex] >= 5 and ex in feed_quarantine:
                    feed_quarantine.remove(ex)
                    app.logger.info('Feed recovered: %s', ex)
        await notify_market_changes()
        app.logger.info('Market feed status: %s %s excluded=%s', fresh, statuses, sorted(feed_quarantine))
        app.logger.info('Latency metrics (local only): %s', latency_snapshot())


async def run():
    global quote_event, ACTIVE_SYMBOLS, SUPPORTED_BY_EXCHANGE, market_overlap_report, PAIR_UNIVERSE
    quote_event = asyncio.Event()
    # Catalog-first selection: no unsupported subscriptions and no full-market WS fanout.
    try:
        report = await asyncio.to_thread(build_report)
        with LOCK:
            market_overlap_report = report
        if report.get('complete'):
            matching = {}
            for entries in report['ranked_pairs'].values():
                for entry in entries:
                    matching[entry['pair']] = set(entry['exchanges'])
            PAIR_UNIVERSE = tuple(sorted(matching)) if ALL_PAIRS_TEST else SYMBOLS
            ACTIVE_SYMBOLS = tuple(s for s in PAIR_UNIVERSE if len(matching.get(s, ())) >= 2)
            SUPPORTED_BY_EXCHANGE = {ex: tuple(s for s in ACTIVE_SYMBOLS if ex in matching[s])
                                     for ex in EXCHANGES}
        else:
            app.logger.warning('Catalog incomplete; falling back to original eight USDT pairs')
            ACTIVE_SYMBOLS = tuple(s for s in SYMBOLS if s.endswith('/USDT'))
            SUPPORTED_BY_EXCHANGE = {ex: ACTIVE_SYMBOLS for ex in EXCHANGES}
    except Exception as exc:
        app.logger.warning('Catalog selection failed: %s', type(exc).__name__)
        ACTIVE_SYMBOLS = tuple(s for s in SYMBOLS if s.endswith('/USDT'))
        SUPPORTED_BY_EXCHANGE = {ex: ACTIVE_SYMBOLS for ex in EXCHANGES}
    app.logger.info('Active quote currencies: %s; selected pairs: %s; subscriptions: %s',
                    QUOTE_CURRENCIES, len(ACTIVE_SYMBOLS),
                    {ex: len(v) for ex, v in SUPPORTED_BY_EXCHANGE.items()})
    for sym in ACTIVE_SYMBOLS:
        notification_pair_checks.setdefault(sym, 0)
        notification_pair_recovery_checks.setdefault(sym, 0)
    # Cycle through the complete overlap universe in bounded WebSocket windows.
    # Every pair supported on >=2 exchanges is visited; never subscribe to all at once.
    async def window_jobs(current_pairs):
        jobs = []
        def add(ex, url, subscription, parser):
            if current_pairs[ex]:
                jobs.append(consume(ex, url, [subscription], parser))
        add('okx', 'wss://ws.okx.com:8443/ws/v5/public',
            {'op': 'subscribe', 'args': [{'channel': 'tickers', 'instId': s.replace('/', '-')} for s in current_pairs['okx']]}, okx_parser)
        add('mexc', 'wss://wbs-api.mexc.com/ws',
            {'method': 'SUBSCRIPTION', 'params': [f"spot@public.aggre.bookTicker.v3.api.pb@10ms@{s.replace('/', '')}" for s in current_pairs['mexc']]}, mexc_parser)
        add('bitget', 'wss://ws.bitget.com/v2/ws/public',
            {'op': 'subscribe', 'args': [{'instType': 'SPOT', 'channel': 'ticker', 'instId': s.replace('/', '')} for s in current_pairs['bitget']]}, bitget_parser)
        add('gate', 'wss://api.gateio.ws/ws/v4/',
            {'time': int(time.time()), 'channel': 'spot.book_ticker', 'event': 'subscribe', 'payload': [s.replace('/', '_') for s in current_pairs['gate']]}, gate_parser)
        add('kraken', 'wss://ws.kraken.com/v2',
            {'method': 'subscribe', 'params': {'channel': 'ticker', 'symbol': list(current_pairs['kraken']), 'event_trigger': 'bbo'}}, kraken_parser)
        add('coinbase', 'wss://advanced-trade-ws.coinbase.com',
            {'type': 'subscribe', 'product_ids': [s.replace('/', '-') for s in current_pairs['coinbase']], 'channel': 'ticker'}, coinbase_parser)
        return jobs
    

    persistent = [asyncio.create_task(alert_loop()), asyncio.create_task(diagnostics_loop()),
                  asyncio.create_task(exchange_clock_loop()), asyncio.create_task(market_overlap_loop())]
    offset = 0
    try:
        while True:
            window = set(ACTIVE_SYMBOLS[offset:offset + MAX_WS_PAIRS_PER_EXCHANGE])
            current_pairs = {ex: tuple(s for s in SUPPORTED_BY_EXCHANGE[ex] if s in window) for ex in EXCHANGES}
            with LOCK:
                for key in list(quotes):
                    if key[1] not in current_pairs.get(key[0], ()):
                        del quotes[key]
            jobs = [asyncio.create_task(job) for job in await window_jobs(current_pairs)]
            app.logger.info('All-pairs test window offset=%s, subscriptions=%s, universe=%s',
                            offset, {ex: len(v) for ex, v in current_pairs.items()}, len(ACTIVE_SYMBOLS))
            try:
                await asyncio.sleep(PAIR_ROTATION_SECONDS)
            finally:
                for job in jobs:
                    job.cancel()
                await asyncio.gather(*jobs, return_exceptions=True)
            offset = (offset + MAX_WS_PAIRS_PER_EXCHANGE) % max(1, len(ACTIVE_SYMBOLS))
    finally:
        for job in persistent:
            job.cancel()
        await asyncio.gather(*persistent, return_exceptions=True)

def background():
    if os.getenv('TELEGRAM_STARTUP_TEST') == '1':
        try:
            result = telegram_send('WebSocket V2: test notification; no trades executed.')
            app.logger.info('Telegram test HTTP status: %s', result)
        except Exception as exc:
            app.logger.error('Telegram test failed: %s', type(exc).__name__)
    asyncio.run(run())


@app.get('/')
def home():
    with LOCK:
        return jsonify({'service': 'Crypto Arbitrage WS Prototype', 'status': 'online', 'connections': dict(connections), 'telegram_configured': bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID), 'active_quote_count': sum(time.monotonic()-v['updated'] <= MAX_QUOTE_AGE for v in quotes.values())})


@app.get('/dashboard')
def dashboard():
    return send_file(os.path.join(os.path.dirname(__file__), 'dashboard.html'))


@app.get('/latency')
def latency():
    if not authorized():
        return jsonify({'error': 'Unauthorized'}), 401
    return jsonify(latency_snapshot())


@app.get('/markets-overlap')
def markets_overlap():
    if not authorized():
        return jsonify({'error': 'Unauthorized'}), 401
    with LOCK:
        return jsonify(market_overlap_report)


@app.get('/results')
def results():
    if not authorized():
        return jsonify({'error': 'Unauthorized'}), 401
    now = time.monotonic()
    with LOCK:
        fresh = {f'{ex}:{sym}': {'bid': v['bid'], 'ask': v['ask'], 'age_seconds': round(now - v['updated'], 2)} for (ex, sym), v in quotes.items() if now - v['updated'] <= MAX_QUOTE_AGE}
        status = dict(connections)
        errs = dict(errors)
    return jsonify({'started_at': started_at, 'monitored_symbols': list(SYMBOLS), 'connections': status, 'errors': errs, 'excluded_feeds': sorted(feed_quarantine), 'fresh_quotes': fresh, 'opportunities': opportunities()[:30]})


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
