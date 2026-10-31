import unittest
from unittest.mock import patch
import app as scanner
from app import update_quote, opportunities, quotes, LOCK, okx_parser, mexc_parser, mexc_binary_parser, bitget_parser, gate_parser, kraken_parser, coinbase_parser

class ScannerTests(unittest.TestCase):
    def setUp(self):
        with LOCK:
            quotes.clear()

    def test_spread(self):
        update_quote('okx', 'BTC/USDT', 100, 101)
        update_quote('kraken', 'BTC/USDT', 104, 105)
        found = opportunities()
        self.assertTrue(any(x['buy_exchange'] == 'okx' and x['sell_exchange'] == 'kraken' for x in found))
        self.assertTrue(all(x['estimated_profit_usdt'] > 0 for x in found))

    def test_spread_below_total_cost_is_excluded(self):
        update_quote('okx', 'BTC/USDT', 100, 101)
        update_quote('kraken', 'BTC/USDT', 102, 103)
        with patch.dict(scanner.TAKER_FEES_PCT, {'okx': 0.35, 'kraken': 0.80}), patch.object(scanner, 'SLIPPAGE_PCT', 0.05):
            self.assertEqual(opportunities(), [])

    def test_stale_quote_is_excluded(self):
        update_quote('okx', 'BTC/USDT', 100, 101)
        update_quote('kraken', 'BTC/USDT', 104, 105)
        with LOCK:
            quotes[('okx', 'BTC/USDT')]['updated'] -= scanner.MAX_QUOTE_AGE + 1
        self.assertEqual(opportunities(), [])

    def test_invalid_quote(self):
        update_quote('okx', 'BTC/USDT', 105, 100)
        self.assertEqual(quotes, {})

    def test_okx(self):
        msg = {'arg': {'channel': 'tickers', 'instId': 'BTC-USDT'}, 'data': [{'bidPx': '100', 'askPx': '101'}]}
        self.assertEqual(okx_parser(msg)[0][:3], ('BTC/USDT', '100', '101'))

    def test_mexc(self):
        msg = {'c': 'spot@public.bookTicker.v3.api@BTCUSDT', 's': 'BTCUSDT', 'd': {'b': '100', 'a': '101'}}
        # The legacy JSON ticker is deliberately ignored; production uses protobuf.
        self.assertEqual(mexc_parser(msg), [])

    def test_mexc_binary(self):
        # Protocol wire layout: symbol field 3, book ticker wrapper field 315.
        # Synthetic prices and quantities keep this fixture deterministic.
        book = b'\x0a\x03100\x12\x012\x1a\x03101\x22\x013'
        raw = b'\x1a\x07BTCUSDT\xda\x13' + bytes([len(book)]) + book
        self.assertEqual(mexc_binary_parser(raw), [('BTC/USDT', '100', '101', '2', '3')])

    def test_mexc_truncated_binary(self):
        with self.assertRaises(ValueError):
            mexc_binary_parser(b'\x1a\x07BTC')

    def test_bitget(self):
        msg = {'arg': {'channel': 'ticker', 'instId': 'BTCUSDT'}, 'data': [{'bidPr': '100', 'askPr': '101'}]}
        self.assertEqual(bitget_parser(msg)[0][:3], ('BTC/USDT', '100', '101'))

    def test_gate(self):
        msg = {'channel': 'spot.book_ticker', 'event': 'update', 'result': {'s': 'BTC_USDT', 'b': '100', 'a': '101'}}
        self.assertEqual(gate_parser(msg)[0][:3], ('BTC/USDT', '100', '101'))

    def test_kraken(self):
        msg = {'channel': 'ticker', 'type': 'update', 'data': [{'symbol': 'BTC/USDT', 'bid': 100, 'ask': 101}]}
        self.assertEqual(kraken_parser(msg)[0][:3], ('BTC/USDT', 100, 101))

    def test_coinbase(self):
        msg = {'channel': 'ticker', 'events': [{'tickers': [{'product_id': 'BTC-USDT', 'best_bid': '100', 'best_ask': '101'}]}]}
        self.assertEqual(coinbase_parser(msg)[0][:3], ('BTC/USDT', '100', '101'))

if __name__ == '__main__':
    unittest.main()
