import unittest
from app import update_quote, opportunities, quotes, LOCK, okx_parser, mexc_parser, bitget_parser, gate_parser, kraken_parser, coinbase_parser

class ScannerTests(unittest.TestCase):
    def setUp(self):
        with LOCK:
            quotes.clear()

    def test_spread(self):
        update_quote('okx', 'BTC/USDT', 100, 101)
        update_quote('kraken', 'BTC/USDT', 102, 103)
        found = opportunities()
        self.assertTrue(any(x['buy_exchange'] == 'okx' and x['sell_exchange'] == 'kraken' for x in found))

    def test_invalid_quote(self):
        update_quote('okx', 'BTC/USDT', 105, 100)
        self.assertEqual(quotes, {})

    def test_okx(self):
        msg = {'arg': {'channel': 'tickers', 'instId': 'BTC-USDT'}, 'data': [{'bidPx': '100', 'askPx': '101'}]}
        self.assertEqual(okx_parser(msg)[0][:3], ('BTC/USDT', '100', '101'))

    def test_mexc(self):
        msg = {'c': 'spot@public.bookTicker.v3.api@BTCUSDT', 's': 'BTCUSDT', 'd': {'b': '100', 'a': '101'}}
        self.assertEqual(mexc_parser(msg)[0][:3], ('BTC/USDT', '100', '101'))

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
