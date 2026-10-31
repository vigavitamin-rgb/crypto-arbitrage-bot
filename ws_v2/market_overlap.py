"""Read-only spot-market overlap report for six exchanges. No order placement."""
import json
import time
from collections import defaultdict
from urllib.request import Request, urlopen

URLS = {
    "okx": "https://www.okx.com/api/v5/public/instruments?instType=SPOT",
    "mexc": "https://api.mexc.com/api/v3/exchangeInfo",
    "bitget": "https://api.bitget.com/api/v3/market/instruments?category=SPOT",
    "gate": "https://api.gateio.ws/api/v4/spot/currency_pairs",
    "kraken": "https://api.kraken.com/0/public/AssetPairs",
    "coinbase": "https://api.exchange.coinbase.com/products",
}
EXCHANGES = tuple(URLS)


def normalize(base, quote):
    if not base or not quote:
        return None
    base, quote = str(base).upper(), str(quote).upper()
    aliases = {"XBT": "BTC", "XXBT": "BTC", "XDG": "DOGE"}
    return aliases.get(base, base) + "/" + aliases.get(quote, quote)


def fetch(exchange, timeout=12):
    request = Request(URLS[exchange], headers={"User-Agent": "market-overlap-audit/1.0", "Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def parse(exchange, data):
    pairs = set()
    if exchange == "okx":
        rows = data["data"]
        for x in rows:
            if x.get("state") == "live":
                pairs.add(normalize(x.get("baseCcy"), x.get("quoteCcy")))
    elif exchange == "mexc":
        for x in data["symbols"]:
            if str(x.get("status", "")).upper() in ("1", "ENABLED", "TRADING") and x.get("isSpotTradingAllowed", True):
                pairs.add(normalize(x.get("baseAsset"), x.get("quoteAsset")))
    elif exchange == "bitget":
        for x in data["data"]:
            if x.get("status") == "online":
                pairs.add(normalize(x.get("baseCoin"), x.get("quoteCoin")))
    elif exchange == "gate":
        for x in data:
            if x.get("trade_status") == "tradable":
                pairs.add(normalize(x.get("base"), x.get("quote")))
    elif exchange == "kraken":
        if data.get("error"):
            raise ValueError(str(data["error"]))
        for x in data["result"].values():
            if x.get("status", "online") == "online" and "/" in x.get("wsname", ""):
                base, quote = x["wsname"].split("/", 1)
                pairs.add(normalize(base, quote))
    elif exchange == "coinbase":
        for x in data:
            if x.get("status") == "online" and not x.get("trading_disabled", False):
                pairs.add(normalize(x.get("base_currency"), x.get("quote_currency")))
    return {p for p in pairs if p and "None" not in p}


def build_report():
    by_exchange, errors = {}, {}
    for ex in EXCHANGES:
        try:
            by_exchange[ex] = parse(ex, fetch(ex))
        except Exception as exc:
            errors[ex] = type(exc).__name__ + ": " + str(exc)[:120]
    if errors:
        return {"complete": False, "errors": errors, "successful_exchanges": sorted(by_exchange),
                "counts": {ex: len(p) for ex, p in by_exchange.items()},
                "warning": "No full six-exchange ranking while any source failed"}
    occurrences = defaultdict(list)
    for ex, pairs in by_exchange.items():
        for pair in pairs:
            occurrences[pair].append(ex)
    buckets = {str(n): [] for n in range(6, 1, -1)}
    usdt = {str(n): [] for n in range(6, 1, -1)}
    for pair, exchanges in occurrences.items():
        n = len(exchanges)
        if n >= 2:
            entry = {"pair": pair, "exchanges": sorted(exchanges)}
            buckets[str(n)].append(entry)
            if pair.endswith("/USDT"):
                usdt[str(n)].append(entry)
    for group in (buckets, usdt):
        for values in group.values():
            values.sort(key=lambda x: x["pair"])
    return {"complete": True, "generated_at_unix": time.time(),
            "exchange_counts": {ex: len(p) for ex, p in by_exchange.items()},
            "rank_counts": {n: len(v) for n, v in buckets.items()},
            "usdt_rank_counts": {n: len(v) for n, v in usdt.items()},
            "ranked_pairs": buckets, "usdt_pairs": usdt,
            "coinbase_note": "Coinbase Exchange products API used; Advanced Trade availability may differ",
            "note": "Symbol matching does not establish liquidity, order permissions or equal token identity"}


if __name__ == "__main__":
    print(json.dumps(build_report(), ensure_ascii=False))
