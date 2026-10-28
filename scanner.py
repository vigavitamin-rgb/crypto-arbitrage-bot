import asyncio, csv, time
from datetime import datetime, timezone
import ccxt.async_support as ccxt

EXCHANGES = ["binance", "bybit", "okx", "mexc", "bitget", "gate", "kraken", "coinbase"]
SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
TIMEOUT = 12000

async def check(name):
    exchange = getattr(ccxt, name)({"enableRateLimit": True, "timeout": TIMEOUT})
    started = time.perf_counter()
    try:
        markets = await exchange.load_markets()
        supported = [s for s in SYMBOLS if s in markets and markets[s].get("spot")]
        if not supported:
            return name, round((time.perf_counter()-started)*1000), "no common spot pairs", {}
        tickers = {}
        for symbol in supported:
            ticker = await exchange.fetch_ticker(symbol)
            bid, ask = ticker.get("bid"), ticker.get("ask")
            if bid and ask and bid > 0 and ask > 0 and bid <= ask:
                tickers[symbol] = (bid, ask)
        return name, round((time.perf_counter()-started)*1000), "ok", tickers
    except Exception as exc:
        return name, round((time.perf_counter()-started)*1000), str(exc)[:180], {}
    finally:
        await exchange.close()

async def main():
    results = await asyncio.gather(*(check(x) for x in EXCHANGES))
    stamp = datetime.now(timezone.utc).isoformat()
    with open("connectivity.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["utc", "exchange", "elapsed_ms", "status", "spot_symbols"])
        for name, elapsed, status, tickers in results:
            writer.writerow([stamp, name, elapsed, status, ",".join(tickers)])
            print(f"{name:10} {elapsed:6} ms {status} {list(tickers)}")
    opportunities = []
    for symbol in SYMBOLS:
        for buy_name, _, _, buy_data in results:
            for sell_name, _, _, sell_data in results:
                if buy_name == sell_name or symbol not in buy_data or symbol not in sell_data:
                    continue
                ask = buy_data[symbol][1]
                bid = sell_data[symbol][0]
                gross = (bid / ask - 1) * 100
                opportunities.append((gross, symbol, buy_name, sell_name))
    print("\nIndicative gross spreads (not executable quotes; no fees/slippage):")
    for gross, symbol, buy, sell in sorted(opportunities, reverse=True)[:12]:
        print(f"{symbol}: buy {buy}, sell {sell}: {gross:+.3f}%")
    print("\nResults saved to connectivity.csv")

if __name__ == "__main__":
    asyncio.run(main())
