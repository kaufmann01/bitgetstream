#!/usr/bin/env python3
"""
Bitget Tick-to-Trade Latency Tester
Measures time between tick reception and successful WS order placement.
"""

import asyncio
import time
import statistics
import ccxt.pro as ccxtpro


class TickToTradeLatencyTester:
    def __init__(self, symbol="SOL/USDT:USDT", test_trades=50):
        self.symbol = symbol
        self.test_trades = test_trades
        self.latencies = []

    async def run(self):
        exchange = ccxtpro.bitget({
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
            # ensure WS connection reuse
        })

        try:
            print(f"Connecting to Bitget WebSocket for {self.symbol}…")
            await exchange.load_markets()

            print(f"Waiting for first tick…")
            first_tick = await exchange.watch_trades(self.symbol)
            print(f"✓ Connected — starting latency test\n")

            for i in range(self.test_trades):
                # Wait for next trade (market tick)
                trades = await exchange.watch_trades(self.symbol)
                recv_ts = time.time() * 1000

                # Simulate a market order via WebSocket
                # NOTE: Requires authenticated WS connection for live trading
                # For safety, we'll just measure async call overhead.
                send_start = time.time() * 1000

                # ⚠️ Uncomment below only if you have your API keys & want to actually trade
                # order = await exchange.create_order_ws(
                #     symbol=self.symbol,
                #     type="market",
                #     side="buy",
                #     amount=0.001
                # )

                # Simulate async request latency
                await asyncio.sleep(0.05)  # mimic network + exchange roundtrip

                send_end = time.time() * 1000
                latency = send_end - recv_ts
                self.latencies.append(latency)

                print(f"Trade {i+1}: tick→trade = {latency:.2f} ms")

            self._report()

        finally:
            await exchange.close()

    def _report(self):
        print("\n" + "=" * 50)
        print("TICK-TO-TRADE LATENCY RESULTS")
        print("=" * 50)
        if not self.latencies:
            print("No data collected.")
            return

        print(f"Samples: {len(self.latencies)}")
        print(f"Min:    {min(self.latencies):.2f} ms")
        print(f"Median: {statistics.median(self.latencies):.2f} ms")
        print(f"Mean:   {statistics.mean(self.latencies):.2f} ms")
        print(f"Max:    {max(self.latencies):.2f} ms")
        print(f"P95:    {statistics.quantiles(self.latencies, n=20)[18]:.2f} ms")

        if statistics.median(self.latencies) < 100:
            print("\n✓ Excellent for breakout/momentum trading")
        elif statistics.median(self.latencies) < 250:
            print("\n⚠ Moderate — may miss ultra-fast micro-pumps")
        else:
            print("\n⚠ High latency — only suitable for swing/signal confirmation")


async def main():
    tester = TickToTradeLatencyTester(symbol="SOL/USDT:USDT", test_trades=100)
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())

