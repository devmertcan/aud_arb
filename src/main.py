import asyncio
import os

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook
from reporter.csv_reporter import CSVReporter

PAIR = os.environ.get("PAIR", "BTC/AUD")
CSV_PATH = os.environ.get("CSV_PATH", "/opt/aud_arb/out/ir_boa.csv")

async def demo_rest_snapshot():
    print("== REST snapshot test ==")
    rest = CCXTRestConnector("independentreserve")
    ob = await rest.get_orderbook_rest(PAIR)
    await rest.close()
    print("REST snapshot:", ob["bids"][:1], ob["asks"][:1])

async def demo_ir_ws():
    print("== Independent Reserve WS (orderbook) ==")
    reporter = CSVReporter(CSV_PATH)

    async def on_ob(update):
        bids, asks, ts = update["bids"], update["asks"], update["timestamp"]
        # print a compact view
        if bids and asks:
            print(f"TOB: bid {bids[0]} | ask {asks[0]} | spread {asks[0][0]-bids[0][0]:.2f}")
            reporter.write_top_of_book("independentreserve", PAIR, bids, asks, ts, notes="live")

    await subscribe_ir_orderbook(PAIR, on_ob)

async def main():
    await demo_rest_snapshot()
    # Run WS (Ctrl+C to stop)
    await demo_ir_ws()

if __name__ == "__main__":
    asyncio.run(main())
