import asyncio
from orderbook.exchange_orderbook_manager import ExchangeOrderBookManager

async def fake_resync():
    print("[REST] Fetching fresh snapshot...")
    return {
        "bids": [(100.0, 1.0), (99.0, 2.0)],
        "asks": [(101.0, 1.5), (102.0, 3.0)],
        "timestamp": 1234567890,
    }

async def run_tests():
    ob = ExchangeOrderBookManager("test-exchange", depth=5, gap_threshold=2)
    ob.resync_callback = fake_resync

    # Apply initial snapshot
    await ob.apply_snapshot([(100.0, 1.0)], [(101.0, 1.5)], ts=111)
    print("Initial midprice:", ob.midprice())

    # Simulate NewOrder
    ob._add_order(order_id="abc", side="bid", price=99.5, volume=0.8)
    print("Best bid after new order:", ob.best_bid())

    # Simulate correct nonce sequence
    await ob.check_seq(1)
    await ob.check_seq(2)
    print("Seq OK, in_sync:", ob.in_sync)

    # Simulate gap (jump from 2 → 5)
    await ob.check_seq(5)
    print("After gap (below threshold), in_sync:", ob.in_sync)

    # Simulate another gap (resync should trigger)
    await ob.check_seq(8)
    print("After 2nd gap (threshold reached), in_sync:", ob.in_sync)

    # Check spread
    print("Spread:", ob.spread())

if __name__ == "__main__":
    asyncio.run(run_tests())
