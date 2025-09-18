import asyncio
from connectors.ws_connectors import subscribe_ir_orderbook

async def main():
    async def print_ob(data):
        print("Orderbook update:", data)
    await subscribe_ir_orderbook("BTC/AUD", print_ob)

asyncio.run(main())
