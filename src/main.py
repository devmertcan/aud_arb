import asyncio
from connectors.ws_connectors import subscribe_ir_orderbook

async def main():
    async def print_ob(data):
        # For now this will just print RAW messages
        pass

    await subscribe_ir_orderbook("BTC/AUD", print_ob)

if __name__ == "__main__":
    asyncio.run(main())
