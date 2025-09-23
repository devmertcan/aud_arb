import asyncio
from typing import Dict, List, Optional

from connectors.ccxt_rest import CCXTRest
from connectors.ws_connectors import OKXWS, KrakenWS

class ExchangeOrderbookManager:
    """
    Creates one producer task per exchange (WS if available, else REST) that feeds a shared queue with best bid/ask snapshots.
    Consumers (detector) read from the queue and maintain a latest-snapshot map keyed by (exchange, pair).
    """

    WS_CAPABLE = {"okx", "kraken"}

    def __init__(self, exchanges: List[str], pairs: List[str], poll_interval_ms_rest: int, max_concurrency: int = 200):
        self.exchanges = [e.lower() for e in exchanges]
        self.pairs = pairs
        self.poll_rest_ms = poll_interval_ms_rest
        self.max_concurrency = max_concurrency
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=4_096)
        self._tasks: List[asyncio.Task] = []
        self._stopped = False

    async def start(self):
        sem = asyncio.Semaphore(self.max_concurrency)
        for ex in self.exchanges:
            if ex in self.WS_CAPABLE:
                producer = self._start_ws(ex, self.pairs, sem)
            else:
                producer = self._start_rest(ex, self.pairs, sem)
            self._tasks.append(asyncio.create_task(producer))

    async def stop(self):
        self._stopped = True
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _start_ws(self, exchange: str, pairs: List[str], sem: asyncio.Semaphore):
        if exchange == "okx":
            ws = OKXWS()
        elif exchange == "kraken":
            ws = KrakenWS()
        else:
            return
        try:
            async with sem:
                await ws.stream_pairs(pairs, self.queue)
        finally:
            await ws.close()

    async def _start_rest(self, exchange: str, pairs: List[str], sem: asyncio.Semaphore):
        client = CCXTRest(exchange, poll_interval_ms=self.poll_rest_ms)
        try:
            async with sem:
                await client.stream_pairs(pairs, self.queue)
        finally:
            await client.close()
