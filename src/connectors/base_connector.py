import abc
from typing import Callable, Awaitable, Dict, Any

class BaseConnector(abc.ABC):
    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id

    @abc.abstractmethod
    async def connect_ws(self) -> None:
        """Open a WS connection if supported."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_orderbook_rest(self, symbol: str) -> Dict[str, Any]:
        """Return {'bids': [(p, v)...], 'asks': [(p, v)...], 'timestamp': ...}"""
        raise NotImplementedError

    @abc.abstractmethod
    async def subscribe_orderbook(self, symbol: str, on_orderbook) -> None:
        """Subscribe to live OB updates and call on_orderbook(dict)."""
        raise NotImplementedError
