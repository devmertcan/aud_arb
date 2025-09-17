import abc

class BaseConnector(abc.ABC):
    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id

    @abc.abstractmethod
    async def connect_ws(self):
        """Connect to exchange WebSocket if available"""
        pass

    @abc.abstractmethod
    async def get_orderbook_rest(self, symbol: str):
        """Fetch orderbook snapshot via REST (fallback)"""
        pass

    @abc.abstractmethod
    async def subscribe_orderbook(self, symbol: str, callback):
        """Subscribe to live orderbook updates and call callback(data)"""
        pass
