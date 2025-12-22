from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class Position:
    symbol: str
    qty: float
    current_price: float
    market_value: float
    unrealized_pl: float

@dataclass
class AccountInfo:
    equity: float
    buying_power: float
    cash: float
    currency: str = "USD"

class BrokerInterface(ABC):
    """
    Strict contract for any broker implementation (Alpaca, IBKR, etc.).
    All methods must be asynchronous to fit the event loop.
    """

    @abstractmethod
    async def get_account(self) -> AccountInfo:
        """Fetch current account balance and equity."""
        pass

    @abstractmethod
    async def get_positions(self) -> List[Position]:
        """Fetch all open positions."""
        pass

    @abstractmethod
    async def get_last_price(self, symbol: str) -> float:
        """Fetch the most recent trade price instantly."""
        pass

    @abstractmethod
    async def submit_order(self, symbol: str, qty: float, side: str, 
                         order_type: str = "market", 
                         limit_price: Optional[float] = None) -> Dict:
        """Submit an order to the broker."""
        pass
    
    @abstractmethod
    async def close_position(self, symbol: str):
        """Close the position for a specific symbol."""
        pass

    @abstractmethod
    async def close_all_positions(self):
        """Emergency: Flatten the book."""
        pass
