# --- The purpose of this file is to maintain orderbook feed in real time
# --- in order to interpret the webscocket order book feed
# --- Currently, only supports kraken exchange
import heapq
import json
import math
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class OrderEntry:
    offset: float
    price: float
    size: float


def serialize(self):
    return (self.offset, self.price, self.size)


@dataclass
class LimitOrderBook:
    bids: Dict[str, OrderEntry] = field(init=False, default_factory=dict)
    asks: Dict[str, OrderEntry] = field(init=False, default_factory=dict)
    bid_heap: List[List[float]] = field(init=False, default_factory=list)
    ask_heap: List[List[float]] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.bids = {}
        self.asks = {}
        self.bid_heap = []
        self.ask_heap = []

    def update_bids(self, message: Dict[str, str]) -> None:
        if len(message["bids"]) == 0:
            return

    def update_asks(self, message: Dict[str, str]) -> None:
        if len(message["asks"]) == 0:
            return
