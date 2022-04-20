"""Maintains an orderbook"""
import heapq
from dataclasses import dataclass, field
import json
import math
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

    most_recent_offset: float = field(init=False, default=None)

    index: int = field(init=False, default=0)

    def __post_init__(self):
        self.bids = {}
        self.asks = {}
        self.bid_heap = []
        self.ask_heap = []

    def bid_size(self) -> int:
        return len(self.bids)

    def ask_size(self) -> int:
        return len(self.asks)

    def best_bid(self) -> OrderEntry:
        if self.bid_size() == 0:
            raise IndexError("Last offset doesn't have bid data")

        best_bid = self.bid_heap[0][-1]
        while best_bid and (
            self.bids.get(best_bid.price) is None
            or not is_equal_entry(best_bid, self.bids.get(best_bid.price))
        ):
            heapq.heappop(self.bid_heap)
            best_bid = self.bid_heap[0][-1]

        return best_bid

    def best_ask(self) -> OrderEntry:
        if self.ask_size() == 0:
            raise IndexError("Last offset doesn't have ask data")

        best_ask = self.ask_heap[0][-1]
        while best_ask and (
            self.asks.get(best_ask.price) is None
            or not is_equal_entry(best_ask, self.asks.get(best_ask.price))
        ):
            heapq.heappop(self.ask_heap)
            best_ask = self.ask_heap[0][-1]

        return best_ask

    def update_bids(self, message: Dict[str, str]) -> None:
        if len(message["bids"]) == 0:
            return

        offset = float(message["offset"])
        self.most_recent_offset = max(
            offset, self.most_recent_offset if self.most_recent_offset else -math.inf
        )
        for level in message["bids"]:
            bid_price = float(level[0])
            bid_size = float(level[1])
            if bid_size == 0:
                del self.bids[bid_price]
                continue

            entry = OrderEntry(offset, bid_price, bid_size)
            heapq.heappush(self.bid_heap, (bid_price, offset, bid_size, entry))
            self.bids[bid_price] = entry

    def update_asks(self, message: Dict[str, str]) -> None:
        if len(message["asks"]) == 0:
            return

        offset = float(message["offset"])
        self.most_recent_offset = max(
            offset, self.most_recent_offset if self.most_recent_offset else -math.inf
        )
        for level in message["asks"]:
            ask_price = float(level[0])
            ask_size = float(level[1])
            if ask_size == 0:
                del self.asks[ask_price]
                continue

            entry = OrderEntry(offset, ask_price, ask_size)
            heapq.heappush(self.ask_heap, (-ask_price, offset, ask_size, entry))
            self.asks[ask_price] = entry

    def to_frame(self) -> dict:
        serialized_asks = [v.serialize() for _, v in self.asks.items()]
        serialized_bids = [v.serialize() for _, v in self.bids.items()]
        return {
            "asks": json.dumps(serialized_asks),
            "bids": json.dumps(serialized_bids),
            "trxn_time": self.most_recent_offset,
        }


def is_equal_entry(a: OrderEntry, b: OrderEntry):
    return a.offset == b.offset and a.price == b.price and a.size == b.size
