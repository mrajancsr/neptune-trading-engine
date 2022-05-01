import asyncio
import logging
import math
import os
from dataclasses import dataclass, field
from json import dumps, loads
from typing import Any, Dict, List, Optional

import uvloop
from exchange_feeds.constants import (
    EXCHANGEPATH,
    KRAKEN_BASE_WS,
    KRAKEN_SUBSCRIPTION_PAYLOAD,
    Exchange,
    KrakenBookMessageIndex,
)
from exchange_feeds.websocket import EchoWebSocket
from websockets.client import WebSocketClientProtocol

# -- Notes
# -- Kraken Exchange requires a Subscription payload,
# -- see ref: https://docs.kraken.com/websockets/#message-subscribe

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

path_to_log_file = os.path.join(EXCHANGEPATH, "exchangelogs.log")

logging.basicConfig(filename=path_to_log_file, level=logging.INFO)
logger: logging.Logger = logging.getLogger()


@dataclass(eq=False)
class KrakenOrderBook(EchoWebSocket):
    symbol: str
    stream_name: str
    url: str = field(init=False)
    websocket: Optional[WebSocketClientProtocol] = None
    logger: logging.Logger = field(init=False)
    exchange: str = field(init=False)

    def __post_init__(self) -> None:
        self.url = KRAKEN_BASE_WS
        self.exchange = Exchange.KRAKEN.value
        self.logger = logger
        super().__init__(self.url, self.stream_name)

    async def handle_trade_message(self, message: List[Any]) -> Dict:
        asks = message.get("as", [])
        bids = message.get("bs", [])
        asks = message.get("a", asks)
        bids = message.get("b", bids)

        sorted_asks = sorted(
            asks, key=lambda a: a[KrakenBookMessageIndex.TIME.value], reverse=True
        )
        sorted_bids = sorted(
            bids, key=lambda b: b[KrakenBookMessageIndex.TIME.value], reverse=True
        )

        latest_ask_timestamp = (
            float(sorted_asks[0][KrakenBookMessageIndex.TIME.value])
            if sorted_asks
            else -math.inf
        )
        latest_bid_timestamp = (
            float(sorted_bids[0][KrakenBookMessageIndex.TIME.value])
            if sorted_bids
            else -math.inf
        )
        offset = max(latest_ask_timestamp, latest_bid_timestamp)

        return {
            "bids": bids,
            "asks": asks,
            "offset": offset,
            "symbol": self.symbol.replace("-", "").replace("/", ""),
        }

    async def receive(self) -> Optional[List[Dict]]:
        message = loads(await self.websocket.recv())
        if type(message) != list:
            return

        payload = message[1]
        return [await self.handle_trade_message(payload)]

    async def send(self) -> None:
        # TODO sys.argv(1) or the like here to sub in for the 'market' argument
        KRAKEN_SUBSCRIPTION_PAYLOAD["pair"] = [self.symbol]
        KRAKEN_SUBSCRIPTION_PAYLOAD["subscription"]["name"] = "book"
        await self.websocket.send(dumps(KRAKEN_SUBSCRIPTION_PAYLOAD))
        print(f"Subscribed to {self.exchange} book channel successfully")

    async def stream(self, save_stream: bool = False) -> None:
        await super().stream(handle_lob=True, save_stream=save_stream)
