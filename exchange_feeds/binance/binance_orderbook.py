import asyncio
import logging
import os
from dataclasses import dataclass, field
from json import dumps, loads
from typing import Dict, List, Optional

import uvloop
from exchange_feeds.constants import (
    BINANCE_BASE_WS,
    BINANCE_ORDERBOOK_MAPPING,
    BINANCE_QUOTE_L1,
    BINANCE_SUBSCRIPTION_PAYLOAD,
    EXCHANGEPATH,
    Exchange,
)
from websockets.client import WebSocketClientProtocol
from exchange_feeds.websocket import EchoWebSocket

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

path_to_log_file = os.path.join(EXCHANGEPATH, "exchangelogs.log")

logging.basicConfig(filename=path_to_log_file, level=logging.INFO)
logger: logging.Logger = logging.getLogger()


@dataclass(eq=False)
class BinanceOrderBook(EchoWebSocket):
    symbol: str
    stream_name: str
    url: str = field(init=False)
    websocket: Optional[WebSocketClientProtocol] = None
    logger: logging.Logger = field(init=False)
    exchange: str = field(init=False)

    def __post_init__(self) -> None:
        self.url = BINANCE_BASE_WS
        self.exchange = Exchange.BINANCE.value
        self.logger = logger

    async def receive(self) -> List[Dict]:
        msg = loads(await self.websocket.recv())
        if "result" in msg:
            return None
        return [
            {
                new_key: msg[old_key]
                for (new_key, old_key) in BINANCE_ORDERBOOK_MAPPING.items()
            }
        ]

    async def send(self) -> None:
        BINANCE_SUBSCRIPTION_PAYLOAD["params"] = [f"{self.symbol}{BINANCE_QUOTE_L1}"]
        await self.websocket.send(dumps(BINANCE_SUBSCRIPTION_PAYLOAD))
        print("Socket open!")

    async def stream(self, save: bool = False) -> None:
        await super().stream(handle_lob=False, save=save)
