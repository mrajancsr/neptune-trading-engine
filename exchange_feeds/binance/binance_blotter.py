import asyncio
import logging
import os
from dataclasses import dataclass, field
from json import dumps, loads
from typing import Dict, List, Optional

import uvloop
from exchange_feeds.constants import (
    BINANCE_BASE_WS,
    BINANCE_BLOTTER_MAPPING,
    BINANCE_SUBSCRIPTION_PAYLOAD,
    BINANCE_TRADE_BLOTTER,
    EXCHANGEPATH,
    Exchange,
)
from exchange_feeds.websocket import EchoWebSocket
from websockets.client import WebSocketClientProtocol

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

path_to_log_file = os.path.join(EXCHANGEPATH, "exchangelogs.log")

logging.basicConfig(filename=path_to_log_file, level=logging.INFO)
logger: logging.Logger = logging.getLogger()


# -- Note: At the moment, the file stops running once an error is caught


@dataclass(eq=False)
class BinanceBlotter(EchoWebSocket):
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
        super().__init__(self.url, self.stream_name)

    async def receive(self) -> Optional[List[Dict]]:
        message = loads(await self.websocket.recv())
        if "result" in message:
            return None
        return [
            {
                new_key: message[old_key]
                for (new_key, old_key) in BINANCE_BLOTTER_MAPPING.items()
            }
        ]

    async def send(self) -> None:
        BINANCE_SUBSCRIPTION_PAYLOAD["params"] = [
            f"{self.symbol}{BINANCE_TRADE_BLOTTER}"
        ]
        await self.websocket.send(dumps(BINANCE_SUBSCRIPTION_PAYLOAD))
        print("Socket open!")

    async def stream(self, save: bool = False) -> None:
        await super().stream(handle_lob=False, save=save)
