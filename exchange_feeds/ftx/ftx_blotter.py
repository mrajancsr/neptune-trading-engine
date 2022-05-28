import asyncio
import logging
import os
import threading
from dataclasses import dataclass, field
from json import dumps, loads
from typing import Dict, List, Optional

import pandas as pd
import uvloop
from exchange_feeds.constants import (
    EXCHANGEPATH,
    FTX_BASE_WS,
    FTX_BLOTTER_MAPPING,
    FTX_SUBSCRIPTION_PAYLOAD,
    FTXUS_BASE_WS,
    Exchange,
)
from exchange_feeds.socketmanager import EchoWebSocket
from websockets.client import WebSocketClientProtocol

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

path_to_log_file = os.path.join(EXCHANGEPATH, "exchangelogs.log")

logging.basicConfig(filename=path_to_log_file, level=logging.INFO)
logger: logging.Logger = logging.getLogger()


@dataclass(eq=False)
class FTXBlotter(EchoWebSocket):
    symbol: str
    stream_name: str
    url: str = field(init=False)
    websocket: Optional[WebSocketClientProtocol] = None
    logger: logging.Logger = field(init=False)
    exchange: str = field(init=False)

    def __post_init__(self) -> None:
        self.url = FTX_BASE_WS if self.stream_name.find("us") < 0 else FTXUS_BASE_WS
        self.exchange = (
            Exchange.FTX.value
            if self.stream_name.find("us") < 0
            else Exchange.FTXUS.value
        )
        self.logger = logger
        super().__init__(self.url, self.stream_name)

    async def send(self):
        FTX_SUBSCRIPTION_PAYLOAD["market"] = self.symbol
        FTX_SUBSCRIPTION_PAYLOAD["channel"] = "trades"
        await self.websocket.send(dumps(FTX_SUBSCRIPTION_PAYLOAD))
        print("Socket open!")
