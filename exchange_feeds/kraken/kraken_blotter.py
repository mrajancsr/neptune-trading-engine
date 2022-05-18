import asyncio
import logging
import os
import threading
from dataclasses import dataclass, field
from json import dumps, loads
from typing import Any, Dict, List, Optional

import pandas as pd
import uvloop
from exchange_feeds.constants import (
    BAR_CUT_DELAY_IN_SECONDS,
    CLOSE,
    EXCHANGEPATH,
    HIGH,
    KRAKEN_BASE_WS,
    KRAKEN_BLOTTER_MAPPING,
    KRAKEN_SUBSCRIPTION_PAYLOAD,
    LOW,
    OPEN,
    SYMBOL,
    TIME,
    Exchange,
    KrakenTradeMessageIndex,
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
class KrakenBlotter(EchoWebSocket):
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
        self._dataframe = pd.DataFrame()
        self._initialized = False
        super().__init__(self.url, self.stream_name)

    async def handle_trade_message(self, message: List[Any]) -> Dict:
        msg = {}
        msg["symbol"] = self.symbol
        msg.update(
            {
                KRAKEN_BLOTTER_MAPPING[index]: message[index.value]
                for index in KrakenTradeMessageIndex
            }
        )
        return msg

    async def receive(self) -> Optional[List[Dict]]:
        message = loads(await self.websocket.recv())
        if type(message) != list:
            return

        trade_messages = message[1]
        return [await self.handle_trade_message(m) for m in trade_messages]

    async def send(self) -> None:
        KRAKEN_SUBSCRIPTION_PAYLOAD["pair"] = [self.symbol]
        KRAKEN_SUBSCRIPTION_PAYLOAD["subscription"]["name"] = "trade"
        await self.websocket.send(dumps(KRAKEN_SUBSCRIPTION_PAYLOAD))
        print(f"Subscribed to {self.exchange} trade channel successfully")

    async def stream(self, save_stream: bool = False) -> None:
        await super().stream(
            handle_lob=False, save_stream=save_stream, max_record_count=10
        )
