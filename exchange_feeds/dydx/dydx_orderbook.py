import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from json import dumps, loads
from pathlib import Path
from typing import Any, Dict, List, Optional

import uvloop
from exchange_feeds.constants import (
    DYDX_BASE_WS,
    DYDX_SUBSCRIPTION_PAYLOAD,
    EXCHANGEPATH,
    Exchange,
)
from exchange_feeds.websocket import EchoWebSocket
from redistoolz import LOCALHOST, push_raw_feeds_to_redis
from websockets import connect
from websockets.client import WebSocketClientProtocol

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

path_to_log_file = os.path.join(EXCHANGEPATH, "exchangelogs.log")

logging.basicConfig(filename=path_to_log_file, level=logging.INFO)
logger: logging.Logger = logging.getLogger()

# -- Notes
# -- Dydx Exchange requires a Subscription payload,
# -- see ref: https://docs.dydx.exchange/#orderbook


@dataclass(eq=False)
class DYDXOrderBook(EchoWebSocket):
    url: str
    symbol: str
    websocket: Optional[WebSocketClientProtocol] = None
    exchange: str = field(init=False)

    def __post_init__(self) -> None:
        self.exchange = Exchange.DYDX.value
        super().__init__(self.url)

    async def handle_trade_message(self, message: Dict[str, Any]) -> Dict:
        return message["contents"]

    async def receive(self) -> Optional[List[Dict]]:
        message = loads(await self.websocket.recv())
        message_type = message["type"]
        exchange = self.exchange
        if message_type == "connected":
            print(f"Success: Subscribed to {exchange}")
        elif message_type == "subscribed":
            print(f"Success: Subscribed to {exchange} channel {message['id']}")
        elif message_type == "info":
            print(f"info message received: {message}")
            if message["code"] == 20001:
                print("connection closed - we should reconnect")
        elif message_type == "error":
            raise Exception(message)
        elif message_type == "channel_data":
            message = await self.handle_trade_message(message)
            return [message]

    async def send(self) -> None:
        # TODO sys.argv(1) or the like here to sub in for the 'market' argument
        # DYDX_SUBSCRIPTION_PAYLOAD["includeOffsets"] = False
        DYDX_SUBSCRIPTION_PAYLOAD["channel"] = "v3_orderbook"
        DYDX_SUBSCRIPTION_PAYLOAD["id"] = self.symbol
        DYDX_SUBSCRIPTION_PAYLOAD["includeOffsets"] = "true"
        await self.websocket.send(dumps(DYDX_SUBSCRIPTION_PAYLOAD))
        print("Socket open!")

    async def stream(self) -> None:
        await super().stream(handle_lob=True)
