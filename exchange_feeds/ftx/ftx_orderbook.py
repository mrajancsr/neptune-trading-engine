import asyncio
import logging
import os
from dataclasses import dataclass, field
from json import dumps, loads
from typing import Any, Dict, List, Optional

import uvloop
from exchange_feeds.constants import (
    EXCHANGEPATH,
    FTX_BASE_WS,
    FTX_SUBSCRIPTION_PAYLOAD,
    FTXUS_BASE_WS,
    Exchange,
)
from exchange_feeds.websocket import EchoWebSocket
from websockets.client import WebSocketClientProtocol

# -- Notes
# -- FTX Exchange requires a Subscription payload,
# -- see ref: https://docs.ftx.us/#websocket-api

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

path_to_log_file = os.path.join(EXCHANGEPATH, "exchangelogs.log")

logging.basicConfig(filename=path_to_log_file, level=logging.INFO)
logger: logging.Logger = logging.getLogger()


@dataclass(eq=False)
class FTXOrderBook(EchoWebSocket):
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

    async def handle_trade_message(self, message: Dict[str, Any]) -> Dict:
        data = message["data"]
        time = data["time"]
        asks = data.get("asks", [])
        bids = data.get("bids", [])

        return {
            "offset": time,
            "asks": asks,
            "bids": bids,
            "symbol": self.symbol.replace("-", ""),
        }

    async def receive(self) -> Optional[List[Dict]]:
        message = loads(await self.websocket.recv())
        message_type = message["type"]
        if message_type == "subscribed":
            print(f"Successfully subscribed to {message['market']}")
            return
        elif message_type == "unsubscribed":
            print(f"Unsubscribed to {message['market']}")
        elif message_type == "info":
            print(f"info message received: {message}")
            if message["code"] == 20001:
                print("connection closed - we should reconnect")
        elif message_type == "error":
            raise Exception(message)
        result = await self.handle_trade_message(message)
        return [result]

    async def send(self) -> None:
        # TODO sys.argv(1) or the like here to sub in for the 'market' argument
        FTX_SUBSCRIPTION_PAYLOAD["market"] = self.symbol
        FTX_SUBSCRIPTION_PAYLOAD["channel"] = "orderbook"
        await self.websocket.send(dumps(FTX_SUBSCRIPTION_PAYLOAD))
        print("Socket open!")

    async def stream(self, save: bool = False) -> None:
        await super().stream(handle_lob=True, save=save, max_record_count=20)
