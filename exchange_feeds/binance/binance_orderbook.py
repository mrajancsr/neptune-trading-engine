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
from exchange_feeds.socketmanager import EchoWebSocket

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

path_to_log_file = os.path.join(EXCHANGEPATH, "exchangelogs.log")

logging.basicConfig(filename=path_to_log_file, level=logging.INFO)
logger: logging.Logger = logging.getLogger()


@dataclass
class BinanceOrderBook(EchoWebSocket):
    symbol: str
    stream_name: str
    url: str = field(init=False)
    exchange: str = field(init=False)

    def __post_init__(self) -> None:
        self.url = BINANCE_BASE_WS
        self.exchange = Exchange.BINANCE.value
        super().__init__(self.url, self.stream_name)

    async def receive(self) -> Optional[List[Dict]]:
        message = await self.recv()
        yield message

    async def send(self) -> None:
        BINANCE_SUBSCRIPTION_PAYLOAD["params"] = [f"{self.symbol}{BINANCE_QUOTE_L1}"]
        await self.websocket.send(dumps(BINANCE_SUBSCRIPTION_PAYLOAD))
        print(f"Subscribed to {self.exchange} exchange for {BINANCE_QUOTE_L1}!")

    async def stream(self, save: bool = False) -> None:
        while True:
            async for record in self.receive():
                print(record)


async def main():
    async with BinanceOrderBook("btcusdt", "binance-orderbook") as book:
        await book.stream()


if __name__ == "__main__":
    asyncio.run(main())
