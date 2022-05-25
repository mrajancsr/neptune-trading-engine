import asyncio
from dataclasses import dataclass, field
from json import dumps
from typing import Dict, List, Optional

import uvloop
from exchange_feeds.constants import (
    BINANCE_BASE_WS,
    BINANCE_ORDERBOOK_MAPPING,
    BINANCE_QUOTE_L1,
    BINANCE_SUBSCRIPTION_PAYLOAD,
    Exchange,
)
from exchange_feeds.socketmanager import EchoWebSocket

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


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

    async def receive(self) -> Optional[Dict]:
        message = await self.recv()
        if "result" in message:
            yield {}
        else:
            yield {
                new_key: message[old_key]
                for (new_key, old_key) in BINANCE_ORDERBOOK_MAPPING.items()
            }

    async def send(self) -> None:
        BINANCE_SUBSCRIPTION_PAYLOAD["params"] = [f"{self.symbol}{BINANCE_QUOTE_L1}"]
        await self.websocket.send(dumps(BINANCE_SUBSCRIPTION_PAYLOAD))
        print(f"Subscribed to {self.exchange} exchange for {BINANCE_QUOTE_L1}!")


async def main():
    async with BinanceOrderBook("btcusdt", "binance-orderbook") as book:
        await book.stream(save_stream=False, max_record_count=50)


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
