import asyncio
from dataclasses import dataclass, field
from json import dumps
from typing import Dict, Iterator, List, Optional

import uvloop
from exchange_feeds.constants import (
    KRAKEN_BASE_WS,
    KRAKEN_SUBSCRIPTION_PAYLOAD,
    Exchange,
)
from exchange_feeds.socketmanager import EchoWebSocket

# -- Notes
# -- Kraken Exchange requires a Subscription payload,
# -- see ref: https://docs.kraken.com/websockets/#message-subscribe

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@dataclass
class KrakenOrderBook(EchoWebSocket):
    symbol: str
    stream_name: str
    depth: int
    url: str = field(init=False)
    exchange: str = field(init=False)

    def __post_init__(self) -> None:
        self.url = KRAKEN_BASE_WS
        self.exchange = Exchange.KRAKEN.value
        assert self.depth in {10, 25, 100}
        super().__init__(self.url, self.stream_name)

    def handle_traded_message(self, message: Dict[str, List[List[str]]]):
        if "as" in message and "bs" in message:
            return dict(asks=message["as"], bids=message["bs"])
        elif "a" in message or "b" in message:
            pass

    async def receive(self) -> Optional[Iterator[Dict]]:
        message = await self.recv()
        if not isinstance(message, list):
            return {}
        else:
            payload = message[1]
            return self.handle_trade_message(payload)

    async def send(self) -> None:
        KRAKEN_SUBSCRIPTION_PAYLOAD["pair"] = [self.symbol]
        KRAKEN_SUBSCRIPTION_PAYLOAD["subscription"]["name"] = "book"
        KRAKEN_SUBSCRIPTION_PAYLOAD["subscription"]["depth"] = self.depth
        await self.websocket.send(dumps(KRAKEN_SUBSCRIPTION_PAYLOAD))
        print(f"Subscribed to {self.exchange} book channel successfully")


async def main():
    async with KrakenOrderBook("BTC/USD", "kraken-orderbook", 30) as book:
        while True:
            print(await book.recv())


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
