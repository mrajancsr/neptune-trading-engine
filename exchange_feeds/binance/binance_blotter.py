import asyncio
from dataclasses import dataclass, field
from json import dumps
from typing import Dict, List, Optional

import uvloop
from exchange_feeds.constants import (
    BINANCE_BASE_WS,
    BINANCE_SUBSCRIPTION_PAYLOAD,
    BINANCE_TRADE_BLOTTER,
    Exchange,
)
from exchange_feeds.socketmanager import EchoWebSocket

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# -- Note: At the moment, the file stops running once an error is caught


@dataclass
class BinanceBlotter(EchoWebSocket):
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
        if "result" in message:
            yield {}
        elif "m" in message:
            message["m"] = str(message["m"])
            yield message

    async def send(self) -> None:
        BINANCE_SUBSCRIPTION_PAYLOAD["params"] = [
            f"{self.symbol}{BINANCE_TRADE_BLOTTER}"
        ]
        await self.websocket.send(dumps(BINANCE_SUBSCRIPTION_PAYLOAD))
        print(f"Subscribed to {self.exchange} Exchange!")


async def main():
    async with BinanceBlotter("btcusdt", "binance-blotter") as bl:
        await bl.stream()


if __name__ == "__main__":
    asyncio.run(main())
