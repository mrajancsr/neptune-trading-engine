import asyncio
import logging
from dataclasses import dataclass, field
from json import dumps
from typing import Any, Dict, Iterator, List, Optional

import uvloop
from exchange_feeds.constants import (
    KRAKEN_BASE_WS,
    KRAKEN_BLOTTER_MAPPING,
    KRAKEN_SUBSCRIPTION_PAYLOAD,
    Exchange,
    KrakenTradeMessageIndex,
)
from exchange_feeds.socketmanager import EchoWebSocket
from websockets.client import WebSocketClientProtocol

# -- Notes
# -- Kraken Exchange requires a Subscription payload,
# -- see ref: https://docs.kraken.com/websockets/#message-subscribe

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@dataclass
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
        super().__init__(self.url, self.stream_name)

    def handle_trade_message(self, message: List[Any]) -> Dict[str, str]:
        msg = {}
        msg["symbol"] = self.symbol
        msg.update(
            {
                KRAKEN_BLOTTER_MAPPING[index]: message[index.value]
                for index in KrakenTradeMessageIndex
            }
        )
        return msg

    async def receive(self) -> Optional[Iterator[Dict]]:
        message = await self.recv()

        if not isinstance(message, list):
            yield {}
        else:
            traded_messages = message[1]
            for m in traded_messages:
                yield self.handle_trade_message(m)

    async def send(self) -> None:
        KRAKEN_SUBSCRIPTION_PAYLOAD["pair"] = [self.symbol]
        KRAKEN_SUBSCRIPTION_PAYLOAD["subscription"]["name"] = "trade"
        await self.websocket.send(dumps(KRAKEN_SUBSCRIPTION_PAYLOAD))
        print(f"Subscribed to {self.exchange} trade channel successfully")


async def main():
    async with KrakenBlotter("ETH/USD", "kraken-blotter") as bl:
        await bl.stream(save_stream=True, max_record_count=5)


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
