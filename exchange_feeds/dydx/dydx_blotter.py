import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from json import dumps, loads
from pathlib import Path
from typing import Any, Dict, List, Optional

import aioredis
import uvloop
import websockets
from exchange_feeds.constants import (
    DYDX_BASE_WS,
    DYDX_BLOTTER_MAPPING,
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


@dataclass(eq=False)
class DyDxBlotter(EchoWebSocket):
    url: str
    symbol: str
    websocket: Optional[WebSocketClientProtocol] = None
    exchange: str = field(init=False)

    def __post_init__(self) -> None:
        self.exchange = Exchange.DYDX.value
        super().__init__(self.url)

    async def handle_trade_message(
        self, message: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        messages = []
        for m in message["contents"]["trades"]:
            msg = {
                new_key: m[old_key]
                for (new_key, old_key) in DYDX_BLOTTER_MAPPING.items()
            }
            msg["symbol"] = self.symbol.replace("-", "")
            messages.append(msg)
        return messages

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
            result = await self.handle_trade_message(message)
            return result

    async def send(self) -> None:
        # TODO sys.argv(1) or the like here to sub in for the 'market' argument
        DYDX_SUBSCRIPTION_PAYLOAD["channel"] = "v3_trades"
        DYDX_SUBSCRIPTION_PAYLOAD["id"] = self.symbol
        await self.websocket.send(dumps(DYDX_SUBSCRIPTION_PAYLOAD))
        print("Socket open!")


async def stream():
    symbol = "ETH-USD"
    url = DYDX_BASE_WS
    stream_name: str = f"{Exchange.DYDX.value.lower()}-ethusd-blotter"
    redis = await aioredis.from_url(LOCALHOST)
    exists = await redis.exists(stream_name)
    if not exists:
        raise KeyError(f"stream {stream_name} doesn't exist")
    async for socket in connect(url, ping_timeout=None):
        try:
            blotter = DyDxBlotter(url, symbol, websocket=socket)
            async with redis.pipeline() as pipe:
                await push_raw_feeds_to_redis(blotter, stream_name, pipe)
        except websockets.ConnectionClosed as e:
            msg = f"""Socket Closed at {datetime.now()}; \n
            close reason: {socket.close_reason}, close code: {socket.close_code}"""
            logger.info(msg)
            logger.info(f"exception occured at {Path(__file__).stem}")
            logger.info(e)
            continue
        else:
            await blotter.websocket.close_connection()
            break


if __name__ == "__main__":
    asyncio.run(stream())
