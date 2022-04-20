import asyncio
import logging
import os
import threading
from dataclasses import dataclass, field
from json import dumps, loads
from typing import Dict, List, Optional

import Momentum
import pandas as pd
import uvloop
from exchange_feeds.constants import (
    BAR_CUT_DELAY_IN_SECONDS,
    CLOSE,
    EXCHANGEPATH,
    FTX_BASE_WS,
    FTX_BLOTTER_MAPPING,
    FTX_SUBSCRIPTION_PAYLOAD,
    FTXUS_BASE_WS,
    HIGH,
    LOW,
    OPEN,
    SYMBOL,
    TIME,
    Exchange,
)
from exchange_feeds.websocket import EchoWebSocket
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
        self._dataframe = pd.DataFrame()
        self._initialized = False
        self.f_stop = threading.Event()
        self.cut_bar()
        self.signal = Momentum.Momentum(
            token_pair=self.symbol,
            frequencies=[2, 5, 7.5],
            exit_frequency=5,
            slowest_moving_avg=7.5,
            zscore=1.96,
            slow_atr=20,
            fast_atr=15,
            roll_period="480T",
            backtest=False,
            discord_channel="sbf-says",
        )

    async def handle_trade_message(
        self, message: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        messages = []
        for m in message["data"]:
            msg = {
                new_key: m[old_key]
                for (new_key, old_key) in FTX_BLOTTER_MAPPING.items()
            }
            msg["symbol"] = self.symbol.replace("-", "")
            msg["liquidation"] = str(msg["liquidation"])
            self._dataframe = self._dataframe.append(msg, ignore_index=True)

            messages.append(msg)
        return messages

    async def receive(self) -> Optional[List[Dict[str, str]]]:
        message = loads(await self.websocket.recv())
        message_type = message["type"]
        if message_type == "subscribed" or message_type == "unsubscribed":
            print(f"Successfully subscribed to {message['market']}")
            return
        elif message_type == "info":
            print(f"info message received: {message}")
            if message["code"] == 20001:
                print("connection closed - we should reconnect")
        elif message_type == "error":
            raise Exception(message)
        result = await self.handle_trade_message(message)
        return result

    async def send(self):
        FTX_SUBSCRIPTION_PAYLOAD["market"] = self.symbol
        FTX_SUBSCRIPTION_PAYLOAD["channel"] = "trades"
        await self.websocket.send(dumps(FTX_SUBSCRIPTION_PAYLOAD))
        print("Socket open!")

    async def stream(self, save: bool = False) -> None:
        await super().stream(handle_lob=False, save=save, max_record_count=10)

    def cut_bar(self, f_stop=False):
        if len(self._dataframe) > 0:
            self._dataframe[TIME] = self._dataframe["trxn_time"]
            self._dataframe.set_index(TIME, inplace=True, drop=False)
            self._dataframe.index = pd.to_datetime(self._dataframe.index)
            self._dataframe["trxn_price"] = self._dataframe["trxn_price"].astype(
                "float"
            )
            bar_dict = {}
            bar_dict[TIME] = [pd.Timestamp("now")]
            prices = self._dataframe["trxn_price"]
            bar_dict[OPEN] = [prices[0]]
            bar_dict[HIGH] = [prices.max()]
            bar_dict[LOW] = [prices.min()]
            bar_dict[CLOSE] = [prices[len(prices) - 1]]
            new_bar = pd.DataFrame.from_dict(bar_dict)
            new_bar.set_index(TIME, inplace=True, drop=False)
            self._dataframe = self._dataframe.iloc[0:0]
            self.signal.append_bar(new_bar)
        else:
            print("Dataframe is empty for this bar")
            if self._initialized:  # i.e. don't create a bar during setup
                bar_dict = {}
                bar_dict[TIME] = [pd.Timestamp("now")]
                bar_dict[OPEN] = [None]
                bar_dict[HIGH] = [None]
                bar_dict[LOW] = [None]
                bar_dict[CLOSE] = [None]
                new_bar = pd.DataFrame.from_dict(bar_dict)
                new_bar.set_index(TIME, inplace=True, drop=False)
                self._dataframe = self._dataframe.iloc[0:0]
                self.signal.append_bar(new_bar, empty_bar=True)
            self._initialized = True
        if not self.f_stop.is_set():
            threading.Timer(BAR_CUT_DELAY_IN_SECONDS, self.cut_bar, [f_stop]).start()
