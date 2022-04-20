import os
from enum import Enum, unique
from pathlib import Path
from typing import Dict, List

MAX_RETRIES = 3
PHOBOSPATH = os.environ["PHOBOSTRADINGENGINEPATH"]
BAR_CUT_DELAY_IN_SECONDS = 60
EXCHANGEPATH = Path(os.path.join(PHOBOSPATH, "exchange_feeds"))

VOL_BREAKOUT_SIGNIFICANCE_BARRIER = 2.33
THIRTY_DAYS_MINUTES = "43200T"

BINANCE_BASE_WS: str = "wss://fstream.binance.com/ws/"
BINANCE_REST_ENDPOINT: str = "https://fapi.binance.com"
BINANCE_TRADE_BLOTTER: str = "@aggTrade"
BINANCE_QUOTE_L1: str = "@bookTicker"
BINANCE_BLOTTER_MAPPING: Dict[str, str] = {
    "trxn_time": "T",
    "symbol": "s",
    "trxn_price": "p",
    "trxn_size": "q",
}
BINANCE_SUBSCRIPTION_PAYLOAD: Dict[str, str] = {
    "method": "SUBSCRIBE",
    "params": [""],
    "id": 1,
}

BINANCE_ORDERBOOK_MAPPING: Dict[str, str] = {
    "trxn_time": "T",
    "symbol": "s",
    "bid": "b",
    "bid_size": "B",
    "ask": "a",
    "ask_size": "A",
}

FTX_BASE_WS: str = "wss://ftx.com/ws/"
FTXUS_BASE_WS: str = "wss://ftx.us/ws/"

FTX_SUBSCRIPTION_PAYLOAD: Dict[str, str] = {
    "op": "subscribe",
    "market": "",
    "channel": "",
}
FTX_BLOTTER_MAPPING: Dict[str, str] = {
    "symbol": "price",
    "trxn_price": "price",
    "trxn_size": "size",
    "side": "side",
    "liquidation": "liquidation",
    "trxn_time": "time",
}

DYDX_BASE_WS: str = "wss://api.dydx.exchange/v3/ws"
DYDX_SUBSCRIPTION_PAYLOAD: Dict[str, str] = {
    "type": "subscribe",
    "channel": "",
    "id": "",
}
DYDX_BLOTTER_MAPPING: Dict[str, str] = {
    "symbol": "side",
    "trxn_size": "size",
    "side": "side",
    "trxn_price": "price",
    "trxn_time": "createdAt",
}


@unique
class StreamName(Enum):
    BINANCE_BLOTTER = "binance-blotter"
    BINANCE_L1 = "binance-L1"
    FTX_BLOTTER = "ftx-blotter"
    FTX_ORDERBOOK = "ftx-orderbook"
    KRAKEN_BLOTTER = "kraken-blotter"
    KRAKEN_ORDERBOOK = "kraken-orderbook"
    FTXUS_BLOTTER = "ftxus-blotter"
    FTXUS_ORDERBOOK = "ftxus-orderbook"


STREAM_NAMES: List = [
    StreamName.BINANCE_BLOTTER.value,
    StreamName.BINANCE_L1.value,
    StreamName.FTX_BLOTTER.value,
    StreamName.FTX_ORDERBOOK.value,
    StreamName.KRAKEN_BLOTTER.value,
    StreamName.KRAKEN_ORDERBOOK.value,
    StreamName.FTXUS_BLOTTER.value,
    StreamName.FTXUS_ORDERBOOK.value,
]

KRAKEN_BASE_WS: str = "wss://ws.kraken.com/"
KRAKEN_SUBSCRIPTION_PAYLOAD: Dict[str, str] = {
    "event": "subscribe",
    "pair": "",
    "subscription": {
        "name": "",
    },
}


@unique
class KrakenTradeMessageIndex(Enum):
    PRICE = 0
    VOLUME = 1
    TIME = 2
    SIDE = 3
    ORDER_TYPE = 4


@unique
class KrakenBookMessageIndex(Enum):
    PRICE = 0
    VOLUME = 1
    TIME = 2


KRAKEN_BLOTTER_MAPPING = {
    KrakenTradeMessageIndex.PRICE: "trxn_price",
    KrakenTradeMessageIndex.VOLUME: "volume",
    KrakenTradeMessageIndex.TIME: "trxn_time",
    KrakenTradeMessageIndex.SIDE: "side",
    KrakenTradeMessageIndex.ORDER_TYPE: "order_type",
}


@unique
class Exchange(Enum):
    BINANCE = "Binance"
    DYDX = "DyDx"
    FTX = "Ftx"
    FTXUS = "FtxUS"
    KRAKEN = "Kraken"


class RedisActionType(Enum):
    WRITE_ONLY = "writeonly"
    READ_ONLY = "readonly"


OPEN = "open"
HIGH = "high"
LOW = "low"
CLOSE = "close"
TIME = "time"
SYMBOL = "symbol"

# -- PREFECT
ACCOUNT = "devphoboscapfund"
WORKSPACE_NAME = "live-feed-ingestor"
FLOW_NAME = "live_feeds_to_redis_pipeline"

L2_ENABLED_STREAMS = [StreamName.FTX_ORDERBOOK.value, StreamName.KRAKEN_ORDERBOOK.value]
