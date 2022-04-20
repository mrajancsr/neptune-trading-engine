import hmac
import json
import time
from typing import List, Dict

from requests import Request, Session, PreparedRequest
from urllib.parse import urlencode

from exchange_order_dispatchers.constants import (
    KRAKEN_ORDER_ENDPOINT,
    KRAKEN_CANCEL_ALL_ORDERS_ENDPOINT,
    KRAKEN_OPEN_ORDERS_ENDPOINT,
)
from exchange_order_dispatchers.order_dispatcher import OrderDispatcher
from exchange_order_dispatchers.kraken.kraken_api import KrakenAPI


class KrakenOrderDispatcher(OrderDispatcher):
    def __init__(self, api_key, api_secret):
        self.api = KrakenAPI(api_key, api_secret)

    @property
    def api_key(self) -> str:
        return self._api_key

    @property
    def url(self) -> str:
        return self._url

    def buy_order(
        self,
        market: str = None,
        price: float = 0,
        order_type: str = "limit",
        size: float = 0,
        post_only: bool = True,
        client_id: str = None,
    ) -> List:
        payload = {
            "pair": market,
            "type": "buy",
            "price": price,
            "ordertype": order_type,
            "volume": size,
        }
        res = self.api.query_private(KRAKEN_ORDER_ENDPOINT, payload)
        return res

    def get_orders(self) -> List:
        res = self.api.query_private(KRAKEN_OPEN_ORDERS_ENDPOINT)
        return res

    def cancel_all_order(self) -> Dict:
        res = self.api.query_private(KRAKEN_CANCEL_ALL_ORDERS_ENDPOINT)
        return res

    def prepare_request(cls, method, url):
        pass
