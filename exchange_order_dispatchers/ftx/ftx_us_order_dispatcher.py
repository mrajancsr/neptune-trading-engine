import hmac
import json
import time
from typing import List, Dict

from requests import Request, Session, PreparedRequest
from http import HTTPStatus

from exchange_order_dispatchers.constants import (
    FTX_BASE_REST_API,
    FTX_ORDER_ENDPOINT,
    HttpMethod,
    FTX_US_KEY_HEADER,
    FTX_US_SIGNATURE_HEADER,
    FTX_US_TIMESTAMP_HEADER,
)
from exchange_order_dispatchers.order_dispatcher import OrderDispatcher

MS_PER_SECOND = 1000


class FtxUsOrderDispatcher(OrderDispatcher):
    def __init__(self, api_key, api_secret):
        self._api_key = api_key
        self._api_secret = api_secret
        self._url = FTX_BASE_REST_API

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
            "market": market,
            "side": "buy",
            "price": price,
            "type": order_type,
            "size": size,
            "postOnly": post_only,
        }
        prepared = FtxUsOrderDispatcher.prepare_request(
            self, HttpMethod.POST.value, f"{FTX_ORDER_ENDPOINT}", payload
        )
        s = Session()
        res = s.send(prepared)
        res = json.loads(res.text)
        return res

    def get_orders(self) -> List:
        prepared = FtxUsOrderDispatcher.prepare_request(
            self, HttpMethod.GET.value, f"{FTX_ORDER_ENDPOINT}"
        )
        s = Session()
        res = s.send(prepared)
        res = json.loads(res.text)
        return res["result"]

    def cancel_all_order(self) -> Dict:
        prepared = FtxUsOrderDispatcher.prepare_request(
            self, HttpMethod.DELETE.value, f"{FTX_ORDER_ENDPOINT}"
        )
        s = Session()
        res = s.send(prepared)
        res = json.loads(res.text)
        return res

    @classmethod
    def get_ts(self) -> int:
        return int(time.time() * MS_PER_SECOND)

    def prepare_request(self, method, path_url, body=None) -> PreparedRequest:
        ts = int(time.time() * MS_PER_SECOND)
        if body is not None:
            request = Request(method, f"{self._url}{path_url}", json=body)
        else:
            request = Request(method, f"{self._url}{path_url}")
        prepared = request.prepare()
        signature_payload = f"{ts}{prepared.method}{prepared.path_url}".encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(
            self._api_secret.encode(), signature_payload, "sha256"
        ).hexdigest()

        prepared.headers[FTX_US_KEY_HEADER] = self._api_key
        prepared.headers[FTX_US_SIGNATURE_HEADER] = signature
        prepared.headers[FTX_US_TIMESTAMP_HEADER] = str(ts)
        # Maybe needed, need to look at our biz mechanics first
        # prepared.headers[f'FTXUS-SUBACCOUNT'] = urllib.parse.quote('Test')
        return prepared
