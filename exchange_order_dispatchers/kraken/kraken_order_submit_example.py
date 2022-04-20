from exchange_order_dispatchers.constants import KRAKEN_API_SECRET, KRAKEN_API_KEY
from exchange_order_dispatchers.kraken.kraken_order_dispatcher import (
    KrakenOrderDispatcher,
)


def place_test_orders():
    kraken_dispatcher = KrakenOrderDispatcher(KRAKEN_API_KEY, KRAKEN_API_SECRET)
    kraken_dispatcher.buy_order(market="BTC/USD", price=1, size=0.1)
    orders = kraken_dispatcher.get_orders()
    print(orders)
    cancelled = kraken_dispatcher.cancel_all_order()
    print(cancelled)


if __name__ == "__main__":
    place_test_orders()
