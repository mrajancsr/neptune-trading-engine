from exchange_order_dispatchers.constants import FTX_US_API_SECRET, FTX_US_API_KEY
from exchange_order_dispatchers.ftx.ftx_us_order_dispatcher import FtxUsOrderDispatcher


def place_test_orders():
    ftx_dispatcher = FtxUsOrderDispatcher(FTX_US_API_KEY, FTX_US_API_SECRET)
    ftx_dispatcher.buy_order(market="BTC/USD", price=1, size=0.1)
    orders = ftx_dispatcher.get_orders()
    print(orders)
    cancelled = ftx_dispatcher.cancel_all_order()
    print(cancelled)


if __name__ == "__main__":
    place_test_orders()
