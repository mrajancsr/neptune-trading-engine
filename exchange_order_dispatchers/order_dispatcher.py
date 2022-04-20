class OrderDispatcher:
    def __init__(self):
        raise NotImplementedError("Order dispatcher was not subclassed")

    def buy_order(
        self, market=None, price=0, type="limit", size=0, post_only=True, client_id=None
    ):
        raise NotImplementedError("Buy order was not subclassed!")

    def sell_order(
        self, market=None, price=0, type="limit", size=0, post_only=True, client_id=None
    ):
        raise NotImplementedError("Sell order was not subclassed!")

    def cancel_order(self, order_id):
        raise NotImplementedError("Cancel order was not subclassed!")

    def cancel_all_order(self):
        raise NotImplementedError("Cancel all orders was not subclassed!")

    @classmethod
    def prepare_request(cls, method, url):
        raise NotImplementedError("Auth headers was not subclassed!")
