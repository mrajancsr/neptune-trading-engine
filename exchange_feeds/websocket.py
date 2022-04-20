from abc import ABCMeta, abstractmethod

from redistoolz import connect_to_redis, push_raw_feeds_to_redis
from websockets import connect

from exchange_feeds.constants import RedisActionType


class EchoWebSocket(metaclass=ABCMeta):
    def __init__(self, url: str, stream_name: str):
        self.url = url
        self.stream_name = stream_name

    async def __aenter__(self):
        self._conn = connect(self.url, ping_interval=None)
        self.websocket = await self._conn.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._conn.__aexit__(*args, **kwargs)

    @abstractmethod
    async def send(self, message):
        pass

    @abstractmethod
    async def receive(self):
        pass

    async def stream(
        self, handle_lob: bool = False, save: bool = False, max_record_count: int = 500
    ) -> None:
        print("Entered Streamer")

        redis = await connect_to_redis(action=RedisActionType.WRITE_ONLY)

        # ensures indefinite connection to exchange
        async with redis.pipeline() as pipe:
            await push_raw_feeds_to_redis(
                self, self.stream_name, pipe, handle_lob, save, max_record_count
            )
