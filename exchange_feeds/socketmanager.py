import asyncio
import json
import logging
import os
from abc import ABCMeta, abstractmethod
from asyncio import AbstractEventLoop
from enum import Enum
from random import random
from socket import gaierror
from typing import Any, Dict, Optional

from redistoolz import connect_to_redis
from websockets import WebSocketClientProtocol, connect, protocol
from websockets.exceptions import ConnectionClosedError

from exchange_feeds.constants import EXCHANGEPATH, RedisActionType

path_to_log_file = os.path.join(EXCHANGEPATH, "exchangelogs.log")

logging.basicConfig(filename=path_to_log_file, level=logging.INFO)


class WebSocketUnableToConnect(Exception):
    pass


class WSListenerState(Enum):
    INITIALISING = "Initialising"
    STREAMING = "Streaming"
    RECONNECTING = "Reconnecting"
    EXITING = "Exiting"


class EchoWebSocket(metaclass=ABCMeta):
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 10
    NO_MESSAGE_RECONNECT_TIMEOUT = 60
    MAX_QUEUE_SIZE = 100

    def __init__(self, url: str, stream_name: str):
        self.url = url
        self.stream_name = stream_name
        self._socket = None
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.ws_state = WSListenerState.INITIALISING
        self._queue = asyncio.Queue()
        self._handle_read_loop = None
        self._loop: AbstractEventLoop = asyncio.get_event_loop()
        self._log = logging.Logger = logging.getLogger()
        self._reconnects: int = 0

    async def __aenter__(self):
        print("attempting to connect")
        await self.connect()
        await self.send()
        return self

    async def connect(self):
        msg = (
            f"connected to exchange: {self.exchange} for streamname {self.stream_name}"
        )
        self.ws_state = WSListenerState.STREAMING
        self._conn = connect(self.url, ping_interval=None)
        try:
            self.websocket = await self._conn.__aenter__()
            print(msg)
            self._log.info(msg)
            self._reconnects = 0
        except Exception as e:
            self._log.debug(f"attempting reconnecting {e}")
            await self._reconnect()
            return
        if not self._handle_read_loop:
            print("handling thread safe")
            self._handle_read_loop = self._loop.call_soon_threadsafe(
                asyncio.create_task, self._read_loop()
            )

    def _handle_message(self, message) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(message)
        except Exception as e:
            self._log.debug(f"error parsing json {message} with error {e}")
            return None

    async def __aexit__(self, *args, **kwargs):
        print(f"websockets State: {WSListenerState.EXITING}")
        self.ws_state = WSListenerState.EXITING
        if self.websocket:
            self.websocket.fail_connection()
        if self._conn and hasattr(self._conn, "protocol"):
            await self._conn.__aexit__(*args, **kwargs)
        self.websocket = None
        if self._handle_read_loop:
            self._log.error("CANCEL read_loop")
            await self._kill_read_loop()

    async def _read_loop(self):
        print("entered read_loop")
        try:
            while True:
                try:
                    if self.ws_state == WSListenerState.RECONNECTING:
                        await self._run_reconnect()

                    if not self.websocket or self.ws_state != WSListenerState.STREAMING:
                        await self._wait_for_reconnect()
                        break
                    elif self.ws_state == WSListenerState.EXITING:
                        break
                    elif self.websocket.state == protocol.State.CLOSING:
                        await asyncio.sleep(0.1)
                        continue
                    elif self.websocket.state == protocol.State.CLOSED:
                        await self._reconnect()
                    elif self.ws_state == WSListenerState.STREAMING:
                        res = await asyncio.wait_for(
                            self.websocket.recv(), timeout=self.TIMEOUT
                        )
                        res = self._handle_message(res)
                        if res:
                            if self._queue.qsize() < self.MAX_QUEUE_SIZE:
                                await self._queue.put(res)
                            else:
                                self._log.debug(
                                    f"Queue overflow {self.MAX_QUEUE_SIZE}. Message not filled"
                                )
                                await self._queue.put(
                                    {
                                        "e": "error",
                                        "m": "Queue overflow. Message not filled",
                                    }
                                )
                                raise WebSocketUnableToConnect
                except asyncio.TimeoutError:
                    self._log.debug(f"no message in {self.TIMEOUT} seconds")
                    # _no_message_received_reconnect
                except asyncio.CancelledError as e:
                    self._log.debug(f"cancelled error {e}")
                    break
                except asyncio.IncompleteReadError as e:
                    self._log.debug(f"incomplete read error ({e})")
                except ConnectionClosedError as e:
                    self._log.debug(f"connection close error ({e})")
                except gaierror as e:
                    self._log.debug(f"DNS Error ({e})")
                except WebSocketUnableToConnect as e:
                    self._log.debug(f"WebsocketUnableToConnect ({e})")
                    break
                except Exception as e:
                    self._log.debug(f"Unknown exception ({e})")
                    continue
        finally:
            self._handle_read_loop = None  # Signal the coro is stopped
            self._reconnects = 0

    async def _kill_read_loop(self):
        self.ws_state = WSListenerState.EXITING
        while self._handle_read_loop:
            await asyncio.sleep(0.1)

    async def _run_reconnect(self) -> None:
        """Applies reconnection by checking websocket state on each iteration

        Raises
        ------
        WebSocketUnableToConnect
            _description_
        """
        await self.before_reconnect()
        if self._reconnects < self.MAX_RECONNECTS:
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            self._log.debug(
                f"websocket reconnecting. {self.MAX_RECONNECTS - self._reconnects} reconnects left - "
                f"waiting {reconnect_wait}"
            )
            await asyncio.sleep(reconnect_wait)
            await self.connect()
            await self.send()
        else:
            self._log.error(f"Max reconnections {self.MAX_RECONNECTS} reached:")
            # Signal the error
            await self._queue.put({"e": "error", "m": "Max reconnect retries reached"})
            raise WebSocketUnableToConnect

    async def _wait_for_reconnect(self):
        while (
            self.ws_state != WSListenerState.STREAMING
            and self.ws_state != WSListenerState.EXITING
        ):
            await asyncio.sleep(0.1)

    def _get_reconnect_wait(self, attempts: int) -> int:
        expo = 2**attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def before_reconnect(self) -> None:
        """Ensures the socket and websocket are closed prior to reconnect"""
        if self.websocket and self._conn:
            await self._conn.__aexit__(None, None, None)
            self.websocket.fail_connection()
            self.websocket = None
            self._conn = None
        self._reconnects += 1

    def _no_message_received_reconnect(self):
        self._log.debug("No message received, reconnecting")
        self.ws_state = WSListenerState.RECONNECTING

    async def _reconnect(self) -> None:
        self.ws_state = WSListenerState.RECONNECTING

    @abstractmethod
    async def send(self, message):
        pass

    @abstractmethod
    async def receive(self):
        pass

    async def stream(
        self,
        save_stream: bool = False,
        max_record_count: int = 5,
    ) -> None:
        print("Entered Streamer")

        redis = await connect_to_redis(RedisActionType.WRITE_ONLY)
        record_count = 0
        async with redis.pipeline() as pipe:
            while True:
                async for record in self.receive():
                    if not record:
                        continue
                    print(record)
                    if save_stream:
                        await pipe.xadd(self.stream_name, record)
                        record_count += 1
                    if record_count >= max_record_count:
                        await pipe.execute()
                        print("Pushed records to Redis")
                        record_count = 0

    async def recv(self):
        record = None
        while not record:
            try:
                record = await asyncio.wait_for(self._queue.get(), timeout=self.TIMEOUT)
            except asyncio.TimeoutError:
                self._log.debug(f"no message in {self.TIMEOUT} seconds")
        return record
