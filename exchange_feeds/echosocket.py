from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict

import websocket


@dataclass
class EchoWebSocket(metaclass=ABCMeta):
    url: str
    stream_name: str
    ws: websocket.WebsocketApp = field(init=False)

    def __post__init__(self) -> None:
        self.ws = websocket.WebSocketApp(
            self.url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

    @abstractmethod
    def on_message(self, message: Dict[str, Any]):
        pass

    @abstractmethod
    def on_error(self, error):
        pass

    @abstractmethod
    def on_close(self):
        pass

    @abstractmethod
    def on_open(self):
        pass
