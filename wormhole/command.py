import struct
import time
from typing import *

from .registry import get_primary_wormhole
from .session import WormholeSession

if TYPE_CHECKING:
    from .basic import BasicWormhole


class WormholeCommandResult:
    def __init__(self, command: "WormholeCommand", session: WormholeSession):
        self.__session = session
        self.__command = command

    def poll(self):
        return self.__session.poll()

    def wait(self):
        return self.__command.deserialize_response(self.__session.wait())


class WormholeCommand:
    HEADER: bytes = None

    @classmethod
    def strip_header(cls, data: bytes):
        return data[len(cls.HEADER):]

    @classmethod
    def handle(cls, data: bytes) -> bytes:
        raise NotImplementedError()

    def serialize_request(self) -> bytes:
        raise NotImplementedError()

    def deserialize_response(self, data: Optional[bytes]):
        raise NotImplementedError()

    def __serialize_for_sending(self):
        return self.HEADER + self.serialize_request()

    def send(self, receiver_id: str, wormhole: Optional["BasicWormhole"] = None):
        if wormhole is None:
            wormhole = get_primary_wormhole()
        session = wormhole.send(receiver_id, self.__serialize_for_sending())
        return WormholeCommandResult(self, session)


class WormholePingCommand(WormholeCommand):
    HEADER = b"p"

    @classmethod
    def handle(cls, data: bytes) -> bytes:
        return data

    def serialize_request(self) -> bytes:
        return struct.pack("d", time.time())

    def deserialize_response(self, data: bytes):
        return time.time() - struct.unpack("d", data)[0]

