from typing import Any

import abc


class WormholeEncoder(metaclass=abc.ABCMeta):
    """
    Encodes and decodes python objects to/from bytes for the channel
    """
    def encode(self, data: Any) -> bytes:
        raise NotImplementedError

    def decode(self, data: bytes) -> Any:
        raise NotImplementedError