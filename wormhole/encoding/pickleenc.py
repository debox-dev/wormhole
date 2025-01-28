from typing import Any

import gzip

from ..error import WormholeDecodeError

try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps, UnpicklingError

from .base import WormholeEncoder


class WormholePickleEncoder(WormholeEncoder):
    MINIMUM_LENGTH_TO_COMPRESS = 2048
    COMPRESSION_HEADER = b'$'
    UNPICKLED_DATA_HEADER = b'%'
    PICKLED_DATA_HEADER = b'^'

    @classmethod
    def __compress_data_if_needed(cls, data: bytes) -> bytes:
        if len(data) > cls.MINIMUM_LENGTH_TO_COMPRESS:
            return cls.COMPRESSION_HEADER + gzip.compress(data)
        return data

    def __decompress_data_if_needed(self, data: bytes) -> bytes:
        if data[0] == self.COMPRESSION_HEADER[0]:
            return gzip.decompress(data[1:])
        return data

    def encode(self, obj: Any) -> bytes:
        if isinstance(obj, bytes):
            data = self.UNPICKLED_DATA_HEADER + obj
        else:
            data = dumps(obj)
        data = self.__compress_data_if_needed(data)
        return data

    def decode(self, data: bytes) -> Any:
        data = self.__decompress_data_if_needed(data)
        if data[0] == self.UNPICKLED_DATA_HEADER[0]:
            return data[1:]
        try:
            return loads(data)
        except Exception as e:
            raise WormholeDecodeError(f"Error decoding data: {e} {repr(data)}")
