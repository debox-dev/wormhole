import json
import mgzip

from wormhole.error import WormholeDecodeError

try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps, UnpicklingError

from typing import *


class WormholeJsonEncoder:
    @staticmethod
    def encode(data: Any):
        return json.dumps(data)

    @staticmethod
    def decode(data: str):
        return json.loads(data)


class WormholePickleEncoder(object):
    MINIMUM_LENGTH_TO_COMPRESS = 2048
    COMPRESSION_HEADER = b'$'
    UNPICKED_DATA_HEADER = b'%'
    PICKLED_DATA_HEADER = b'^'

    @classmethod
    def __compress_data_if_needed(cls, data: bytes) -> bytes:
        if len(data) > cls.MINIMUM_LENGTH_TO_COMPRESS:
            return cls.COMPRESSION_HEADER + mgzip.compress(data)
        return data

    @classmethod
    def __decompress_data_if_needed(cls, data: bytes) -> bytes:
        if data[0] == cls.COMPRESSION_HEADER[0]:
            return mgzip.decompress(data[1:])
        return data

    @classmethod
    def encode(cls, obj):
        if isinstance(obj, bytes):
            data = cls.UNPICKED_DATA_HEADER + obj
        else:
            data = dumps(obj)
        data = cls.__compress_data_if_needed(data)
        return data

    @classmethod
    def decode(cls, data):
        data = cls.__decompress_data_if_needed(data)
        if data[0] == cls.UNPICKED_DATA_HEADER[0]:
            return data[1:]
        try:
            return loads(data)
        except Exception as e:
            raise WormholeDecodeError(f"Error decoding data: {e} {repr(data)}")
