import json

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
    @staticmethod
    def encode(obj):
        if isinstance(obj, bytes):
            return b"%" + obj
        return dumps(obj)

    @staticmethod
    def decode(data):
        if data[0] == b"%"[0]:
            return data[1:]
        try:
            return loads(data)
        except Exception as e:
            raise WormholeDecodeError(f"Error decoding data: {e} {repr(data)}")
