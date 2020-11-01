import json
try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps

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
        return dumps(obj)

    @staticmethod
    def decode(data):
        return loads(data)
