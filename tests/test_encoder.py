from typing import *

from tests.test_objects import Vector3
from wormhole.encoding.base import WormholeEncoder
from wormhole.encoding.pickleenc import WormholePickleEncoder


class TestWormholeEncoder:
    tested_encoder: Optional[WormholeEncoder]

    def setup_method(self):
        self.tested_encoder = WormholePickleEncoder()

    def teardown_method(self):
        self.tested_encoder = None

    def test_encode_decode_10000_str(self):
        for i in range(0, 10000):
            data = str(i * 20)
            encoded = self.tested_encoder.encode(data)
            assert isinstance(encoded, bytes)
            assert self.tested_encoder.decode(encoded) == data

    def test_encode_decode_10000_vector3(self):
        for i in range(0, 10000):
            data = Vector3(i, i * 2, i * i)
            encoded = self.tested_encoder.encode(data)
            assert isinstance(encoded, bytes)
            assert self.tested_encoder.decode(encoded) == data

    def test_encode_decode_10000_dicts(self):
        for i in range(0, 10000):
            data = dict(a=i,b=i*i,c=i*5)
            encoded = self.tested_encoder.encode(data)
            assert isinstance(encoded, bytes)
            assert self.tested_encoder.decode(encoded) == data
