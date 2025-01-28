from typing import Any

import json

from .base import WormholeEncoder

class WormholeJsonEncoder(WormholeEncoder):
    """
    A simple json encoder, it can only deserialize dicts
    """
    def encode(self, data: Any) -> bytes:
        return json.dumps(data).encode()

    def decode(self, data: bytes) -> dict:
        return json.loads(data.decode())