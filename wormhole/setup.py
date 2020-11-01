from enum import Enum, auto

from wormhole.channel import WormholeRedisChannel
from wormhole.basic import BasicWormhole


class WormholeAsyncType(Enum):
    NONE = auto()
    GEVENT = auto()


def basic_wormhole_setup(channel_uri: str = "redis://localhost:6379/1",
                         async_type: WormholeAsyncType = WormholeAsyncType.NONE):
    channel = WormholeRedisChannel(channel_uri)
    if async_type == WormholeAsyncType.NONE:
        return BasicWormhole(channel)
    if async_type:
        from .async_implementations.async_gevent import GeventWormhole
        return GeventWormhole(channel)
    raise Exception("Unknown async type")
