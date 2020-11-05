from enum import Enum, auto

from wormhole.channel import WormholeRedisChannel
from wormhole.basic import BasicWormhole
from wormhole.error import BaseWormholeException
from wormhole.registry import set_primary_wormhole, get_primary_wormhole

from typing import *


class WormholeAsyncType(Enum):
    NONE = auto()
    GEVENT = auto()


class WormholeSetupError(BaseWormholeException):
    pass


def basic_wormhole_setup(channel_uri: str = "redis://localhost:6379/1",
                         async_type: Union[WormholeAsyncType, str] = WormholeAsyncType.NONE):
    if get_primary_wormhole() is not None:
        raise WormholeSetupError("Primary wormhole already set up")
    channel = WormholeRedisChannel(channel_uri)
    wormhole: Optional[BasicWormhole] = None
    if async_type == WormholeAsyncType.NONE:
        wormhole = BasicWormhole(channel)
    if async_type in ("gevent", WormholeAsyncType.GEVENT):
        from .async_implementations.async_gevent import GeventWormhole
        wormhole = GeventWormhole(channel)
    if wormhole is None:
        raise WormholeSetupError(f"Unknown async type specified: {async_type}")
    set_primary_wormhole(wormhole)
    return wormhole
