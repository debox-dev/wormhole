import pytest
import redis

from tests.test_objects import Vector3MixinHandler, Vector3Message
from wormhole.channel import WormholeRedisChannel
from wormhole.async_implementations.async_gevent import GeventWormhole

from typing import *

from wormhole.error import WormholeHandlingError, WormholeWaitForReplyError
from wormhole.utils import wait_all

if TYPE_CHECKING:
    from wormhole.channel import AbstractWormholeChannel


class TestWormholeHandlerInstanceMixin:
    TEST_REDIS = "redis://localhost:6379/1"
    wormhole: Optional[GeventWormhole]
    wormhole_channel: Optional["AbstractWormholeChannel"]

    def setup_method(self):
        rdb = redis.Redis.from_url(self.TEST_REDIS)
        rdb.flushdb()
        rdb.close()
        self.wormhole_channel = WormholeRedisChannel(self.TEST_REDIS, max_connections=10)
        self.wormhole = GeventWormhole(self.wormhole_channel)
        self.wormhole.process_async(max_parallel=10)

    def teardown_method(self):
        self.wormhole.stop(wait=True)
        self.wormhole_channel.close()
        self.wormhole = None
        self.wormhole_channel = None

    def test_simple_setup(self):
        instance = Vector3MixinHandler(self.wormhole)
        with pytest.raises(WormholeWaitForReplyError):
            Vector3Message(1, 5, 3).send(wormhole=self.wormhole).wait(timeout=1)
        wait_all(instance.activate_all_handlers())
        wait_all([Vector3Message(1, 5, 3).send(wormhole=self.wormhole) for _ in range(50)])
        wait_all(instance.deactivate_all_handlers())
        with pytest.raises(WormholeWaitForReplyError):
            Vector3Message(1, 5, 3).send(wormhole=self.wormhole).wait(timeout=1)
