from tests.test_objects import Vector3Handler, Vector3Message
from wormhole.helpers import register_handler_instance
from wormhole.registry import get_primary_wormhole
from wormhole.setup import basic_wormhole_setup, WormholeAsyncType
from gevent.monkey import patch_all

patch_all()

TEST_REDIS = "redis://localhost:6379/1"
basic_wormhole_setup(TEST_REDIS, async_type=WormholeAsyncType.GEVENT)
wh = get_primary_wormhole()
wh.process_async()
handler = Vector3Handler()
register_handler_instance(handler)


class TestWormholeGevent:

    def test_simple(self):

        promises = []
        for i in range(0, 100):
            m = Vector3Message(i, i * 2, i * i)
            m.delay = 1
            p = m.send()
            promises.append((m, p))
        for m, p in promises:
            assert p.wait() == m.magnitude

    def test_delayed_simple(self):
        promises = []
        for i in range(0, 30):
            m = Vector3Message(i, i * 2, i * i)
            m.delay = 1
            p = m.send()
            promises.append((m, p))
        for m, p in promises:
            assert p.wait() == m.magnitude
