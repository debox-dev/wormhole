import pytest
import redis

from tests.test_objects import Vector3Handler, Vector3Message
from wormhole.async_implementations.async_gevent import GeventWormhole
from wormhole.basic import WormholeWaitable
from wormhole.channel import WormholeRedisChannel, AbstractWormholeChannel
from wormhole.error import WormholeHandlingError
from gevent.monkey import patch_all
from typing import *

patch_all()


class TestWormholeGeventCloseSequence:
    TEST_REDIS = "redis://localhost:6379/1"

    def test_simple(self):
        wormhole_channel = WormholeRedisChannel(self.TEST_REDIS)
        wormhole = GeventWormhole(wormhole_channel)
        handler = Vector3Handler()
        wormhole.register_all_handlers_of_instance(handler)
        wormhole.process_async()
        wormhole.stop(wait=True)
        wormhole_channel.close()


class BaseTestWormholeGevent:
    TEST_REDIS = "redis://localhost:6379/1"
    wormhole: Optional[GeventWormhole]
    wormhole_channel: Optional[AbstractWormholeChannel]

    def setup_method(self):
        self.wormhole_channel = WormholeRedisChannel(self.TEST_REDIS, max_connections=10)
        self.wormhole = GeventWormhole(self.wormhole_channel)
        handler = Vector3Handler()
        self.wormhole.register_all_handlers_of_instance(handler)
        self.wormhole.process_async(max_parallel=10)

    def teardown_method(self):
        self.wormhole.stop(wait=True)
        self.wormhole_channel.close()
        self.wormhole = None
        self.wormhole_channel = None


class TestWormholeGeventSession(BaseTestWormholeGevent):
    wormholes: List[GeventWormhole]
    channels: List[WormholeRedisChannel]

    def setup_method(self):
        super().setup_method()
        # start many wormholes and channels so there will be a chance for the test to fail on wrong receiver id
        self.channels = []
        self.wormholes = []
        for _ in range(3):
            ch = WormholeRedisChannel(self.TEST_REDIS, max_connections=10)
            self.channels.append(ch)
            wh = GeventWormhole(ch)
            self.wormholes.append(wh)
            handler = Vector3Handler()
            wh.register_all_handlers_of_instance(handler)
            wh.process_async()

    def teardown_method(self):
        super().teardown_method()
        for wh in self.wormholes:
            wh.stop()
        for ch in self.channels:
            ch.close()
        self.channels = []
        self.wormholes = []

    def test_session_simple(self):
        self.wormhole.process_async()
        i = 1
        m = Vector3Message(i, i * 2, i * i)
        session = m.send(wormhole=self.wormhole)
        assert session.wait() == m.magnitude
        i = 5
        for _ in range(20):
            m2 = Vector3Message(i, i * 2, i * i)
            session2 = m2.send(wormhole=session)

            assert session2.wait() == m2.magnitude
            assert session2.receiver_id == session.receiver_id

        # Test sanity check to see other wormholes are working
        m3 = Vector3Message(i, i * 2, i * i)
        ok = False
        for _ in range(20):
            r = m3.send(wormhole=self.wormhole)
            r.wait()
            if r.receiver_id != self.wormhole:
                ok = True
                break
        assert ok


class TestWormholeGevent(BaseTestWormholeGevent):
    def test_ping_self(self):
        for _ in range(0, 100):
            delay = self.wormhole.ping(self.wormhole.id)
            assert delay < 0.05

    def test_simple(self):
        promises = []
        for i in range(0, 30):
            m = Vector3Message(i, i * 2, i * i)
            m.delay = 0.1
            p = m.send(wormhole=self.wormhole)
            promises.append((m, p))
        for m, p in promises:
            assert p.wait() == m.magnitude

    def test_delayed_simple(self):
        promises = []
        for i in range(0, 30):
            m = Vector3Message(i, i * 2, i * i)
            m.delay = 1
            p = m.send(wormhole=self.wormhole)
            promises.append((m, p))
        for m, p in promises:
            assert p.wait() == m.magnitude

    def test_wait_for_any(self):
        reply_data = "asd1242FAS"
        v = Vector3Message(1, 2, 3)
        self.wormhole.unregister_all_handlers()
        async_result = v.send(wormhole=self.wormhole)
        r = redis.Redis.from_url(self.TEST_REDIS)
        wait_result = self.wormhole.wait_for_any(Vector3Message, "asd", timeout=5)
        assert not wait_result.timeout
        assert wait_result.item == Vector3Message
        assert wait_result.tag is None
        assert wait_result.data is not None
        assert wait_result.data == v
        wait_result.reply(reply_data)
        assert async_result.wait() == reply_data
        dummy_data = "lalala"
        async_result = self.wormhole.send("asd", dummy_data)
        wait_result = self.wormhole.wait_for_any(Vector3Message, "asd", timeout=3)
        assert wait_result.tag is None
        assert not wait_result.timeout
        wait_result.reply(dummy_data)
        assert async_result.wait() == dummy_data
        # Send data with a tag
        tag = "tag111"
        async_result = self.wormhole.send("asd", dummy_data, tag)
        wait_result = self.wormhole.wait_for_any(WormholeWaitable("asd", tag), "asd", timeout=3)
        assert wait_result.tag == tag
        assert not wait_result.timeout
        wait_result.reply(dummy_data)
        assert async_result.wait() == dummy_data
        # Now send without the tag and make sure the non tag one is handled
        async_result = self.wormhole.send("asd", dummy_data)
        wait_result = self.wormhole.wait_for_any(WormholeWaitable("asd", tag), "asd", timeout=3)
        assert wait_result.tag is None
        assert not wait_result.timeout
        wait_result.reply(dummy_data)
        assert async_result.wait() == dummy_data
        # Send one more time and make sure we timeout
        with pytest.raises(WormholeHandlingError):
            self.wormhole.send("asd", dummy_data).wait(timeout=2)
