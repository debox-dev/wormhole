﻿import time

import gevent
import pytest
import redis

from tests.test_objects import Vector3Handler, Vector3Message, TextMessage, TextMessageHandler
from wormhole.async_implementations.async_gevent import GeventWormhole
from wormhole.basic import WormholeWaitable
from wormhole.channel import WormholeRedisChannel, AbstractWormholeChannel
from wormhole.command import WormholePingCommand
from wormhole.error import WormholeHandlingError, WormholeWaitForReplyError
from gevent.monkey import patch_all
from typing import *

from wormhole.handler import WormholeHandler
from wormhole.session import WormholeSession
from wormhole.utils import wait_all

patch_all()


class TestWormholeGeventCloseSequence:
    TEST_REDIS = "redis://localhost:6379/1"

    def test_simple(self):
        wormhole_channel = WormholeRedisChannel(self.TEST_REDIS)
        wormhole = GeventWormhole(wormhole_channel)
        handler = Vector3Handler()
        WormholeHandler.register_all_handlers_of_instance(wormhole, handler)
        wormhole.process_async()
        wormhole.stop(wait=True)
        wormhole_channel.close()


class BaseTestWormholeGevent:
    TEST_REDIS = "redis://localhost:6379/1"
    wormhole: Optional[GeventWormhole]
    wormhole_channel: Optional[AbstractWormholeChannel]

    def setup_method(self):
        rdb = redis.Redis.from_url(self.TEST_REDIS)
        rdb.flushdb()
        rdb.close()
        self.wormhole_channel = WormholeRedisChannel(self.TEST_REDIS, max_connections=10)
        self.wormhole = GeventWormhole(self.wormhole_channel)
        handler1 = Vector3Handler()
        handler2 = TextMessageHandler()
        WormholeHandler.register_all_handlers_of_instance(self.wormhole, handler1)
        WormholeHandler.register_all_handlers_of_instance(self.wormhole, handler2)
        self.wormhole.process_async(max_parallel=10)

    def teardown_method(self):
        self.wormhole.stop(wait=True)
        self.wormhole_channel.close()
        self.wormhole = None
        self.wormhole_channel = None


class TestMultipleWormholeTooManyConnections(BaseTestWormholeGevent):
    wormholes: List[GeventWormhole]
    channel: WormholeRedisChannel

    def setup_method(self):
        super().setup_method()
        # start many wormholes and channels so there will be a chance for the test to fail on wrong receiver id
        self.wormholes = []
        self.channel = WormholeRedisChannel(self.TEST_REDIS, max_connections=6)
        for _ in range(5):
            wh = GeventWormhole(self.channel)
            self.wormholes.append(wh)
            handler = Vector3Handler()
            WormholeHandler.register_all_handlers_of_instance(wh, handler)
            wh.process_async()

    def test_stop_all(self):
        super().teardown_method()
        for wh in self.wormholes:
            wh.stop(False)

        while True:
            gevent.sleep(0.1)
            for wh in self.wormholes:
                if wh.is_running:
                    continue
            break

    def teardown_method(self):
        self.wormholes = []
        self.channel.close()


class TestMultipleWormhole(BaseTestWormholeGevent):
    wormholes: List[GeventWormhole]
    channels: List[WormholeRedisChannel]

    def setup_method(self):
        super().setup_method()
        # start many wormholes and channels so there will be a chance for the test to fail on wrong receiver id
        self.channels = []
        self.wormholes = []
        for _ in range(5):
            ch = WormholeRedisChannel(self.TEST_REDIS, max_connections=10)
            self.channels.append(ch)
            wh = GeventWormhole(ch)
            self.wormholes.append(wh)
            handler = Vector3Handler()
            WormholeHandler.register_all_handlers_of_instance(wh, handler)
            wh.process_async()

    def teardown_method(self):
        super().teardown_method()
        for wh in self.wormholes:
            wh.stop()
        for ch in self.channels:
            ch.close()
        self.channels = []
        self.wormholes = []

    def test_groups(self):
        group_name = "group111"
        group_name2 = "group222"
        self.wormhole.add_to_group(group_name).wait()
        assert self.wormhole.id in self.wormhole.find_group_members(group_name)
        Vector3Message(2, 5, 6).send(wormhole=self.wormhole, group=group_name).wait()
        self.wormhole.add_to_group(group_name2).wait()
        Vector3Message(2, 5, 6).send(wormhole=self.wormhole, group=group_name2).wait()
        self.wormhole.remove_from_group(group_name).wait()
        assert self.wormhole.id not in self.wormhole.find_group_members(group_name)
        async_result = Vector3Message(2, 5, 6).send(wormhole=self.wormhole, group=group_name)
        gevent.sleep(1)
        assert not async_result.poll()
        Vector3Message(2, 5, 6).send(wormhole=self.wormhole, group=group_name2).wait()

    def test_group_multi(self):
        group_name = "mass_group"
        # add all to the group
        wait_all([wh.add_to_group(group_name) for wh in self.wormholes])
        sessions = [Vector3Message(2, 5, 6).send(wormhole=self.wormhole, group=group_name) for _ in range(400)]
        wait_all(sessions)
        assert len(set([s.receiver_id for s in sessions])) == 5

    def test_session_simple(self):
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
        messages = [Vector3Message(i, i * 2, i * i) for i in range(100)]
        promises = [m.send(wormhole=self.wormhole) for m in messages]
        message_promise_couples = zip(messages, promises)
        assert all([p.wait() == m.magnitude for m, p in message_promise_couples])

    def test_large_message(self):
        text_data = "abcdef" * 124 * 1024
        messages = [TextMessage(text_data) for i in range(100)]
        promises = [m.send(wormhole=self.wormhole) for m in messages]
        message_promise_couples = zip(messages, promises)
        assert all([p.wait() == m.text[::-1] for m, p in message_promise_couples])

    def test_uptime(self):
        gevent.sleep(1)
        assert self.wormhole.uptime(self.wormhole.id) >= 1
        gevent.sleep(2)
        assert self.wormhole.uptime(self.wormhole.id) >= 3

    def test_delayed_simple(self):
        promises = []
        for i in range(0, 20):
            m = Vector3Message(i, i * 2, i * i)
            m.delay = 0.2
            p = m.send(wormhole=self.wormhole)
            promises.append((m, p))
        for m, p in promises:
            assert p.wait() == m.magnitude

    def test_commands(self):
        self.wormhole.learn_command(WormholePingCommand)
        duration = WormholePingCommand().send(self.wormhole.id, wormhole=self.wormhole).wait()
        assert duration <= 0.05

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
        with pytest.raises(WormholeWaitForReplyError):
            self.wormhole.send("asd", dummy_data).wait(timeout=1)

    def test_max_parallel(self):
        for i in range(self.wormhole.max_parallel * 6):
            v = Vector3Message(1, 2, 3)
            v.delay = 2
            v.send(wormhole=self.wormhole)
        v = Vector3Message(4, 5, 6)
        s: WormholeSession
        with pytest.raises(WormholeWaitForReplyError):
            s = v.send(wormhole=self.wormhole)
            s.wait(timeout=1, retries=0)
        assert s.is_error
        # Verify the wormhole did not take the message because it was too busy
        assert s.receiver_id == ''

    def asddas_test_max_parallel2(self):
        for i in range(self.wormhole.max_parallel):
            v = Vector3Message(1, 2, 3)
            v.delay = 3
            v.send(wormhole=self.wormhole)
        v = Vector3Message(1, 2, 3)
        s: WormholeSession
        other_wormhole = GeventWormhole(channel=self.wormhole_channel)
        other_wormhole.process_async(2)
        handler = Vector3Handler()
        WormholeHandler.register_all_handlers_of_instance(other_wormhole, handler)
        s = v.send(wormhole=self.wormhole)
        s.wait(timeout=1)
        assert not s.is_error
        # Verify the first wormhole did not take the message because it was too busy
        assert s.receiver_id == other_wormhole.id
        other_wormhole.stop(wait=True)
