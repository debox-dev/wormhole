﻿import redis

from tests.test_objects import Vector3
from wormhole.channel import WormholeRedisChannel, WormholeAsync

from typing import *


class TestRedisChannel:
    TEST_REDIS_URL = "redis://localhost:6379/1"
    redis_client: Optional[redis.Redis]
    tested_channel: Optional[WormholeRedisChannel]

    def setup_method(self):
        self.redis_client = redis.from_url(self.TEST_REDIS_URL)
        self.redis_client.flushdb()
        self.tested_channel = WormholeRedisChannel(self.TEST_REDIS_URL)

    def teardown_method(self):
        self.redis_client.flushdb()
        self.redis_client = None
        self.tested_channel = None

    def test_core_send_and_reply_str(self):
        imaginary_receiver_id = "receiver1"
        test_queue_name = "my_queue"
        dummy_queue_name = "i am another queue"
        test_payload_data = "data123data321"
        test_reply_data = "reply10001011"

        # Send data
        channel = self.tested_channel
        result_async: WormholeAsync = channel.send(test_queue_name, test_payload_data, 10)
        assert result_async.channel == channel
        result_queue_name, result_message_id, result_data = channel.pop_next(imaginary_receiver_id,
                                                                             [test_queue_name, dummy_queue_name],
                                                                             timeout=1)
        assert not channel.check_for_reply(result_message_id)
        assert result_message_id == result_async.message_id
        assert result_queue_name == test_queue_name
        assert result_data == test_payload_data

        # Test replying to the received data
        channel.reply(result_message_id, test_reply_data, False, 1)
        assert channel.check_for_reply(result_message_id)
        is_success, reply_data, reply_receiver_id = channel.wait_for_reply(result_message_id, 1)
        assert is_success
        assert reply_data == test_reply_data
        assert reply_receiver_id == imaginary_receiver_id
        assert not channel.check_for_reply(result_message_id)

    def test_wormhole_async_send_and_reply_str(self):
        imaginary_receiver_id = "receiver1"
        test_queue_name = "my_queue"
        dummy_queue_name = "i am another queue"
        test_payload_data = "data123data321"
        test_reply_data = "reply10001011"

        # Send data
        channel = self.tested_channel
        result_async: WormholeAsync = channel.send(test_queue_name, test_payload_data, 10)
        assert result_async.channel == channel
        result_queue_name, result_message_id, result_data = channel.pop_next(imaginary_receiver_id,
                                                                             [test_queue_name, dummy_queue_name],
                                                                             timeout=1)
        assert not result_async.poll()
        assert result_message_id == result_async.message_id
        assert result_queue_name == test_queue_name
        assert result_data == test_payload_data

        # Test replying to the received data using the async object
        channel.reply(result_message_id, test_reply_data, False, 1)
        result_async_data = result_async.wait()
        assert result_async_data == test_reply_data
        assert not result_async.is_error
        assert result_async.poll()

    def test_wormhole_send_instances_of_objects(self):
        imaginary_receiver_id = "receiver1"
        test_queue_name = "my_queue"
        dummy_queue_name = "i am another queue"
        test_payload_data = Vector3(1, 5, 8)
        test_reply_data = Vector3(1, 0, 1)

        # Send data
        channel = self.tested_channel
        result_async: WormholeAsync = channel.send(test_queue_name, test_payload_data, 10)
        assert result_async.channel == channel
        result_queue_name, result_message_id, result_data = channel.pop_next(imaginary_receiver_id,
                                                                             [test_queue_name, dummy_queue_name],
                                                                             timeout=1)
        assert not result_async.poll()
        assert result_message_id == result_async.message_id
        assert result_queue_name == test_queue_name
        assert result_data == test_payload_data
        assert isinstance(result_data, Vector3)
        assert result_data.magnitude == test_payload_data.magnitude

        # Test replying to the received data using the async object
        channel.reply(result_message_id, test_reply_data, False, 1)
        result_async_data = result_async.wait()
        assert result_async_data == test_reply_data
        assert not result_async.is_error
        assert result_async.poll()
        assert isinstance(result_async.wait(), Vector3)
        assert result_async.wait().magnitude == test_reply_data.magnitude
