import time
import redis

from tests.test_objects import Vector3
from wormhole.channel import WormholeRedisChannel

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

    def test_locks(self):
        lock1_name = "lock1"
        secret = self.tested_channel.lock(lock1_name)
        assert secret
        assert self.tested_channel.is_locked(lock1_name)
        assert not self.tested_channel.lock(lock1_name, block=False)
        assert not self.tested_channel.lock(lock1_name, block_timeout=1)
        assert self.tested_channel.release(lock1_name, secret)
        assert not self.tested_channel.is_locked(lock1_name)
        # test lock timeout
        secret = self.tested_channel.lock(lock1_name, lock_timeout=1)
        assert secret
        assert self.tested_channel.is_locked(lock1_name)
        time.sleep(1.1)
        assert not self.tested_channel.is_locked(lock1_name)
        assert not self.tested_channel.release(lock1_name, secret)
        # Test release with forces
        assert self.tested_channel.lock(lock1_name)
        assert self.tested_channel.is_locked(lock1_name)
        assert self.tested_channel.release(lock1_name, "invalid_secret", force=True)
        assert not self.tested_channel.is_locked(lock1_name)

    def test_core_send_and_reply_str(self):
        imaginary_receiver_id = "receiver1"
        test_queue_name = "my_queue"
        dummy_queue_name = "i am another queue"
        test_payload_data = "data123data321"
        test_reply_data = "reply10001011"

        # Send data
        channel = self.tested_channel
        message_id = channel.send(imaginary_receiver_id, test_queue_name, test_payload_data, 10)
        result_queue_name, result_message_id, result_data = channel.pop_next(imaginary_receiver_id,
                                                                             [test_queue_name, dummy_queue_name],
                                                                             timeout=1)
        assert not channel.check_for_reply(result_message_id)
        assert result_message_id == message_id
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
        message_id = channel.send(imaginary_receiver_id, test_queue_name, test_payload_data, 10)
        result_queue_name, result_message_id, result_data = channel.pop_next(imaginary_receiver_id,
                                                                             [test_queue_name, dummy_queue_name],
                                                                             timeout=1)
        assert result_message_id == message_id
        assert result_queue_name == test_queue_name
        assert result_data == test_payload_data

        # Test replying to the received data using the async object
        channel.reply(result_message_id, test_reply_data, False, 1)
        is_success, data, receiver_id = self.tested_channel.wait_for_reply(message_id, 2)
        assert is_success
        assert data == test_reply_data

    def test_wormhole_send_instances_of_objects(self):
        imaginary_receiver_id = "receiver1"
        test_queue_name = "my_queue"
        dummy_queue_name = "i am another queue"
        test_payload_data = Vector3(1, 5, 8)
        test_reply_data = Vector3(1, 0, 1)

        # Send data
        channel = self.tested_channel
        message_id = channel.send(imaginary_receiver_id, test_queue_name, test_payload_data, 10)
        result_queue_name, result_message_id, result_data = channel.pop_next(imaginary_receiver_id,
                                                                             [test_queue_name, dummy_queue_name],
                                                                             timeout=1)
        assert result_message_id == message_id
        assert result_queue_name == test_queue_name
        assert result_data == test_payload_data
        assert isinstance(result_data, Vector3)
        assert result_data.magnitude == test_payload_data.magnitude

        # Test replying to the received data using the async object
        channel.reply(result_message_id, test_reply_data, False, 1)
        # Test replying to the received data using the async object
        channel.reply(result_message_id, test_reply_data, False, 1)
        is_success, data, receiver_id = self.tested_channel.wait_for_reply(message_id, 2)
        assert is_success
        assert data == test_reply_data
        assert isinstance(data, Vector3)
        assert data.magnitude == test_reply_data.magnitude

    def test_stats(self):
        imaginary_receiver_id = "receiver1"
        test_queue_name = "my_queue"
        dummy_queue_name = "i am another queue"
        test_payload_data = Vector3(1, 5, 8)
        test_reply_data = Vector3(1, 0, 1)

        channel = self.tested_channel
        # Send data
        for _ in range(0, 3000):
            message_id = channel.send(imaginary_receiver_id, test_queue_name, test_payload_data, 10)
            result_queue_name, result_message_id, result_data = channel.pop_next(imaginary_receiver_id,
                                                                                 [test_queue_name, dummy_queue_name],
                                                                                 timeout=10)
            assert result_message_id == message_id
            assert result_queue_name == test_queue_name
            assert result_data == test_payload_data
            assert isinstance(result_data, Vector3)
            assert result_data.magnitude == test_payload_data.magnitude
        stats = channel.get_stats()
        assert stats.sends_per_second > 60
        assert stats.processing_per_second > 60

    def test_disable_stats(self):
        imaginary_receiver_id = "receiver1"
        test_queue_name = "my_queue"
        dummy_queue_name = "i am another queue"
        test_payload_data = Vector3(1, 5, 8)
        test_reply_data = Vector3(1, 0, 1)

        channel = self.tested_channel
        channel.stats_enabled = False
        # Send data
        for _ in range(0, 3000):
            message_id = channel.send(imaginary_receiver_id, test_queue_name, test_payload_data, 10)
            result_queue_name, result_message_id, result_data = channel.pop_next(imaginary_receiver_id,
                                                                                 [test_queue_name, dummy_queue_name],
                                                                                 timeout=10)
            assert result_message_id == message_id
            assert result_queue_name == test_queue_name
            assert result_data == test_payload_data
            assert isinstance(result_data, Vector3)
            assert result_data.magnitude == test_payload_data.magnitude
        stats = channel.get_stats()
        assert stats.sends_per_second == -1
        assert stats.processing_per_second == -1
