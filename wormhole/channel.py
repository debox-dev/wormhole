﻿import random
import time

import redis

from typing import *

from redis import BlockingConnectionPool

from wormhole.encoding.base import WormholeEncoder
from wormhole.error import WormholeWaitForReplyError, WormholeChannelClosedError, \
    WormholeChannelConnectionError
from wormhole.registry import DEFAULT_MESSAGE_TIMEOUT, DEFAULT_REPLY_TIMEOUT, get_default_encoder
from wormhole.utils import generate_uid


class WormholeChannelStats(NamedTuple):
    sends_per_second: int
    processing_per_second: int


class AbstractWormholeChannel:
    MESSAGE_FLAG_DONT_REPLY: ClassVar[int] = 1

    def is_open(self):
        raise NotImplementedError()

    def get_stats(self):
        raise NotImplementedError()

    def pop_next(self, wh_receiver_id: str, queue_names: List[str], timeout: int = 0) -> \
            Optional[Tuple[str, Union[str, bytes]]]:
        raise NotImplementedError()

    def send(self, wh_sender_id: str, queue_name: str, data: Union[bytes, str], queue_timeout: int = None, flags: int = 0) -> str:
        raise NotImplementedError()

    def delete(self, message_id: str) -> None:
        raise NotImplementedError()

    def check_for_reply(self, message_id: str) -> bool:
        raise NotImplementedError()

    def wait_for_reply(self, message_id: str, timeout: int = 30) -> Tuple[bool, Any, str]:
        """Returns tuple(isSuccess, data)"""
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def touch_for_groups(self, group_names: List[str], receiver_id: str, timeout: int = 5):
        raise NotImplementedError()

    def remove_from_groups(self, group_names: List[str], receiver_id: str):
        raise NotImplementedError()

    def lock(self, lock_name: str, block: bool = True, block_timeout: int = 0, lock_timeout: int = 0) -> Optional[str]:
        raise NotImplementedError()

    def release(self, lock_name: str, lock_secret: str, force=False):
        raise NotImplementedError()

    def threshold_lock(self, lock_name: str, max_amount: int, duration: float):
        raise NotImplementedError()

    def is_locked(self, lock_name: str):
        raise NotImplementedError()


class WormholeRedisChannel(AbstractWormholeChannel):
    MESSAGE_DATA_HKEY: str = "in"
    MESSAGE_FLAGS_KEY: str = "meta"
    MESSAGE_RESPONSE_HKEY = "out"
    MESSAGE_ERROR_HKEY = "err"
    MESSAGE_WORMHOLE_RECEIVER_ID_HKEY = "hid"
    GROUP_REGISTRY_PREFIX = "whgm://"
    LOCK_PREFIX = "whlk://"
    LOCK_SIGNAL_PREFIX = "whlks://"
    THRESHOLD_LOCK_PREFIX = "whth://"
    STATS_PREFIX = "whstats://"

    __encoder: WormholeEncoder
    __send_timeout: int
    __reply_expiration: int

    def __init__(self, redis_uri: str = "redis://localhost:6379/1", max_connections=20, send_timeout: int = DEFAULT_MESSAGE_TIMEOUT, reply_expiration: int = DEFAULT_REPLY_TIMEOUT, redis_pool: BlockingConnectionPool = None):
        if redis_pool is None:
            self.__connection_pool = BlockingConnectionPool.from_url(redis_uri, max_connections=max_connections)
        else:
            self.__connection_pool = redis_pool
        self.__encoder = get_default_encoder()
        self.__closed = False
        self.__send_timeout = send_timeout
        self.__reply_expiration = reply_expiration
        self.__send_rate = -1
        self.__receive_rate = -1
        self.stats_enabled = True

    def is_open(self):
        return not self.__closed

    def __get_rdb(self):
        if self.__closed:
            raise WormholeChannelClosedError("Wormhole channel was closed, cannot use")
        return redis.Redis(connection_pool=self.__connection_pool)

    def get_stats(self):
        return WormholeChannelStats(self.__send_rate, self.__receive_rate)

    def touch_for_groups(self, group_names: List[str], receiver_id: str, timeout: int = 5):
        rdb = self.__get_rdb()
        transaction = rdb.pipeline()
        for group_name in group_names:
            key_name = f"{self.GROUP_REGISTRY_PREFIX}{group_name}/{receiver_id}"
            transaction.setex(key_name, timeout, receiver_id)
        transaction.execute()
        transaction.close()

    def remove_from_groups(self, group_names: List[str], receiver_id: str):
        rdb = self.__get_rdb()
        transaction = rdb.pipeline()
        for group_name in group_names:
            key_name = f"{self.GROUP_REGISTRY_PREFIX}{group_name}/{receiver_id}"
            transaction.delete(key_name)
        transaction.execute()
        transaction.close()

    def find_group_members(self, group_name):
        rdb = self.__get_rdb()
        prefix = f"{self.GROUP_REGISTRY_PREFIX}{group_name}/"
        member_keys = rdb.keys(f"{prefix}*")
        return [d.decode()[len(prefix):] for d in member_keys]

    def send(self, wh_sender_id: str, queue_name: str, data: Any,
             queue_timeout: int = None, flags: int = 0) -> str:
        if queue_timeout is None:
            queue_timeout = self.__send_timeout
        actual_timeout = queue_timeout + 2
        message_id = f"wh:{generate_uid()}"
        rdb = self.__get_rdb()

        # Stats
        if self.stats_enabled:
            now = time.time()
            stats_counter_key = f"{self.STATS_PREFIX}{wh_sender_id}:sends"
            stats_last_update_key = f"{self.STATS_PREFIX}{wh_sender_id}:sends_touch_time"
            last_stat_time: Union[bytes, float] = rdb.get(stats_last_update_key)
            if last_stat_time is None:
                last_stat_time = time.time()
                rdb.set(stats_last_update_key, last_stat_time)
            else:
                last_stat_time = float(last_stat_time.decode())
        ############

        transaction = rdb.pipeline()
        transaction.hset(message_id, self.MESSAGE_DATA_HKEY, self.__encoder.encode(data))
        transaction.hset(message_id, self.MESSAGE_FLAGS_KEY, str(flags).encode('utf-8'))
        transaction.expire(message_id, actual_timeout)
        transaction.lpush(queue_name, message_id)
        transaction.expire(queue_name, actual_timeout)
        transaction.execute()
        assert rdb.exists(message_id)

        # STATS
        if self.stats_enabled:
            total_sends_since = rdb.incr(stats_counter_key)
            seconds_since = now - last_stat_time
            if seconds_since >= 60 or total_sends_since > 2000:
                self.__send_rate = total_sends_since / seconds_since
                transaction = rdb.pipeline()
                transaction.set(stats_counter_key, 0)
                transaction.set(stats_last_update_key, now)
                transaction.execute()
        #############

        return message_id

    def check_for_reply(self, message_id: str):
        response_queue = "response:" + message_id
        rdb = self.__get_rdb()
        return rdb.llen(response_queue) > 0

    def wait_for_reply(self, message_id: str, timeout: int = DEFAULT_MESSAGE_TIMEOUT) -> Tuple[bool, Any, str]:
        response_queue = "response:" + message_id
        rdb = self.__get_rdb()
        result = rdb.brpop(response_queue, timeout)
        data = rdb.hget(message_id, self.MESSAGE_RESPONSE_HKEY)
        error = rdb.hget(message_id, self.MESSAGE_ERROR_HKEY)
        receiver_id = rdb.hget(message_id, self.MESSAGE_WORMHOLE_RECEIVER_ID_HKEY)
        if not result:
            if receiver_id is None:
                return False, WormholeWaitForReplyError(f"Message timed out, no handlers found for message {message_id}"), ""
            return False, WormholeWaitForReplyError(f"Timeout waiting for results from {receiver_id}"), ""
        if isinstance(receiver_id, bytes):
            receiver_id = receiver_id.decode()
        rdb.delete(message_id)
        if error is not None:
            return False, self.__encoder.decode(error), receiver_id
        if data is None:
            return True, None, receiver_id
        return True, self.__encoder.decode(data), receiver_id

    def delete(self, message_id):
        self.__get_rdb().delete(message_id)

    def reply(self, message_id: str, data: Any, is_error: bool,
              timeout: int = None):
        if not timeout:
            timeout = self.__reply_expiration
        try:
            response_queue = "response:" + message_id
            rdb = self.__get_rdb()
            transaction = rdb.pipeline()
            if is_error:
                data_hkey = self.MESSAGE_ERROR_HKEY
                signal_reply = "error"
            else:
                data_hkey = self.MESSAGE_RESPONSE_HKEY
                signal_reply = "handled"
            if data is not None:
                transaction.hset(message_id, data_hkey, self.__encoder.encode(data))
            transaction.lpush(response_queue, signal_reply)
            transaction.expire(response_queue, timeout)
            transaction.expire(message_id, timeout)
            transaction.execute()
            transaction.close()
        except redis.exceptions.ConnectionError as e:
            if self.__closed:
                raise WormholeChannelClosedError("Cannot reply using a closed channel")
            raise WormholeChannelConnectionError(f"Connection error during reply: {e}")

    def pop_next(self, wh_receiver_id: str, queue_names: List[str], timeout: int = 5) -> Optional[
        Tuple[str, str, bytes, int]]:
        rdb = self.__get_rdb()
        random.shuffle(queue_names)
        result: Optional[Tuple[bytes, bytes]] = rdb.brpop(queue_names, timeout)
        did_timeout = result is None
        if did_timeout:
            return None
        result_queue_name = result[0].decode()
        result_message_id = result[1].decode()
        result_payload = rdb.hgetall(result_message_id)

        # Stats
        if self.stats_enabled:
            now = time.time()
            stats_counter_key = f"{self.STATS_PREFIX}{wh_receiver_id}:receive"
            stats_last_update_key = f"{self.STATS_PREFIX}{wh_receiver_id}:receive_touch_time"
            last_stat_time: Union[bytes, float] = rdb.get(stats_last_update_key)
            if last_stat_time is None:
                last_stat_time = time.time()
                rdb.set(stats_last_update_key, last_stat_time)
            else:
                last_stat_time = float(last_stat_time.decode())
            total_count_since = rdb.incr(stats_counter_key)
            seconds_since = now - last_stat_time
            if seconds_since >= 60 or total_count_since > 2000:
                self.__receive_rate = total_count_since / seconds_since
                rdb.set(stats_counter_key, 0)
                rdb.set(stats_last_update_key, now)
        #############

        # If the queued message already expired - return none
        if result_payload is None:
            return None
        if self.MESSAGE_DATA_HKEY.encode() not in result_payload:
            return None  # empty stale message
        if self.MESSAGE_FLAGS_KEY.encode() in result_payload:
            flags = int(result_payload[self.MESSAGE_FLAGS_KEY.encode()].decode('utf-8'))
        else:
            flags = 0
        try:
            rdb.hset(result_message_id, self.MESSAGE_WORMHOLE_RECEIVER_ID_HKEY, wh_receiver_id)
            message_data = result_payload[self.MESSAGE_DATA_HKEY.encode()]
        except KeyError:
            return None
        return result_queue_name, result_message_id, self.__encoder.decode(message_data), flags

    def close(self):
        self.__closed = True
        self.__connection_pool.disconnect()

    def threshold_lock(self, lock_name: str, max_amount: int, duration: int):
        assert duration >= 1, "'duration' cannot be smaller than 1"
        assert max_amount >= 1, "'max_amount' cannot be smaller than 1"
        full_lock_name = f"{self.THRESHOLD_LOCK_PREFIX}{lock_name}"
        rdb = self.__get_rdb()
        result = rdb.incr(full_lock_name)
        if result > max_amount:
            return False
        if result == 1:
            rdb.expire(full_lock_name, duration)
        return True

    def lock(self, lock_name: str, block: bool = True, block_timeout: int = 0, lock_timeout: int = 0) -> Optional[str]:
        lock_secret = generate_uid()
        key = self.__get_lock_key(lock_name)
        while not self.__get_rdb().setnx(key, lock_secret):
            if not block:
                return None
            result = self.__get_rdb().brpop(self.__get_lock_signal_key(lock_name), timeout=block_timeout)
            if result is None:  # Timeout
                return None
        if lock_timeout > 0:
            self.__get_rdb().expire(key, lock_timeout)
        return lock_secret

    def release(self, lock_name: str, lock_secret: str, force=False):
        key = self.__get_lock_key(lock_name)
        locker_secret: bytes = self.__get_rdb().get(key)
        if locker_secret is None:
            return False  # not locked
        if not force and locker_secret.decode() != lock_secret:
            raise KeyError("Invalid lock secret, not the owner of this lock")
        self.__get_rdb().delete(key)
        self.__get_rdb().lpush(self.__get_lock_signal_key(lock_name), 1)
        self.__get_rdb().expire(key, 10)
        return True

    def is_locked(self, lock_name: str):
        key = self.__get_lock_key(lock_name)
        return self.__get_rdb().exists(key)

    def __get_lock_key(self, lock_name: str):
        return f"{self.LOCK_PREFIX}{lock_name}"

    def __get_lock_signal_key(self, lock_name: str):
        return f"{self.LOCK_SIGNAL_PREFIX}{lock_name}"


def create_default_channel():
    return WormholeRedisChannel()
