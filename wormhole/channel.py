import redis

from typing import *

from redis import BlockingConnectionPool

from wormhole.encoding import WormholePickleEncoder
from wormhole.error import WormholeChannelError
from wormhole.registry import DEFAULT_MESSAGE_TIMEOUT, DEFAULT_REPLY_TIMEOUT
from wormhole.utils import generate_uid


class AbstractWormholeChannel:
    def is_open(self):
        raise NotImplementedError()

    def pop_next(self, wh_receiver_id: str, queue_names: List[str], timeout: int = 0) -> \
            Optional[Tuple[str, Union[str, bytes]]]:
        raise NotImplementedError()

    def send(self, queue_name: str, data: Union[bytes, str]) -> str:
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


class WormholeRedisChannel(AbstractWormholeChannel):
    MESSAGE_DATA_HKEY = "in"
    MESSAGE_RESPONSE_HKEY = "out"
    MESSAGE_ERROR_HKEY = "err"
    MESSAGE_WORMHOLE_RECEIVER_ID_HKEY = "hid"
    GROUP_REGISTRY_PREFIX = "whgm://"

    def __init__(self, redis_uri: str = "redis://localhost:6379/1", max_connections=20):
        self.__connection_pool = BlockingConnectionPool.from_url(redis_uri, max_connections=max_connections)
        self.__encoder = WormholePickleEncoder()
        self.__closed = False

    def is_open(self):
        return not self.__closed

    def __get_rdb(self):
        if self.__closed:
            raise WormholeChannelError("Wormhole channel was closed, cannot use")
        return redis.Redis(connection_pool=self.__connection_pool)

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

    def send(self, queue_name: str, data: Any,
             queue_timeout: int = DEFAULT_MESSAGE_TIMEOUT) -> str:
        actual_timeout = queue_timeout + 2
        message_id = f"wh:{generate_uid()}"
        rdb = self.__get_rdb()
        transaction = rdb.pipeline()
        transaction.hset(message_id, self.MESSAGE_DATA_HKEY, self.__encoder.encode(data))
        transaction.expire(message_id, actual_timeout)
        transaction.lpush(queue_name, message_id)
        transaction.expire(queue_name, actual_timeout)
        transaction.execute()
        transaction.close()
        assert rdb.exists(message_id)
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
                return False, f"Message timed out, no handlers found for message {message_id}", ""
            return False, f"Timeout waiting for results from {receiver_id}", ""
        if isinstance(receiver_id, bytes):
            receiver_id = receiver_id.decode()
        rdb.delete(message_id)
        if error is not None:
            return False, self.__encoder.decode(error), receiver_id
        if data is None:
            return True, None, receiver_id
        return True, self.__encoder.decode(data), receiver_id

    def reply(self, message_id: str, data: Union[bytes, str], is_error: bool,
              timeout: int = DEFAULT_REPLY_TIMEOUT):
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

    def pop_next(self, wh_receiver_id: str, queue_names: List[str], timeout: int = 5) -> Optional[
        Tuple[str, str, bytes]]:
        rdb = self.__get_rdb()
        result: Optional[Tuple[bytes, bytes]] = rdb.brpop(queue_names, timeout)
        did_timeout = result is None
        if did_timeout:
            return None
        result_queue_name = result[0].decode()
        result_message_id = result[1].decode()
        result_payload = rdb.hgetall(result_message_id)
        # If the queued message already expired - return none
        if result_payload is None:
            return None
        try:
            rdb.hset(result_message_id, self.MESSAGE_WORMHOLE_RECEIVER_ID_HKEY, wh_receiver_id)
            message_data = result_payload[self.MESSAGE_DATA_HKEY.encode()]
        except KeyError:
            raise KeyError(f"Message {result_message_id} payload missing data: {repr(result_payload)}")
        return result_queue_name, result_message_id, self.__encoder.decode(message_data)

    def close(self):
        self.__closed = True
        self.__connection_pool.disconnect()


def create_default_channel():
    return WormholeRedisChannel()
