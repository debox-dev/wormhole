import redis

from typing import *

from wormhole.encoding import WormholePickleEncoder
from wormhole.error import BaseWormholeException
from wormhole.registry import DEFAULT_MESSAGE_TIMEOUT, DEFAULT_REPLY_TIMEOUT
from wormhole.utils import generate_uid


class WormholeHandlingError(BaseWormholeException):
    pass


class AbstractWormholeChannel:
    def pop_next(self, wh_receiver_id: str, queue_names: List[str], timeout: int = 0) -> \
            Optional[Tuple[str, Union[str, bytes]]]:
        raise NotImplementedError()

    def send(self, queue_name: str, data: Union[bytes, str]) -> "WormholeAsync":
        raise NotImplementedError()

    def check_for_reply(self, message_id: str) -> bool:
        raise NotImplementedError()

    def wait_for_reply(self, message_id: str, timeout: int = 30) -> Tuple[bool, Any, str]:
        """Returns tuple(isSuccess, data)"""
        raise NotImplementedError()


class WormholeAsync:
    def __init__(self, message_id: str, channel: AbstractWormholeChannel):
        self.message_id = message_id
        self.channel = channel
        self.__reply_cache = None
        self.__did_get_reply = False
        self.__is_error = False
        self.__wh_receiver_id: Optional[str] = None

    @property
    def receiver_id(self):
        return self.__wh_receiver_id

    @property
    def is_error(self):
        return self.__is_error

    def poll(self):
        if not self.__did_get_reply:
            if not self.channel.check_for_reply(self.message_id):
                return False
            self.wait(raise_on_error=False)
        return True

    def wait(self, raise_on_error=True) -> Any:
        if not self.__did_get_reply:
            is_success, reply_data, wh_receiver_id = self.channel.wait_for_reply(self.message_id)
            self.__reply_cache = reply_data
            self.__is_error = not is_success
            self.__did_get_reply = True
            self.__wh_receiver_id = wh_receiver_id
        if self.is_error and raise_on_error:
            raise WormholeHandlingError(f"Message processing error: {self.__reply_cache}")
        return self.__reply_cache


class WormholeRedisChannel(AbstractWormholeChannel):
    MESSAGE_DATA_HKEY = "in"
    MESSAGE_RESPONSE_HKEY = "out"
    MESSAGE_ERROR_HKEY = "err"
    MESSAGE_WORMHOLE_RECEIVER_ID_HKEY = "hid"

    def __init__(self, redis_uri: str = "redis://localhost:6379/1"):
        self.__rdb = redis.Redis.from_url(redis_uri, decode_responses=False)
        self.__encoder = WormholePickleEncoder()

    def send(self, queue_name: str, data: Any,
             queue_timeout: int = DEFAULT_MESSAGE_TIMEOUT) -> WormholeAsync:
        actual_timeout = queue_timeout + 2
        message_id = "wh:" + generate_uid()
        transaction = self.__rdb.pipeline()
        transaction.hset(message_id, self.MESSAGE_DATA_HKEY, self.__encoder.encode(data))
        transaction.expire(message_id, actual_timeout)
        transaction.lpush(queue_name, message_id)
        transaction.expire(queue_name, actual_timeout)
        transaction.execute()
        return WormholeAsync(message_id, self)

    def check_for_reply(self, message_id: str):
        response_queue = "response:" + message_id
        return self.__rdb.llen(response_queue) > 0

    def wait_for_reply(self, message_id: str, timeout: int = DEFAULT_MESSAGE_TIMEOUT) -> Tuple[bool, Any, str]:
        response_queue = "response:" + message_id
        result = self.__rdb.brpop(response_queue, timeout)
        data = self.__rdb.hget(message_id, self.MESSAGE_RESPONSE_HKEY)
        error = self.__rdb.hget(message_id, self.MESSAGE_ERROR_HKEY)
        receiver_id = self.__rdb.hget(message_id, self.MESSAGE_WORMHOLE_RECEIVER_ID_HKEY)
        if not result:
            if receiver_id is None:
                return False, "Message timed out, no handlers found", ""
            return False, f"Timeout waiting for results from {receiver_id}", ""
        if isinstance(receiver_id, bytes):
            receiver_id = receiver_id.decode()
        self.__rdb.delete(message_id)
        if error is not None:
            return False, self.__encoder.decode(error), receiver_id
        if data is None:
            return True, None, receiver_id
        return True, self.__encoder.decode(data), receiver_id

    def reply(self, message_id: str, data: Union[bytes, str], is_error: bool,
              timeout: int = DEFAULT_REPLY_TIMEOUT):
        response_queue = "response:" + message_id
        transaction = self.__rdb.pipeline()
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

    def pop_next(self, wh_receiver_id: str, queue_names: List[str], timeout: int = 5) -> Optional[
        Tuple[str, str, bytes]]:
        result: Optional[Tuple[bytes, bytes]] = self.__rdb.brpop(queue_names, timeout)
        did_timeout = result is None
        if did_timeout:
            return None
        result_queue_name = result[0].decode()
        result_message_id = result[1].decode()
        result_payload = self.__rdb.hgetall(result_message_id)
        # If the queued message already expired - return none
        if result_payload is None:
            return None
        try:
            self.__rdb.hset(result_message_id, self.MESSAGE_WORMHOLE_RECEIVER_ID_HKEY, wh_receiver_id)
            message_data = result_payload[self.MESSAGE_DATA_HKEY.encode()]
        except KeyError:
            raise KeyError(f"Message {result_message_id} payload missing data: {repr(result_payload)}")
        return result_queue_name, result_message_id, self.__encoder.decode(message_data)


def create_default_channel():
    return WormholeRedisChannel()
