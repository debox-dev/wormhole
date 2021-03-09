from typing import *

from .error import WormholeHandlingError, WormholeWaitForReplyError
from .registry import DEFAULT_MESSAGE_TIMEOUT

if TYPE_CHECKING:
    from .basic import BasicWormhole


class WormholeSession:
    def __init__(self, message_id: str, wormhole: "BasicWormhole", resend_delegate: Callable = None):
        self.message_id = message_id
        self.wormhole = wormhole
        self.__reply_cache = None
        self.__did_get_reply = False
        self.__is_error = False
        self.__resend_delegate = resend_delegate
        self.__wh_receiver_id: Optional[str] = None

    @property
    def receiver_id(self):
        return self.__wh_receiver_id

    @property
    def is_error(self):
        return self.__is_error

    def poll(self):
        if not self.__did_get_reply:
            if not self.wormhole.channel.check_for_reply(self.message_id):
                return False
            self.wait(raise_on_error=False)
        return True

    def wait(self, raise_on_error=True, timeout: int = DEFAULT_MESSAGE_TIMEOUT, retries: int = 2) -> Any:
        reply_data: Union[Any, Exception] = None
        if not self.__did_get_reply:
            is_success, reply_data, wh_receiver_id = self.wormhole.channel.wait_for_reply(self.message_id,
                                                                                          timeout=timeout)
            if self.__resend_delegate is not None:
                while retries > 0:
                    retries -= 1
                    if not is_success and not wh_receiver_id:
                        self.__resend_delegate()
                        is_success, reply_data, wh_receiver_id = self.wormhole.channel.wait_for_reply(self.message_id,
                                                                                                      timeout=timeout)
            self.__reply_cache = reply_data
            self.__is_error = not is_success
            self.__did_get_reply = True
            self.__wh_receiver_id = wh_receiver_id
        if self.is_error and raise_on_error:
            if isinstance(reply_data, WormholeWaitForReplyError):
                raise reply_data
            if self.__did_get_reply and reply_data is not None:
                raise WormholeHandlingError(reply_data)
            else:
                raise WormholeWaitForReplyError()
        return self.__reply_cache
