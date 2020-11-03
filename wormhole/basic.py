from typing import *

from .error import BaseWormholeException
from .registry import PRINT_HANDLER_EXCEPTIONS
from .utils import generate_uid, merge_queue_name_with_tag

if TYPE_CHECKING:
    from .channel import AbstractWormholeChannel
    from .message import WormholeMessage


class BaseWormholeQueueException(BaseWormholeException):
    def __init__(self, queue_name: str):
        self.queue_name = queue_name


class WormholeHandlerAlreadyExists(BaseWormholeQueueException):
    def __str__(self):
        return f"A handler is already registered for queue {self.queue_name}"


class WormholeHandlerNotRegistered(BaseWormholeQueueException):
    def __str__(self):
        return f"A handler is not registered for queue {self.queue_name}"


class WormholeInvalidQueueName(BaseWormholeQueueException):
    def __str__(self):
        return f"Not a valid wormhole queue name: {self.queue_name}"


class WormholeQueueNameFormatter:
    PREFIX = "wh:"

    @classmethod
    def user_queue_to_wh_queue(cls, user_queue_name: str):
        return f"{cls.PREFIX}{user_queue_name}"

    @classmethod
    def wh_queue_to_user_queu(cls, wh_queue_name: str):
        if not wh_queue_name.startswith(cls.PREFIX):
            raise WormholeInvalidQueueName(wh_queue_name)
        return wh_queue_name[len(cls.PREFIX):]


class BasicWormhole:
    POP_TIMEOUT = 1

    def __init__(self, channel: Optional["AbstractWormholeChannel"] = None):
        if channel is None:
            from .channel import create_default_channel
            channel = create_default_channel()
        self.__handlers: Dict[str, Callable] = dict()
        self.__channel = channel
        self.__running = False
        self.__receiver_id = generate_uid()
        self.__register_internal_handlers()

    @property
    def id(self) -> str:
        return self.__receiver_id

    @property
    def is_running(self):
        return self.__running

    def process_blocking(self):
        self.__running = True
        while self.__running:
            self.__pop_and_handle_next(1)
        self.__handlers.clear()

    def process_async(self):
        raise NotImplementedError("Please use an async implementation like GeventWormhole")

    def stop(self):
        self.__running = False
        self.send(self.__receiver_id, '')

    def register_message_handler(self, message_class: Type["WormholeMessage"], handler_func: Callable,
                                 queue_name: Optional[str] = None, tag: Optional[str] = None):
        if queue_name is None:
            queue_name = message_class.get_base_queue_name()
        self.register_handler(queue_name, handler_func, tag)

    def unregister_message_handler(self, message_class: Type["WormholeMessage"], queue_name: Optional[str] = None,
                                   tag: Optional[str] = None):
        if queue_name is None:
            queue_name = message_class.get_base_queue_name()
        self.unregister_handler(queue_name, tag)

    def register_handler(self, queue_name: str, handler_func: Callable, tag: Optional[str] = None):
        queue_name = merge_queue_name_with_tag(queue_name, tag)
        if queue_name in self.__handlers:
            raise WormholeHandlerAlreadyExists(queue_name)
        self.__handlers[queue_name] = handler_func

    def unregister_handler(self, queue_name: str, tag: Optional[str] = None):
        queue_name = merge_queue_name_with_tag(queue_name, tag)
        if queue_name not in self.__handlers:
            raise WormholeHandlerNotRegistered(queue_name)
        del self.__handlers[queue_name]

    def execute_handler(self, handler_func: Callable, data: Any, on_response: Callable):
        try:
            reply_data = handler_func(data)
            on_response(reply_data, False)
        except Exception as e:
            if PRINT_HANDLER_EXCEPTIONS:
                import traceback
                print("=" * 80)
                print(f"DATA: {repr(data)}")
                print("HANDLING EXCEPTION")
                traceback.print_exc()
                print("=" * 80)
            on_response(str(e), True)

    def send(self, queue_name: str, data: Any, tag: Optional[str] = None):
        queue_name = merge_queue_name_with_tag(queue_name, tag)
        wh_queue_name = WormholeQueueNameFormatter.user_queue_to_wh_queue(queue_name)
        return self.__channel.send(wh_queue_name, data)

    def __get_handler_by_queue_names(self) -> Dict[str, Callable]:
        handlers_by_queue_name: Dict[str, Callable] = {}
        for base_user_queue_name, handler_func in list(self.__handlers.items()):
            handlers_by_queue_name[base_user_queue_name] = handler_func
            if base_user_queue_name != self.id:
                handlers_by_queue_name[f"{base_user_queue_name}:{self.id}"] = handler_func
        return handlers_by_queue_name

    def __pop_and_handle_next(self, timeout: int = 5) -> None:
        handlers = self.__get_handler_by_queue_names()
        channel_queue_names = [WormholeQueueNameFormatter.user_queue_to_wh_queue(q) for q in handlers.keys()]
        result = self.__channel.pop_next(self.id, channel_queue_names, timeout)
        
        did_timeout = result is None
        if did_timeout:
            return
        popped_wh_queue_name, message_id, data = result
        popped_user_queue_name = WormholeQueueNameFormatter.wh_queue_to_user_queu(popped_wh_queue_name)

        handler_func = handlers[popped_user_queue_name]
        self.execute_handler(handler_func, data,
                             lambda d, e: self.__on_handler__response(message_id, d, e))

    def __on_handler__response(self, message_id: str, reply_data: Any, is_error: bool):

        self.__channel.reply(message_id, reply_data, is_error)

    def __internal_handler_private_queue(self, data: bytes):
        command = data
        if command == 'stop':
            self.stop()
            self.send(self.__receiver_id, 'stop')

    def __register_internal_handlers(self):
        self.__handlers[self.__receiver_id] = self.__internal_handler_private_queue
