import time
from enum import Enum, auto
from typing import *

from .error import WormholeInvalidQueueName, WormholeHandlerAlreadyExists, WormholeHandlerNotRegistered
from .registry import PRINT_HANDLER_EXCEPTIONS
from .session import WormholeSession
from .utils import generate_uid, merge_queue_name_with_tag
from .waitable import WormholeWaitable
from .message import WormholeMessage

if TYPE_CHECKING:
    from .channel import AbstractWormholeChannel


class WormholeState(Enum):
    INACTIVE = auto()
    ACTIVE = auto()
    DEACTIVATING = auto()


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


class WormholeWaitResult:
    def __init__(self, item: Any, data: Any, reply: Optional[Callable], tag: Optional[str] = None):
        self.tag = tag
        self.timeout = item is None
        self.reply = reply
        self.item = item
        self.data = data


class BasicWormhole:
    POP_TIMEOUT = 1

    def __init__(self, channel: Optional["AbstractWormholeChannel"] = None):
        if channel is None:
            from .channel import create_default_channel
            channel = create_default_channel()
        self.__handlers: Dict[str, Callable] = dict()
        self.__channel = channel
        self.__state: WormholeState = WormholeState.INACTIVE
        self.__receiver_id = generate_uid()
        self.__register_internal_handlers()

    @property
    def channel(self):
        return self.__channel

    @property
    def id(self) -> str:
        return self.__receiver_id

    @property
    def is_running(self):
        return self.__state != WormholeState.INACTIVE

    def process_blocking(self):
        self.__state = WormholeState.ACTIVE
        while self.__state == WormholeState.ACTIVE:
            self.__pop_and_handle_next(1)
        self.unregister_all_handlers()
        self.__state = WormholeState.INACTIVE

    def process_async(self):
        raise NotImplementedError("Please use an async implementation like GeventWormhole")

    def sleep(self, duration):
        time.sleep(duration)

    def wait_for_any(self, *args: Union[str, Type[WormholeMessage], WormholeWaitable],
                     timeout: int = 0) -> WormholeWaitResult:
        channel_queue_names: List[str] = []
        waitable_by_queue: Dict[str, WormholeWaitable] = dict()
        for item in args:
            waitable = WormholeWaitable.from_item(item)
            queue_name = merge_queue_name_with_tag(waitable.queue_name, waitable.tag)
            channel_queue_names.append(queue_name)
            waitable_by_queue[queue_name] = waitable
        channel_queue_names = [WormholeQueueNameFormatter.user_queue_to_wh_queue(q) for q in channel_queue_names]
        result = self.__channel.pop_next(self.id, channel_queue_names, timeout)
        did_timeout = result is None
        if did_timeout:
            return WormholeWaitResult(None, None, None)
        popped_wh_queue_name, message_id, data = result
        popped_user_queue_name = WormholeQueueNameFormatter.wh_queue_to_user_queu(popped_wh_queue_name)

        def reply_func(data, is_error=False):
            nonlocal self
            self.channel.reply(message_id, data, is_error)

        item = waitable_by_queue[popped_user_queue_name].item
        tag = waitable_by_queue[popped_user_queue_name].tag
        wait_result = WormholeWaitResult(item, data, reply_func, tag)

        return wait_result

    def stop(self, wait=True):
        self.send(self.__receiver_id, 'stop')
        if wait:
            while self.__state != WormholeState.INACTIVE:
                self.sleep(0.1)

    def unregister_all_handlers(self):
        for queue_name in list(self.__handlers.keys()):
            if queue_name == self.__receiver_id:
                continue
            self.send(self.id, 'refresh')
            del self.__handlers[queue_name]

    def register_all_handlers_of_instance(self, instance: object):
        for attr_name in dir(instance):
            attr = getattr(instance, attr_name)
            if not callable(attr):
                continue
            if not hasattr(attr, 'wormhole_handler'):
                continue
            wormhole_queue_name = getattr(attr, 'wormhole_queue_name')
            wormhole_queue_tag = getattr(attr, 'wormhole_queue_tag')
            self.register_handler(wormhole_queue_name, attr, wormhole_queue_tag)

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
        message_id = self.__channel.send(wh_queue_name, data)
        return WormholeSession(message_id, self)

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
            self.__state = WormholeState.DEACTIVATING

    def __register_internal_handlers(self):
        self.__handlers[self.__receiver_id] = self.__internal_handler_private_queue
