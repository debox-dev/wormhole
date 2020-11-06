import re
import time
import struct
from enum import Enum, auto
from typing import *

from .command import WormholeCommand, WormholePingCommand
from .error import WormholeInvalidQueueName, WormholeHandlerAlreadyExists, WormholeHandlerNotRegistered
from .registry import PRINT_HANDLER_EXCEPTIONS
from .session import WormholeSession
from .utils import generate_uid
from .waitable import WormholeWaitable
from .message import WormholeMessage
from .error import WormholeHandlingError

if TYPE_CHECKING:
    from .channel import AbstractWormholeChannel


class WormholeState(Enum):
    INACTIVE = auto()
    ACTIVE = auto()
    DEACTIVATING = auto()


class WormholeQueue:
    PREFIX = "wh://"
    URI_RE = re.compile(f"^{PREFIX}([^:/]+)(:([^/]+))?(/([^/]+))?$")

    def __init__(self, base_queue_name: str, tag: Optional[str] = None, group: Optional[str] = None):
        self.group = group
        self.tag = tag
        self.base_queue_name = base_queue_name

    def copy(self):
        return WormholeQueue(self.base_queue_name, self.tag, self.group)

    @classmethod
    def format(cls, queue_name: str, tag: Optional[str] = None, group: Optional[str] = None):
        result = f"{cls.PREFIX}{queue_name}"
        if group is not None:
            result += f":{group}"
        if tag is not None:
            result += f"/{tag}"
        return result

    def __str__(self):
        return self.format(self.base_queue_name, self.tag, self.group)

    @classmethod
    def chop_group(cls, uri: str):
        queue_name, tag_name, group_name = cls.parse_uri(uri)
        return cls.format(queue_name, tag_name)

    @classmethod
    def parse_uri(cls, uri: str) -> Tuple[str, str, str]:
        re_match = cls.URI_RE.match(uri)
        if re_match is None:
            raise ValueError(f"Invalid queue URI: '{uri}'")
        queue_name, _, group_name, _, tag_name = re_match.groups()
        return queue_name, tag_name, group_name

    @classmethod
    def from_string(cls, uri: str):
        queue_name, tag_name, group_name = cls.parse_uri(uri)
        return cls(queue_name, tag_name, group_name)

    def __hash__(self):
        return hash(str(self))


class WormholeWaitResult:
    def __init__(self, item: Any, data: Any, reply: Optional[Callable], tag: Optional[str] = None):
        self.tag = tag
        self.timeout = item is None
        self.reply = reply
        self.item = item
        self.data = data


class BasicWormhole:
    POP_TIMEOUT = 1

    BUILT_IN_COMMANDS = [WormholePingCommand]

    def __init__(self, channel: Optional["AbstractWormholeChannel"] = None):
        if channel is None:
            from .channel import create_default_channel
            channel = create_default_channel()
        self.__handlers: Dict[str, Callable] = dict()
        self.__channel = channel
        self.__state: WormholeState = WormholeState.INACTIVE
        self.__receiver_id = generate_uid()
        self.__groups: Set[str] = set()
        self.__previous_groups: Set[str] = set()
        self.__processing_start_time: Optional[float] = None
        self.__commands: Dict[int, Type[WormholeCommand]] = {}
        for command in self.BUILT_IN_COMMANDS:
            self.learn_command(command)

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
        if self.__state == WormholeState.ACTIVE:
            raise RuntimeError("Already processing")
        self.__state = WormholeState.ACTIVE
        self.__processing_start_time = time.time()
        while self.__state == WormholeState.ACTIVE:
            self.__pop_and_handle_next(1)
        self.__processing_start_time = None
        self.__groups.clear()
        self.__handlers.clear()
        self.__state = WormholeState.INACTIVE

    def add_to_group(self, group_name: str):
        self.__groups.add(group_name)
        return self.__send_refresh()

    def remove_from_group(self, group_name: str):
        self.__groups.remove(group_name)
        return self.__send_refresh()

    def find_group_members(self, group_name: str):
        return self.__channel.find_group_members(group_name)

    def process_async(self, max_parallel: int = 0):
        raise NotImplementedError("Please use an async implementation like GeventWormhole")

    def sleep(self, duration):
        time.sleep(duration)

    def wait_for_any(self, *args: Union[str, Type[WormholeMessage], WormholeWaitable],
                     timeout: int = 0) -> WormholeWaitResult:
        channel_queue_names: List[str] = []
        waitable_by_queue: Dict[str, WormholeWaitable] = dict()
        for item in args:
            waitable = WormholeWaitable.from_item(item)
            queue_name = WormholeQueue.format(waitable.queue_name, waitable.tag)
            channel_queue_names.append(queue_name)
            waitable_by_queue[queue_name] = waitable
        result = self.__channel.pop_next(self.id, channel_queue_names, timeout)
        did_timeout = result is None
        if did_timeout:
            return WormholeWaitResult(None, None, None)
        queue_name, message_id, data = result

        def reply_func(data, is_error=False):
            nonlocal self
            self.channel.reply(message_id, data, is_error)

        item = waitable_by_queue[queue_name].item
        tag = waitable_by_queue[queue_name].tag
        wait_result = WormholeWaitResult(item, data, reply_func, tag)

        return wait_result

    def ping(self, receiver_id: str):
        return WormholePingCommand().send(receiver_id, self).wait()

    def uptime(self, receiver_id: str):
        uptime_data = self.send(receiver_id, b"u").wait(timeout=3)
        if uptime_data is None:
            raise RuntimeError("Did not receive own time")
        uptime, = struct.unpack("d", uptime_data)
        return uptime

    def stop(self, wait=True):
        if self.__state == WormholeState.ACTIVE:
            self.send(self.__receiver_id, b"s").wait(raise_on_error=False)
        if wait:
            while self.__state != WormholeState.INACTIVE:
                self.sleep(0.1)

    def __send_refresh(self):
        return self.send(self.id, b"r")

    def unregister_all_handlers(self):
        self.__handlers.clear()
        if self.__channel.is_open():
            self.__send_refresh()

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

    def learn_command(self, command: Type[WormholeCommand]):
        self.__commands[command.HEADER[0]] = command

    def unlearn_command(self, command: Type[WormholeCommand]):
        del self.__commands[command.HEADER[0]]

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
        queue_name = WormholeQueue.format(queue_name, tag)
        if queue_name in self.__handlers:
            raise WormholeHandlerAlreadyExists(queue_name)
        self.__handlers[queue_name] = handler_func

    def unregister_handler(self, queue_name: str, tag: Optional[str] = None):
        queue_name = WormholeQueue.format(queue_name, tag)
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

    def send(self, queue_name: str, data: Any, tag: Union[None, str, WormholeSession] = None,
             session: Optional[WormholeSession] = None, group: Optional[str] = None):
        if isinstance(tag, WormholeSession):
            session = tag
            tag = None
        if session is not None:
            if tag is not None or group is not None:
                raise WormholeHandlingError("Cannot specify both tag/group and session when sending")
            group = session.receiver_id

        queue_name = WormholeQueue.format(queue_name, tag, group)
        message_id = self.__channel.send(queue_name, data)
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
        channel_queue_names: List[str] = []
        for queue_name in handlers.keys():
            wh_queue = WormholeQueue.from_string(queue_name)
            channel_queue_names.append(queue_name)
            for group_name in self.__groups | {self.id}:
                wh_queue.group = group_name
                channel_queue_names.append(str(wh_queue))
        internal_channel = WormholeQueue.format(self.id)
        channel_queue_names.append(internal_channel)
        remove_from_groups = list(self.__previous_groups - self.__groups)
        if len(remove_from_groups) > 0:
            self.__channel.remove_from_groups(remove_from_groups, self.id)
        self.__previous_groups = set(self.__groups)
        self.__channel.touch_for_groups(list(self.__groups), self.id, timeout + 5)
        result = self.__channel.pop_next(self.id, channel_queue_names, timeout)

        did_timeout = result is None
        if did_timeout:
            return
        popped_queue_name, message_id, data = result
        wh_queue = WormholeQueue.from_string(popped_queue_name)
        wh_queue.group = None
        if wh_queue.base_queue_name == self.__receiver_id:
            handler_func = self.__internal_handler_private_queue
        else:
            handler_func = handlers[str(wh_queue)]
        self.execute_handler(handler_func, data,
                             lambda d, e: self.__on_handler__response(message_id, d, e))

    def __on_handler__response(self, message_id: str, reply_data: Any, is_error: bool):

        self.__channel.reply(message_id, reply_data, is_error)

    def __internal_handler_private_queue(self, data: bytes):
        command = data
        command_id = command[0]
        if command[0] == b"s"[0]:  # stop
            self.__state = WormholeState.DEACTIVATING
        elif command[0] == b"r"[0]:  # refresh
            pass
        elif command[0] == b"u"[0]:  # uptime
            return struct.pack("d", time.time() - self.__processing_start_time)
        elif command_id in self.__commands:
            return self.__commands[command_id].handle(command[1:])
        else:
            raise WormholeHandlingError(f"No such command: {repr(data)}")
