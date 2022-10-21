import re
import time
import struct
from enum import Enum, auto
from typing import *

from .command import WormholeCommand, WormholePingCommand
from .error import WormholeHandlerAlreadyExists, WormholeHandlerNotRegistered, WormholeSendError, \
    WormholeUnknownHandlerCommandError, WormholeChannelClosedError, WormholeChannelConnectionError
from .registry import PRINT_HANDLER_EXCEPTIONS
from .session import WormholeSession
from .utils import generate_uid
from .waitable import WormholeWaitable
from .message import WormholeMessage

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
            try:
                self.__pop_and_handle_next(1)
            except WormholeChannelClosedError:
                break
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
        try:
            result = self.__channel.pop_next(self.id, channel_queue_names, timeout)
        except KeyError as e:
            if PRINT_HANDLER_EXCEPTIONS:
                import traceback
                print("=" * 80)
                print("POP EXCEPTION")
                print(f"ERROR: {e}")
                traceback.print_exc()
                print("=" * 80)
            result = None

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

    def _refresh(self):
        self.__send_refresh()

    def __send_refresh(self):
        return self.send(self.id, b"r")

    def unregister_all_handlers(self):
        self.__handlers.clear()
        if self.__channel.is_open():
            self.__send_refresh()

    def learn_command(self, command: Type[WormholeCommand]):
        self.__commands[command.HEADER[0]] = command

    def unlearn_command(self, command: Type[WormholeCommand]):
        del self.__commands[command.HEADER[0]]

    def register_handler(self, queue_name: str, handler_func: Callable, tag: Optional[str] = None):
        queue_name = WormholeQueue.format(queue_name, tag)
        if queue_name in self.__handlers:
            raise WormholeHandlerAlreadyExists(queue_name)
        self.__handlers[queue_name] = handler_func
        return self.__send_refresh()

    def unregister_handler(self, queue_name: str, tag: Optional[str] = None):
        queue_name = WormholeQueue.format(queue_name, tag)
        if queue_name not in self.__handlers:
            raise WormholeHandlerNotRegistered(queue_name)
        del self.__handlers[queue_name]
        return self.__send_refresh()

    def execute_handler(self, handler_func: Callable, data: Any, on_response: Callable):
        try:
            try:
                reply_data = handler_func(data)
            except Exception as e:
                if PRINT_HANDLER_EXCEPTIONS:
                    import traceback
                    print("=" * 80)
                    print("HANDLING EXCEPTION")
                    print(f"DATA: {repr(data)}")
                    traceback.print_exc()
                    print("=" * 80)
                on_response(e, True)
                return
            on_response(reply_data, False)
        except WormholeChannelClosedError:
            pass  # Nothing we can do if the channel closed
        except WormholeChannelConnectionError as e:
            if PRINT_HANDLER_EXCEPTIONS:
                import traceback
                print("=" * 80)
                print(f"CONNECTION ERROR: {e}")
                traceback.print_exc()
                print("=" * 80)

    def send(self, queue_name: str, data: Any, tag: Union[None, str, WormholeSession] = None,
             session: Optional[WormholeSession] = None, group: Optional[str] = None):
        if isinstance(tag, WormholeSession):
            session = tag
            tag = None
        if session is not None:
            if tag is not None or group is not None:
                raise WormholeSendError("Cannot specify both tag/group and session when sending")
            target_group = session.receiver_id
        else:
            target_group = group
        queue_name = WormholeQueue.format(queue_name, tag, target_group)
        message_id = self.__channel.send(self.id, queue_name, data)
        return WormholeSession(message_id, self, lambda: self.send(queue_name, data, tag, session, group))

    def __get_handler_by_queue_names(self) -> Dict[str, Callable]:
        handlers_by_queue_name: Dict[str, Callable] = {}
        for base_user_queue_name, handler_func in list(self.__handlers.items()):
            handlers_by_queue_name[base_user_queue_name] = handler_func
            if base_user_queue_name != self.id:
                handlers_by_queue_name[f"{base_user_queue_name}:{self.id}"] = handler_func
        return handlers_by_queue_name

    def _is_handling_enabled(self):
        return True

    def __pop_and_handle_next(self, timeout: int = 5) -> None:
        handlers = self.__get_handler_by_queue_names()
        internal_channel = WormholeQueue.format(self.id)
        channel_queue_names: List[str] = [internal_channel]
        if self._is_handling_enabled():
            for queue_name in handlers.keys():
                wh_queue = WormholeQueue.from_string(queue_name)
                channel_queue_names.append(queue_name)
                for group_name in self.__groups | {self.id}:
                    wh_queue.group = group_name
                    channel_queue_names.append(str(wh_queue))
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
                             lambda d, e: self.__on_handler_response(message_id, d, e))

    def __on_handler_response(self, message_id: str, reply_data: Any, is_error: bool):
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
            raise WormholeUnknownHandlerCommandError(f"No such command: {repr(data)}")
