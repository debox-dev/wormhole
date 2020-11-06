from wormhole.utils import get_full_type_path, hash_string, merge_queue_name_with_tag

from typing import *

from .error import BaseWormholeException, InvalidWormholeMessageHandler
from .helpers import wormhole_handler
from .registry import get_primary_wormhole
from .session import WormholeSession

if TYPE_CHECKING:
    from .basic import BasicWormhole


class WormholeMessageException(BaseWormholeException):
    pass


class WormholeMessage:
    @classmethod
    def get_base_queue_name(cls):
        return hash_string(get_full_type_path(cls))

    @classmethod
    def set_wormhole(cls, **kwargs):
        queue_name = kwargs.get('override_queue_name', None) or cls.get_base_queue_name()
        return wormhole_handler(queue_name, **kwargs)

    @classmethod
    def register_handler_of_instance(cls, wormhole: "BasicWormhole", handler: Callable):
        if handler.__name__ == 'Vector3Handler':
            raise KeyError("DAS")
        queue_name, tag = cls.__parse_wormhole_message_handler_data(handler)
        return wormhole.register_handler(queue_name, handler, tag)

    @classmethod
    def unregister_handler_of_instance(cls, wormhole: "BasicWormhole", handler: Callable):
        queue_name, tag = cls.__parse_wormhole_message_handler_data(handler)
        return wormhole.unregister_handler(queue_name)

    @classmethod
    def register_all_handlers_of_instance(cls, wormhole: "BasicWormhole", instance: object):
        return [
            cls.register_handler_of_instance(wormhole, getattr(instance, attr_name))
            for attr_name in dir(instance) if cls.is_message_handler(getattr(instance, attr_name))
        ]

    @classmethod
    def unregister_all_handlers_of_instance(cls, wormhole: "BasicWormhole", instance: object):
        return [
            cls.unregister_handler_of_instance(wormhole, getattr(instance, attr_name))
            for attr_name in dir(instance) if cls.is_message_handler(getattr(instance, attr_name))
        ]

    @staticmethod
    def is_message_handler(handler: Callable):
        return callable(handler) and hasattr(handler, 'wormhole_handler')

    @staticmethod
    def __parse_wormhole_message_handler_data(handler: Callable):
        if not callable(handler):
            raise InvalidWormholeMessageHandler(f"Not a callable {handler.__name__}")
        if not hasattr(handler, 'wormhole_handler'):
            raise InvalidWormholeMessageHandler(f"Not a wormhole message handler {handler.__name__}")
        wormhole_queue_name = getattr(handler, 'wormhole_queue_name')
        wormhole_queue_tag = getattr(handler, 'wormhole_queue_tag')
        return wormhole_queue_name, wormhole_queue_tag

    def pre_send(self):
        pass

    def post_send(self):
        pass

    def send(self, tag: Optional[str] = None, wormhole: Union[None, "BasicWormhole", "WormholeSession"] = None,
             override_queue_name: Optional[str] = None, group: Optional[str] = None):
        session: Optional["WormholeSession"] = None
        if isinstance(wormhole, WormholeSession):
            session = wormhole
            wormhole = wormhole.wormhole
        wormhole = self.__get_wormhole(wormhole)
        queue_name = override_queue_name or self.get_base_queue_name()
        wormhole_async = wormhole.send(queue_name, self, tag, session=session, group=group)
        return wormhole_async

    @classmethod
    def register_simple_handler(cls, handler_func: Callable, wormhole: Optional["BasicWormhole"] = None,
                                tag: Optional[str] = None):
        wormhole = cls.__get_wormhole(wormhole)
        wormhole.register_handler(cls.get_base_queue_name(), handler_func, tag=tag)

    @staticmethod
    def __get_wormhole(wormhole: Optional["BasicWormhole"] = None) -> "BasicWormhole":
        if wormhole is not None:
            return wormhole
        wormhole = get_primary_wormhole()
        if wormhole is not None:
            return wormhole
        raise WormholeMessageException(
            "No wormhole specified and no primary wormhole defined - did you run 'setup_wormhole'?")
