from wormhole.utils import get_full_type_path, hash_string, merge_queue_name_with_tag

from typing import *

from .error import BaseWormholeException
from .helpers import wormhole_handler
from .registry import get_primary_wormhole

if TYPE_CHECKING:
    from .receiver import BasicWormhole


class WormholeMessageException(BaseWormholeException):
    pass


class WormholeMessage:
    @classmethod
    def get_base_queue_name(cls):
        return hash_string(get_full_type_path(cls))

    @classmethod
    def register_instance_handler(cls, **kwargs):
        queue_name = kwargs.get('override_queue_name', None) or cls.get_base_queue_name()
        return wormhole_handler(queue_name, **kwargs)

    def pre_send(self):
        pass

    def post_send(self):
        pass

    def send(self, tag: Optional[str] = None, wormhole: Optional["BasicWormhole"] = None,
             override_queue_name: Optional[str] = None):
        wormhole = self.__get_wormhole(wormhole)
        queue_name = override_queue_name or self.get_base_queue_name()
        wormhole_async = wormhole.send(queue_name, self, tag)
        return wormhole_async

    @classmethod
    def register_simple_handler(cls, handler_func: Callable, wormhole: Optional["BasicWormhole"] = None,
                         tag: Optional[str] = None):
        wormhole = cls.__get_wormhole(wormhole)
        wormhole.register_message_handler(cls, handler_func, queue_name=None, tag=tag)

    @staticmethod
    def __get_wormhole(wormhole: Optional["BasicWormhole"] = None) -> "BasicWormhole":
        if wormhole is not None:
            return wormhole
        wormhole = get_primary_wormhole()
        if wormhole is not None:
            return wormhole
        raise WormholeMessageException(
            "No wormhole specified and no primary wormhole defined - did you run 'setup_wormhole'?")
