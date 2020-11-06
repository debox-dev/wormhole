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
