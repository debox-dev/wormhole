from typing import *

from .message import WormholeMessage
from .registry import get_primary_wormhole
from .utils import overridable

if TYPE_CHECKING:
    from .basic import BasicWormhole
    from .session import WormholeSession


class WormholeHandlerInstanceMixin:
    @overridable
    def _get_wormhole(self) -> Optional["BasicWormhole"]:
        return get_primary_wormhole()

    def activate_all_handlers(self) -> List["WormholeSession"]:
        return WormholeMessage.register_all_handlers_of_instance(self._get_wormhole(), self)

    def deactivate_all_handlers(self) -> List["WormholeSession"]:
        return WormholeMessage.unregister_all_handlers_of_instance(self._get_wormhole(), self)
