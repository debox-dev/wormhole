from typing import *

from .handler import WormholeHandler
from .registry import get_primary_wormhole
from .utils import overridable

if TYPE_CHECKING:
    from .basic import BasicWormhole
    from .session import WormholeSession


class WormholeHandlerInstanceMixin:
    wormhole: Optional["BasicWormhole"] = None

    @overridable
    def _create_wormhole(self):
        self.wormhole = get_primary_wormhole()

    @overridable
    def _get_wormhole(self) -> Optional["BasicWormhole"]:
        if self.wormhole is None:
            self.wormhole = self._create_wormhole()
        return self.wormhole

    def activate_all_handlers(self) -> List["WormholeSession"]:
        return WormholeHandler.register_all_handlers_of_instance(self._get_wormhole(), self)

    def deactivate_all_handlers(self) -> List["WormholeSession"]:
        return WormholeHandler.unregister_all_handlers_of_instance(self._get_wormhole(), self)
