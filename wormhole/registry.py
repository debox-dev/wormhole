from typing import *

if TYPE_CHECKING:
    from .basic import BasicWormhole

__PRIMARY_WORMHOLE: Optional["BasicWormhole"] = None
DEFAULT_MESSAGE_TIMEOUT = 10
DEFAULT_REPLY_TIMEOUT = 10
PRINT_HANDLER_EXCEPTIONS = True


def get_primary_wormhole():
    global __PRIMARY_WORMHOLE
    return __PRIMARY_WORMHOLE


def set_primary_wormhole(wh: "BasicWormhole"):
    global __PRIMARY_WORMHOLE
    __PRIMARY_WORMHOLE = wh
    
