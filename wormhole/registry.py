from typing import *

from .encoding.base import WormholeEncoder
from .encoding.pickleenc import WormholePickleEncoder

if TYPE_CHECKING:
    from .basic import BasicWormhole

__PRIMARY_WORMHOLE: Optional["BasicWormhole"] = None
DEFAULT_MESSAGE_TIMEOUT = 60
DEFAULT_REPLY_TIMEOUT = 60
PRINT_HANDLER_EXCEPTIONS = True
DEFAULT_ENCODER: "WormholeEncoder" = WormholePickleEncoder()


def get_primary_wormhole():
    global __PRIMARY_WORMHOLE
    return __PRIMARY_WORMHOLE


def set_primary_wormhole(wh: "BasicWormhole"):
    global __PRIMARY_WORMHOLE
    __PRIMARY_WORMHOLE = wh
    
def get_default_encoder() -> "WormholeEncoder":
    return DEFAULT_ENCODER
