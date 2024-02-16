import math

import gevent

from wormhole.message import WormholeMessage
from wormhole.mixin import WormholeHandlerInstanceMixin

from typing import *

if TYPE_CHECKING:
    from wormhole.basic import BasicWormhole


class Vector3:
    def __init__(self, x, y, z):
        self.x, self.y, self.z = x, y, z

    @property
    def magnitude(self):
        return math.sqrt(self.x * self.x + self.y * self.y + self.z * self.z)

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y and self.z == other.z

    def __str__(self):
        return f"({self.x}, {self.y}, {self.z})"


class Vector3Message(Vector3, WormholeMessage):
    delay = 0
    pass

    def __repr__(self):
        return f"Vector3Message{str(self)}"


class Vector3Handler:
    @Vector3Message.set_wormhole()
    def on_vector3(self, message: Vector3Message):
        if message.delay > 0:
            gevent.sleep(message.delay)
        return message.magnitude


class Vector3MixinHandler(Vector3Handler, WormholeHandlerInstanceMixin):
    def __init__(self, wormhole: "BasicWormhole"):
        self.wormhole = wormhole

    def _get_wormhole(self) -> Optional["BasicWormhole"]:
        return self.wormhole


class TextMessage(WormholeMessage):
    def __init__(self, text: str):
        self.text = text


class TextMessageHandler:
    @TextMessage.set_wormhole()
    def on_text_msg(self, m: TextMessage):
        return m.text[::-1]
