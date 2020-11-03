import math

import gevent

from wormhole.message import WormholeMessage


class Vector3:
    def __init__(self, x, y, z):
        self.x, self.y, self.z = x, y, z

    @property
    def magnitude(self):
        return math.sqrt(self.x * self.x + self.y * self.y + self.z * self.z)

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y and self.z == other.z

    def __str__(self):
        return f"({self.x}, {self.y}, {self.z}"


class Vector3Message(Vector3, WormholeMessage):
    delay = 0
    pass


class Vector3Handler:
    @Vector3Message.register_instance_handler()
    def on_vector3(self, message: Vector3Message):
        if message.delay > 0:
            gevent.sleep(message.delay)
        return message.magnitude
