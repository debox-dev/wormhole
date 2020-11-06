from wormhole.setup import basic_wormhole_setup
from wormhole.message import WormholeMessage
from typing import *


class SumMessage(WormholeMessage):
    def __init__(self, numbers: List[int]):
        self.numbers = numbers


wormhole = basic_wormhole_setup()
assert SumMessage([1, 2, 3]).send().wait() == 6
assert wormhole.send("multiply", 5).wait() == 10
