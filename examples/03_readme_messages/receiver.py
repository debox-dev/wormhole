from wormhole.helpers import wormhole_handler
from wormhole.mixin import WormholeHandlerInstanceMixin
from wormhole.setup import basic_wormhole_setup
from wormhole.message import WormholeMessage
from typing import *


class SumMessage(WormholeMessage):
    def __init__(self, numbers: List[int]):
        self.numbers = numbers


class MessageHandler(WormholeHandlerInstanceMixin):
    @wormhole_handler("multiply")
    def on_multiply(self, data: int):
        return data * 2

    @SumMessage.set_wormhole()
    def on_sum_message(self, message: SumMessage):
        return sum(message.numbers)


handler = MessageHandler()
wormhole = basic_wormhole_setup()
# Activate the handlers
handler.activate_all_handlers()
# Start serving
wormhole.process_blocking()
handler.deactivate_all_handlers()
