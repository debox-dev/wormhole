from wormhole.setup import basic_wormhole_setup
from wormhole.message import WormholeMessage
from typing import *


class SumMessage(WormholeMessage):
    def __init__(self, numbers: List[int]):
        self.numbers = numbers


class MessageHandler:
    @SumMessage.set_wormhole()
    def on_sum_message(self, message: SumMessage):
        return sum(message.numbers)


handler = MessageHandler()
wormhole = basic_wormhole_setup()
# Register the instance of Messagehandler
WormholeMessage.register_all_handlers_of_instance(wormhole, handler)
wormhole.process_blocking()
