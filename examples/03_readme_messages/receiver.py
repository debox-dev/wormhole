from wormhole.setup import basic_wormhole_setup
from wormhole.message import WormholeMessage
from typing import *


class SumMessage(WormholeMessage):
    def __init__(self, numbers: List[int]):
        self.numbers = numbers


class MessageHandler:
    @SumMessage.register_instance_handler()
    def on_sum_message(self, message: SumMessage):
        return sum(message.numbers)


handler = MessageHandler()
wormhole = basic_wormhole_setup()
# Register the instance of Messagehandler
wormhole.register_all_handlers_of_instance(handler)
wormhole.process_blocking()
