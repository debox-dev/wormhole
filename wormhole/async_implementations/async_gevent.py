import gevent

from ..basic import BasicWormhole

from typing import *


class GeventWormhole(BasicWormhole):
    PARALLEL = True

    def process_async(self):
        return gevent.spawn(self.process_blocking)

    def execute_handler(self, handler_func: Callable, data: Any, on_response: Callable):
        if not self.PARALLEL:
            super().execute_handler(handler_func, data, on_response)
            return

        def async_handler():
            nonlocal handler_func, data, on_response, self
            BasicWormhole.execute_handler(self, handler_func, data, on_response)

        gevent.spawn(async_handler)
