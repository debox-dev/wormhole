import gevent

from ..basic import BasicWormhole

from typing import *


class GeventWormhole(BasicWormhole):
    PARALLEL = True
    __greenlet = None
    __current_handling_count = 0

    def process_async(self):
        self.__greenlet = gevent.spawn(self.process_blocking)
        while not self.is_running:  # TODO: Listen to signal
            gevent.sleep(0.1)

    def execute_handler(self, handler_func: Callable, data: Any, on_response: Callable):
        if not self.PARALLEL:
            super().execute_handler(handler_func, data, on_response)
            return

        def async_handler():
            nonlocal handler_func, data, on_response, self
            BasicWormhole.execute_handler(self, handler_func, data, on_response)
            self.__current_handling_count -= 1

        self.__current_handling_count += 1
        gevent.spawn(async_handler)

    def sleep(self, duration):
        gevent.sleep(duration)

    def stop(self, wait=True):
        if not self.is_running:
            raise RuntimeError("Not running")
        if wait:
            while self.__current_handling_count > 0:
                self.sleep(0.1)
        super().stop(wait=wait)
        if wait:
            gevent.wait([self.__greenlet])
