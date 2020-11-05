import gevent

from gevent.event import Event

from ..basic import BasicWormhole

from typing import *


class GeventWormhole(BasicWormhole):
    PARALLEL: bool = False
    max_parallel: Optional[int]
    __greenlet = None
    __current_handling_count = 0
    __handling_complete_event: Event

    def process_async(self, max_parallel: int = 5):
        if max_parallel == 0:
            self.PARALLEL = False
        else:
            self.PARALLEL = True
            self.max_parallel = max_parallel
        self.__handling_complete_event = Event()
        self.__greenlet = gevent.spawn(self.process_blocking)
        while not self.is_running:  # TODO: Listen to signal
            gevent.sleep(0.1)

    def execute_handler(self, handler_func: Callable, data: Any, on_response: Callable):
        if not self.PARALLEL:
            super().execute_handler(handler_func, data, on_response)
            return

        while self.__current_handling_count >= self.max_parallel and self.PARALLEL:
            self.__handling_complete_event.wait()
            self.__handling_complete_event.clear()
        if not self.PARALLEL:
            return
        self.__current_handling_count += 1
        gevent.spawn(self.async_handler, handler_func, data, on_response)

    def async_handler(self, handler_func, data, on_response):
        BasicWormhole.execute_handler(self, handler_func, data, on_response)
        self.__current_handling_count -= 1
        self.__handling_complete_event.set()

    def sleep(self, duration):
        gevent.sleep(duration)

    def stop(self, wait=True):
        self.PARALLEL = False
        if not self.is_running:
            raise RuntimeError("Not running")
        if wait:
            while self.__current_handling_count > 0:
                self.__handling_complete_event.set()
                self.sleep(0.1)
        super().stop(wait=wait)
        if wait:
            gevent.wait([self.__greenlet])
