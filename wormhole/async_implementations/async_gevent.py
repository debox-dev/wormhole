import gevent

from gevent.event import Event

from ..basic import BasicWormhole

from typing import *


class GeventWormhole(BasicWormhole):
    PARALLEL: bool = False
    max_parallel: Optional[int]
    __debug_seq = 0
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

    def _is_handling_enabled(self):
        return not self.PARALLEL or self.__current_handling_count < self.max_parallel

    def __can_handle_async(self):
        return self._is_handling_enabled() and self.PARALLEL

    def execute_handler(self, handler_func: Callable, data: Any, on_response: Callable):
        self.__debug_seq += 1
        dseq = self.__debug_seq
        if not self.PARALLEL:
            super().execute_handler(handler_func, data, on_response)
            return
        # Technically we will never get to this condition... never ever ever WIIIUUU WIUUUU PCHHHHHHHHH
        # We dont need to handle this because _is_handling_enabled disables the queue listening on all but the internal queue
        #while not self.__can_handle_async():
        #    print("WAITING")
        #    self.__handling_complete_event.wait()
        #    self.__handling_complete_event.clear()

        gevent.spawn(self.async_handler, handler_func, data, on_response)

    def async_handler(self, handler_func, data, on_response):
        try:
            self.__current_handling_count += 1
            BasicWormhole.execute_handler(self, handler_func, data, on_response)
            needs_refresh = self.max_parallel < self.__current_handling_count
        except Exception as e:
            self.__current_handling_count -= 1
            self.__handling_complete_event.set()
            raise e

        self.__current_handling_count -= 1
        self.__handling_complete_event.set()
        if needs_refresh:
            self._refresh()

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
