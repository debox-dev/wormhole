import gevent

from ..basic import BasicWormhole


class GeventWormhole(BasicWormhole):
    def process_async(self):
        return gevent.spawn(self.process_blocking)
