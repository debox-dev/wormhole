from gevent.monkey import patch_all

from test_msg import TestReply, TestMessage
from wormhole.helpers import register_handler_instance
from wormhole.message import WormholeMessage
from wormhole.setup import basic_wormhole_setup, WormholeAsyncType

patch_all()

import gevent

wh = basic_wormhole_setup(async_type=WormholeAsyncType.GEVENT)


def print_any(v):
    print(repr(v))
    return TestReply(v.a, 101)


class MyHandlerInstace:

    @TestMessage.register_instance_handler()
    def handle_this(self, m: TestMessage):
        print("YAY")
        return m


def serv():
    h = MyHandlerInstace()
    #wh.register_handler("q", lambda x: x + 1)
    #wh.register_handler("q2", print_any)
    #TestMessage.register_simple_handler(print_any)
    register_handler_instance(h)
    wh.process_async()


#serv()
#gevent.wait()
# mm = TestMessage()
# print(repr(mm.send().wait()))

import ipdb;

ipdb.set_trace()
