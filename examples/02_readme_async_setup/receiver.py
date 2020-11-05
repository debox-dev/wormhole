import gevent

from gevent.monkey import patch_all

patch_all()
from wormhole.setup import basic_wormhole_setup

wormhole = basic_wormhole_setup(async_type="gevent")


def remote_sum(items: list):
    delay = 1
    print(f"Delaying for {delay} seconds")
    gevent.sleep(delay)
    print("Done!")
    return sum(items)


wormhole.register_handler("sum", remote_sum)
wormhole.process_async(max_parallel=20)
gevent.wait()
