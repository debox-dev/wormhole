from wormhole.setup import basic_wormhole_setup

wormhole = basic_wormhole_setup()


def remote_sum(items: list):
    return sum(items)


wormhole.register_handler("sum", remote_sum)
wormhole.process_blocking()
