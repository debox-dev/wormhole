from wormhole.setup import basic_wormhole_setup

wormhole = basic_wormhole_setup()

# Send blocking via .send().wait()
assert wormhole.send("sum", [1, 1, 3]).wait() == sum([1, 1, 3])

