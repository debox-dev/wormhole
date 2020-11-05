from wormhole.setup import basic_wormhole_setup

wormhole = basic_wormhole_setup()
reply_data = wormhole.send("my_queue1", "DATA1").wait()
print(reply_data)
