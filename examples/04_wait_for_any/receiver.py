from wormhole.setup import basic_wormhole_setup

wormhole = basic_wormhole_setup()
# No need to call process on wormhole, we will wait ourselves
num = 0

while True:
    wait_result: "WormholeWaitResult" = wormhole.wait_for_any("my_queue1", timeout=3)
    if wait_result.timeout:
        print("Still waiting for messages...")
        continue
    print("I got a message!")
    assert wait_result.item == "my_queue1"
    assert wait_result.data == "DATA1"
    num += 1
    wait_result.reply("Reply num: " + str(num))
