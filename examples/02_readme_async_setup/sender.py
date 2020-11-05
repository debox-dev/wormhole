from wormhole.setup import basic_wormhole_setup

wormhole = basic_wormhole_setup()

# Send multiple non-blocking and wait for results later
sessions: "WormholeSession" = []
for i in range(40):
    session = wormhole.send("sum", [i, i * 2, i * i])
    sessions.append(session)
for s in sessions:
    print(f"The sum was: {s.wait()}")
