from wormhole.message import WormholeMessage


class TestMessage(WormholeMessage):
    def __init__(self, a):
        self.a = a

    def __repr__(self):
        return f"TestMessage({self.a})"


class TestReply:
    def __init__(self, r1, r2):
        self.r1 = r1
        self.r2 = r2
