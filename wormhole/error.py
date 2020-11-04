class BaseWormholeException(Exception):
    pass


class BaseWormholeQueueException(BaseWormholeException):
    def __init__(self, queue_name: str):
        self.queue_name = queue_name


class WormholeHandlerAlreadyExists(BaseWormholeQueueException):
    def __str__(self):
        return f"A handler is already registered for queue {self.queue_name}"


class WormholeHandlerNotRegistered(BaseWormholeQueueException):
    def __str__(self):
        return f"A handler is not registered for queue {self.queue_name}"


class WormholeInvalidQueueName(BaseWormholeQueueException):
    def __str__(self):
        return f"Not a valid wormhole queue name: {self.queue_name}"


class WormholeDecodeError(BaseWormholeException):
    pass


class WormholeHandlingError(BaseWormholeException):
    pass


class WormholeChannelError(BaseWormholeException):
    pass
