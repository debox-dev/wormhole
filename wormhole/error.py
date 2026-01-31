class BaseWormholeException(Exception):
    pass


class WormholeChannelError(BaseWormholeException):
    pass


class WormholeChannelConnectionError(WormholeChannelError):
    """Thrown when an error occured during send/receive of the channel"""
    pass


class WormholeChannelClosedError(WormholeChannelError):
    """Thrown when trying to use channel functionality that require an open channel on a closed channel"""
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


class WormholeChannelPopError(BaseWormholeException):
    def __init__(self, result_queue_name: str, result_message_id: str, message: str, inner_exception: Exception):
        super().__init__(message)
        self.result_queue_name = result_queue_name
        self.result_message_id = result_message_id
        self.inner_exception = inner_exception


class WormholeSendError(BaseWormholeException):
    pass


class WormholeUnknownHandlerCommandError(BaseWormholeException):
    """Raised remotely when a wormhole instance receives an internal command code that is unknown"""
    pass


class WormholeWaitForReplyError(BaseWormholeException):
    """Raised when there was an error receiving a reply from a sent message"""
    pass


class WormholeHandlingError(BaseWormholeException):
    def __init__(self, original_exception: Exception):
        self.ex = original_exception


class InvalidWormholeMessageHandler(BaseWormholeException):
    pass
