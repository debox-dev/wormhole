from typing import *

from .error import WormholeInvalidQueueName
from .message import WormholeMessage


class WormholeWaitable:
    def __init__(self, item: Union[Type["WormholeMessage"], str], tag: Optional[str]):
        self.item = item
        self.tag = tag
        if isinstance(item, str):
            self.queue_name = item
        elif issubclass(item, WormholeMessage):
            self.queue_name = item.get_base_queue_name()
        else:
            raise WormholeInvalidQueueName(f"Don't know how to wait for type {type(item)}")

    @classmethod
    def from_item(cls, item: Any) -> "WormholeWaitable":
        if isinstance(item, WormholeWaitable):
            return item
        return cls(item, None)
