from typing import *

from wormhole.error import InvalidWormholeMessageHandler

if TYPE_CHECKING:
    from wormhole.basic import BasicWormhole


class WormholeHandler:
    @classmethod
    def register_handler_of_instance(cls, wormhole: "BasicWormhole", handler: Callable):
        queue_name, tag = cls.__parse_wormhole_message_handler_data(handler)
        return wormhole.register_handler(queue_name, handler, tag)

    @classmethod
    def unregister_handler_of_instance(cls, wormhole: "BasicWormhole", handler: Callable):
        queue_name, tag = cls.__parse_wormhole_message_handler_data(handler)
        return wormhole.unregister_handler(queue_name)

    @classmethod
    def register_all_handlers_of_instance(cls, wormhole: "BasicWormhole", instance: object):
        return [
            cls.register_handler_of_instance(wormhole, getattr(instance, attr_name))
            for attr_name in dir(instance) if cls.is_message_handler(getattr(instance, attr_name))
        ]

    @classmethod
    def unregister_all_handlers_of_instance(cls, wormhole: "BasicWormhole", instance: object):
        return [
            cls.unregister_handler_of_instance(wormhole, getattr(instance, attr_name))
            for attr_name in dir(instance) if cls.is_message_handler(getattr(instance, attr_name))
        ]

    @staticmethod
    def is_message_handler(handler: Callable):
        return callable(handler) and hasattr(handler, 'wormhole_handler')

    @staticmethod
    def __parse_wormhole_message_handler_data(handler: Callable):
        if not callable(handler):
            raise InvalidWormholeMessageHandler(f"Not a callable {handler.__name__}")
        if not hasattr(handler, 'wormhole_handler'):
            raise InvalidWormholeMessageHandler(f"Not a wormhole message handler {handler.__name__}")
        wormhole_queue_name = getattr(handler, 'wormhole_queue_name')
        wormhole_queue_tag = getattr(handler, 'wormhole_queue_tag')
        return wormhole_queue_name, wormhole_queue_tag
