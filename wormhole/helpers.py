from typing import *

from wormhole.registry import get_primary_wormhole

if TYPE_CHECKING:
    from .registry import BasicWormhole


def register_handler_instance(instance: object):
    for attr_name in dir(instance):
        attr = getattr(instance, attr_name)
        if not callable(attr):
            continue
        if not hasattr(attr, 'wormhole_handler'):
            continue
        wormhole_queue_name = getattr(attr, 'wormhole_queue_name')
        wormhole_queue_tag = getattr(attr, 'wormhole_queue_tag')
        wormhole: "BasicWormhole" = getattr(attr, 'wormhole')
        if wormhole is None:
            wormhole = get_primary_wormhole()
        wormhole.register_handler(wormhole_queue_name, attr, wormhole_queue_tag)
        

# Decorator
def wormhole_handler(queue_name: str, **kwargs):
    def wrapper(callable_object: Callable):
        nonlocal queue_name
        callable_object.wormhole_handler = True
        callable_object.wormhole_queue_name = queue_name
        callable_object.wormhole_queue_tag = kwargs.get('tag', None)
        callable_object.wormhole = kwargs.get('wormhole', None) or get_primary_wormhole()
        return callable_object

    return wrapper
