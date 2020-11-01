import uuid
import hashlib

from typing import *


def generate_uid():
    return uuid.uuid4().hex


def dynamic_import(name: str):
    components = name.split('.')
    if len(components) == 1:
        return eval(name)  # Workaround for __main__ classes
    class_name = components.pop()
    py_module_path = '.'.join(components)
    mod = __import__(py_module_path, fromlist=[py_module_path])
    mod = getattr(mod, class_name)
    return mod


def get_full_type_path(obj: Union[Type, object]) -> str:
    class_name = get_class_name(obj)
    if obj.__module__ == '__main__':
        return class_name
    return f'{obj.__module__}.{class_name}'


def get_class_name(obj: Union[Type, object]) -> str:
    if isinstance(obj, Type):
        return obj.__name__
    return obj.__class__.__name__


def hash_string(data: str) -> str:
    md5 = hashlib.md5(data.encode())
    return md5.hexdigest()


def merge_queue_name_with_tag(queue_name: str, tag: Optional[str]):
    if tag is None:
        return queue_name
    return f"{queue_name}:{tag}"

