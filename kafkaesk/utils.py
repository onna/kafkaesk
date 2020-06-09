from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from typing import Any
from typing import Callable

import asyncio

executor = ThreadPoolExecutor(max_workers=30)


async def run_async(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    func_to_run = partial(func, *args, **kwargs)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, func_to_run)


def deep_compare(d1: Any, d2: Any) -> bool:
    if type(d1) != type(d2):
        return False

    if isinstance(d1, dict):
        if list(sorted(d1.keys())) != list(sorted(d2.keys())):
            return False
        for k, v in d1.items():
            if k not in d2:
                return False
            if not deep_compare(v, d2.get(k)):
                return False
    elif isinstance(d1, list):
        if len(d1) != len(d2):
            return False
        for idx, v in enumerate(d1):
            if not deep_compare(v, d2[idx]):
                return False
    else:
        return d1 == d2
    return True


def resolve_dotted_name(name: str) -> Any:
    """
    import the provided dotted name

    >>> resolve_dotted_name('foo.bar')
    <object bar>

    :param name: dotted name
    """
    if not isinstance(name, str):
        return name  # already an object
    names = name.split(".")
    used = names.pop(0)
    found = __import__(used)
    for n in names:
        used += "." + n
        try:
            found = getattr(found, n)
        except AttributeError:
            __import__(used)
            found = getattr(found, n)

    return found
