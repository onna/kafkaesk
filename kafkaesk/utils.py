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


def resolve_dotted_name(name: str) -> Any:
    """
    import the provided dotted name
    >>> resolve_dotted_name('foo.bar')
    <object bar>
    :param name: dotted name
    """
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
