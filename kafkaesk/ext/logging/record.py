from types import TracebackType
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import logging
import pydantic


class PydanticLogRecord(logging.LogRecord):
    def __init__(
        self,
        name: str,
        level: int,
        fn: str,
        lno: int,
        msg: str,
        args: Tuple,
        exc_info: Union[
            Tuple[type, BaseException, Optional[TracebackType]], Tuple[None, None, None], None
        ],
        func: Optional[str],
        sinfo: Optional[str],
    ):
        super().__init__(name, level, fn, lno, msg, args, exc_info, func, sinfo)

        self._pydantic_data: List[pydantic.BaseModel] = []
        self.exc_type: Optional[str] = None
        self.stack_text: Optional[str] = None


def factory(
    name: str,
    level: int,
    fn: str,
    lno: int,
    msg: str,
    args: Tuple,
    exc_info: Union[
        Tuple[type, BaseException, Optional[TracebackType]], Tuple[None, None, None], None
    ],
    func: Optional[str],
    sinfo: Optional[str],
) -> PydanticLogRecord:
    data: List[pydantic.BaseModel] = []

    new_args = []
    for arg in args:
        if isinstance(arg, pydantic.BaseModel):
            if hasattr(arg, "_is_log_model") and getattr(arg, "_is_log_model", False) is True:
                data.append(arg)
                continue
        new_args.append(arg)

    args = tuple(new_args)

    record = PydanticLogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)

    if data:
        record._pydantic_data = data

    return record
