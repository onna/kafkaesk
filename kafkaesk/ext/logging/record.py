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
        func: Optional[str] = None,
        sinfo: Optional[str] = None,
        pydantic_data: Optional[List[pydantic.BaseModel]] = None,
    ):
        super().__init__(name, level, fn, lno, msg, args, exc_info, func, sinfo)

        self.pydantic_data = pydantic_data or []
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
    func: Optional[str] = None,
    sinfo: Optional[str] = None,
) -> PydanticLogRecord:
    pydantic_data: List[pydantic.BaseModel] = []

    new_args = []
    for arg in args:
        if isinstance(arg, pydantic.BaseModel):
            if hasattr(arg, "_is_log_model") and getattr(arg, "_is_log_model", False) is True:
                pydantic_data.append(arg)
                continue
        new_args.append(arg)

    args = tuple(new_args)

    record = PydanticLogRecord(
        name, level, fn, lno, msg, args, exc_info, func, sinfo, pydantic_data
    )

    return record


if logging.getLogRecordFactory() != factory:
    logging.setLogRecordFactory(factory)
