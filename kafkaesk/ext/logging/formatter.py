from .record import factory
from .record import PydanticLogRecord
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Type

import logging
import pydantic
import time


class BaseLogFormat(pydantic.BaseModel):
    timestamp: str = pydantic.Field(alias="asctime")
    logger: str = pydantic.Field(alias="name")
    severity: str = pydantic.Field(alias="levelname")
    level: int = pydantic.Field(alias="levelno")
    message: str
    exception: Optional[str] = pydantic.Field(alias="exc_type")
    trace: Optional[str] = pydantic.Field(alias="exc_text")
    stack: Optional[str] = pydantic.Field(alias="stack_info")


class PydanticLogModel(pydantic.BaseModel):
    class Config:
        extra = pydantic.Extra.allow


class PydanticFormatter(logging.Formatter):

    converter = time.gmtime

    def __init__(
        self,
        model: Type[pydantic.BaseModel] = BaseLogFormat,
        fmt: Optional[str] = None,
        datefmt: Optional[str] = None,
        style: str = "%",
    ):
        self.format_class = model
        self._init_logrecord_factory()
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)

    def _init_logrecord_factory(self) -> None:
        if logging.getLogRecordFactory() != factory:
            logging.setLogRecordFactory(factory)

    def _format_base_log(self, record: PydanticLogRecord) -> Dict[str, Any]:
        return self.format_class(**record.__dict__).dict(exclude_none=True)

    def _format_extra_logs(self, record: PydanticLogRecord) -> Dict[str, Any]:
        extra_logs: Dict[str, Any] = {}

        for log in record.pydantic_data:
            extra_logs.update(log.dict(exclude_none=True, exclude={"_is_log_model",}))

        return extra_logs

    def formatExceptionName(self, exception: Tuple) -> str:
        return exception[0].__name__

    def format(self, record: PydanticLogRecord) -> PydanticLogModel:  # type: ignore

        record.message = record.getMessage()
        record.asctime = self.formatTime(record, self.datefmt)

        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
                record.exc_type = self.formatExceptionName(record.exc_info)

        if record.stack_info:
            record.stack_text = self.formatStack(record.stack_info)

        base_log = self._format_base_log(record)
        extra = self._format_extra_logs(record)

        extra.update(base_log)

        return PydanticLogModel(**extra)
