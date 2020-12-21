from kafkaesk.app import Application
from kafkaesk.ext.logging import handler
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.asyncio


def test_close_log_handler():
    mock = MagicMock()
    h = handler.PydanticKafkaeskHandler(MagicMock(), "stream", mock)
    h.close()
    mock.close.assert_called_once()


def test_format_log_exc():
    h = handler.PydanticKafkaeskHandler(MagicMock(), "stream", MagicMock())
    record = MagicMock()
    record.exc_text = None
    record.exc_info = (Exception(), None, None)

    data = h._format_base_log(record)
    assert data["stack"]


def test_swallows_schema_conflict():
    app = Application()
    handler.PydanticKafkaeskHandler(app, "stream", MagicMock())
    handler.PydanticKafkaeskHandler(app, "stream", MagicMock())


def test_get_k8s_ns():

    with patch("kafkaesk.ext.logging.handler._K8S_NS", handler._not_set), patch(
        "kafkaesk.ext.logging.handler.os.path.exists", return_value=True
    ), patch("kafkaesk.ext.logging.handler.open") as open_file:
        fi = MagicMock()
        cm = MagicMock()
        cm.__enter__.return_value = fi
        open_file.return_value = cm
        fi.read.return_value = "foobar\n"
        assert handler.get_k8s_ns() == "foobar"


class TestQueue:
    def test_not_running(self):
        qq = handler.KafkaeskQueue(MagicMock())
        assert not qq.running

    def test_not_running_task_done(self):
        qq = handler.KafkaeskQueue(MagicMock())
        qq._task = MagicMock()
        qq._task.done.return_value = True
        assert not qq.running

    def test_running(self):
        qq = handler.KafkaeskQueue(MagicMock())
        qq._task = MagicMock()
        qq._task.done.return_value = False
        assert qq.running

    async def test_runtime_error_not_running(self):
        qq = handler.KafkaeskQueue(MagicMock())
        with pytest.raises(RuntimeError):
            assert await qq._run()
