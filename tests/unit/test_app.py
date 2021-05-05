from asyncio.futures import Future
from kafkaesk.app import Application
from kafkaesk.app import published_callback
from kafkaesk.app import run
from kafkaesk.app import run_app
from kafkaesk.app import SchemaRegistration
from opentracing.scope_managers.contextvars import ContextVarsScopeManager
from tests.utils import record_factory
from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import json
import kafkaesk
import kafkaesk.exceptions
import opentracing
import pydantic
import pytest
import time

pytestmark = pytest.mark.asyncio


class TestApplication:
    async def test_app_events(self):
        app = Application()

        async def on_finalize():
            pass

        app.on("finalize", on_finalize)
        assert len(app._event_handlers["finalize"]) == 1

    async def test_app_finalize_event(self):
        app = Application()

        class CallTracker:
            def __init__(self):
                self.called = False

            async def on_finalize(self):
                self.called = True

        tracker = CallTracker()
        app.on("finalize", tracker.on_finalize)
        await app.finalize()

        assert tracker.called is True

    def test_publish_callback(self, metrics):
        fut = Future()
        fut.set_result(record_factory())
        published_callback("topic", time.time() - 1, fut)

        metrics["PUBLISHED_MESSAGES"].labels.assert_called_with(
            stream_id="topic", partition=0, error="none"
        )
        metrics["PUBLISHED_MESSAGES"].labels().inc()

        metrics["PRODUCER_TOPIC_OFFSET"].labels.assert_called_with(stream_id="topic", partition=0)
        metrics["PRODUCER_TOPIC_OFFSET"].labels().set.assert_called_with(0)

        metrics["PUBLISHED_MESSAGES_TIME"].labels.assert_called_with(stream_id="topic")
        assert metrics["PUBLISHED_MESSAGES_TIME"].labels().observe.mock_calls[0].args[
            0
        ] == pytest.approx(1, 0.1)

    def test_publish_callback_exc(self, metrics):
        fut = Future()
        fut.set_exception(Exception())
        published_callback("topic", time.time(), fut)

        metrics["PUBLISHED_MESSAGES"].labels.assert_called_with(
            stream_id="topic", partition=-1, error="Exception"
        )
        metrics["PUBLISHED_MESSAGES"].labels().inc()

    def test_mount_router(self):
        app = Application()

        router = kafkaesk.Router()

        @router.schema("Foo", streams=["foo.bar"])
        class Foo(pydantic.BaseModel):
            bar: str

        @router.subscribe("foo.bar", group="test_group")
        async def consume(data: Foo, schema, record):
            ...

        app.mount(router)

        assert app.subscriptions == router.subscriptions
        assert app.schemas == router.schemas
        assert app.event_handlers == router.event_handlers

    async def test_consumer_health_check(self):
        app = kafkaesk.Application()
        subscription_consumer = AsyncMock()
        app._subscription_consumers.append(subscription_consumer)
        subscription_consumer.consumer._client.ready.return_value = True
        await app.health_check()

    async def test_consumer_health_check_raises_exception(self):
        app = kafkaesk.Application()
        subscription_consumer = kafkaesk.BatchConsumer(
            stream_id="foo",
            group_id="group",
            coro=lambda record: 1,
            app=app,
        )
        app._subscription_consumers.append(subscription_consumer)
        subscription_consumer._consumer = AsyncMock()
        subscription_consumer._consumer._client.ready.return_value = False
        with pytest.raises(kafkaesk.exceptions.ConsumerUnhealthyException):
            await app.health_check()

    async def test_consumer_health_check_producer_healthy(self):
        app = kafkaesk.Application()
        app._producer = MagicMock()
        app._producer._sender.sender_task.done.return_value = False
        await app.health_check()

    async def test_consumer_health_check_producer_unhealthy(self):
        app = kafkaesk.Application()
        app._producer = MagicMock()
        app._producer._sender.sender_task.done.return_value = True
        with pytest.raises(kafkaesk.exceptions.ProducerUnhealthyException):
            await app.health_check()

    async def test_configure_kafka_producer(self):
        app = kafkaesk.Application(
            kafka_settings={
                "metadata_max_age_ms": 100,
                "max_batch_size": 100,
                # invalid for producer so should not be applied here
                "max_partition_fetch_bytes": 100,
            }
        )
        # verify it is created correctly
        app.producer_factory()

        # now, validate the wiring
        with patch("kafkaesk.app.aiokafka.AIOKafkaProducer") as mock:
            app.producer_factory()
            mock.assert_called_with(
                bootstrap_servers=None,
                loop=ANY,
                api_version="auto",
                metadata_max_age_ms=100,
                max_batch_size=100,
            )

    async def test_configure_kafka_consumer(self):
        app = kafkaesk.Application(
            kafka_settings={
                "max_partition_fetch_bytes": 100,
                "fetch_max_wait_ms": 100,
                "metadata_max_age_ms": 100,
                # invalid for consumer so should not be applied here
                "max_batch_size": 100,
            }
        )
        # verify it is created correctly
        app.consumer_factory(group_id="foobar")

        # now, validate the wiring
        with patch("kafkaesk.app.aiokafka.AIOKafkaConsumer") as mock:
            app.consumer_factory(group_id="foobar")
            mock.assert_called_with(
                bootstrap_servers=None,
                loop=ANY,
                group_id="foobar",
                api_version="auto",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                max_partition_fetch_bytes=100,
                fetch_max_wait_ms=100,
                metadata_max_age_ms=100,
            )

    def test_configure(self):
        app = kafkaesk.Application()
        app.configure(
            kafka_servers=["kafka_servers"],
            topic_prefix="topic_prefix",
            kafka_settings={"kafka_settings": "kafka_settings"},
            api_version="api_version",
            replication_factor="replication_factor",
        )
        assert app._kafka_servers == ["kafka_servers"]
        assert app._topic_prefix == "topic_prefix"
        assert app._kafka_settings == {"kafka_settings": "kafka_settings"}
        assert app._kafka_api_version == "api_version"
        assert app._replication_factor == "replication_factor"

        # now make sure none values do not overwrite
        app.configure(
            kafka_servers=None,
            topic_prefix=None,
            kafka_settings=None,
            api_version=None,
            replication_factor=None,
        )
        assert app._kafka_servers == ["kafka_servers"]
        assert app._topic_prefix == "topic_prefix"
        assert app._kafka_settings == {"kafka_settings": "kafka_settings"}
        assert app._kafka_api_version == "api_version"
        assert app._replication_factor == "replication_factor"

    async def test_initialize_with_unconfigured_app_raises_exception(self):
        app = kafkaesk.Application()
        with pytest.raises(kafkaesk.exceptions.AppNotConfiguredException):
            await app.initialize()

    async def test_publish_propagates_headers(self):
        app = kafkaesk.Application(kafka_servers=["foo"])

        class Foo(pydantic.BaseModel):
            bar: str

        producer = AsyncMock()
        app._get_producer = AsyncMock(return_value=producer)
        app._topic_mng = MagicMock()
        app._topic_mng.get_topic_id.return_value = "foobar"
        app._topic_mng.topic_exists = AsyncMock(return_value=True)

        await app.publish("foobar", Foo(bar="foo"), headers=[("foo", b"bar")])
        producer.send.assert_called_with(
            "foobar",
            value=b'{"schema":"Foo:1","data":{"bar":"foo"}}',
            key=None,
            headers=[("foo", b"bar")],
        )

    async def test_publish_configured_retention_policy(self):
        app = kafkaesk.Application(kafka_servers=["foo"])

        @app.schema(retention=100)
        class Foo(pydantic.BaseModel):
            bar: str

        producer = AsyncMock()
        app._get_producer = AsyncMock(return_value=producer)
        app._topic_mng = MagicMock()
        app._topic_mng.get_topic_id.return_value = "foobar"
        app._topic_mng.topic_exists = AsyncMock(return_value=False)
        app._topic_mng.create_topic = AsyncMock()

        await app.publish("foobar", Foo(bar="foo"), headers=[("foo", b"bar")])
        app._topic_mng.create_topic.assert_called_with(
            "foobar", replication_factor=None, retention_ms=100 * 1000
        )

    async def test_publish_injects_tracing(self):
        app = kafkaesk.Application(kafka_servers=["foo"])
        producer = AsyncMock()
        app._get_producer = AsyncMock(return_value=producer)

        tracer = opentracing.global_tracer()
        tracer._scope_manager = ContextVarsScopeManager()

        tracer.scope_manager.activate("foobar", True)

        with patch.object(tracer, "inject") as mock:
            await app.raw_publish("foobar", b"foobar")
            mock.assert_called_once()


class TestSchemaRegistration:
    def test_schema_registration_repr(self):
        reg = SchemaRegistration(id="id", version=1, model=None)
        assert repr(reg) == "<SchemaRegistration id, version: 1 >"


test_app = Application()


def app_callable():
    return test_app


class TestRun:
    def test_run(self):
        rapp = AsyncMock()
        with patch("kafkaesk.app.run_app", rapp), patch("kafkaesk.app.cli_parser") as cli_parser:
            args = Mock()
            args.app = "tests.unit.test_app:test_app"
            args.kafka_servers = "foo,bar"
            args.kafka_settings = json.dumps({"foo": "bar"})
            args.topic_prefix = "prefix"
            args.api_version = "api_version"
            cli_parser.parse_args.return_value = args

            run()

            rapp.assert_called_once()
            assert test_app._kafka_servers == ["foo", "bar"]
            assert test_app._kafka_settings == {"foo": "bar"}
            assert test_app._topic_prefix == "prefix"
            assert test_app._kafka_api_version == "api_version"

    def test_run_callable(self):
        rapp = AsyncMock()
        with patch("kafkaesk.app.run_app", rapp), patch("kafkaesk.app.cli_parser") as cli_parser:
            args = Mock()
            args.app = "tests.unit.test_app:app_callable"
            args.kafka_settings = None
            cli_parser.parse_args.return_value = args

            run()

            rapp.assert_called_once()

    async def test_run_app(self):
        app_mock = AsyncMock()
        app_mock.consume_forever.return_value = ([], [])
        loop = MagicMock()
        with patch("kafkaesk.app.asyncio.get_event_loop", return_value=loop):
            await run_app(app_mock)
        app_mock.consume_forever.assert_called_once()
        assert len(loop.add_signal_handler.mock_calls) == 2
