from .produce import Foo
from .produce import producer
from kafkaesk import Application

import asyncio
import pytest
import signal

TOPIC = "test-run"
GROUP = "test-run2"

pytestmark = pytest.mark.asyncio

test_app = Application()

test_app.schema(streams=[TOPIC])(Foo)


@test_app.subscribe(TOPIC, group=GROUP)
async def _consumer(ob: Foo, record, app):
    ...


async def test_run_exits_cleanly_while_consuming(kafka, topic_prefix):
    kserver = f"{kafka[0]}:{kafka[1]}"
    # kafka_settings = {"security_protocol": "PLAINTEXT"}
    app = Application([kserver], topic_prefix=topic_prefix)
    async with app:
        pro = asyncio.create_task(producer(app, TOPIC))

        proc = await asyncio.create_subprocess_exec(
            "kafkaesk",
            "tests.acceptance.test_run:test_app",
            "--kafka-servers",
            kserver,
            "--topic-prefix",
            topic_prefix,
            # cwd=_test_dir,
        )

        await asyncio.sleep(5)
        pro.cancel()

        proc.send_signal(signal.SIGINT)
        await proc.wait()

        assert proc.returncode == 0

        results = await app.topic_mng.list_consumer_group_offsets(GROUP)
        topic_id = app.topic_mng.get_topic_id(TOPIC)
        count = 0
        for tp, pos in results.items():
            if tp.topic != topic_id:
                continue
            count += pos.offset
        assert count > 0
