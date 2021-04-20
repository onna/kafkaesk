from kafkaesk import Application
from kafkaesk import run_app
from pydantic import BaseModel

import asyncio
import logging
import random


logging.basicConfig(level=logging.INFO)


app = Application()


@app.schema("Foobar",
            streams=[
                "content.foo",
                "slow.content.foo",
                "failed.content.foo"
            ])
class Foobar(BaseModel):
    timeout: int


async def consumer_logic(data: Foobar, record, subscriber):
    try:
        print(f"{data}: waiting {data.timeout}s...")
        await asyncio.sleep(data.timeout)
        print(f"{data}: done...")
    except asyncio.CancelledError:
        # Slow topic
        print(f"{data} timeout message, sending to slow topic...")
        await subscriber.publish(f"slow.{record.topic}", record)
    except Exception:
        await subscriber.publish(f"failed.{record.topic}", record)


async def generate_data(app):
    idx = 0
    while True:
        timeout = random.randint(0, 10)
        await app.publish("content.foo", Foobar(timeout=timeout))
        idx += 1
        await asyncio.sleep(0.1)


async def run():
    app.configure(kafka_servers=["localhost:9092"])
    task = asyncio.create_task(generate_data(app))

    # Regular tasks should be consumed in less than 5s
    app.subscribe("content.*",
                  group="example_content_group",
                  concurrency=10,
                  timeout_seconds=5)(consumer_logic)

    # Timeout taks (slow) can be consumed independendly, with different configuration and logic
    app.subscribe("slow.content.*",
                  group="timeout_example_content_group",
                  concurrency=1,
                  timeout_seconds=None)(consumer_logic)

    await run_app(app)


if __name__ == "__main__":
    asyncio.run(run())
