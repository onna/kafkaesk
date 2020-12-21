from kafkaesk import Application
from kafkaesk import run_app
from pydantic import BaseModel

import asyncio
import logging

logging.basicConfig(level=logging.INFO)


app = Application()


@app.schema("Foobar")
class Foobar(BaseModel):
    foo: str
    bar: str


@app.subscribe("content.*", group="example_content_group")
async def messages(data: Foobar, record):
    await asyncio.sleep(0.1)
    print(f"{data.foo}: {data.bar}: {record}")


async def generate_data(app):
    idx = 0
    while True:
        await app.publish("content.foo", Foobar(foo=str(idx), bar="yo"))
        idx += 1
        await asyncio.sleep(0.1)


async def run():
    app.configure(kafka_servers=["localhost:9092"])
    task = asyncio.create_task(generate_data(app))
    await run_app(app)
    # await app.consume_forever()


if __name__ == "__main__":
    asyncio.run(run())
