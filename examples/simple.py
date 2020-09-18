from kafkaesk import Application
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
async def messages(data: Foobar):
    print(f"{data.foo}: {data.bar}")


async def generate_data():
    app.configure(kafka_servers=["localhost:9092"])
    async with app:
        for idx in range(1000):
            await app.publish("content.foo", Foobar(foo=str(idx), bar="yo"))


if __name__ == "__main__":
    asyncio.run(generate_data())
