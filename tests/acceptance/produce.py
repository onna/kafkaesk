import asyncio
import pydantic


class Foo(pydantic.BaseModel):
    foo: str


async def producer(app, topic):
    while True:
        try:
            await app.publish(topic, Foo(foo="bar"))
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            return
