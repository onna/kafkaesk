# kafkaesk

This project is meant to help facilitate easily publishing and subscribing to events with python and Kafka.

Guiding principal:
 - simple http, language agnostic contracts built on top of kafka.

Alternatives:
 - pure aiokafka: can be complex to scale correctly
 - guillotina_kafka: complex, tied to guillotina
 - faust: requires additional data layers, not language agnostic
 - confluent kafka + avro: close but ends up being like grpc. compilation for languages. No asyncio.

(consider this python project as syntatic sugar around these ideas)

## Publish

using pydantic but can be done with pure json

```python
import kafkaesk
from pydantic import BaseModel

app = kafkaesk.Application()

@app.schema("Content", version=1, retention=24 * 60 * 60)
class ContentMessage(BaseModel):
    foo: str


async def foobar():
    # ...
    # doing something in an async func
    await app.publish("content.edited.Resource", data=ContentMessage(foo="bar"))
```


## Subscribe


```python
import kafkaesk
from pydantic import BaseModel

app = kafkaesk.Application()

@app.schema("Content", version=1, retention=24 * 60 * 60)
class ContentMessage(BaseModel):
    foo: str


@app.subscribe('content.*', 'group_id')
async def get_messages(data: ContentMessage):
    print(f"{data.foo}")

```


### Retry

Clients may configure a subscriber with different retry behaviors depending on what exception was raised by the subscription callback. Subscriptions accept a dictionary of `Exception` types and `kafkaesk.retry.RetryHandler`s as an argument when registered.

The default `RetryHandler` is `kafkaesk.retry.Raise`. The `Raise` handler will cause the exception to be re-raised and terminate the subscriber.

```python
import kafkaesk
from pydantic import BaseModel

app = kafkaesk.Application()

@app.schema("Content", version=1, retention=24 * 60 * 60)
class ContentMessage(BaseModel):
    foo: str


@app.subscribe("content.*", "group_id", retry_handlers={Exception: kafkaesk.retry.Forward("dlx.content")})
async def get_messages(data: ContentMessage):
    raise Exception("UnhandledException")

```

## Avoiding global object

If you do not want to have global application configuration, you can lazily configure
the application and register schemas/subscribers separately.

```python
import kafkaesk
from pydantic import BaseModel

router = kafkaesk.Router()

@router.schema("Content", version=1, retention=24 * 60 * 60)
class ContentMessage(BaseModel):
    foo: str


@router.subscribe('content.*')
async def get_messages(data: ContentMessage):
    print(f"{data.foo}")


if __name__ == '__main__':
    app = kafkaesk.Application()
    app.mount(router)
    kafkaesk.run(app)

```


Optional consumer injected parameters:

- schema: str
- record: aiokafka.structs.ConsumerRecord
- app: kafkaesk.app.Application
- subscriber: kafkaesk.app.SubscriptionConsumer

Depending on the type annotation for the first parameter, you will get different data injected:

- `async def get_messages(data: ContentMessage)`: parses pydantic schema
- `async def get_messages(data: bytes)`: give raw byte data
- `async def get_messages(record: aiokafka.structs.ConsumerRecord)`: give kafka record object
- `async def get_messages(data)`: raw json data in message


## manual commit

To accomplish a manual commit strategy yourself:

```
app = kafkaesk.Application(auto_commit=False)

@app.subscribe('content.*')
async def get_messages(data: ContentMessage, subscriber):
    print(f"{data.foo}")
    await subscriber.commit()
```


## kafkaesk contract

This is just a library around using kafka.
Kafka itself does not enforce these concepts.

- every message must provide a json schema
- messages produced will be validated against json schema
- each topic will have only one schema
- a single schema can be used for multiple topics
- consumed message schema validation is up to the consumer
- messages will be consumed at least once. Considering this, your handling should be idempotent

### message format

```json
{
    "schema": "schema_name:1",
    "data": { ... }
}
```


# Worker

```bash
kafkaesk mymodule:app --kafka-servers=localhost:9092
```

Options:

 - --kafka-servers: comma separated list of kafka servers
 - --kafka-settings: json encoded options to be passed to https://aiokafka.readthedocs.io/en/stable/api.html#aiokafkaconsumer-class
 - --topic-prefix: prefix to use for topics
 - --replication-factor: what replication factor topics should be created with. Defaults to min(number of servers, 3).


## Application.publish

- stream_id: str: name of stream to send data to
- data: class that inherits from pydantic.BaseModel
- key: Optional[bytes]: key for message if it needs one

## Application.subscribe

- stream_id: str: fnmatch pattern of streams to subscribe to
- group: Optional[str]: consumer group id to use. Will use name of function if not provided


## Application.schema

- id: str: id of the schema to store
- version: Optional[int]: version of schema to store
- streams: Optional[List[str]]: if streams are known ahead of time, we can pre-create them before we push data
- retention: Optional[int]: retention policy in seconds


## Application.configure

- kafka_servers: Optional[List[str]]: kafka servers to connect to
- topic_prefix: Optional[str]: topic name prefix to subscribe to
- kafka_settings: Optional[Dict[str, Any]]: additional aiokafka settings to pass in
- replication_factor: Optional[int]: what replication factor topics should be created with. Defaults to min(number of servers, 3).
- kafka_api_version: str: default `auto`
- auto_commit: bool: default `True`
- auto_commit_interval_ms: int: default `5000`

## Dev

```bash
poetry install
```

Run tests:

```bash
docker-compose up
KAFKA=localhost:9092 poetry run pytest tests
```

# Extensions
## Logging
This extension includes classes to extend python's logging framework to publish structured log messages to a kafka topic.  This extension is made up of three main components: an extended `logging.LogRecord` and some custom `logging.Handler`s.

See `logger.py` in examples directory.

### Log Record
`kafkaesk.ext.logging.record.factory` is a function that will return `kafkaesk.ext.logging.record.PydanticLogRecord` objects.  The `factory()` function scans through any `args` passed to a logger and checks each item to determine if it is a subclass of `pydantid.BaseModel`.  If it is a base model instance and `model._is_log_model` evaluates to `True` the model will be removed from `args` and added to `record._pydantic_data`.  After that `factory()` will use logging's existing logic to finish creating the log record.

### Handler
This extensions ships with two handlers capable of handling `kafkaesk.ext.logging.handler.PydanticLogModel` classes: `kafakesk.ext.logging.handler.PydanticStreamHandler` and `kafkaesk.ext.logging.handler.PydanticKafkaeskHandler`.  

The stream handler is a very small wrapper around `logging.StreamHandler`, the signature is the same, the only difference is that the handler will attempt to convert any pydantic models it receives to a human readable log message.

The kafkaesk handler has a few more bits going on in the background.  The handler has two required inputs, a `kafkaesk.app.Application` instance and a stream name.  Once initialized any logs emitted by the handler will be saved into an internal queue.  There is a worker task that handles pulling logs from the queue and writing those logs to the specified topic.

# Naming things

It's hard and "kafka" is already a fun name. Hopefully this library isn't literally "kafkaesque" for you.
