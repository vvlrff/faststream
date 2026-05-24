from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from functools import lru_cache
from typing import TYPE_CHECKING, Any, TypedDict, Union

from faststream.asgi.annotations import Request
from faststream.asgi.handlers import PostHandler, post
from faststream.asgi.response import AsgiResponse, JSONResponse
from faststream.exceptions import SubscriberNotFound

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase
    from faststream._internal.testing.broker import TestBroker
    from faststream.specification.schema import Tag, TagDict


class TryItOutOptions(TypedDict, total=False):
    sendToRealBroker: bool
    timestamp: str


class TryItOutMessage(TypedDict, total=False):
    """Wrapper sent by asyncapi-try-it-plugin.

    The plugin always wraps the user's payload inside a nested ``message``
    field together with operation metadata::

        {
            "operation_id": "...",
            "operation_type": "...",
            "message": <actual_user_payload>
        }
    """

    operation_id: str
    operation_type: str
    message: Any


class TryItOutForm(TypedDict):
    channelName: str
    message: TryItOutMessage
    options: TryItOutOptions


class TryItOutProcessor:
    """Dispatch try-it-out requests.

    Primary: `operation_id` from the plugin payload looked up in the
    operation→broker map built by the AsyncAPI v3+ generator. This is the
    standard AsyncAPI 3.0 identifier and gives unambiguous routing even when
    multiple brokers expose the same destination.

    Fallback (single-broker apps, payloads without operation_id, AsyncAPI v2.6):
    parse `channelName`, find the first broker that owns the destination.
    """

    def __init__(
        self,
        brokers: Iterable["BrokerUsecase[Any, Any]"],
        operation_to_broker: Mapping[str, "BrokerUsecase[Any, Any]"] | None = None,
    ) -> None:
        registry = _get_broker_registry()
        self._entries: list[tuple[BrokerUsecase[Any, Any], type[TestBroker[Any]]]] = []
        self._test_broker_cls: dict[int, type[TestBroker[Any]]] = {}
        for broker in brokers:
            for br_cls, test_broker_cls in registry.items():
                if isinstance(broker, br_cls):
                    self._entries.append((broker, test_broker_cls))
                    self._test_broker_cls[id(broker)] = test_broker_cls
                    break
            else:
                msg = f"TestBroker not available for {broker}. Please, inspect your dependencies."
                raise ValueError(msg)

        if not self._entries:
            msg = "TryItOutProcessor requires at least one broker."
            raise ValueError(msg)

        self._operation_to_broker: Mapping[str, BrokerUsecase[Any, Any]] = (
            operation_to_broker or {}
        )

    async def process(self, body: TryItOutForm) -> AsgiResponse:
        """Process parsed body: validate, dry-run or publish. Returns response."""
        destination, *_ = body.get("channelName", "").split(":")

        if not destination:
            return JSONResponse({"details": "Missing channelName"}, 400)

        operation_id = body.get("message", {}).get("operation_id") or ""

        # Primary: operation_id → broker (AsyncAPI 3.0 standard identifier).
        broker = self._operation_to_broker.get(operation_id)
        test_broker_cls: type[TestBroker[Any]] | None = (
            self._test_broker_cls.get(id(broker)) if broker is not None else None
        )

        # Fallback: by destination (single broker, v2.6, or unknown operation_id).
        if broker is None or test_broker_cls is None:
            if len(self._entries) == 1:
                broker, test_broker_cls = self._entries[0]
            else:
                entry = next(
                    (
                        e
                        for e in self._entries
                        if destination in _iter_broker_destinations(e[0])
                    ),
                    None,
                )
                if entry is None:
                    return JSONResponse(
                        {"details": f"{destination} destination not found."}, 404
                    )
                broker, test_broker_cls = entry

        payload: Any = body.get("message", {}).get("message")
        use_real_broker = body.get("options", {}).get("sendToRealBroker", False)

        try:
            if use_real_broker:
                await broker.publish(payload, destination)
                return JSONResponse("ok", 200)

            async with test_broker_cls(broker) as br:
                data = await br.request(payload, destination, timeout=30)
                decoded = None
                with suppress(Exception):
                    decoded = await data.decode()
                return JSONResponse(
                    decoded if decoded is not None and decoded != b"" else "ok", 200
                )

        except SubscriberNotFound:
            return JSONResponse({"details": f"{destination} destination not found."}, 404)

        except Exception as e:
            return JSONResponse({"details": repr(e)}, 500)


def make_try_it_out_handler(
    brokers: Iterable["BrokerUsecase[Any, Any]"],
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
    include_in_schema: bool = False,
    operation_to_broker: Mapping[str, "BrokerUsecase[Any, Any]"] | None = None,
) -> "PostHandler":
    """POST handler for asyncapi-try-it-plugin.

    Dispatch priority: operation_id from payload (standard AsyncAPI 3.0 ID),
    then destination fallback for single-broker / v2.6 / unknown ops.
    """
    processor = TryItOutProcessor(brokers, operation_to_broker=operation_to_broker)

    @post(
        description=description,
        tags=tags,
        unique_id=unique_id,
        include_in_schema=include_in_schema,
    )
    async def try_it_out(request: Request) -> AsgiResponse:
        try:
            body: TryItOutForm = await request.json()

        except Exception as e:
            return JSONResponse({"details": f"Invalid JSON: {e}"}, 400)

        return await processor.process(body)

    return try_it_out


def _iter_broker_destinations(broker: "BrokerUsecase[Any, Any]") -> set[str]:
    """Destinations declared on the broker (``schema()`` key before ``:``)."""
    destinations: set[str] = set()

    for sub in broker.subscribers:
        with suppress(Exception):
            destinations.update(key.split(":", 1)[0] for key in sub.schema())

    for pub in broker.publishers:
        with suppress(Exception):
            destinations.update(key.split(":", 1)[0] for key in pub.schema())

    return destinations


@lru_cache(maxsize=1)
def _get_broker_registry() -> dict[
    type["BrokerUsecase[Any, Any]"],
    type["TestBroker[Any]"],
]:
    registry: dict[type[BrokerUsecase[Any, Any]], type[TestBroker[Any]]] = {}

    with suppress(ImportError):
        from faststream.confluent import (
            KafkaBroker as ConfluentKafkaBroker,
            TestKafkaBroker as TestConfluentKafkaBroker,
        )

        registry[ConfluentKafkaBroker] = TestConfluentKafkaBroker

    with suppress(ImportError):
        from faststream.kafka import (
            KafkaBroker as AioKafkaBroker,
            TestKafkaBroker as TestAioKafkaBroker,
        )

        registry[AioKafkaBroker] = TestAioKafkaBroker

    with suppress(ImportError):
        from faststream.nats import NatsBroker, TestNatsBroker

        registry[NatsBroker] = TestNatsBroker

    with suppress(ImportError):
        from faststream.rabbit import RabbitBroker, TestRabbitBroker

        registry[RabbitBroker] = TestRabbitBroker

    with suppress(ImportError):
        from faststream.redis import RedisBroker, TestRedisBroker

        registry[RedisBroker] = TestRedisBroker

    with suppress(ImportError):
        from faststream.mqtt import MQTTBroker, TestMQTTBroker

        registry[MQTTBroker] = TestMQTTBroker

    return registry
