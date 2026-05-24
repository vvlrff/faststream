from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from faststream.asgi import AsgiFastStream, AsyncAPIRoute
from faststream.asgi.factories.asyncapi.try_it_out import (
    TryItOutProcessor,
    _iter_broker_destinations,
)
from faststream.kafka import KafkaBroker, TestKafkaBroker
from faststream.redis import RedisBroker, TestRedisBroker


def _payload(channel: str, body: Any) -> dict[str, Any]:
    return {
        "channelName": channel,
        "message": {
            "operation_id": "op",
            "operation_type": "subscribe",
            "message": body,
        },
        "options": {"sendToRealBroker": False},
    }


class TestIterDestinations:
    def test_kafka_subscriber_destinations(self) -> None:
        broker = KafkaBroker()

        @broker.subscriber("topic-a")
        async def handler_a(msg: Any) -> None: ...

        @broker.subscriber("topic-b")
        async def handler_b(msg: Any) -> None: ...

        destinations = _iter_broker_destinations(broker)
        assert "topic-a" in destinations
        assert "topic-b" in destinations


class TestMultiBrokerDispatch:
    @pytest.mark.asyncio()
    async def test_routes_to_correct_broker_by_channel(self) -> None:
        kafka = KafkaBroker()
        redis = RedisBroker()

        kafka_mock = MagicMock()
        redis_mock = MagicMock()

        @kafka.subscriber("kafka-only")
        async def kh(msg: Any) -> None:
            kafka_mock(msg)

        @redis.subscriber("redis-only")
        async def rh(msg: Any) -> None:
            redis_mock(msg)

        app = AsgiFastStream(
            kafka, redis, asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True)
        )

        async with TestKafkaBroker(kafka), TestRedisBroker(redis):
            with TestClient(app) as client:
                r = client.post("/asyncapi/try", json=_payload("kafka-only", "x"))
                assert r.status_code == 200, r.json()
                r = client.post("/asyncapi/try", json=_payload("redis-only", "y"))
                assert r.status_code == 200, r.json()

        kafka_mock.assert_called_once_with("x")
        redis_mock.assert_called_once_with("y")

    @pytest.mark.asyncio()
    async def test_collision_first_match_wins(self) -> None:
        """When two brokers share a channel name, the first registered broker handles it."""
        kafka = KafkaBroker()
        redis = RedisBroker()

        kafka_mock = MagicMock()
        redis_mock = MagicMock()

        @kafka.subscriber("shared")
        async def kh(msg: Any) -> None:
            kafka_mock(msg)

        @redis.subscriber("shared")
        async def rh(msg: Any) -> None:
            redis_mock(msg)

        app = AsgiFastStream(
            kafka, redis, asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True)
        )

        async with TestKafkaBroker(kafka), TestRedisBroker(redis):
            with TestClient(app) as client:
                r = client.post("/asyncapi/try", json=_payload("shared", "hi"))
                assert r.status_code == 200, r.json()

        kafka_mock.assert_called_once_with("hi")
        redis_mock.assert_not_called()

    @pytest.mark.asyncio()
    async def test_channel_not_found_anywhere_returns_404(self) -> None:
        kafka = KafkaBroker()
        redis = RedisBroker()

        app = AsgiFastStream(
            kafka, redis, asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True)
        )

        async with TestKafkaBroker(kafka), TestRedisBroker(redis):
            with TestClient(app) as client:
                r = client.post("/asyncapi/try", json=_payload("missing", "x"))

        assert r.status_code == 404

    @pytest.mark.asyncio()
    async def test_single_broker_backcompat(self) -> None:
        kafka = KafkaBroker()
        mock = MagicMock()

        @kafka.subscriber("queue")
        async def h(msg: Any) -> None:
            mock(msg)

        app = AsgiFastStream(
            kafka, asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True)
        )

        async with TestKafkaBroker(kafka):
            with TestClient(app) as client:
                r = client.post("/asyncapi/try", json=_payload("queue", "hi"))
                assert r.status_code == 200, r.json()

        mock.assert_called_once_with("hi")


class TestProcessorUnit:
    def test_empty_brokers_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one broker"):
            TryItOutProcessor([])


def _payload_op(channel: str, op_id: str, body: Any) -> dict[str, Any]:
    return {
        "channelName": channel,
        "message": {
            "operation_id": op_id,
            "operation_type": "subscribe",
            "message": body,
        },
        "options": {"sendToRealBroker": False},
    }


class TestOperationIdDispatch:
    """Dispatch via operation_id is the AsyncAPI 3.0 standard path.

    The plugin sends `operation_id` from the spec; we look it up in the
    factory-built `operation_to_broker` map to pick the right broker even
    when multiple brokers serve the same destination.
    """

    @pytest.mark.asyncio()
    async def test_dispatch_by_operation_id_picks_correct_broker(self) -> None:
        kafka = KafkaBroker()
        redis = RedisBroker()

        kafka_mock = MagicMock()
        redis_mock = MagicMock()

        # Different function names → distinct channel/operation keys:
        # ping:KafkaPingSubscribe (kafka), ping:RedisPingSubscribe (redis).
        @kafka.subscriber("ping")
        async def kafka_ping(msg: Any) -> None:
            kafka_mock(msg)

        @redis.subscriber("ping")
        async def redis_ping(msg: Any) -> None:
            redis_mock(msg)

        app = AsgiFastStream(
            kafka, redis, asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True)
        )

        async with TestKafkaBroker(kafka), TestRedisBroker(redis):
            with TestClient(app) as client:
                # The plugin click on "ping:RedisPing" must go to Redis, not Kafka.
                r = client.post(
                    "/asyncapi/try",
                    json=_payload_op("ping:RedisPing", "ping:RedisPingSubscribe", "hi"),
                )
                assert r.status_code == 200, r.json()

        redis_mock.assert_called_once_with("hi")
        kafka_mock.assert_not_called()

    @pytest.mark.asyncio()
    async def test_dispatch_falls_back_when_operation_id_unknown(self) -> None:
        """Unknown operation_id → fall back to destination-based first-owner-wins."""
        kafka = KafkaBroker()
        redis = RedisBroker()

        kafka_mock = MagicMock()

        @kafka.subscriber("kafka-only")
        async def kh(msg: Any) -> None:
            kafka_mock(msg)

        app = AsgiFastStream(
            kafka, redis, asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True)
        )

        async with TestKafkaBroker(kafka), TestRedisBroker(redis):
            with TestClient(app) as client:
                r = client.post(
                    "/asyncapi/try",
                    json=_payload_op("kafka-only", "bogus-op-id", "x"),
                )
                assert r.status_code == 200, r.json()

        kafka_mock.assert_called_once_with("x")
