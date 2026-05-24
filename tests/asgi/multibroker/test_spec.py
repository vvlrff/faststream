import json
from typing import Any

import pytest

from faststream.kafka import KafkaBroker
from faststream.redis import RedisBroker
from faststream.specification.asyncapi.factory import AsyncAPI
from faststream.specification.asyncapi.utils import ChannelKeyCollisionWarning


class TestSpecKeys:
    def test_anti_pattern_handler_name_collision_no_overwrite(self) -> None:
        """Two brokers with the same subscriber function name (`handler`)
        used to overwrite each other in `channels` map. Now they coexist
        via a `_2` discriminator and the user gets a `RuntimeWarning`
        nudging them toward distinct subscriber names."""
        kafka = KafkaBroker()
        redis = RedisBroker()

        @kafka.subscriber("ping")
        async def handler(msg: Any) -> None: ...  # noqa: F811

        @redis.subscriber("ping")
        async def handler(msg: Any) -> None: ...  # noqa: F811

        with pytest.warns(
            ChannelKeyCollisionWarning,
            match=r"already used across brokers; renamed to",
        ):
            schema = AsyncAPI(kafka, redis).to_specification()

        d = json.loads(schema.to_json())
        channel_keys = list(d["channels"])
        assert "ping:Handler" in channel_keys
        assert "ping:Handler_2" in channel_keys

        # Each channel keeps its own server-ref.
        servers_a = {s["$ref"] for s in d["channels"]["ping:Handler"]["servers"]}
        servers_b = {s["$ref"] for s in d["channels"]["ping:Handler_2"]["servers"]}
        assert servers_a != servers_b

    def test_typical_multibroker_keys_unchanged(self) -> None:
        """Different function names across brokers → keys already unique,
        no discriminator suffix added."""
        kafka = KafkaBroker()
        redis = RedisBroker()

        @kafka.subscriber("ping")
        async def kafka_ping(msg: Any) -> None: ...

        @redis.subscriber("ping")
        async def redis_ping(msg: Any) -> None: ...

        schema = AsyncAPI(kafka, redis).to_specification()
        d = json.loads(schema.to_json())
        channel_keys = list(d["channels"])
        assert channel_keys == ["ping:KafkaPing", "ping:RedisPing"]

    def test_operation_channel_refs_remain_consistent_after_rename(self) -> None:
        """When a channel key gets a `_2` suffix, the operation built for it
        is named after the renamed channel (so its key encodes its channel)
        and its channel.$ref + messages[*].$ref point to the renamed channel."""
        kafka = KafkaBroker()
        redis = RedisBroker()

        @kafka.subscriber("ping")
        async def handler(msg: Any) -> None: ...  # noqa: F811

        @redis.subscriber("ping")
        async def handler(msg: Any) -> None: ...  # noqa: F811

        schema = AsyncAPI(kafka, redis).to_specification()
        d = json.loads(schema.to_json())

        # ping:HandlerSubscribe   → channel ping:Handler   (kafka)
        # ping:Handler_2Subscribe → channel ping:Handler_2 (redis)
        op_b = d["operations"]["ping:Handler_2Subscribe"]
        assert op_b["channel"]["$ref"] == "#/channels/ping:Handler_2"
        for msg in op_b["messages"]:
            assert msg["$ref"].startswith("#/channels/ping:Handler_2/messages/")


class TestOperationToBroker:
    def test_map_built_after_to_specification(self) -> None:
        kafka = KafkaBroker()
        redis = RedisBroker()

        @kafka.subscriber("kafka-only")
        async def kh(msg: Any) -> None: ...

        @redis.subscriber("redis-only")
        async def rh(msg: Any) -> None: ...

        factory = AsyncAPI(kafka, redis)
        assert factory.operation_to_broker == {}

        factory.to_specification()
        assert set(factory.operation_to_broker) == {
            "kafka-only:KhSubscribe",
            "redis-only:RhSubscribe",
        }
        assert factory.operation_to_broker["kafka-only:KhSubscribe"] is kafka
        assert factory.operation_to_broker["redis-only:RhSubscribe"] is redis

    def test_map_resets_on_rebuild(self) -> None:
        """Rebuilding the spec replaces, not appends to, the map."""
        kafka = KafkaBroker()

        @kafka.subscriber("topic-a")
        async def a(msg: Any) -> None: ...

        factory = AsyncAPI(kafka)
        factory.to_specification()
        first = dict(factory.operation_to_broker)

        factory.to_specification()
        assert factory.operation_to_broker == first

    def test_single_broker_map_populated(self) -> None:
        """Single-broker apps still get a map (used by try-it-out as a hint
        but functionally falls back to destination resolution)."""
        kafka = KafkaBroker()

        @kafka.subscriber("queue")
        async def h(msg: Any) -> None: ...

        factory = AsyncAPI(kafka)
        factory.to_specification()
        assert "queue:HSubscribe" in factory.operation_to_broker

    def test_publisher_operation_in_map(self) -> None:
        """Publisher operations are also registered in operation_to_broker so
        try-it-out can dispatch by operation_id (AsyncAPI 3.0 standard ID).
        For publishers the operation key equals the channel key by spec
        convention — no synthetic suffix added."""
        kafka = KafkaBroker()
        redis = RedisBroker()

        @kafka.publisher("k-out")
        async def kp() -> str: ...

        @redis.publisher("r-out")
        async def rp() -> str: ...

        factory = AsyncAPI(kafka, redis)
        factory.to_specification()

        assert set(factory.operation_to_broker) == {
            "k-out:Publisher",
            "r-out:Publisher",
        }
        assert factory.operation_to_broker["k-out:Publisher"] is kafka
        assert factory.operation_to_broker["r-out:Publisher"] is redis
