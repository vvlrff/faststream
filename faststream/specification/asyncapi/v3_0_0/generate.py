import string
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.parse import urlparse

from faststream._internal._compat import DEF_KEY
from faststream._internal.constants import ContentTypes
from faststream.specification.asyncapi.utils import (
    resolve_key,
    clear_key,
    convert_list_of_dict_to_dict,
    move_pydantic_refs,
)
from faststream.specification.asyncapi.v3_0_0.refs import channel_ref, message_ref
from faststream.specification.asyncapi.v3_0_0.schema import (
    ApplicationInfo,
    ApplicationSchema,
    Channel,
    Components,
    Contact,
    ExternalDocs,
    License,
    Message,
    Operation,
    Reference,
    Server,
    Tag,
)
from faststream.specification.asyncapi.v3_0_0.schema.bindings import (
    OperationBinding,
    http as http_bindings,
)
from faststream.specification.asyncapi.v3_0_0.schema.operations import Action

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyHttpUrl
    from faststream._internal.broker import BrokerUsecase
    from faststream._internal.types import ConnectionType, MsgType
    from faststream.asgi.handlers import HttpHandler
    from faststream.specification.schema.extra import (
        Contact as SpecContact,
        ContactDict,
        ExternalDocs as SpecDocs,
        ExternalDocsDict,
        License as SpecLicense,
        LicenseDict,
        Tag as SpecTag,
        TagDict,
    )


def get_app_schema(
    *brokers: "BrokerUsecase[Any, Any]",
    title: str,
    app_version: str,
    schema_version: str,
    description: str | None,
    terms_of_service: Optional["AnyHttpUrl"],
    contact: Union["SpecContact", "ContactDict", dict[str, Any]] | None,
    license: Union["SpecLicense", "LicenseDict", dict[str, Any]] | None,
    identifier: str | None,
    tags: Sequence[Union["SpecTag", "TagDict", dict[str, Any]]] | None,
    external_docs: Union["SpecDocs", "ExternalDocsDict", dict[str, Any]] | None,
    http_handlers: list[tuple[str, "HttpHandler"]],
    operation_to_broker: dict[str, "BrokerUsecase[Any, Any]"] | None = None,
) -> ApplicationSchema:
    """Get the application schema.

    If `operation_to_broker` is provided, it is populated with
    `{operation_key: broker}` after key uniqueness adjustments.
    """
    if any(br.specification.security for br in brokers):
        list_of_specification_security = (
            br.specification.security.get_schema()
            for br in brokers
            if br.specification.security
        )
        security_schemes: dict[str, dict[str, Any]] | None = convert_list_of_dict_to_dict(
            list_of_specification_security,
            "specification security",
        )
    else:
        security_schemes = None

    servers, broker_servers = get_broker_server(*brokers)

    channels: dict[str, Channel] = {}
    operations: dict[str, Operation] = {}

    for broker, srv_names in broker_servers.items():
        populate_broker_spec(
            broker, srv_names, channels, operations, operation_to_broker,
        )

    messages: dict[str, Message] = {}
    payloads: dict[str, dict[str, Any]] = {}

    added_channels, added_operations = get_asgi_routes(http_handlers)
    channels.update(added_channels)
    operations.update(added_operations)

    for channel_name, channel in channels.items():
        msgs: dict[str, Message | Reference] = {}
        for message_name, message in channel.messages.items():
            assert isinstance(message, Message)

            msgs[message_name] = _resolve_msg_payloads(
                message_name,
                message,
                channel_name,
                payloads,
                messages,
            )

        channel.messages = msgs

    return ApplicationSchema(
        info=ApplicationInfo(
            title=title,
            version=app_version,
            description=description,
            termsOfService=terms_of_service,
            contact=Contact.from_spec(contact),
            license=License.from_spec(license),
            tags=[Tag.from_spec(tag) for tag in tags] if tags else None,
            externalDocs=ExternalDocs.from_spec(external_docs),
        ),
        asyncapi=schema_version,
        defaultContentType=ContentTypes.JSON.value,
        id=identifier,
        servers=servers,
        channels=channels,
        operations=operations,
        components=Components(
            messages=messages,
            schemas=payloads,
            securitySchemes=security_schemes,
        ),
    )


def get_broker_server(
    *brokers: "BrokerUsecase[MsgType, ConnectionType]",
) -> tuple[
    dict[str, Server],
    dict["BrokerUsecase[Any, Any]", list[str]],
]:
    """Get the broker server for an application."""
    servers: list[Server] = []
    broker_servers: list[tuple[BrokerUsecase[Any, Any], Server]] = []

    for broker in brokers:
        specification = broker.specification

        broker_meta: dict[str, Any] = {
            "protocol": specification.protocol,
            "protocolVersion": specification.protocol_version,
            "description": specification.description,
            "tags": [Tag.from_spec(tag) for tag in specification.tags] or None,
            # TODO
            # "variables": "",
            # "bindings": "",
        }

        if specification.security is not None:
            broker_meta["security"] = [
                Reference(**{"$ref": f"#/components/securitySchemes/{sec}"})
                for security_item in specification.security.get_requirement()
                for sec in security_item
            ]

        for url in specification.url:
            parsed_url = urlparse(url if "://" in url else f"//{url}")
            server = Server(
                host=parsed_url.netloc,
                pathname=parsed_url.path,
                **broker_meta,
            )

            # deduplicate servers
            broker_servers.append((broker, server))
            if server not in servers:
                servers.append(server)

    servers_by_names: dict[str, Server] = {}
    single_server = len(servers) == 1
    for i, server in enumerate(servers, 1):
        server_name = "development" if single_server else f"Server{i}"
        servers_by_names[server_name] = server

    broker_server_names: dict[BrokerUsecase[Any, Any], list[str]] = {}
    for name, server in servers_by_names.items():
        for broker, br_server in broker_servers:
            if server == br_server:
                broker_server_names.setdefault(broker, []).append(name)

    return servers_by_names, broker_server_names


def populate_broker_spec(
    broker: "BrokerUsecase[MsgType, ConnectionType]",
    servers: list[str] | None,
    channels: dict[str, Channel],
    operations: dict[str, Operation],
    operation_to_broker: dict[str, "BrokerUsecase[Any, Any]"] | None = None,
) -> None:
    # Snapshot keys owned by earlier brokers — used to tell cross-broker
    # collisions (rename + ChannelKeyCollisionWarning) from within-broker
    # ones (overwrite + legacy RuntimeWarning).
    pre_channels = set(channels)
    pre_operations = set(operations)

    server_refs = [
        Reference(**{"$ref": f"#/servers/{server_name}"})
        for server_name in (servers or ())
    ] or None

    for sub in filter(lambda s: s.specification.include_in_schema, broker.subscribers):
        for sub_key, sub_channel in sub.schema().items():
            ch_key = resolve_key(clear_key(sub_key), channels, pre_channels, "channel")
            channels[ch_key] = Channel.from_sub(sub_key, sub_channel, servers=server_refs)

            title = sub.specification.config.title_
            op_base = f"{ch_key}Subscribe" if title in (None, "/") else title
            op_key = resolve_key(op_base, operations, pre_operations, "operation")
            operations[op_key] = Operation.from_sub(
                messages=[message_ref(ch_key, m) for m in channels[ch_key].messages],
                channel=channel_ref(ch_key),
                operation=sub_channel.operation,
            )
            if operation_to_broker is not None:
                operation_to_broker[op_key] = broker

    for pub in filter(lambda p: p.specification.include_in_schema, broker.publishers):
        for pub_key, pub_channel in pub.schema().items():
            ch_key = resolve_key(clear_key(pub_key), channels, pre_channels, "channel")
            channels[ch_key] = Channel.from_pub(pub_key, pub_channel, servers=server_refs)
            operations[ch_key] = Operation.from_pub(
                messages=[message_ref(ch_key, m) for m in channels[ch_key].messages],
                channel=channel_ref(ch_key),
                operation=pub_channel.operation,
            )
            if operation_to_broker is not None:
                operation_to_broker[ch_key] = broker


def get_broker_channels(
    broker: "BrokerUsecase[MsgType, ConnectionType]",
    servers: list[str] | None = None,
) -> tuple[dict[str, Channel], dict[str, Operation]]:
    """Deprecated. Use `populate_broker_spec` and pass accumulators in.

    Kept as a thin shim for backwards compatibility with external code
    (plugins, custom AsyncAPI generators) that imported this name.
    """
    warnings.warn(
        "get_broker_channels is deprecated; use populate_broker_spec instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    channels: dict[str, Channel] = {}
    operations: dict[str, Operation] = {}
    populate_broker_spec(broker, servers, channels, operations, None)
    return channels, operations


def get_asgi_routes(
    http_handlers: list[tuple[str, "HttpHandler"]],
) -> tuple[dict[str, Channel], dict[str, Operation]]:
    """Get the ASGI routes for an application."""
    channels: dict[str, Channel] = {}
    operations: dict[str, Operation] = {}
    for path, asgi_app in http_handlers:
        if asgi_app.include_in_schema:
            channel = Channel(
                description=asgi_app.description,
                address=path,
                messages={},
            )
            channel_name = "".join(
                char
                for char in path.strip("/").replace("/", "_")
                if char in string.ascii_letters + string.digits + "_"
            )
            channel_name = f"{channel_name}:HttpChannel"
            channels[channel_name] = channel
            operation = Operation(
                action=Action.RECEIVE,
                channel=channel_ref(channel_name),
                bindings=OperationBinding(
                    http=http_bindings.OperationBinding(
                        method=_get_http_binding_method(asgi_app.methods),
                        bindingVersion="0.3.0",
                    ),
                ),
            )
            operations[channel_name] = operation
    return channels, operations


def _get_http_binding_method(methods: Sequence[str]) -> str:
    return next((method for method in methods if method != "HEAD"), "HEAD")


def _resolve_msg_payloads(
    message_name: str,
    m: Message,
    channel_name: str,
    payloads: dict[str, Any],
    messages: dict[str, Any],
) -> Reference:
    assert isinstance(m.payload, dict)

    m.payload = move_pydantic_refs(m.payload, DEF_KEY)

    message_name = clear_key(message_name)
    channel_name = clear_key(channel_name)

    if DEF_KEY in m.payload:
        payloads.update(m.payload.pop(DEF_KEY))

    one_of = m.payload.get("oneOf", None)
    if isinstance(one_of, dict):
        one_of_list = []
        processed_payloads: dict[str, dict[str, Any]] = {}
        for name, payload in one_of.items():
            # Promote nested Pydantic $defs from each payload into components/schemas
            # so that referenced nested models are available globally.
            if isinstance(payload, dict) and DEF_KEY in payload:
                defs = payload.pop(DEF_KEY) or {}
                for def_name, def_schema in defs.items():
                    payloads[clear_key(def_name)] = def_schema
            processed_payloads[clear_key(name)] = payload
            one_of_list.append(Reference(**{"$ref": f"#/components/schemas/{name}"}))

        payloads.update(processed_payloads)
        m.payload["oneOf"] = one_of_list
        assert m.title
        messages[clear_key(m.title)] = m
        return Reference(
            **{"$ref": f"#/components/messages/{channel_name}:{message_name}"},
        )

    payloads.update(m.payload.pop(DEF_KEY, {}))
    payload_name = m.payload.get("title", f"{channel_name}:{message_name}:Payload")
    payload_name = clear_key(payload_name)

    if payload_name in payloads and payloads[payload_name] != m.payload:
        warnings.warn(
            f"Overwriting the message schema, data types have the same name: `{payload_name}`",
            RuntimeWarning,
            stacklevel=1,
        )

    payloads[payload_name] = m.payload
    m.payload = {"$ref": f"#/components/schemas/{payload_name}"}
    assert m.title
    messages[clear_key(m.title)] = m
    return Reference(
        **{"$ref": f"#/components/messages/{channel_name}:{message_name}"},
    )
