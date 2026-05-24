from faststream.specification.asyncapi.v3_0_0.schema import Reference


def channel_ref(key: str) -> Reference:
    return Reference(**{"$ref": f"#/channels/{key}"})


def message_ref(channel_key: str, message_name: str) -> Reference:
    return Reference(**{"$ref": f"#/channels/{channel_key}/messages/{message_name}"})
