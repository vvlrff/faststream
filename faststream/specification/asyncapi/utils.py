import warnings
from collections.abc import Iterable
from typing import Any


class ChannelKeyCollisionWarning(RuntimeWarning):
    """A channel/operation key collided across brokers and was renamed.

    Subclass of `RuntimeWarning` so existing `RuntimeWarning` filters keep
    catching it; downstream code can also filter on this class directly
    for stable, text-independent matching.
    """


def to_camelcase(*names: str) -> str:
    return " ".join(names).replace("_", " ").title().replace(" ", "")


def convert_list_of_dict_to_dict(
    list_of: Iterable[dict[str, Any]],
    warn: str,
) -> dict[str, Any]:
    items: dict[str, Any] = {}
    for it in list_of:
        for key, value in it.items():
            if (exist := items.get(key)) and value != exist:
                warnings.warn(
                    f"Overwrite broker {warn} for an application, {warn} have the same names: `{key}`",
                    RuntimeWarning,
                    stacklevel=1,
                )
            items[key] = value

    return items


def _next_free_key(key: str, existing: dict[str, Any]) -> str:
    i = 2
    while f"{key}_{i}" in existing:
        i += 1
    return f"{key}_{i}"


def resolve_key(
    key: str,
    existing: dict[str, Any],
    pre_existing: set[str],
    kind: str,
) -> str:
    # stacklevel=4 walks: warn → resolve_key → populate_broker_* → generate caller,
    # so the warning surfaces at the user-facing entry. Update if the call chain changes.
    if key not in existing:
        return key
    if key in pre_existing:
        new = _next_free_key(key, existing)
        warnings.warn(
            f"`{key}` already used across brokers; renamed to `{new}`. "
            "Give subscribers/publishers distinct names to avoid this.",
            ChannelKeyCollisionWarning,
            stacklevel=4,
        )
        return new
    warnings.warn(
        f"Overwrite {kind} handler, {kind}s have the same names: `{key}`",
        RuntimeWarning,
        stacklevel=4,
    )
    return key


def resolve_payloads(
    payloads: list[tuple["dict[str, Any]", str]],
    extra: str = "",
    served_words: int = 1,
) -> "dict[str, Any]":
    ln = len(payloads)
    payload: dict[str, Any]
    if ln > 1:
        one_of_payloads = {}

        for body, handler_name in payloads:
            title = body["title"]
            words = title.split(":")

            if len(words) > 1:  # not pydantic model case
                body["title"] = title = ":".join(
                    filter(
                        bool,
                        (
                            handler_name,
                            extra if extra not in words else "",
                            *words[served_words:],
                        ),
                    ),
                )

            one_of_payloads[title] = body

        payload = {"oneOf": one_of_payloads}

    elif ln == 1:
        payload = payloads[0][0]

    else:
        payload = {}

    return payload


def clear_key(key: str) -> str:
    return key.replace("/", ".")


def move_pydantic_refs(
    original: Any,
    key: str,
) -> Any:
    """Remove pydantic references and replacem them by real schemas."""
    if not isinstance(original, dict):
        return original

    data = original.copy()

    for k in data:
        item = data[k]

        if isinstance(item, str):
            if key in item:
                data[k] = data[k].replace(key, "components/schemas")

        elif isinstance(item, dict):
            data[k] = move_pydantic_refs(data[k], key)

        elif isinstance(item, list):
            for i in range(len(data[k])):
                data[k][i] = move_pydantic_refs(item[i], key)

    if (
        isinstance(desciminator := data.get("discriminator"), dict)
        and "propertyName" in desciminator
    ):
        data["discriminator"] = desciminator["propertyName"]

    return data
