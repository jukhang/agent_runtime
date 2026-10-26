"""Project-independent wire primitives."""

import unicodedata
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, StringConstraints, model_validator
from pydantic.alias_generators import to_camel


def _printable_identifier(value: str) -> str:
    r"""Reject control/format/surrogate code points that break logs, JSON and indexes.

    The JSON Schema pattern (``^[^\s\p{Cc}\p{Cf}]+$``) states the same rule for other
    consumers; this validator keeps the check explicit in Python and also covers
    surrogate code points, which a pattern cannot express portably.
    """
    for character in value:
        if unicodedata.category(character) in {"Cc", "Cf", "Cs"} or character.isspace():
            raise ValueError(
                "identifier must be printable text without control, format or separator characters"
            )
    return value


#: Free-form label: any printable Unicode text without control/format characters.
Identifier = Annotated[
    str,
    StringConstraints(strict=True, min_length=1, max_length=255, pattern=r"^[^\s\p{Cc}\p{Cf}]+$"),
    AfterValidator(_printable_identifier),
]

#: Machine key for dedup/producer/handler identities: printable ASCII only.
#: Persisted in unique indexes and compared across drivers, so it must not contain
#: whitespace, control characters or non-ASCII look-alikes.
MachineKey = Annotated[
    str,
    StringConstraints(strict=True, min_length=1, max_length=255, pattern=r"^[\x21-\x7e]+$"),
]


def _bounded_revision(value: str) -> str:
    if int(value) > 2**63 - 1:
        raise ValueError("revision exceeds PostgreSQL BIGINT")
    return value


Revision = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^(0|[1-9][0-9]*)$", max_length=19),
    AfterValidator(_bounded_revision),
]


class Contract(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
        frozen=True,
    )

    @model_validator(mode="before")
    @classmethod
    def exact_protocol_revision(cls, value: object) -> object:
        if isinstance(value, dict):
            for key in (
                "schemaRevision",
                "schema_revision",
                "protocolRevision",
                "protocol_revision",
            ):
                if key in value and type(value[key]) is not int:
                    raise ValueError("schemaRevision must be an integer, not bool or float")
        return value
