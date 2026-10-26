"""Canonical JSON (RFC 8785 / JCS) and the single SHA-256 digest rule.
DS-P01-03/P1-1: aliases, key ordering, encoding, separators, string escaping and number
formatting must be identical everywhere a digest, producer key or content hash is computed,
otherwise two components can disagree about the same logical content. Callers pass an already
alias-expanded mapping (``model_dump(mode="json", by_alias=True)``) before digesting a contract
so the canonical text is stable across Python and TypeScript consumers. The rule is RFC 8785
(JSON Canonicalization Scheme): - object members are sorted by UTF-16 code unit of the member
name (not by Unicode code point, so astral names and ``U+E000..U+FFFF`` names sort differently
from Python's default), then serialized with compact separators; - strings are serialized with
ECMAScript ``JSON.stringify`` escaping while non-ASCII text stays literal UTF-8; - numbers are
serialized with ECMAScript ``Number::toString`` (shortest round-trip, decimal form for ``1e-6 <=
|x| < 1e21``, exponent otherwise, ``-0`` becomes ``0``). Only values the specification allows
are accepted: strings, booleans, null, arrays, objects with string keys, and numbers that are
exactly representable as IEEE-754 doubles. Rejected inputs raise :class:`CanonicalJSONError`:
non-finite numbers, integers that are not exactly representable as doubles — either because they
would round (``2**53 + 1``, ``10**30``, ``2**400 + 1``) or because they exceed the double range
(``10**400``) — non-string keys, lone surrogates and any other non-JSON value. Exact integers
above ``2**53 - 1`` are legal and are serialized with ECMAScript's shortest text (``2**60``
becomes ``1152921504606847000``), so the accepted set is the IEEE-754 double set rather than a
Python-specific safe-integer subset. Boundary helpers convert the error into a failure envelope;
this module stays free of runtime imports. Cross-language evidence:
``contracts/fixtures/canonical-vectors.json`` holds the golden vectors that both implementations
must reproduce, and ``scripts/verify_canonical_vectors.mjs`` re-derives each text and digest
with an independent Node implementation (plus an ECMAScript number-formatting oracle used by
``tests/test_canonical.py``). Any allowed digest input is therefore byte-identical across Python
and JavaScript; there is no "portable subset" caveat left.
"""

import hashlib
import json
import math

__all__ = [
    "CanonicalJSONError",
    "canonical_bytes",
    "canonical_json",
    "canonical_number",
    "content_digest",
]

#: Largest integer magnitude that round-trips exactly through an IEEE-754 double.
MAX_SAFE_INTEGER = 2**53 - 1


class CanonicalJSONError(ValueError):
    """The value has no canonical JSON encoding (unsupported, non-finite, unencodable).

    Subclasses ``ValueError`` so Pydantic model validators still report a
    ``ValidationError`` instead of leaking an unexpected exception type.
    """


def _utf16_key(key: str) -> bytes:
    """Sort key for RFC 8785: big-endian UTF-16 code units compare as code units."""
    return key.encode("utf-16-be")


def canonical_number(value: int | float) -> str:
    """ECMAScript ``Number::toString`` text for a finite JSON number (RFC 8785 §3.2.2.3).

    JSON has no integer type, so an integer input is only canonicalizable when it is
    exactly representable as an IEEE-754 double: ``2**60`` is, ``2**53 + 1`` is not and
    would silently round. Exact integers above ``2**53-1`` are therefore serialized
    through the double path, which yields ECMAScript's shortest text (``2**60`` prints
    as ``1152921504606847000``), never the original decimal digits.
    """
    if value == 0:
        return "0"  # also normalizes -0.0
    if isinstance(value, float):
        return _float_number(value)
    if abs(value) <= MAX_SAFE_INTEGER:
        return str(value)
    try:
        exact = float(value)
    except OverflowError as exc:
        raise CanonicalJSONError("canonical JSON numbers must fit an IEEE-754 double") from exc
    if int(exact) != value:
        raise CanonicalJSONError(
            "canonical JSON integers must be exactly representable as IEEE-754 doubles"
        )
    return _float_number(exact)


def _float_number(value: float) -> str:
    if not math.isfinite(value):
        raise CanonicalJSONError("canonical JSON numbers must be finite")
    negative = value < 0
    mantissa, exponent = _shortest(value)
    text = _es6_form(mantissa, exponent)
    return f"-{text}" if negative else text


def _shortest(value: float) -> tuple[str, int]:
    """Split ``repr`` of a finite non-zero float into its shortest mantissa and exponent."""
    text = repr(abs(value))
    mantissa, _, exponent_text = text.lower().partition("e")
    exponent = int(exponent_text) if exponent_text else 0
    return mantissa, exponent


def _es6_form(mantissa: str, exponent: int) -> str:
    """Apply the ECMAScript decimal/exponent thresholds to a shortest decimal string."""
    integer_part, _, fraction_part = mantissa.partition(".")
    digits = (integer_part + fraction_part).lstrip("0")
    scale = exponent - len(fraction_part)
    stripped = digits.rstrip("0")
    if stripped:
        scale += len(digits) - len(stripped)
        digits = stripped
    count = len(digits)
    point = count + scale  # value == 0.<digits> * 10**point
    if count <= point <= 21:
        return digits + "0" * (point - count)
    if 0 < point <= 21:
        return f"{digits[:point]}.{digits[point:]}"
    if -6 < point <= 0:
        return "0." + "0" * -point + digits
    exponent_text = f"{'+' if point - 1 >= 0 else '-'}{abs(point - 1)}"
    if count == 1:
        return f"{digits}e{exponent_text}"
    return f"{digits[0]}.{digits[1:]}e{exponent_text}"


def canonical_json(value: object) -> str:
    """RFC 8785 canonical JSON text; see the module docstring for the exact rule."""
    parts: list[str] = []
    try:
        _serialize(value, parts)
    except RecursionError as exc:
        raise CanonicalJSONError("value has no canonical JSON encoding") from exc
    return "".join(parts)


def _serialize(value: object, parts: list[str]) -> None:
    if value is None:
        parts.append("null")
    elif isinstance(value, bool):
        parts.append("true" if value else "false")
    elif isinstance(value, str):
        parts.append(_escaped_string(value))
    elif isinstance(value, int):
        parts.append(canonical_number(value))
    elif isinstance(value, float):
        parts.append(canonical_number(value))
    elif isinstance(value, list | tuple):
        parts.append("[")
        for index, item in enumerate(value):
            if index:
                parts.append(",")
            _serialize(item, parts)
        parts.append("]")
    elif isinstance(value, dict):
        keys = list(value)
        escaped: dict[str, str] = {}
        for key in keys:
            if not isinstance(key, str):
                raise CanonicalJSONError("canonical JSON object keys must be strings")
            escaped[key] = _escaped_string(key)
        parts.append("{")
        for index, key in enumerate(sorted(keys, key=_utf16_key)):
            if index:
                parts.append(",")
            parts.append(escaped[key])
            parts.append(":")
            _serialize(value[key], parts)
        parts.append("}")
    else:
        raise CanonicalJSONError(f"value has no canonical JSON encoding: {type(value).__name__}")


def _escaped_string(value: str) -> str:
    """ECMAScript ``JSON.stringify`` string escaping with a UTF-8 validity check."""
    try:
        text = json.dumps(value, ensure_ascii=False)
        text.encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CanonicalJSONError("canonical JSON text must be valid UTF-8") from exc
    return text


def canonical_bytes(value: object) -> bytes:
    return canonical_json(value).encode("utf-8")


def content_digest(value: object) -> str:
    """Lowercase SHA-256 hex digest of the canonical JSON encoding of ``value``."""
    return hashlib.sha256(canonical_bytes(value)).hexdigest()
