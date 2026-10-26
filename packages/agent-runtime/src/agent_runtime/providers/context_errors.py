"""A bounded vendor error grammar, not a semantic message-keyword classifier."""

import re

_VLLM_CONTEXT = re.compile(
    r"This model's maximum context length is ([0-9]{1,18}) tokens\. "
    r"However, you requested ([0-9]{1,18}) output tokens and your prompt contains "
    r"at least ([0-9]{1,18}) input tokens, for a total of at least ([0-9]{1,18}) tokens\. "
    r"Please reduce the length of the input prompt or the number of requested output tokens\. "
    r"\(parameter=input_tokens, value=([0-9]{1,18})\)"
)


def vllm_context_error(error: object) -> bool:
    """Accept only the observed vLLM BadRequest contract with consistent counts."""
    if (
        not isinstance(error, dict)
        or error.get("type") != "BadRequestError"
        or type(error.get("code")) is not int
        or error["code"] != 400
        or not isinstance(error.get("message"), str)
        or len(error["message"]) > 2048
    ):
        return False
    match = _VLLM_CONTEXT.fullmatch(error["message"])
    if match is None:
        return False
    maximum, output, prompt, total, value = map(int, match.groups())
    return (
        maximum > 0
        and output > 0
        and prompt > 0
        and prompt == value
        and prompt + output == total
        and total > maximum
    )
