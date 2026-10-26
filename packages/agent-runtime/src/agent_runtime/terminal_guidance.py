"""Public terminal guidance is not an execution or retry authorization."""

from dataclasses import dataclass
from typing import Literal

from agent_runtime.contracts.failure import FailureEnvelope, RecoveryOption


@dataclass(frozen=True, slots=True)
class TerminalGuidance:
    message: str
    next_steps: tuple[RecoveryOption, ...]


def terminal_guidance(
    status: Literal["failed", "cancelled", "partial"],
    *,
    failure: FailureEnvelope | None = None,
    retained_count: int = 0,
    unresolved_effects: bool = False,
) -> TerminalGuidance:
    retained = f" 已保留 {retained_count} 项已有成果。" if retained_count else ""
    if status == "cancelled":
        return TerminalGuidance(
            "任务已按你的要求取消，执行已确认停止。" + retained,
            (RecoveryOption(action="ask_user", label="基于已有成果提出下一步任务"),),
        )
    if status == "partial":
        return TerminalGuidance(
            "任务仅部分完成，请查看已交付内容及尚未完成的范围。" + retained,
            (RecoveryOption(action="ask_user", label="说明需要继续完成或调整的部分"),),
        )
    if failure is None:
        raise ValueError("a failed terminal requires a structured reason")
    reason = failure.public_message.strip() or "本轮任务未能完成。"
    if unresolved_effects or failure.code == "SIDE_EFFECT_UNKNOWN":
        return TerminalGuidance(
            reason + retained + " 部分操作的执行结果尚待核实，请勿重复提交相同操作。",
            (RecoveryOption(action="manual_attention", label="先核实外部操作结果，再决定下一步"),),
        )
    if failure.code == "USAGE_UNRESOLVED":
        return TerminalGuidance(
            reason + retained + " 请先人工核实模型调用用量并结算未知预留，不要直接重试。",
            (RecoveryOption(action="manual_attention", label="核实模型用量并结算未知预留后继续"),),
        )
    labels = {
        "authorization": "检查资源权限或联系管理员后再继续",
        "context": "补充必要资料，或缩小本轮处理范围后继续",
        "budget": "调整任务范围或运行预算后继续",
        "concurrency": "刷新目标资源，确认最新版本后继续",
        "validation": "检查并补充任务参数后继续",
    }
    option = RecoveryOption(
        action="ask_user",
        label=labels.get(failure.category, "查看已有成果，选择稍后重试或调整处理方式"),
    )
    return TerminalGuidance(reason + retained, (option,))


__all__ = ["TerminalGuidance", "terminal_guidance"]
