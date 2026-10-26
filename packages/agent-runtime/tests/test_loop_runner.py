"""Recovery gates pending effects and passes only durable observations to the next model call."""

from dataclasses import replace
from uuid import uuid4

import pytest
from test_agent_loop import build

from agent_runtime.action_dispatch import DispatchResult
from agent_runtime.contracts.decisions import decision_adapter
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import (
    ApprovalRequest,
    PendingExecution,
    ToolCancelled,
    ToolFailed,
    ToolWaitingApproval,
)
from agent_runtime.loop_runner import AgentStepRunner, RecoveredDecision


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_wait", [None, "identity", "checkpoint", "status"])
async def test_pending_execution_recovery_releases_worker_without_model_or_dispatch(bad_wait):
    from agent_runtime.contracts.state import RunStatus

    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    call = frame.action.payload.validated_calls[0]
    pending = PendingExecution(
        status="pending_execution",
        tool_call_id=call.tool_call_id,
        run_id=state.run_id,
        operation_id=uuid4(),
        execution_ref=ResourceRef.model_validate(
            {
                "resourceType": "execution",
                "resourceId": str(uuid4()),
                "resourceVersion": "1",
                "scope": {"scopeId": state.scope_id},
            }
        ),
    )
    recovered = RecoveredDecision(frame, "staged:1", (pending,), (), ())
    order = []

    class Recovery:
        async def recover(self):
            return recovered

    class Never:
        async def decide(self, *args, **kwargs):
            raise AssertionError("waiting execution must not call the model")

        async def dispatch_result(self, *args):
            raise AssertionError("waiting execution must not redispatch")

        async def commit(self, *args):
            raise AssertionError("waiting execution must use its wait committer")

    class Budgets:
        async def read(self, state):
            return budget

    class Output:
        async def finalize_tool(self, frame):
            order.append("sealed")

    class Waiter:
        async def park(self, state, observed):
            assert observed == recovered
            order.append("parked")
            update = {
                "status": RunStatus.WAITING_TOOL,
                "state_version": str(int(state.state_version) + 1),
                "checkpoint_ref": "waiting:1",
            }
            if bad_wait == "identity":
                update["run_id"] = uuid4()
            elif bad_wait == "checkpoint":
                update["checkpoint_ref"] = state.checkpoint_ref
            elif bad_wait == "status":
                update["status"] = RunStatus.RUNNING
            return state.model_copy(update=update)

    runner = AgentStepRunner(
        loop=Never(),
        dispatch=Never(),
        recovery=Recovery(),
        budgets=Budgets(),
        committer=Never(),
        tool_output=Output(),
        execution_waiter=Waiter(),
    )
    if bad_wait is not None:
        with pytest.raises(AgentRuntimeError) as error:
            await runner.run(state, inputs)
        assert error.value.failure.code == "AUDIT_UNAVAILABLE"
        return
    result = await runner.run(state, inputs)
    assert order == ["sealed", "parked"]
    assert result.state.status == RunStatus.WAITING_TOOL
    assert result.outcomes == (pending,)


@pytest.mark.asyncio
async def test_approval_recovery_parks_without_model_or_tool_replay():
    import json
    from importlib.resources import files

    from agent_runtime.contracts.state import RunStatus

    _, state, inputs, budget, _, _ = build("tool")
    data = json.loads(
        files("agent_runtime.contracts").joinpath("fixtures/tools.json").read_text()
    )
    approval = ApprovalRequest.model_validate(data["outcomes"][4]["approval"])
    loop, *_ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    waiting = ToolWaitingApproval(
        status="waiting_approval",
        tool_call_id=frame.action.payload.validated_calls[0].tool_call_id,
        run_id=state.run_id,
        operation_id=approval.binding.operation_id,
        approval=approval,
    )

    class Recovery:
        async def recover(self):
            return RecoveredDecision(frame, "staged:approval", (waiting,), (), ())

    class Never:
        async def decide(self, *args, **kwargs):
            raise AssertionError("approval wait must not call the model")

        async def dispatch_result(self, *args):
            raise AssertionError("approval wait must not execute a tool")

        async def commit(self, *args):
            raise AssertionError("approval wait must not commit another action")

    class Budgets:
        async def read(self, current):
            return budget

    class Waiter:
        async def park(self, current, observed):
            assert observed.outcomes == (waiting,)
            return current.model_copy(
                update={
                    "status": RunStatus.WAITING_USER,
                    "state_version": str(int(current.state_version) + 1),
                    "checkpoint_ref": "waiting:approval",
                }
            )

    result = await AgentStepRunner(
        loop=Never(),
        dispatch=Never(),
        recovery=Recovery(),
        budgets=Budgets(),
        committer=Never(),
        execution_waiter=Waiter(),
    ).run(state, inputs)
    assert result.state.status == RunStatus.WAITING_USER
    assert result.outcomes == (waiting,)


@pytest.mark.asyncio
async def test_pending_call_is_not_reexecuted_after_recovery():
    _, state, inputs, budget, _, _ = build("tool")

    class Loop:
        async def decide(self, *args, **kwargs):
            raise AssertionError("recovery must not call the model")

    class Dispatch:
        async def dispatch_result(self, *args):
            raise AssertionError("recovery must not execute the tool")

    class Recovery:
        async def recover(self):
            from test_decision_validation import facts, tool_action

            from agent_runtime.agent_loop import DecisionFrame
            from agent_runtime.contracts.transactions import ActorContext

            _, context, _ = facts()
            return RecoveredDecision(
                frame=DecisionFrame(
                    actor=ActorContext(
                        scope_id=state.scope_id,
                        actor_id="owner",
                        auth_revision="1",
                        grants_ref="grant:1",
                    ),
                    context=context,
                    attempt_id=uuid4(),
                    action=decision_adapter.validate_python(tool_action()),
                ),
                checkpoint_ref="staged:1",
                outcomes=(),
                unresolved_call_ids=(uuid4(),),
                observation_refs=(),
            )

    class Budgets:
        async def read(self, state):
            return budget

    class Committer:
        async def commit(self, *args):
            raise AssertionError("unresolved tools must not commit an action")

    runner = AgentStepRunner(
        loop=Loop(),
        dispatch=Dispatch(),
        recovery=Recovery(),
        budgets=Budgets(),
        committer=Committer(),
    )
    with pytest.raises(AgentRuntimeError) as error:
        await runner.run(state, inputs)
    assert error.value.failure.code == "SIDE_EFFECT_UNKNOWN"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["failed", "cancelled", "pending_execution"])
async def test_recovered_effect_needing_reconciliation_stops_model(status):
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    call = frame.action.payload.validated_calls[0]
    operation_id = uuid4()
    if status == "pending_execution":
        outcome = PendingExecution(
            tool_call_id=call.tool_call_id,
            run_id=state.run_id,
            operation_id=operation_id,
            status="pending_execution",
            execution_ref=ResourceRef.model_validate(
                {
                    "resourceType": "execution",
                    "resourceId": str(uuid4()),
                    "resourceVersion": "1",
                    "scope": {"scopeId": state.scope_id},
                }
            ),
        )
    else:
        outcome_type = ToolFailed if status == "failed" else ToolCancelled
        outcome = outcome_type(
            tool_call_id=call.tool_call_id,
            run_id=state.run_id,
            operation_id=operation_id,
            status=status,
            effect="unknown",
            effect_refs=(),
            failure=FailureEnvelope(
                code="SIDE_EFFECT_UNKNOWN",
                category="tool",
                retryable=False,
                public_message="Remote effect has not been reconciled.",
            ),
        )

    class NoModel:
        async def decide(self, *args, **kwargs):
            raise AssertionError("unknown effect must not reach the model")

    class NoDispatch:
        async def dispatch_result(self, *args):
            raise AssertionError("unknown effect must not be redispatched")

    class Recovery:
        async def recover(self):
            return RecoveredDecision(frame, "staged:1", (outcome,), (), ())

    class Budgets:
        async def read(self, _):
            return budget

    class NoCommitter:
        async def commit(self, *args):
            raise AssertionError("unknown effect must not commit another action")

    runner = AgentStepRunner(
        loop=NoModel(),
        dispatch=NoDispatch(),
        recovery=Recovery(),
        budgets=Budgets(),
        committer=NoCommitter(),
    )
    with pytest.raises(AgentRuntimeError) as error:
        await runner.run(state, inputs)
    assert error.value.failure.code == (
        "CONTROL_PENDING" if status == "pending_execution" else "SIDE_EFFECT_UNKNOWN"
    )


@pytest.mark.asyncio
async def test_new_unknown_tool_effect_stops_next_decision_in_same_run():
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    call = frame.action.payload.validated_calls[0]
    outcome = ToolFailed(
        tool_call_id=call.tool_call_id,
        run_id=state.run_id,
        operation_id=uuid4(),
        status="failed",
        effect="unknown",
        effect_refs=(),
        failure=FailureEnvelope(
            code="SIDE_EFFECT_UNKNOWN",
            category="tool",
            retryable=False,
            public_message="Remote effect has not been reconciled.",
        ),
    )
    calls = 0

    class OneDecision:
        async def decide(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls > 1:
                raise AssertionError("unknown effect must stop before another model call")
            return frame

    class Dispatch:
        async def dispatch_result(self, current, _, candidate):
            assert candidate == frame
            staged = current.model_copy(update={"checkpoint_ref": "staged:1"})
            return DispatchResult(staged, (outcome,))

    class Recovery:
        def __init__(self):
            self.calls = 0

        async def recover(self):
            self.calls += 1
            if self.calls == 1:
                return None
            return RecoveredDecision(frame, "staged:1", (outcome,), (), ())

    class Budgets:
        async def read(self, _):
            return budget

    class NoCommitter:
        async def commit(self, *_):
            raise AssertionError("unknown effect must not commit another action")

    runner = AgentStepRunner(
        loop=OneDecision(),
        dispatch=Dispatch(),
        recovery=Recovery(),
        budgets=Budgets(),
        committer=NoCommitter(),
    )
    with pytest.raises(AgentRuntimeError) as error:
        await runner.run(state, inputs)
    assert error.value.failure.code == "SIDE_EFFECT_UNKNOWN"
    assert calls == 1


@pytest.mark.asyncio
async def test_completed_recovered_tool_result_becomes_next_context_source():
    loop, state, inputs, budget, _, _ = build("tool")
    tool_frame = await loop.decide(state, inputs, budget)
    answer_frame = replace(
        tool_frame,
        action=decision_adapter.validate_python(
            {"kind": "respond", "payload": {"answerRequestRef": "answer:1"}}
        ),
    )
    ref = ResourceRef.model_validate(
        {
            "resourceType": "tool_call_result",
            "resourceId": str(uuid4()),
            "resourceVersion": "1",
            "scope": {"scopeId": state.scope_id},
        }
    )
    published: list[str] = []

    class Loop:
        async def decide(self, current, current_inputs, _, *, observation_refs=()):
            assert published == ["tool-output"]
            assert current.checkpoint_ref == "staged:1"
            assert current_inputs.checkpoint_ref == "staged:1"
            assert observation_refs == (ref,)
            return answer_frame

    class Dispatch:
        async def dispatch_result(self, current, _, frame):
            assert frame == answer_frame
            return DispatchResult(current.model_copy(update={"checkpoint_ref": "staged:2"}), ())

    class Recovery:
        async def recover(self):
            return RecoveredDecision(
                frame=tool_frame,
                checkpoint_ref="staged:1",
                outcomes=(),
                unresolved_call_ids=(),
                observation_refs=(ref,),
            )

    class Budgets:
        async def read(self, _):
            return budget

    class Committer:
        async def commit(self, current, frame):
            assert frame == answer_frame
            return current.model_copy(
                update={"state_version": "2", "checkpoint_ref": "committed:2"}
            )

    class ToolOutput:
        async def finalize_tool(self, frame):
            assert frame == tool_frame
            published.append("tool-output")

    runner = AgentStepRunner(
        loop=Loop(),
        dispatch=Dispatch(),
        recovery=Recovery(),
        budgets=Budgets(),
        committer=Committer(),
        tool_output=ToolOutput(),
    )
    result = await runner.run(state, inputs)
    assert result.action.kind == "respond"
    assert result.state.checkpoint_ref == "committed:2"
    assert result.state.state_version == "2"


@pytest.mark.asyncio
async def test_recovered_answer_is_committed_without_second_model_call():
    loop, state, inputs, budget, _, _ = build("answer")
    frame = await loop.decide(state, inputs, budget)

    class NoModel:
        async def decide(self, *args, **kwargs):
            raise AssertionError("staged answer must not be decided again")

    class NoDispatch:
        async def dispatch_result(self, *args):
            raise AssertionError("staged answer must not be dispatched again")

    class Recovery:
        async def recover(self):
            return RecoveredDecision(frame, "staged:answer", (), (), ())

    class NoBudget:
        async def read(self, _):
            raise AssertionError("staged answer needs no new model budget")

    class Committer:
        async def commit(self, current, recovered):
            assert current.checkpoint_ref == "staged:answer"
            assert recovered == frame
            return current.model_copy(
                update={"state_version": "2", "checkpoint_ref": "committed:answer"}
            )

    result = await AgentStepRunner(
        loop=NoModel(),
        dispatch=NoDispatch(),
        recovery=Recovery(),
        budgets=NoBudget(),
        committer=Committer(),
    ).run(state, inputs)
    assert result.state.state_version == "2"


@pytest.mark.asyncio
async def test_runner_rejects_commit_adapter_that_did_not_advance_state():
    loop, state, inputs, budget, _, _ = build("answer")

    class Dispatch:
        async def dispatch_result(self, current, _, frame):
            return DispatchResult(current, ())

    class Recovery:
        async def recover(self):
            return None

    class Budgets:
        async def read(self, _):
            return budget

    class BrokenCommitter:
        async def commit(self, current, _):
            return current

    runner = AgentStepRunner(
        loop=loop,
        dispatch=Dispatch(),
        recovery=Recovery(),
        budgets=Budgets(),
        committer=BrokenCommitter(),
    )
    with pytest.raises(AgentRuntimeError) as error:
        await runner.run(state, inputs)
    assert error.value.failure.code == "AUDIT_UNAVAILABLE"
