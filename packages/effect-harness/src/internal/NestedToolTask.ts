/** Nested result/accounting sink for the shared leased invocation pipeline. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import type * as Registry from '../Registry.js'
import * as Task from '../Task.js'
import { TaskExecution } from '../TaskExecution.js'
import type * as TaskAccess from '../TaskExecution.js'
import * as ToolResult from '../ToolResult.js'
import type * as Invocation from '../ToolInvocation.js'
import * as Transaction from '../Transaction.js'
import * as Usage from '../Usage.js'
import type * as StorageFormat from '../StorageFormat.js'
import * as Round from './ToolRound.js'
import * as Nested from './NestedInvocation.js'
import * as ToolTask from './ToolTask.js'

export const contract = {
  kind: 'harness.tool.nested',
  version: 1,
  input: Nested.InputSchema,
  checkpoint: Round.CheckpointSchema,
  success: Nested.CompletionSchema,
  failure: Schema.Json,
} as const
const disposals = new WeakMap<Task.Any, () => void>()
export const clear = (definition: Task.Any) => disposals.get(definition)?.()
export const make = (
  registry: Registry.Service,
  inspectFormat: Effect.Effect<StorageFormat.Format, StorageFormat.Failure>,
) => {
  const runner = ToolTask.pipeline(registry)
  const normalize = (input: Nested.Input): ToolTask.PipelineInput => ({
    call: input.call,
    replay: input.replay,
    offered: input.contract.tool,
    options: { agent: input.agent },
    caller: 'tools',
    retention: input.retention,
    progress: input.progress,
    progressEnabled: input.progressEnabled,
    resultContract: input.contract,
  })
  const sink = (execution: TaskAccess.Access, input: Nested.Input): ToolTask.Sink => {
    const assert = (tx: Transaction.Transaction) =>
      Nested.protection(inspectFormat).pipe(
        Effect.andThen(Nested.assert(tx, execution.conversationId, execution.taskId, input)),
      )
    const record = (
      tx: Transaction.Transaction,
      attempt: Usage.AttemptId,
      outcome: Usage.Outcome,
      spend?: Usage.Cost,
    ) =>
      Usage.record(tx, {
        conversationId: execution.conversationId,
        identity: {
          _tag: 'nestedCall',
          taskId: execution.taskId,
          callId: input.logicalCallId,
          attempt,
        },
        outcome,
        ...(spend === undefined
          ? {}
          : { usage: { _tag: 'tool' as const, name: input.call.name, cost: spend } }),
      })
    const finish = Effect.fnUntraced(function* (
      tx: Transaction.Transaction,
      result: ToolResult.Result,
      status: Invocation.WireReceipt['status'],
      attempt: Usage.AttemptId,
      outcome: Usage.Outcome,
      taskOutcome: Task.Outcome<typeof contract>,
      spend?: Usage.Cost,
    ) {
      const handle = yield* assert(tx)
      const old = yield* Transaction.snapshot(
        tx,
        Nested.results,
        Nested.target(input.parentTaskId, execution.taskId),
      )
      if (Option.isNone(old) || old.value.value.receipt !== undefined)
        return yield* Nested.error(
          'conflict',
          'The parent result member is absent or already settled',
        )
      const unavailable = status !== 'succeeded' && status !== 'failed'
      const projection = ToolResult.nested(result, input.contract.structuredOutput)
      const receipt: Invocation.WireReceipt = {
        parentTaskId: input.parentTaskId,
        key: input.key,
        taskId: execution.taskId,
        callId: input.logicalCallId,
        name: input.call.name,
        attempt,
        status,
        result: projection,
        ...(!unavailable && !input.contract.resultChannels && result.details !== undefined
          ? { legacyValue: result.details }
          : {}),
      }
      if (handle.callId !== receipt.callId)
        return yield* Nested.error('conflict', 'The logical call identity changed')
      yield* record(tx, attempt, outcome, spend)
      yield* Transaction.setDocument(
        tx,
        Nested.results,
        Nested.target(input.parentTaskId, execution.taskId),
        { receipt },
      )
      yield* execution.progress(tx, contract, { status: 'completing', outcome: taskOutcome })
    })
    return {
      assert,
      recordAttempt: (tx, attempt) =>
        assert(tx).pipe(
          Effect.andThen(
            record(tx, attempt, {
              _tag: 'interrupted',
              reason: 'Prior safe nested invocation did not publish a result',
            }),
          ),
        ),
      fatal: (tx, result, error, spend, attempt) =>
        finish(
          tx,
          result,
          'faulted',
          attempt,
          { _tag: 'failed', error },
          { status: 'failed', error: { message: 'Nested invocation failed' }, result: error },
          spend,
        ),
      complete: (tx, result, meta) => {
        const marker = { taskId: execution.taskId, callId: input.logicalCallId }
        let status: Invocation.WireReceipt['status'] = result.isError ? 'failed' : 'succeeded'
        if (meta.denied) status = 'denied'
        if (meta.interrupted && !meta.denied) status = 'interrupted'
        let outcome: Usage.Outcome = result.isError
          ? { _tag: 'failed', error: marker }
          : { _tag: 'completed', value: marker }
        if (meta.interrupted)
          outcome = {
            _tag: 'interrupted',
            reason: 'Unsafe incomplete nested handler was not repeated',
          }
        return finish(
          tx,
          result,
          status,
          meta.attempt,
          outcome,
          result.isError
            ? { status: 'failed', error: { message: 'Nested call failed' }, result: marker }
            : { status: 'completed', result: marker },
          meta.denied ? meta.observedSpend : result.spend,
        )
      },
      cancel: (tx, result, spend, attempt, started) =>
        finish(
          tx,
          result,
          'aborted',
          attempt,
          {
            _tag: 'interrupted',
            reason: started
              ? 'Cancelled started nested handler; external outcome unknown'
              : 'Cancelled before handler',
          },
          { status: 'aborted', reason: 'cancelled' },
          spend,
        ),
    }
  }
  const execute = Effect.fnUntraced(function* ({
    input,
    checkpoint,
  }: Task.PhaseInput<typeof Nested.InputSchema, typeof Round.CheckpointSchema, 'execute'>) {
    const execution = yield* TaskExecution
    yield* Nested.protection(inspectFormat)
    const current = yield* execution.agent
    const binding = input.contract.tool.binding
    const currentOptions = {
      agent: {
        ...current,
        toolkits: {
          inherit: current.toolkits?.inherit ?? true,
          registrations: current.toolkits?.registrations ?? [],
          ...current.toolkits,
          bindings: {
            ...current.toolkits?.bindings,
            [input.call.name]: { binding: binding.id, version: binding.version },
          },
        },
      },
    }
    return yield* runner.execute(
      { ...normalize(input), currentOptions },
      checkpoint,
      sink(execution, input),
    )
  })
  const abort = Effect.fnUntraced(function* ({
    input,
    checkpoint,
  }: {
    readonly input: Nested.Input
    readonly checkpoint: Round.Checkpoint
  }) {
    const execution = yield* TaskExecution
    return yield* runner.abort(normalize(input), checkpoint, sink(execution, input))
  })
  const definition = Task.make({
    ...contract,
    initial: () => ({
      _tag: 'execute' as const,
      phase: 'execute' as const,
      started: false,
      attempt: Usage.AttemptId.make(0),
    }),
    phases: { execute },
    abort,
  })
  disposals.set(definition, runner.clear)
  return definition
}
