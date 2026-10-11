/** One native scheduled child per model call; the progress writer owns final publication. */
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Exit from 'effect/Exit'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import type * as Registry from '../Registry.js'
import * as Record from '../Record.js'
import * as Task from '../Task.js'
import { TaskExecution } from '../TaskExecution.js'
import { TaskError } from '../TaskError.js'
import { ExecutionError } from '../ExecutionError.js'
import { SessionError } from '../SessionError.js'
import { StorageError } from '../StorageError.js'
import { HookExecution } from '../HookExecution.js'
import * as HookReducer from '../HookReducer.js'
import { ToolExecution } from '../ToolExecution.js'
import * as Inventory from '../ToolInventory.js'
import * as Tool from '../Tool.js'
import * as ToolProgress from '../ToolProgress.js'
import * as ToolResult from '../ToolResult.js'
import * as Transaction from '../Transaction.js'
import * as Usage from '../Usage.js'
import * as Stream from 'effect/Stream'
import * as Round from './ToolRound.js'
import * as Progress from './ToolProgress.js'
import * as Preparation from './RequestPreparation.js'

import type * as Prepared from '../PreparedRequest.js'
import type * as ImageReadPolicy from '../coding/ImageReadPolicy.js'
import type * as TaskExecutionAccess from '../TaskExecution.js'
import * as Nested from './NestedInvocation.js'
import * as ConversationInvocation from './ConversationInvocation.js'
export const contract = {
  kind: 'harness.tool',
  version: 1,
  input: Round.InputSchema,
  checkpoint: Round.CheckpointSchema,
  success: Record.EntryId,
  failure: Schema.Json,
} as const

const errorResult = (message: string): ToolResult.Result => ({
  content: [Prompt.textPart({ text: message })],
  isError: true,
  diagnostics: [],
})
const reportingFailure = (
  error: ToolProgress.Failure<import('../TaskError.js').Failure> | ExecutionError,
) =>
  Schema.is(SessionError)(error) ||
  Schema.is(StorageError)(error) ||
  Schema.isSchemaError(error) ||
  Schema.is(ExecutionError)(error)
    ? error
    : new ExecutionError({
        reason: Schema.is(TaskError)(error) && error.reason === 'revoked' ? 'revoked' : 'invalid',
        operation: 'tool.report',
        message: 'Tool reporting failed',
        cause: error,
      })

export interface PipelineInput {
  readonly call: Tool.Call
  readonly replay: Tool.Replay
  readonly offered?: typeof Prepared.ToolSchema.Type
  readonly options: Inventory.Options
  readonly currentOptions?: Inventory.Options
  readonly resultContract?: { readonly resultChannels: boolean; readonly structuredOutput: boolean }
  readonly caller: Tool.Caller
  readonly retention: import('../OutputRetention.js').Policy
  readonly progress: import('../ProgressPolicy.js').Policy
  readonly progressEnabled: boolean
  readonly imageReadPolicy?: ImageReadPolicy.Policy
}
export interface Completion {
  readonly call: Tool.Call
  readonly attempt: Usage.AttemptId
  readonly invoked: boolean
  readonly interrupted: boolean
  readonly denied: boolean
  readonly explicitEnvelope: boolean
  readonly observedSpend: Usage.Cost | undefined
  readonly disposition: import('../ToolControl.js').Receipt['status']
}
type SinkFailure = import('../ToolInvocation.js').Failure
export interface Sink {
  readonly assert: (tx: Transaction.Transaction) => Effect.Effect<unknown, SinkFailure>
  readonly recordAttempt: (
    tx: Transaction.Transaction,
    attempt: Usage.AttemptId,
  ) => Effect.Effect<unknown, SinkFailure>
  readonly fatal: (
    tx: Transaction.Transaction,
    result: ToolResult.Result,
    error: {
      readonly message: string
      readonly branches: ReadonlyArray<{ readonly kind: string; readonly message: string }>
    },
    spend: Usage.Cost | undefined,
    attempt: Usage.AttemptId,
  ) => Effect.Effect<void, SinkFailure>
  readonly complete: (
    tx: Transaction.Transaction,
    result: ToolResult.Result,
    meta: Completion,
  ) => Effect.Effect<void, SinkFailure>
  readonly cancel: (
    tx: Transaction.Transaction,
    result: ToolResult.Result,
    spend: Usage.Cost | undefined,
    attempt: Usage.AttemptId,
    started: boolean,
  ) => Effect.Effect<void, SinkFailure>
}
/** ONE implementation of leased hooks/codecs/replay/reporting for both native definitions. */
export const pipeline = (registry: Registry.Service) => {
  const cancelled = new Map<
    Record.TaskId,
    { readonly snapshot: ToolProgress.Snapshot; readonly spend?: Usage.Cost }
  >()
  const execute = Effect.fnUntraced(function* (
    input: PipelineInput,
    checkpoint: Round.Checkpoint,
    sink: Sink,
  ) {
    const execution = yield* TaskExecution
    if (
      (checkpoint.acceptedCall !== undefined &&
        (checkpoint.acceptedCall.id !== input.call.id ||
          checkpoint.acceptedCall.name !== input.call.name)) ||
      (checkpoint.observedResult !== undefined &&
        (!checkpoint.started || checkpoint.acceptedCall === undefined))
    )
      return yield* Round.invalid('The persisted invocation does not match its original call')
    const lease = yield* registry.acquire
    const offered = input.offered
    const binding = offered?.binding
    const bound = lease.snapshot.bindings.find(
      (item) =>
        item.owner.id === binding?.id &&
        item.owner.version === binding.version &&
        item.declaration.name === input.call.name,
    )
    const options = input.options
    const savedAdmission = Inventory.inspect(lease.snapshot, input.call.name, input.caller, options)
    const currentAdmission = Inventory.inspect(
      lease.snapshot,
      input.call.name,
      input.caller,
      input.currentOptions ?? options,
    )
    const compatible =
      input.currentOptions === undefined ||
      (bound !== undefined &&
        offered !== undefined &&
        Equal.equals(yield* Preparation.declaration(bound), offered) &&
        (input.resultContract === undefined ||
          (input.resultContract.resultChannels === Tool.hasResultChannels(bound.declaration) &&
            input.resultContract.structuredOutput === Tool.hasStructuredOutput(bound.declaration))))
    let attempt = checkpoint.attempt
    const safe =
      input.replay === 'safe' &&
      bound !== undefined &&
      Tool.policy(bound.declaration).replay === 'safe' &&
      (input.currentOptions === undefined ||
        (compatible && savedAdmission._tag === 'allowed' && currentAdmission._tag === 'allowed'))
    const interrupted = checkpoint.started && checkpoint.observedResult === undefined && !safe
    if (checkpoint.started && checkpoint.observedResult === undefined && safe) {
      attempt = yield* Schema.decodeEffect(Usage.AttemptId)(attempt + 1)
      yield* execution.commit((tx) =>
        Effect.gen(function* () {
          yield* sink.assert(tx)
          yield* sink.recordAttempt(tx, checkpoint.attempt)
          const current = yield* Transaction.task(tx, execution.taskId)
          if (Option.isNone(current)) return yield* Round.invalid('The tool child is absent')
          yield* Transaction.putTask(tx, {
            ...current.value,
            state: {
              status: 'running',
              checkpoint: yield* Task.encode(Round.CheckpointSchema, {
                ...checkpoint,
                started: false,
                attempt,
              }),
            },
          })
        }),
      )
    }
    const priorProgress = yield* execution.snapshot(
      ToolProgress.document,
      Round.target(execution.conversationId, Round.progressKey(execution.taskId)),
    )
    const newerAttempt = Option.isSome(priorProgress) && priorProgress.value.value.attempt < attempt
    let recovery: ToolProgress.Options['recovery'] = 'resume'
    if (interrupted) recovery = 'interrupt'
    else if (newerAttempt) recovery = 'retry'
    const writer = yield* Progress.make<import('../TaskError.js').Failure, never>(
      { commit: execution.commit },
      {
        conversationId: execution.conversationId,
        callId: Round.progressKey(execution.taskId),
        attempt,
        retention: input.retention,
        policy: input.progress,
        recovery,
      },
      undefined,
      { persistRunning: input.progressEnabled },
    )
    let active = true
    let reporting = false
    let settled = false
    let observedSpend = checkpoint.observedResult?.spend
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        active = false
        reporting = false
        if (!settled)
          cancelled.set(execution.taskId, {
            snapshot: yield* Progress.freezeCancellation(writer),
            ...(observedSpend === undefined ? {} : { spend: observedSpend }),
          })
      }).pipe(Effect.orDie),
    )
    const guard = Effect.suspend(() =>
      active
        ? Effect.void
        : Effect.fail(
            new ExecutionError({
              reason: 'revoked',
              operation: 'tool.access',
              message: 'Tool invocation has ended',
            }),
          ),
    )
    const gatedAccess = (
      gate: Effect.Effect<void, ExecutionError>,
    ): Pick<ToolExecution['Service'], 'conversationId' | 'snapshot' | 'watch' | 'commit'> => ({
      conversationId: execution.conversationId,
      snapshot: (document, target) =>
        gate.pipe(
          Effect.andThen(execution.snapshot(document, target)),
          Effect.tap(() => gate),
          Effect.mapError(reportingFailure),
        ),
      watch: (document, target) =>
        Stream.unwrap(gate.pipe(Effect.as(execution.watch(document, target)))).pipe(
          Stream.mapEffect((value) => gate.pipe(Effect.as(value))),
          Stream.mapError(reportingFailure),
        ),
      commit: (change) =>
        gate.pipe(
          Effect.andThen(
            execution.commit((tx) =>
              gate.pipe(
                Effect.andThen(change(tx)),
                Effect.tap(() => gate),
              ),
            ),
          ),
          Effect.mapError((error) =>
            Schema.is(TaskError)(error) ? reportingFailure(error) : error,
          ),
        ),
    })
    const reportGuard = Effect.suspend(() =>
      reporting
        ? guard
        : Effect.fail(
            new ExecutionError({
              reason: 'revoked',
              operation: 'tool.report',
              message: 'Tool handler has ended',
            }),
          ),
    )
    const access: ToolExecution['Service'] = {
      ...gatedAccess(reportGuard),
      taskId: execution.taskId,
      callId: input.call.id,
      ...(input.imageReadPolicy === undefined ? {} : { imageReadPolicy: input.imageReadPolicy }),
      invocation: Nested.borrow(execution.invocation, reportGuard),
      subagent: ConversationInvocation.borrow(
        execution.subagent,
        reportGuard,
        input.replay === 'safe' ? 'resume' : 'abandon',
      ),
      output: (chunk) =>
        reportGuard.pipe(Effect.andThen(writer.output(chunk)), Effect.mapError(reportingFailure)),
      details: (value) =>
        reportGuard.pipe(Effect.andThen(writer.details(value)), Effect.mapError(reportingFailure)),
      diagnostic: (value) =>
        reportGuard.pipe(
          Effect.andThen(writer.diagnostic(value)),
          Effect.mapError(reportingFailure),
        ),
    }
    const { modelTools: _modelTools, ...allAgent } = options.agent ?? {}
    const available = Inventory.inventory(lease.snapshot, input.caller, {
      ...options,
      agent: allAgent,
    }).map((item) => item.declaration.name)
    let call = checkpoint.acceptedCall ?? input.call
    let invoked = interrupted || checkpoint.observedResult !== undefined
    let denied = false
    let explicitEnvelope = checkpoint.explicitEnvelope ?? false
    const run = Effect.gen(function* () {
      if (
        input.currentOptions !== undefined &&
        (!compatible || savedAdmission._tag === 'denied' || currentAdmission._tag === 'denied')
      ) {
        denied = true
        if (currentAdmission._tag === 'denied') return errorResult(currentAdmission.denial.message)
        if (savedAdmission._tag === 'denied') return errorResult(savedAdmission.denial.message)
        return errorResult(`Pinned tool ${input.call.name} is incompatible`)
      }
      if (interrupted)
        return errorResult(
          'Tool execution was interrupted. It was not repeated because replay is unsafe; its external outcome is unknown.',
        )
      const admitted = Inventory.inspect(lease.snapshot, input.call.name, input.caller, options)
      if (offered === undefined || admitted._tag === 'denied' || bound === undefined) {
        denied = true
        return errorResult(
          admitted._tag === 'denied'
            ? admitted.denial.message
            : `Tool ${input.call.name} is unavailable`,
        )
      }
      if (
        !compatible ||
        (input.currentOptions === undefined &&
          !Equal.equals(yield* Preparation.declaration(bound), offered))
      ) {
        denied = true
        return errorResult(`Pinned tool ${input.call.name} is incompatible`)
      }
      return yield* lease.withTool(
        call.name,
        (item) =>
          Effect.gen(function* () {
            const codec = item.codec
            if (codec === undefined)
              return yield* Round.invalid('The admitted tool has no captured outer codecs')
            const hooks = lease.snapshot.hooks.filter((hook) => {
              const extension = lease.snapshot.extensions.find(
                (item) => item.owner.id === hook.owner.id,
              )
              return (
                extension === undefined ||
                options.agent?.extensions === undefined ||
                options.agent.extensions.includes(extension.declaration.name)
              )
            })
            // withTool retains the complete leased publication, including each independent hook owner.
            // Give each callback its own access epoch while the reducer carries accepted replacements.
            const callbacks: HookReducer.Callback[] = hooks.map((hook) => ({
              event: hook.declaration.event,
              execute: (event) =>
                Effect.suspend(() => {
                  let hookActive = true
                  const hookGuard = Effect.suspend(() =>
                    hookActive
                      ? guard
                      : Effect.fail(
                          new ExecutionError({
                            reason: 'revoked',
                            operation: 'hook.access',
                            message: 'Hook invocation has ended',
                          }),
                        ),
                  )
                  return Effect.scoped(
                    hook.declaration
                      .bind(hook.context)(event)
                      .pipe(
                        Effect.provideService(HookExecution, {
                          ...gatedAccess(hookGuard),
                          taskId: execution.taskId,
                        }),
                      ),
                  ).pipe(
                    Effect.ensuring(
                      Effect.sync(() => {
                        hookActive = false
                      }),
                    ),
                  )
                }),
            }))
            const dispatch = <N extends import('../Hook.js').Name, E>(
              event: import('../Hook.js').Input<N>,
              validation: HookReducer.Options<E>,
            ) =>
              HookReducer.dispatch(callbacks, event, validation).pipe(
                Effect.provideService(HookExecution, {
                  ...gatedAccess(guard),
                  taskId: execution.taskId,
                }),
              )
            let result: ToolResult.Result | undefined = checkpoint.observedResult
            if (checkpoint.acceptedCall === undefined) {
              const before = yield* dispatch(
                { _tag: 'beforeTool', call },
                {
                  validateArguments: codec.validateParameters,
                },
              )
              if (before?._tag === 'block') {
                denied = true
                result = errorResult(before.message)
              }
              if (before?._tag === 'replaceArguments')
                call = { ...call, arguments: before.arguments }
            }
            if (result === undefined) {
              yield* codec.validateParameters(call.arguments)
              yield* execution.commit((tx) =>
                Effect.gen(function* () {
                  yield* sink.assert(tx)
                  const current = yield* Transaction.task(tx, execution.taskId)
                  if (Option.isNone(current))
                    return yield* Round.invalid('The tool child is absent')
                  yield* Transaction.putTask(tx, {
                    ...current.value,
                    state: {
                      status: 'running',
                      checkpoint: yield* Task.encode(Round.CheckpointSchema, {
                        _tag: 'execute',
                        phase: 'execute',
                        started: true,
                        attempt,
                        acceptedCall: call,
                      }),
                    },
                  })
                }),
              )
              invoked = true
              reporting = true
              const handled = yield* Inventory.invoke(lease, call, input.caller, options).pipe(
                Effect.provideService(ToolExecution, access),
                Effect.exit,
              )
              reporting = false
              const released = yield* ConversationInvocation.close(access.subagent, true).pipe(
                Effect.exit,
              )
              if (Exit.isFailure(handled) && Exit.isFailure(released))
                return yield* Effect.failCause(
                  Cause.fromReasons<
                    | Effect.Error<ReturnType<typeof Inventory.invoke>>
                    | import('../Subagent.js').Failure
                  >([...handled.cause.reasons, ...released.cause.reasons]),
                )
              if (Exit.isFailure(released)) return yield* Effect.failCause(released.cause)
              if (Exit.isFailure(handled)) return yield* Effect.failCause(handled.cause)
              result = handled.value
              const envelope = Tool.hasResultChannels(item.declaration)
                ? Option.none()
                : Schema.decodeUnknownOption(Schema.toCodecJson(ToolResult.Envelope))(
                    result.details,
                  )
              if (Option.isSome(envelope)) {
                result = { ...result, content: envelope.value.content }
                explicitEnvelope = true
              }
              observedSpend = result.spend
              yield* execution.commit((tx) =>
                Effect.gen(function* () {
                  yield* sink.assert(tx)
                  const current = yield* Transaction.task(tx, execution.taskId)
                  if (Option.isNone(current))
                    return yield* Round.invalid('The tool child is absent')
                  yield* Transaction.putTask(tx, {
                    ...current.value,
                    state: {
                      status: 'running',
                      checkpoint: yield* Task.encode(Round.CheckpointSchema, {
                        _tag: 'execute',
                        phase: 'execute',
                        started: true,
                        attempt,
                        acceptedCall: call,
                        observedResult: result!,
                        explicitEnvelope,
                      }),
                    },
                  })
                }),
              )
            }
            const after = yield* dispatch(
              { _tag: 'afterTool', call, result },
              {
                validateResult: (value) =>
                  codec.validateResult(value, available).pipe(Effect.asVoid),
              },
            )
            return yield* codec.validateResult(after ?? result, available)
          }),
        { binding: binding!.id, version: binding!.version },
      )
    })
    const outcome = yield* Effect.exit(run)
    if (
      Exit.isFailure(outcome) &&
      outcome.cause.reasons.some(
        (reason) =>
          Cause.isFailReason(reason) &&
          (Schema.is(SessionError)(reason.error) ||
            Schema.is(StorageError)(reason.error) ||
            (Schema.is(HookReducer.HookFailure)(reason.error) &&
              reason.error.reason === 'critical')),
      )
    )
      return yield* new SessionError({
        reason: 'invalid',
        operation: 'tool.invoke',
        message: Cause.pretty(outcome.cause),
        cause: outcome.cause,
      })
    if (Exit.isFailure(outcome) && Cause.hasInterruptsOnly(outcome.cause))
      return yield* Effect.failCause(outcome.cause)
    if (Exit.isFailure(outcome)) {
      const message = Cause.pretty(outcome.cause)
      const branches = outcome.cause.reasons.map((reason) => ({
        kind: reason._tag,
        message: Cause.pretty(Cause.fromReasons([reason])),
      }))
      const result: ToolResult.Result = {
        ...errorResult(message),
        diagnostics: branches.map((branch) => ({
          severity: 'error' as const,
          code: `fatal.${branch.kind}`,
          message: branch.message,
        })),
      }
      yield* Nested.close(execution.invocation, true)
      yield* writer.settle('completed', (tx) =>
        sink.fatal(tx, result, { message, branches }, observedSpend, attempt),
      )
      active = false
      settled = true
      cancelled.delete(execution.taskId)
      return
    }
    const retained = yield* ToolProgress.retainResult(outcome.value, input.retention)
    const result: ToolResult.Result = { ...retained, diagnostics: retained.diagnostics ?? [] }
    let disposition: import('../ToolControl.js').Receipt['status'] = 'succeeded'
    if (denied) disposition = 'denied'
    else if (result.isError) disposition = 'failed'
    yield* Nested.close(execution.invocation, true)
    yield* writer.settle(interrupted ? 'interrupted' : 'completed', (tx) =>
      sink.complete(tx, result, {
        call,
        attempt,
        invoked,
        interrupted,
        denied,
        explicitEnvelope,
        observedSpend,
        disposition,
      }),
    )
    active = false
    settled = true
    cancelled.delete(execution.taskId)
  })
  const abort = Effect.fnUntraced(function* (
    input: PipelineInput,
    checkpoint: Round.Checkpoint,
    sink: Sink,
  ) {
    const execution = yield* TaskExecution
    const buffered = cancelled.get(execution.taskId)
    const accepted = buffered?.snapshot
    const spend = checkpoint.observedResult?.spend ?? buffered?.spend
    if (
      accepted !== undefined &&
      (accepted.attempt !== checkpoint.attempt ||
        accepted.callId !== Round.progressKey(execution.taskId))
    )
      return yield* Round.invalid('The cancelled progress belongs to another invocation attempt')
    const writer = yield* Progress.make<import('../TaskError.js').Failure, never>(
      { commit: execution.commit },
      {
        conversationId: execution.conversationId,
        callId: Round.progressKey(execution.taskId),
        attempt: checkpoint.attempt,
        retention: input.retention,
        policy: input.progress,
      },
      accepted,
      { persistRunning: input.progressEnabled },
    )
    const result = errorResult(
      checkpoint.started
        ? 'Tool call was cancelled; its unreported external outcome is unknown.'
        : 'Tool call was cancelled before invoking its handler.',
    )
    yield* writer.settle('cancelled', (tx) =>
      sink.cancel(tx, result, spend, checkpoint.attempt, checkpoint.started),
    )
    cancelled.delete(execution.taskId)
  })
  return { execute, abort, clear: () => cancelled.clear() }
}

const modelInput = (input: Round.Input): PipelineInput => ({
  call: input.call,
  replay: input.replay,
  caller: 'model',
  retention: input.retention,
  progress: input.progress,
  progressEnabled: true,
  ...(input.preparation.tools.find((item) => item.declaration.name === input.call.name) ===
  undefined
    ? {}
    : {
        offered: input.preparation.tools.find((item) => item.declaration.name === input.call.name)!,
      }),
  options: {
    agent: {
      ...input.preparation.agent,
      toolkits: {
        ...input.preparation.agent.toolkits,
        inherit: false,
        registrations: input.preparation.requiredBindings.map((item) => item.id),
        bindings: Object.fromEntries(
          input.preparation.allTools.map((item) => [
            item.declaration.name,
            { binding: item.binding.id, version: item.binding.version },
          ]),
        ),
      },
    },
  },
  imageReadPolicy:
    input.preparation.model._tag === 'descriptor' &&
    input.preparation.model.definition.capabilities.images === true &&
    input.preparation.model.definition.imageLimits !== undefined
      ? { vision: true, limits: input.preparation.model.definition.imageLimits }
      : { vision: false },
})
const modelSink = (execution: TaskExecutionAccess.Access, input: Round.Input): Sink => ({
  assert: (tx) => Round.slot(tx, execution.conversationId, execution.taskId, input),
  recordAttempt: (tx, attempt) =>
    Usage.record(tx, {
      conversationId: execution.conversationId,
      identity: { _tag: 'toolCall', taskId: execution.taskId, callId: input.call.id, attempt },
      outcome: { _tag: 'interrupted', reason: 'Prior safe invocation did not publish a result' },
    }),
  fatal: (tx, result, error, spend, attempt) =>
    Effect.gen(function* () {
      const { round, found } = yield* Round.slot(
        tx,
        execution.conversationId,
        execution.taskId,
        input,
      )
      if (found.result !== undefined)
        return yield* Round.invalid('The fatal tool slot already settled')
      yield* Usage.record(tx, {
        conversationId: execution.conversationId,
        identity: {
          _tag: 'toolCall',
          taskId: execution.taskId,
          callId: input.call.id,
          attempt,
        },
        outcome: { _tag: 'failed', error },
        ...(spend === undefined
          ? {}
          : { usage: { _tag: 'tool' as const, name: input.call.name, cost: spend } }),
      })
      yield* Round.write(tx, execution.conversationId, input.roundKey, {
        ...round,
        slots: round.slots.map((saved, index) =>
          index === input.index
            ? {
                ...saved,
                fatal: true,
                result,
                receipt: { callId: input.call.id, caller: 'model', status: 'failed' },
              }
            : saved,
        ),
      })
      yield* execution.progress(tx, contract, {
        status: 'completing',
        outcome: { status: 'failed', error, result: error },
      })
    }),
  complete: (tx, result, meta) =>
    Effect.gen(function* () {
      const {
        call,
        attempt,
        invoked,
        interrupted,
        denied,
        explicitEnvelope,
        observedSpend,
        disposition,
      } = meta
      const plain = result.content.flatMap((part) =>
        part.type === 'text' && Object.keys(part.options).length === 0 ? [part.text] : [],
      )
      const value =
        !explicitEnvelope && plain.length === result.content.length
          ? plain.join('\n')
          : yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
              _tag: '@effect-harness/ToolContent',
              content: result.content,
            })
      const output = Prompt.toolMessage({
        content: [
          Prompt.toolResultPart({
            id: call.id,
            name: call.name,
            providerExecuted: false,
            isFailure: result.isError,
            result: value,
          }),
        ],
      })
      return yield* Effect.gen(function* () {
        const { round, found } = yield* Round.slot(
          tx,
          execution.conversationId,
          execution.taskId,
          input,
        )
        if (found.result !== undefined) return yield* Round.invalid('The tool slot already settled')
        // A cold binding denial hides the saved result, but cannot erase its observed charge.
        const spend = denied ? observedSpend : result.spend
        const usage: Usage.Record | undefined =
          invoked && !interrupted
            ? {
                _tag: 'tool',
                name: call.name,
                ...(spend === undefined ? {} : { cost: spend }),
              }
            : undefined
        const entry = yield* Transaction.appendEntry(tx, execution.conversationId, {
          kind: 'tool.result',
          ...(usage === undefined ? {} : { usage }),
          model: yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))([
            output,
          ]),
          data: yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.ResultSchema))(result),
        })
        let receiptOutcome: Usage.Outcome = { _tag: 'completed', value: { entryId: entry.id } }
        if (interrupted)
          receiptOutcome = {
            _tag: 'interrupted',
            reason: 'Unsafe interrupted handler was not repeated',
          }
        else if (result.isError) receiptOutcome = { _tag: 'failed', error: { entryId: entry.id } }
        yield* Usage.record(tx, {
          conversationId: execution.conversationId,
          identity: { _tag: 'toolCall', taskId: execution.taskId, callId: input.call.id, attempt },
          outcome: receiptOutcome,
          linkedEntryId: entry.id,
          ...(usage === undefined ? {} : { usage }),
        })
        yield* Round.write(tx, execution.conversationId, input.roundKey, {
          ...round,
          slots: round.slots.map((saved, index) =>
            index === input.index
              ? {
                  ...saved,
                  result,
                  entryId: entry.id,
                  receipt: {
                    callId: input.call.id,
                    caller: 'model',
                    status: disposition,
                    ...(result.control === undefined ? {} : { control: result.control }),
                  },
                }
              : saved,
          ),
        })
        yield* execution.progress(tx, contract, {
          status: 'completing',
          outcome: result.isError
            ? {
                status: 'failed',
                error: { message: 'Tool call failed' },
                result: { entryId: entry.id },
              }
            : { status: 'completed', result: entry.id },
        })
      })
    }),
  cancel: (tx, result, spend, attempt, started) =>
    Effect.gen(function* () {
      const { round, found } = yield* Round.slot(
        tx,
        execution.conversationId,
        execution.taskId,
        input,
      )
      if (found.result !== undefined)
        return yield* Round.invalid('The cancelled slot already settled')
      yield* Usage.record(tx, {
        conversationId: execution.conversationId,
        identity: {
          _tag: 'toolCall',
          taskId: execution.taskId,
          callId: input.call.id,
          attempt: attempt,
        },
        outcome: {
          _tag: 'interrupted',
          reason: started
            ? 'Cancelled started handler; external spend unknown'
            : 'Cancelled before handler',
        },
        ...(spend === undefined
          ? {}
          : {
              usage: {
                _tag: 'tool' as const,
                name: input.call.name,
                cost: spend,
              },
            }),
      })
      yield* Round.write(tx, execution.conversationId, input.roundKey, {
        ...round,
        slots: round.slots.map((saved, index) =>
          index === input.index
            ? {
                ...saved,
                result,
                receipt: {
                  callId: input.call.id,
                  caller: 'model',
                  status: 'cancelled',
                },
              }
            : saved,
        ),
      })
      yield* execution.progress(tx, contract, {
        status: 'completing',
        outcome: { status: 'aborted', reason: 'cancelled' },
      })
    }),
})

const disposals = new WeakMap<Task.Any, () => void>()
export const clear = (definition: Task.Any) => disposals.get(definition)?.()
export const make = (registry: Registry.Service) => {
  const runner = pipeline(registry)
  const execute = Effect.fnUntraced(function* ({
    input,
    checkpoint,
  }: Task.PhaseInput<typeof Round.InputSchema, typeof Round.CheckpointSchema, 'execute'>) {
    const execution = yield* TaskExecution
    return yield* runner.execute(modelInput(input), checkpoint, modelSink(execution, input))
  })
  const abort = Effect.fnUntraced(function* ({
    input,
    checkpoint,
  }: {
    readonly input: Round.Input
    readonly checkpoint: Round.Checkpoint
  }) {
    const execution = yield* TaskExecution
    return yield* runner.abort(modelInput(input), checkpoint, modelSink(execution, input))
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

/** Parent abort runs after owned child cleanup; unadmitted sequential slots have no handler or spend. */
export const cancelUnadmitted = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
  key: string,
) {
  const round = yield* Round.read(tx, conversationId, key)
  const slots: Round.Slot[] = []
  for (const saved of round.slots) {
    if (saved.result !== undefined) {
      slots.push(saved)
      continue
    }
    if (saved.taskId !== undefined)
      return yield* Round.invalid('An admitted child has no terminal tool slot')
    const result = errorResult(
      'Tool call was cancelled before child admission; no handler was invoked.',
    )
    slots.push({
      ...saved,
      result,
      receipt: { callId: saved.call.id, caller: 'model', status: 'cancelled' },
    })
  }
  yield* Round.write(tx, conversationId, key, { ...round, slots, applied: true })
})
