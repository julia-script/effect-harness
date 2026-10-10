/** In-place native adapter: legacy wire checkpoints remain authoritative. */
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Record from '../Record.js'
import type * as Registry from '../Registry.js'
import * as RegistryImpl from './Registry.js'
import type * as RegistryScope from '../RegistryScope.js'
import type * as Scope from 'effect/Scope'
import type * as AiError from 'effect/ai/AiError'
import { SessionError } from '../SessionError.js'
import * as Session from '../Session.js'
import { StorageError } from '../StorageError.js'
import type { ExecutionError } from '../ExecutionError.js'
import type { HarnessError } from '../HarnessError.js'
import * as Task from '../Task.js'
import { TaskError, type Failure as TaskFailure } from '../TaskError.js'
import { TaskExecution } from '../TaskExecution.js'
import * as TaskResolution from '../TaskResolution.js'
import type * as TaskScheduler from '../TaskScheduler.js'
import * as Transaction from '../Transaction.js'
import type { PhaseResult } from './TaskRegistry.js'
import * as RunLoop from './RunLoop.js'
import * as Run from './RunState.js'
import * as LiveState from '../LiveState.js'
import * as Usage from '../Usage.js'
import * as Inbox from './Inbox.js'
import { NativeWriteFailure } from './NativeWriteFailure.js'
import { StreamPublicationFailure } from './StreamPublicationFailure.js'
import * as Preparation from './RequestPreparation.js'
import { PreparationRestoreFailure } from './PreparationRestoreFailure.js'

interface Invocation {
  readonly record: Record.Task
  readonly lease: RegistryScope.Lease
}
class NativeInvocation extends Context.Service<NativeInvocation, Invocation>()(
  'effect-harness/internal/NativeRunInvocation',
) {}

type BaseServices = Omit<
  RunLoop.Services,
  'write' | 'writeCallback' | 'saveState' | 'invocationActive' | 'complete' | 'refresh'
>

export const make = (
  services: (lease: RegistryScope.Lease) => BaseServices,
  registry: Registry.Service,
  bindings: Pick<RunLoop.Services, 'models' | 'nativeModel' | 'nativeDefault'>,
): {
  readonly adapter: TaskScheduler.NativeRunAdapter
  readonly registration: Registry.Registration
} => {
  const phase = Effect.fnUntraced(function* (): Effect.fn.Return<
    void,
    | TaskScheduler.Failure
    | HarnessError
    | AiError.AiError
    | ExecutionError
    | NativeWriteFailure
    | StreamPublicationFailure
    | import('./ContextPlan.js').ContextPlanError
    | import('../ToolInventory.js').Denial
    | import('../ToolControl.js').InvalidControl
    | PreparationRestoreFailure
    | import('../ModelError.js').ModelError,
    NativeInvocation | TaskExecution | Scope.Scope
  > {
    const { record, lease } = yield* NativeInvocation
    const execution = yield* TaskExecution
    const base = services(lease)
    let active = true
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        active = false
      }),
    )
    const writeCallback: RunLoop.Services['writeCallback'] = (change) =>
      execution.commit((tx) =>
        Effect.gen(function* () {
          const current = yield* Transaction.task(tx, record.id)
          if (Option.isNone(current))
            return yield* new TaskError({
              reason: 'notFound',
              operation: 'run.write',
              message: 'Native run is absent',
            })
          return yield* change(tx, current.value)
        }),
      )
    const write: RunLoop.Services['write'] = <A, E, R>(
      change: (tx: Transaction.Transaction, task: Record.Task) => Effect.Effect<A, E, R>,
    ) =>
      Effect.suspend(() => {
        let staged = false
        return writeCallback((tx, current) =>
          change(tx, current).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                staged = true
              }),
            ),
          ),
        ).pipe(
          Effect.catchCause((cause): Effect.Effect<never, E | TaskFailure | NativeWriteFailure> => {
            const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
            if (
              staged &&
              reason !== undefined &&
              Cause.isFailReason(reason) &&
              Schema.is(StorageError)(reason.error) &&
              (reason.error.reason === 'io' ||
                reason.error.reason === 'invalid' ||
                reason.error.reason === 'conflict' ||
                reason.error.reason === 'notFound')
            )
              return Effect.fail(new NativeWriteFailure({ error: reason.error }))
            return Effect.failCause(cause)
          }),
        )
      })
    yield* RunLoop.run(
      {
        ...base,
        refresh: registry.acquire.pipe(Effect.map((next) => services(next))),
        write,
        writeCallback,
        saveState: (tx, current, checkpoint) =>
          Run.encode(checkpoint).pipe(
            Effect.flatMap((wire) =>
              Transaction.putTask(tx, {
                ...current,
                state: { status: 'running', checkpoint: wire },
              }),
            ),
          ),
        complete: (tx, outcome) =>
          execution.progress(tx, definition, { status: 'completing', outcome }),
        invocationActive: () => active,
      },
      record,
    )
  })
  // Admission supplies the real draft atomically with its submission; this definition's
  // initializer is never used to manufacture a submission or clone a stored run.
  const definition = Task.make({
    kind: 'harness.run',
    version: 1,
    input: Record.SubmissionId,
    checkpoint: Run.RunStateSchema,
    success: Record.EntryId,
    failure: Schema.Json,
    initial: (submissionId) => ({
      _tag: 'queued' as const,
      phase: 'queued' as const,
      submissionId,
      draft: { type: 'input' as const, content: '' },
    }),
    phases: { queued: phase, request: phase, streaming: phase, retry: phase, tools: phase },
  })
  const settle = Effect.fnUntraced(function* (
    record: Record.Task,
    reason: string,
    message?: string,
    observed?: StreamPublicationFailure,
  ) {
    const execution = yield* TaskExecution
    yield* execution.commit((tx) =>
      Effect.gen(function* () {
        const current = yield* Transaction.task(tx, record.id)
        const checkpoint = yield* Run.decode(
          Option.isSome(current) ? current.value.state.checkpoint : record.state.checkpoint,
        )
        if (
          checkpoint._tag === 'streaming' &&
          !(yield* Usage.receipts(tx, record.conversationId)).some(
            (receipt) =>
              receipt.identity._tag === 'requestAttempt' &&
              receipt.identity.requestId === checkpoint.preparation.requestId &&
              receipt.identity.attempt === checkpoint.attempt &&
              receipt.identity.purpose === 'generation',
          )
        ) {
          const usage =
            observed?.requestId === checkpoint.preparation.requestId &&
            observed.attempt === checkpoint.attempt
              ? observed.usage
              : checkpoint.partial?.usage
          yield* Usage.record(tx, {
            conversationId: record.conversationId,
            identity: {
              _tag: 'requestAttempt',
              requestId: checkpoint.preparation.requestId,
              attempt: checkpoint.attempt,
              purpose: 'generation',
            },
            outcome:
              reason === 'aborted'
                ? { _tag: 'interrupted', reason }
                : { _tag: 'failed', error: { message: message ?? reason } },
            ...(usage === undefined ? {} : { usage }),
          })
        }
        const live = yield* Transaction.snapshot(
          tx,
          LiveState.document,
          LiveState.target(record.conversationId),
        )
        if (Option.isSome(live))
          yield* Transaction.setDocument(
            tx,
            LiveState.document,
            LiveState.target(record.conversationId),
            {},
          )
        yield* Inbox.settle(tx, record.conversationId, record.id, checkpoint.submissionId, {
          reason,
          ...(message === undefined ? {} : { detail: { message } }),
        })
        yield* execution.progress(tx, definition, {
          status: 'completing',
          outcome:
            reason === 'aborted'
              ? { status: 'aborted', reason }
              : {
                  status: 'failed',
                  error: { message: message ?? reason },
                  result: { message: message ?? reason },
                },
        })
      }),
    )
  })
  const adapter: TaskScheduler.NativeRunAdapter = {
    definition,
    inspect: (record) =>
      TaskResolution.inspect(definition, record).pipe(
        Effect.flatMap((inspection) =>
          Effect.gen(function* () {
            if (inspection._tag !== 'compatible') return inspection
            const checkpoint = inspection.checkpoint
            if (checkpoint.submissionId !== inspection.input || !Run.validPrefix(checkpoint))
              return {
                _tag: 'blocked' as const,
                error: TaskResolution.blocked(
                  record,
                  'incompatible',
                  'Legacy run identity or committed tool prefix is inconsistent',
                ),
              }
            // Completed results have already passed their pinned codec and hook validation.
            // Their next boundary starts fresh work; only pending I/O needs the old bindings.
            if (
              checkpoint._tag !== 'queued' &&
              checkpoint.preparation !== undefined &&
              (checkpoint._tag !== 'tools' || checkpoint.index < checkpoint.calls.length)
            ) {
              const snapshot = yield* registry.snapshot
              // This read-only check neither renders nor activates the persisted format.
              const error = yield* Preparation.compatible(
                checkpoint.preparation,
                snapshot,
                bindings.models,
                bindings.nativeModel,
                Option.isSome(bindings.nativeDefault),
              ).pipe(
                Effect.as(Option.none()),
                Effect.catch((error) => Effect.succeedSome(error)),
              )
              if (Option.isSome(error))
                return {
                  _tag: 'blocked' as const,
                  error: TaskResolution.blocked(
                    record,
                    'incompatible',
                    error.value.message,
                    error.value,
                  ),
                }
            }
            return inspection
          }),
        ),
      ),
    invoke: (record, mode, lease) =>
      Effect.suspend(() => {
        return (
          mode === 'abort'
            ? settle(record, 'aborted')
            : phase().pipe(Effect.provideService(NativeInvocation, { record, lease }))
        ).pipe(
          Effect.as<PhaseResult>({ _tag: 'returned' }),
          Effect.catchCause((escaping) => {
            const escaped = escaping.reasons.length === 1 ? escaping.reasons[0] : undefined
            const observed =
              escaped !== undefined &&
              Cause.isFailReason(escaped) &&
              escaped.error instanceof StreamPublicationFailure
                ? escaped.error
                : undefined
            // Unwrap only our private publication envelope; original failures and every
            // mixed reason continue through the same health/recovery classification.
            const cause = Cause.map(escaping, (error) =>
              error instanceof StreamPublicationFailure ? error.error : error,
            )
            if (Cause.hasInterruptsOnly(cause))
              return Effect.succeed<PhaseResult>({ _tag: 'interrupted' })
            const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
            // The accepted request remains recoverable. Existing scheduler health stops
            // automatic retries and reports the typed cause until an explicit resume.
            if (
              reason !== undefined &&
              Cause.isFailReason(reason) &&
              reason.error instanceof PreparationRestoreFailure
            )
              return Effect.fail(
                new SessionError({
                  reason: 'invalid',
                  operation: 'run.restore',
                  message:
                    'Accepted request configuration restore failed; resume its saved checkpoint',
                  cause: reason.error.error,
                }),
              )
            // Callback failures retain their original errors. Only a directly escaping
            // native write carries rejection evidence; caught/nested errors cannot borrow it.
            if (
              reason !== undefined &&
              Cause.isFailReason(reason) &&
              reason.error instanceof NativeWriteFailure
            )
              return Session.task(services(lease).session, record.id).pipe(
                Effect.andThen(
                  settle(record, 'failed', Cause.pretty(Cause.fail(reason.error.error)), observed),
                ),
                Effect.as<PhaseResult>({ _tag: 'returned' }),
              )
            // Uncertain commits poison the Session. Read, corrupt/closed, mixed failure,
            // interruption and failed settlement retain recovery checkpoints and causes.
            if (
              Cause.hasInterrupts(cause) ||
              cause.reasons.some(
                (reason) =>
                  Cause.isFailReason(reason) &&
                  (reason.error instanceof NativeWriteFailure ||
                    Schema.is(StorageError)(reason.error) ||
                    Schema.is(SessionError)(reason.error)),
              )
            ) {
              if (
                reason !== undefined &&
                Cause.isFailReason(reason) &&
                (Schema.is(StorageError)(reason.error) || Schema.is(SessionError)(reason.error))
              )
                return Effect.fail(reason.error)
              return Effect.fail(
                new SessionError({
                  reason: 'invalid',
                  operation: 'run.invoke',
                  message: Cause.pretty(cause),
                  cause: Cause.map(cause, (error) =>
                    error instanceof NativeWriteFailure ? error.error : error,
                  ),
                }),
              )
            }
            return settle(record, 'failed', Cause.pretty(cause)).pipe(
              Effect.as<PhaseResult>({ _tag: 'returned' }),
            )
          }),
        )
      }),
  }
  const unavailable = () =>
    new TaskError({
      reason: 'invalidDefinition',
      operation: 'run.admission',
      message: 'Native runs require atomic submission admission; v1 has no version migration',
    })
  return {
    adapter,
    registration: RegistryImpl.nativeRunRegistration(definition, {
      initialize: () => Effect.fail(unavailable()),
      inspect: adapter.inspect,
      upgrade: (_tx, id) =>
        Effect.fail(
          new TaskResolution.Blocked({
            reason: 'missingMigration',
            taskId: id,
            kind: definition.kind,
            storedVersion: 1,
            message: 'Native v1 has no version migration',
          }),
        ),
      invoke: (record, mode) =>
        Effect.scoped(
          registry.acquire.pipe(Effect.flatMap((lease) => adapter.invoke(record, mode, lease))),
        ).pipe(
          Effect.mapError((cause) =>
            Schema.is(StorageError)(cause) ||
            Schema.is(SessionError)(cause) ||
            Schema.isSchemaError(cause)
              ? cause
              : new SessionError({
                  reason: 'invalid',
                  operation: 'run.invoke',
                  message: 'Native invocation boundary failed',
                  cause,
                }),
          ),
        ),
    }),
  }
}
