/** Native run adapter resolves immutable request references before execution. */
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Record from '../Record.js'
import type * as Registry from '../Registry.js'
import * as RegistryImpl from './Registry.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as Scope from 'effect/Scope'
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
import * as Request from './RequestRecord.js'
import { PreparationRestoreFailure } from './PreparationRestoreFailure.js'
import * as Model from '../Model.js'
import * as Prepared from '../PreparedRequest.js'
import { ProviderAffinity } from '../ProviderAffinity.js'
import * as DeferredExecution from './DeferredExecution.js'
import * as ToolTask from './ToolTask.js'

interface Invocation {
  readonly record: Record.Task
  readonly lease: RegistryScope.Lease
}
class NativeInvocation extends Context.Service<NativeInvocation, Invocation>()(
  'effect-harness/internal/NativeRunInvocation',
) {}

type BaseServices = Omit<
  RunLoop.Services,
  | 'write'
  | 'writeCallback'
  | 'saveState'
  | 'invocationActive'
  | 'complete'
  | 'refresh'
  | 'waitCompaction'
  | 'waitTools'
>

export const make = (
  services: (lease: RegistryScope.Lease) => BaseServices,
  registry: Registry.Service,
  bindings: Pick<RunLoop.Services, 'models' | 'nativeModel' | 'nativeDefault' | 'session'>,
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
    | import('../Model.js').DeferredTerminalError
    | import('./ContextPlan.js').ContextPlanError
    | import('../ToolInventory.js').Denial
    | import('../ToolControl.js').InvalidControl
    | PreparationRestoreFailure
    | import('../ModelError.js').ModelError
    | import('../CompactionError.js').CompactionError
    | import('../ContextProjectionError.js').ContextProjectionError,
    NativeInvocation | TaskExecution | Scope.Scope
  > {
    const { record, lease } = yield* NativeInvocation
    const execution = yield* TaskExecution
    const base = services(lease)
    yield* base.requireSubmissionFormat
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
          Run.persist(tx, current, checkpoint).pipe(
            Effect.flatMap(Schema.encodeEffect(Schema.toCodecJson(Run.StoredSchema))),
            Effect.flatMap((wire) =>
              Transaction.putTask(tx, {
                ...current,
                state: { status: 'running', checkpoint: wire },
              }),
            ),
          ),
        complete: (tx, outcome) =>
          execution.progress(tx, definition, { status: 'completing', outcome }),
        waitCompaction: (tx, checkpoint) =>
          Effect.gen(function* () {
            // Task progress encodes its definition directly; use the same safe wire boundary.
            const current = Option.getOrThrow(yield* Transaction.task(tx, record.id))
            const safe = yield* Run.persist(tx, current, checkpoint)
            yield* execution.progress(tx, definition, {
              status: 'waiting',
              checkpoint: safe,
              on: [checkpoint.child],
              policy: 'allSettled',
            })
          }),
        waitTools: (tx, checkpoint) =>
          Effect.gen(function* () {
            const current = Option.getOrThrow(yield* Transaction.task(tx, record.id))
            const saved = yield* Run.persist(tx, current, checkpoint)
            yield* execution.progress(tx, definition, {
              status: 'waiting',
              checkpoint: saved,
              on: checkpoint.children,
              policy: 'allSettled',
            })
          }),
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
    checkpoint: Run.StoredSchema,
    success: Record.EntryId,
    failure: Schema.Json,
    initial: (submissionId) => ({
      _tag: 'queued' as const,
      phase: 'queued' as const,
      submissionId,
      draft: { type: 'input' as const, content: '' },
    }),
    phases: {
      queued: phase,
      request: phase,
      streaming: phase,
      retry: phase,
      'deferred-start': phase,
      deferred: phase,
      toolRound: phase,
      compaction: phase,
    },
  })
  const settle = Effect.fnUntraced(function* (
    record: Record.Task,
    reason: string,
    message?: string,
    observed?: StreamPublicationFailure,
    cancellation?: DeferredExecution.CancelReport,
  ) {
    const execution = yield* TaskExecution
    yield* execution.commit((tx) =>
      Effect.gen(function* () {
        const current = yield* Transaction.task(tx, record.id)
        const checkpoint = yield* Run.read(
          tx,
          Option.isSome(current) ? current.value.state.checkpoint : record.state.checkpoint,
        )
        if (checkpoint._tag === 'toolRound')
          yield* ToolTask.cancelUnadmitted(tx, record.conversationId, checkpoint.roundKey)
        if (
          (checkpoint._tag === 'streaming' ||
            checkpoint._tag === 'deferred' ||
            checkpoint._tag === 'deferred-start') &&
          !(yield* Usage.receipts(tx, record.conversationId)).some(
            (receipt) =>
              receipt.identity._tag === 'requestAttempt' &&
              receipt.identity.requestId === checkpoint.preparation.requestId &&
              receipt.identity.attempt === checkpoint.attempt &&
              receipt.identity.purpose === 'generation',
          )
        ) {
          const storedUsage =
            checkpoint._tag === 'streaming' ? checkpoint.partial?.usage : checkpoint.usage
          const usage =
            observed?.requestId === checkpoint.preparation.requestId &&
            observed.attempt === checkpoint.attempt
              ? observed.usage
              : storedUsage
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
              ? {
                  status: 'aborted',
                  reason,
                  ...(cancellation === undefined ? {} : { result: { cancellation } }),
                }
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
            const stored = inspection.checkpoint
            if (
              stored._tag !== 'queued' &&
              stored.preparation !== undefined &&
              stored.preparation.ownerTaskId !== record.id
            )
              return {
                _tag: 'blocked' as const,
                error: TaskResolution.blocked(
                  record,
                  'incompatible',
                  'The prepared request belongs to another run',
                ),
              }
            const checkpoint = yield* Run.hydrate(inspection.checkpoint, (ref) =>
              Request.inspect(bindings.session, ref),
            )
            if (
              checkpoint.submissionId !== inspection.input ||
              !Run.validPrefix(checkpoint) ||
              (checkpoint._tag === 'toolRound' &&
                checkpoint.roundKey !==
                  `tool-round:${record.id}:${checkpoint.preparation.requestId}`)
            )
              return {
                _tag: 'blocked' as const,
                error: TaskResolution.blocked(
                  record,
                  'incompatible',
                  'Run identity or committed tool prefix is inconsistent',
                ),
              }
            // Completed results have already passed their pinned codec and hook validation.
            // Their next boundary starts fresh work; only pending I/O needs the old bindings.
            if (
              checkpoint._tag !== 'queued' &&
              checkpoint._tag !== 'toolRound' &&
              !record.abortRequested &&
              checkpoint.preparation !== undefined
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
          }).pipe(
            Effect.catch((cause) =>
              Effect.succeed({
                _tag: 'blocked' as const,
                error: TaskResolution.blocked(
                  record,
                  'incompatible',
                  'Prepared request cannot be restored',
                  cause,
                ),
              }),
            ),
          ),
        ),
      ),
    invoke: (record, mode, lease) =>
      Effect.suspend(() => {
        return (
          mode === 'abort'
            ? Effect.gen(function* () {
                yield* services(lease).requireSubmissionFormat
                const checkpoint = yield* Run.get(bindings.session, record.state.checkpoint)
                let report: DeferredExecution.CancelReport | undefined
                if (checkpoint._tag === 'deferred') {
                  const base = services(lease)
                  report = yield* DeferredExecution.cancel(
                    Effect.gen(function* () {
                      const selected = Preparation.selectedModel(
                        checkpoint.preparation,
                        bindings.models,
                      )
                      if (selected === undefined)
                        return yield* Preparation.unavailable(
                          'Pinned deferred cancellation model is unavailable',
                        )
                      const savedModel = checkpoint.preparation.model
                      const configurationCodec = selected.configuration ?? Model.emptyConfiguration
                      if (
                        savedModel._tag !== 'descriptor' ||
                        !Equal.equals(savedModel.binding, {
                          id: JSON.stringify([
                            selected.definition.ref.instanceId ?? selected.definition.ref.provider,
                            selected.definition.ref.modelId,
                          ]),
                          version: selected.version ?? 1,
                        }) ||
                        !Equal.equals(selected.definition, savedModel.definition) ||
                        !Equal.equals(
                          configurationCodec.binding,
                          checkpoint.preparation.configurationBinding,
                        )
                      )
                        return yield* Preparation.unavailable(
                          'Pinned deferred cancellation model or configuration is incompatible',
                        )
                      const descriptor = yield* Model.deferred(selected)
                      const configuration = yield* configurationCodec.restore(
                        checkpoint.preparation.configuration,
                      )
                      const context = Context.merge(
                        Context.add(base.context, ProviderAffinity, {
                          id: checkpoint.preparation.affinity,
                        }),
                        configuration,
                      ).pipe(
                        Context.add(Prepared.PreparedRequest, {
                          current: yield* Preparation.own(checkpoint.preparation),
                        }),
                        Context.add(Scope.Scope, yield* Scope.Scope),
                      )
                      yield* descriptor
                        .cancel(checkpoint.handle, configuration)
                        .pipe(Effect.provideContext(context))
                    }),
                    base.cancellationCleanup,
                  )
                } else if (checkpoint._tag === 'deferred-start') {
                  report = {
                    status: 'failed',
                    message:
                      'Remote create outcome is unknown; no durable handle is available to cancel',
                  }
                }
                yield* settle(record, 'aborted', undefined, undefined, report)
              })
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
