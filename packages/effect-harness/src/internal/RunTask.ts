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
import { TaskError } from '../TaskError.js'
import { TaskExecution } from '../TaskExecution.js'
import * as TaskResolution from '../TaskResolution.js'
import type * as TaskScheduler from '../TaskScheduler.js'
import * as Transaction from '../Transaction.js'
import type { PhaseResult } from './TaskRegistry.js'
import * as RunLoop from './RunLoop.js'
import * as Run from './RunState.js'

interface Invocation {
  readonly record: Record.Task
  readonly lease: RegistryScope.Lease
  readonly rejectedWrites: WeakSet<StorageError>
}
class NativeInvocation extends Context.Service<NativeInvocation, Invocation>()(
  'effect-harness/internal/NativeRunInvocation',
) {}

type BaseServices = Omit<RunLoop.Services, 'write' | 'saveState' | 'invocationActive' | 'complete'>

export const make = (
  services: (lease: RegistryScope.Lease) => BaseServices,
  registry: Registry.Service,
): {
  readonly adapter: TaskScheduler.NativeRunAdapter
  readonly registration: Registry.Registration
} => {
  const phase = Effect.fnUntraced(function* (): Effect.fn.Return<
    void,
    TaskScheduler.Failure | HarnessError | AiError.AiError | ExecutionError,
    NativeInvocation | TaskExecution | Scope.Scope
  > {
    const { record, lease, rejectedWrites } = yield* NativeInvocation
    const execution = yield* TaskExecution
    const base = services(lease)
    let active = true
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        active = false
      }),
    )
    const write: RunLoop.Services['write'] = (change) => {
      let staged = false
      return execution
        .commit((tx) =>
          Effect.gen(function* () {
            const current = yield* Transaction.task(tx, record.id)
            if (Option.isNone(current))
              return yield* new TaskError({
                reason: 'notFound',
                operation: 'run.write',
                message: 'Native run is absent',
              })
            const result = yield* change(tx, current.value)
            staged = true
            return result
          }),
        )
        .pipe(
          Effect.tapError((error) =>
            Effect.sync(() => {
              if (
                staged &&
                Schema.is(StorageError)(error) &&
                (error.reason === 'io' ||
                  error.reason === 'invalid' ||
                  error.reason === 'conflict' ||
                  error.reason === 'notFound')
              )
                rejectedWrites.add(error)
            }),
          ),
        )
    }
    yield* RunLoop.run(
      {
        ...base,
        write,
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
    phases: { queued: phase, request: phase, tools: phase },
  })
  const settle = Effect.fnUntraced(function* (
    record: Record.Task,
    reason: string,
    message?: string,
  ) {
    const execution = yield* TaskExecution
    const checkpoint = yield* Run.decode(record.state.checkpoint)
    yield* execution.commit((tx) =>
      Effect.gen(function* () {
        const submission = yield* Transaction.submission(tx, checkpoint.submissionId)
        if (
          Option.isSome(submission) &&
          submission.value.type === 'input' &&
          submission.value.status !== 'done' &&
          submission.value.status !== 'unanswered'
        )
          yield* Transaction.putSubmission(tx, {
            ...submission.value,
            _tag: 'InputUnanswered',
            status: 'unanswered',
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
        Effect.map((inspection) => {
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
          return inspection
        }),
      ),
    invoke: (record, mode, lease) =>
      Effect.suspend(() => {
        // Evidence belongs to this invocation, even when an adapter reuses error objects.
        const rejectedWrites = new WeakSet<StorageError>()
        return (
          mode === 'abort'
            ? settle(record, 'aborted')
            : phase().pipe(
                Effect.provideService(NativeInvocation, { record, lease, rejectedWrites }),
              )
        ).pipe(
          Effect.as<PhaseResult>({ _tag: 'returned' }),
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause))
              return Effect.succeed<PhaseResult>({ _tag: 'interrupted' })
            // Only a known rejected invocation write may settle on a still-usable Session.
            // Uncertain commits poison that Session; corrupt/closed/read failures and failed
            // settlement persistence keep scheduler health failed and the checkpoint recoverable.
            const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
            if (
              reason !== undefined &&
              Cause.isFailReason(reason) &&
              (Schema.is(StorageError)(reason.error) || Schema.is(SessionError)(reason.error))
            )
              return Schema.is(StorageError)(reason.error) && rejectedWrites.has(reason.error)
                ? Session.task(services(lease).session, record.id).pipe(
                    Effect.andThen(settle(record, 'failed', Cause.pretty(cause))),
                    Effect.as<PhaseResult>({ _tag: 'returned' }),
                  )
                : Effect.fail(reason.error)
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
