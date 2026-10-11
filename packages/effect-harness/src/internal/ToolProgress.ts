import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Semaphore from 'effect/Semaphore'
import * as OutputRetention from '../OutputRetention.js'
import * as ProgressPolicy from '../ProgressPolicy.js'
import * as ToolProgress from '../ToolProgress.js'
import * as ToolResult from '../ToolResult.js'
import * as Transaction from '../Transaction.js'
import * as Value from './DocumentValue.js'

const error = (
  reason: ToolProgress.ToolProgressError['reason'],
  message: string,
  cause?: unknown,
) =>
  new ToolProgress.ToolProgressError({ reason, message, ...(cause === undefined ? {} : { cause }) })
const sameRetention = (a: OutputRetention.Policy, b: OutputRetention.Policy): boolean =>
  a.retain === b.retain && a.maxBytes === b.maxBytes && a.maxLines === b.maxLines

const cancellation = new WeakMap<
  object,
  () => Effect.Effect<ToolProgress.Snapshot, ToolProgress.Failure>
>()
/** Native task cleanup only: detach accepted local state without publishing or acknowledging it. */
export const freezeCancellation = (writer: object) =>
  Effect.suspend(() => {
    const freeze = cancellation.get(writer)
    return freeze === undefined
      ? Effect.fail(error('closed', 'The writer has no cancellation capability.'))
      : freeze()
  })

export const make = Effect.fnUntraced(function* <E, R>(
  gate: ToolProgress.Gate<E, R>,
  options: ToolProgress.Options,
  cancelled?: ToolProgress.Snapshot,
  persistence: { readonly persistRunning?: boolean } = {},
) {
  const { callId, attempt } = options
  const persistRunning = persistence.persistRunning !== false
  let initialStored: ToolProgress.Snapshot | undefined
  const policy = yield* Schema.decodeEffect(ProgressPolicy.Policy)(
    options.policy ?? ProgressPolicy.defaultPolicy,
  ).pipe(
    Effect.mapError((cause) => error('configuration', 'Invalid progress throttle policy.', cause)),
  )
  const buffer = yield* OutputRetention.make(options.retention)
  const maxPending = options.maxPendingDetails ?? 64
  if (
    !callId ||
    callId === 'uninitialized' ||
    !Number.isSafeInteger(attempt) ||
    attempt < 0 ||
    !Number.isSafeInteger(maxPending) ||
    maxPending < 1 ||
    (options.recovery !== undefined && !['resume', 'retry', 'interrupt'].includes(options.recovery))
  )
    return yield* error(
      'configuration',
      'Progress needs a call ID, safe attempt and positive acknowledgement bound.',
    )
  const target = {
    scope: { _tag: 'conversation' as const, conversationId: options.conversationId },
    key: callId,
  }
  const empty: ToolProgress.Snapshot = {
    callId: callId,
    attempt: attempt,
    revision: 0,
    status: 'running',
    retention: buffer.policy,
    output: OutputRetention.snapshot(buffer),
    endsWithNewline: true,
    diagnostics: [],
  }
  const recovery = options.recovery ?? 'resume'
  const initial = yield* gate.commit((tx) =>
    Effect.gen(function* () {
      const found = yield* Transaction.snapshot(tx, ToolProgress.document, target)
      if (Option.isNone(found)) {
        if (!persistRunning) return empty
        yield* Transaction.ensureDocument(tx, ToolProgress.document, target)
        if (persistRunning) yield* Transaction.setDocument(tx, ToolProgress.document, target, empty)
        return empty
      }
      const old = found.value.value
      initialStored = old
      // Fork-initial members carry no parent invocation identity or output.
      if (old.callId === 'uninitialized') {
        if (persistRunning) yield* Transaction.setDocument(tx, ToolProgress.document, target, empty)
        return empty
      }
      if (old.callId !== callId)
        return yield* error('conflict', 'Progress belongs to another call.')
      if (recovery === 'retry') {
        if (attempt <= old.attempt || old.status === 'completed' || old.status === 'cancelled')
          return yield* error(
            'conflict',
            'Safe retry needs a newer attempt and an unsettled prior call.',
          )
        if (persistRunning) yield* Transaction.setDocument(tx, ToolProgress.document, target, empty)
        return empty
      }
      if (old.attempt !== attempt || !sameRetention(old.retention, buffer.policy))
        return yield* error('conflict', 'Resume needs the recorded attempt and retention policy.')
      if (old.status === 'completed' || old.status === 'cancelled')
        return yield* error(
          'closed',
          'The call already has a terminal receipt; replay that receipt.',
        )
      if (recovery === 'interrupt' && old.status !== 'interrupted') {
        const interrupted: ToolProgress.Snapshot = {
          ...old,
          status: 'interrupted',
          revision: old.revision + 1,
          diagnostics: [
            ...old.diagnostics,
            {
              severity: 'error',
              code: 'interrupted',
              message:
                'The previous invocation was interrupted; external effects may have occurred. Output is retained without rerunning it.',
            },
          ],
        }
        if (persistRunning)
          yield* Transaction.setDocument(tx, ToolProgress.document, target, interrupted)
        return interrupted
      }
      return old
    }),
  )
  const accepted =
    cancelled === undefined ? initial : yield* Value.copy(ToolProgress.Snapshot, cancelled)
  if (
    accepted.callId !== callId ||
    accepted.attempt !== attempt ||
    !sameRetention(accepted.retention, initial.retention) ||
    accepted.revision < initial.revision ||
    accepted.output.totalBytes < initial.output.totalBytes ||
    accepted.output.totalLines < initial.output.totalLines
  )
    return yield* error('conflict', 'Cancellation state does not match this durable invocation.')
  // The private abort handoff seeds accepted state locally; only settle may publish it.
  yield* OutputRetention.append(buffer, accepted.output.text)
  buffer.totalBytes = accepted.output.totalBytes
  buffer.endsWithNewline = accepted.endsWithNewline
  buffer.newlines =
    accepted.output.totalLines - (buffer.totalBytes > 0 && !buffer.endsWithNewline ? 1 : 0)
  let revision = accepted.revision
  let committedRevision = initial.revision
  let details = accepted.details
  let diagnostics = [...accepted.diagnostics]
  let status = initial.status
  let mode: 'running' | 'stopping' | 'closed' = 'running'
  let failure: Cause.Cause<ToolProgress.Failure<E>> | undefined
  let nextAt = 0
  let wake = Deferred.makeUnsafe<void>()
  const stop = yield* Deferred.make<void>()
  const write = yield* Semaphore.make(1)
  const admissions = yield* Semaphore.make(maxPending)
  const waiters = new Map<Deferred.Deferred<void, ToolProgress.Failure<E>>, number>()
  let settlementTx: Transaction.Transaction | undefined
  let flushedTx: Transaction.Transaction | undefined
  let final: ToolProgress.Snapshot | undefined
  const boundedReports = (
    nextDetails: Schema.Json | undefined,
    nextDiagnostics: ReadonlyArray<ToolResult.Diagnostic>,
  ) =>
    Effect.gen(function* () {
      if (persistRunning) return
      const encoded = yield* Schema.encodeEffect(
        Schema.fromJsonString(
          Schema.Struct({
            details: Schema.optionalKey(Schema.Json),
            diagnostics: Schema.Array(ToolResult.DiagnosticSchema),
          }),
        ),
      )({
        ...(nextDetails === undefined ? {} : { details: nextDetails }),
        diagnostics: nextDiagnostics,
      })
      if (
        nextDiagnostics.length > maxPending ||
        new TextEncoder().encode(encoded).length > Math.max(32, buffer.policy.maxBytes)
      )
        return yield* new OutputRetention.OutputRetentionError({
          reason: 'overflow',
          message: 'Final-only reports exceeded their bounded local retention budget.',
        })
    })

  const check = Effect.suspend(() => {
    if (failure !== undefined) return Effect.failCause(failure)
    if (mode !== 'running' || status !== 'running')
      return Effect.fail(error('closed', 'The progress invocation is closed or settling.'))
    return Effect.void
  })
  const current = (): ToolProgress.Snapshot => ({
    callId: callId,
    attempt: attempt,
    revision,
    status,
    retention: buffer.policy,
    output: OutputRetention.snapshot(buffer),
    endsWithNewline: buffer.endsWithNewline,
    ...(details === undefined ? {} : { details }),
    diagnostics,
  })
  const poison = (cause: Cause.Cause<ToolProgress.Failure<E>>) =>
    Effect.gen(function* () {
      failure ??= cause
      for (const waiter of waiters.keys()) yield* Deferred.failCause(waiter, failure)
      waiters.clear()
      yield* Deferred.succeed(wake, undefined)
      yield* Deferred.succeed(stop, undefined)
    })
  const mark = (waiter?: Deferred.Deferred<void, ToolProgress.Failure<E>>) =>
    Effect.gen(function* () {
      if (revision === Number.MAX_SAFE_INTEGER)
        return yield* error('conflict', 'Progress revision is exhausted.')
      revision++
      // Register against this accepted revision before waking the writer or another reporter.
      if (waiter !== undefined) waiters.set(waiter, revision)
      yield* Deferred.succeed(wake, undefined)
    })
  const acknowledge = (included: number) =>
    Effect.gen(function* () {
      for (const [waiter, accepted] of waiters) {
        if (accepted > included) continue
        waiters.delete(waiter)
        yield* Deferred.succeed(waiter, undefined)
      }
    })
  const stage = (tx: Transaction.Transaction, snapshot: ToolProgress.Snapshot) =>
    Effect.gen(function* () {
      const found = yield* Transaction.snapshot(tx, ToolProgress.document, target)
      if (!persistRunning) {
        const same = Schema.toEquivalence(ToolProgress.Snapshot)
        if (
          (initialStored === undefined && Option.isSome(found)) ||
          (initialStored !== undefined &&
            (Option.isNone(found) || !same(found.value.value, initialStored)))
        )
          return yield* error('conflict', 'Final-only progress was replaced by another writer.')
        yield* Transaction.ensureDocument(tx, ToolProgress.document, target)
        yield* Transaction.setDocument(tx, ToolProgress.document, target, snapshot)
        return snapshot
      }
      if (
        Option.isNone(found) ||
        found.value.value.attempt !== attempt ||
        found.value.value.revision !== committedRevision ||
        found.value.value.status === 'completed' ||
        found.value.value.status === 'cancelled'
      )
        return yield* error(
          'conflict',
          'Progress was replaced, retired or settled by another writer.',
        )
      yield* Transaction.setDocument(tx, ToolProgress.document, target, snapshot)
      return snapshot
    })
  const worker = Effect.gen(function* () {
    while (
      cancelled === undefined &&
      mode === 'running' &&
      failure === undefined &&
      status === 'running'
    ) {
      if (revision === committedRevision) {
        yield* Deferred.await(wake)
        wake = Deferred.makeUnsafe<void>()
        continue
      }
      const now = Number(yield* Clock.monotonicTimeNanos) / 1e6
      if (nextAt > now) {
        yield* Deferred.await(stop).pipe(
          Effect.timeoutOrElse({
            duration: Math.min(nextAt - now, 0x7fffffff),
            orElse: () => Effect.void,
          }),
        )
        continue
      }
      yield* write.withPermit(
        Effect.gen(function* () {
          if (mode !== 'running' || failure !== undefined) return
          const value = current()
          if (buffer.policy.retain === 'full' && value.output.truncated)
            return yield* new OutputRetention.OutputRetentionError({
              reason: 'overflow',
              message: 'Full progress exceeded its hard retention budget.',
            })
          const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ToolProgress.Snapshot))(
            value,
          )
          const bytes = new TextEncoder().encode(encoded).length
          const started = yield* Clock.monotonicTimeNanos
          yield* gate.commit((tx) => stage(tx, value))
          committedRevision = value.revision
          const pause = Math.max(policy.minInterval, (bytes * 1000) / policy.bytesPerSecond)
          // Long waits are split above; a tiny positive rate never causes an early write.
          nextAt = Number(started) / 1e6 + pause
          yield* acknowledge(value.revision)
        }),
      )
    }
  }).pipe(Effect.catchCause(poison))
  const workerFiber = persistRunning ? yield* worker.pipe(Effect.forkScoped) : undefined
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      mode = 'closed'
      yield* poison(
        Cause.fail(error('closed', 'The progress invocation scope ended before acknowledgement.')),
      )
    }),
  )

  const flush = Effect.fnUntraced(function* (tx: Transaction.Transaction) {
    if (tx !== settlementTx || final === undefined)
      return yield* error(
        'settlement',
        "Flush requires this writer's active gated settlement transaction.",
      )
    if (tx !== flushedTx) {
      yield* stage(tx, final)
      flushedTx = tx
    }
    return yield* Value.copy(ToolProgress.Snapshot, final)
  })
  const writer: ToolProgress.Writer<E, R> = {
    output: (chunk, skipped) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          yield* check
          yield* OutputRetention.append(buffer, chunk, skipped)
          if (chunk.length > 0 || skipped !== undefined) yield* mark()
        }),
      ),
    details: (value) =>
      admissions.withPermit(
        Effect.gen(function* () {
          if (!persistRunning) {
            yield* check
            const owned = yield* Value.copy(Schema.Json, value)
            yield* boundedReports(owned, diagnostics)
            yield* check
            details = owned
            yield* mark()
            return
          }
          const waiter = yield* Deferred.make<void, ToolProgress.Failure<E>>()
          yield* Effect.gen(function* () {
            yield* Effect.uninterruptible(
              Effect.gen(function* () {
                yield* check
                details = yield* Value.copy(Schema.Json, value)
                yield* mark(waiter)
              }),
            )
            yield* Deferred.await(waiter)
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                waiters.delete(waiter)
              }),
            ),
          )
        }),
      ),
    diagnostic: (value) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          yield* check
          const owned = yield* Value.copy(ToolResult.DiagnosticSchema, value)
          yield* boundedReports(details, [...diagnostics, owned])
          diagnostics = [...diagnostics, owned]
          yield* mark()
        }),
      ),
    snapshot: Effect.suspend(() =>
      failure === undefined
        ? Value.copy(ToolProgress.Snapshot, current())
        : Effect.failCause(failure),
    ),
    failure: Effect.sync(() => Option.fromUndefinedOr(failure)),
    flush,
    settle: (ending, change) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (failure !== undefined) return yield* Effect.failCause(failure)
          if (mode !== 'running')
            return yield* error('closed', 'The progress call is already settling or closed.')
          mode = 'stopping'
          yield* Deferred.succeed(stop, undefined)
          yield* Deferred.succeed(wake, undefined)
          const exit = yield* Effect.exit(
            restore(
              write.withPermit(
                Effect.gen(function* () {
                  if (failure !== undefined) return yield* Effect.failCause(failure)
                  yield* OutputRetention.finish(buffer)
                  status = ending
                  yield* mark()
                  final = current()
                  return yield* gate.commit((tx) =>
                    Effect.gen(function* () {
                      settlementTx = tx
                      const snapshot = yield* flush(tx)
                      return yield* change(tx, snapshot)
                    }).pipe(
                      Effect.ensuring(
                        Effect.sync(() => {
                          settlementTx = undefined
                        }),
                      ),
                    ),
                  )
                }),
              ),
            ),
          )
          mode = 'closed'
          if (Exit.isFailure(exit)) {
            yield* poison(
              Cause.fail(
                error(
                  'settlement',
                  'Final progress/receipt batch failed; no acknowledgement was issued.',
                  exit.cause,
                ),
              ),
            )
            return yield* Effect.failCause(exit.cause)
          }
          committedRevision = revision
          yield* acknowledge(revision)
          return exit.value
        }),
      ),
  }
  let frozen: ToolProgress.Snapshot | undefined
  cancellation.set(writer, () =>
    Effect.uninterruptible(
      Effect.gen(function* () {
        if (frozen !== undefined) return yield* Value.copy(ToolProgress.Snapshot, frozen)
        mode = 'closed'
        yield* Deferred.succeed(stop, undefined)
        yield* Deferred.succeed(wake, undefined)
        if (workerFiber !== undefined) yield* Fiber.interrupt(workerFiber)
        yield* poison(
          Cause.fail(error('closed', 'The accepted report was cancelled before acknowledgement.')),
        )
        yield* OutputRetention.finish(buffer)
        frozen = yield* Value.copy(ToolProgress.Snapshot, current())
        return yield* Value.copy(ToolProgress.Snapshot, frozen)
      }),
    ),
  )
  return writer
})

export const retainResult = Effect.fnUntraced(function* <A extends ToolResult.Output<unknown>>(
  result: A,
  policy: OutputRetention.Policy = OutputRetention.defaultPolicy,
) {
  const buffer = yield* OutputRetention.make(policy)
  let first = -1
  let last = -1
  for (let index = 0; index < result.content.length; index++) {
    const part = result.content[index]
    if (part?.type !== 'text') continue
    if (first === -1) first = index
    last = index
    yield* OutputRetention.append(buffer, part.text)
  }
  const retained = yield* OutputRetention.finish(buffer)
  if (!retained.truncated) return result
  const keep = policy.retain === 'tail' ? last : first
  const content: Array<ToolResult.Content[number]> = []
  for (let index = 0; index < result.content.length; index++) {
    const part = result.content[index]
    if (part === undefined) continue
    if (part.type !== 'text') content.push(part)
    else if (index === keep) content.push({ ...part, text: retained.text })
  }
  return {
    ...result,
    content,
    diagnostics: [
      ...(result.diagnostics ?? []),
      {
        severity: 'warning' as const,
        code: 'output_truncated',
        message: `Retained ${retained.retainedBytes} UTF-8 bytes and ${retained.retainedLines} lines (${policy.retain}); skipped ${retained.skippedBytes} bytes and ${retained.skippedLines} lines.`,
      },
    ],
  }
})
