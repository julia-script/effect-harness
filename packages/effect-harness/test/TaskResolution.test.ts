import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Task from 'effect-harness/Task'
import { TaskError } from 'effect-harness/TaskError'
import * as TaskResolution from 'effect-harness/TaskResolution'
import * as Transaction from 'effect-harness/Transaction'
import * as TaskRegistry from '../src/internal/TaskRegistry.js'
import * as TaskTransitions from '../src/internal/TaskTransitions.js'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Registry from 'effect-harness/Registry'
import * as Toolkit from 'effect-harness/Toolkit'

const root = Record.ROOT_CONVERSATION_ID
const input = Schema.Struct({ count: Schema.Natural })
const checkpoint = Schema.Struct({ phase: Schema.Literal('work'), count: Schema.Natural })
const options = {
  kind: 'resolution.counter',
  version: 2,
  input,
  checkpoint,
  success: Schema.Natural,
  failure: Schema.String,
  initial: ({ count }: typeof input.Type) => ({ phase: 'work' as const, count }),
  phases: { work: () => Effect.void },
}
const definition = Task.make(options)
const binding = <T extends Task.Any>(
  declaration: T,
  context: Context.Context<TaskResolution.DecodingServices<T>>,
): TaskRegistry.Binding<T> => ({
  declaration,
  context,
  owner: { id: 'resolution.test', version: 1, enabled: true },
})
const stored = (version = 2): Record.Task => ({
  id: Record.TaskId.make(2),
  conversationId: root,
  kind: options.kind,
  version,
  input: { count: 3 },
  background: false,
  abortRequested: false,
  state: { status: 'pending', checkpoint: { phase: 'work', count: 3 } },
})
const draft = (version = 1): Transaction.TaskCreate => {
  const { id: _id, ...value } = stored(version)
  return value
}
const assertBlocked = <T extends Task.Any>(
  inspection: TaskResolution.Inspection<T>,
  reason: TaskResolution.Blocked['reason'],
) => {
  assert.strictEqual(inspection._tag, 'blocked')
  if (inspection._tag === 'blocked') assert.strictEqual(inspection.error.reason, reason)
}

describe('task definition resolution', () => {
  it.effect('derives typed missing/newer/incompatible/missing-migration reasons', () =>
    Effect.gen(function* () {
      assertBlocked(
        yield* TaskResolution.inspect<typeof definition>(undefined, stored()),
        'missingDefinition',
      )
      assertBlocked(yield* TaskResolution.inspect(definition, stored(3)), 'newerVersion')
      assertBlocked(yield* TaskResolution.inspect(definition, stored(1)), 'missingMigration')
      assertBlocked(
        yield* TaskResolution.inspect(definition, { ...stored(), input: { count: -1 } }),
        'incompatible',
      )
      assertBlocked(
        yield* TaskResolution.inspect(definition, {
          ...stored(),
          state: { status: 'pending', checkpoint: { phase: 'other' } },
        }),
        'incompatible',
      )
      assertBlocked(
        yield* TaskResolution.inspect({ ...definition, kind: 'other' }, stored()),
        'incompatible',
      )
      assertBlocked(
        yield* TaskResolution.inspect({ ...definition, phases: {} }, stored()),
        'incompatible',
      )
      const unsupported = stored()
      Object.defineProperty(unsupported, 'schedulerFormat', {
        enumerable: true,
        value: { version: 1, minimumReader: 2 },
      })
      assertBlocked(yield* TaskResolution.inspect(definition, unsupported), 'unsupportedFormat')
      assertBlocked(yield* TaskResolution.inspectMissing(unsupported), 'unsupportedFormat')
    }),
  )

  it.effect(
    'inspection never invokes initial/phase/abort/migration and reports an exact prior failure',
    () =>
      Effect.gen(function* () {
        let callbacks = 0
        const self = Task.make({
          ...options,
          initial: () => {
            callbacks++
            return { phase: 'work' as const, count: 0 }
          },
          phases: {
            work: () => {
              callbacks++
              return Effect.void
            },
          },
          abort: () => {
            callbacks++
            return Effect.void
          },
          migrate: () => {
            callbacks++
            return Effect.succeed({
              input: { count: 1 },
              checkpoint: { phase: 'work' as const, count: 1 },
            })
          },
        })
        const old = stored(1)
        assert.strictEqual(
          (yield* TaskRegistry.inspect({ tasks: [binding(self, Context.empty())] }, old))._tag,
          'migrationRequired',
        )
        const failure = {
          taskId: old.id,
          definition: self,
          error: TaskResolution.blocked(old, 'migrationFailed', 'Earlier attempt failed'),
        }
        assertBlocked(
          yield* TaskRegistry.inspect({ tasks: [binding(self, Context.empty())] }, old, failure),
          'migrationFailed',
        )
        const replacement = Task.make({
          ...options,
          ...(self.migrate === undefined ? {} : { migrate: self.migrate }),
        })
        assert.strictEqual(
          (yield* TaskRegistry.inspect(
            { tasks: [binding(replacement, Context.empty())] },
            old,
            failure,
          ))._tag,
          'migrationRequired',
        )
        assert.strictEqual((yield* TaskResolution.inspect(self, stored()))._tag, 'compatible')
        assert.strictEqual(callbacks, 0)
        for (const status of ['completing', 'terminal'] as const)
          assert.strictEqual(
            (yield* TaskResolution.inspect<typeof definition>(undefined, {
              ...stored(1),
              state: { status, outcome: { status: 'completed', result: 1 } },
            }))._tag,
            'inactive',
          )
      }),
  )

  it.effect('rejects duplicate/reserved kinds and resolves prototype-like kind/phase keys', () =>
    Effect.gen(function* () {
      assert.strictEqual(
        (yield* TaskRegistry.validate([definition, definition]).pipe(Effect.flip))._tag,
        'TaskError',
      )
      assert.strictEqual(
        (yield* TaskRegistry.validate([{ ...definition, kind: 'harness.run' }]).pipe(Effect.flip))
          ._tag,
        'TaskError',
      )
      const phase = Schema.Struct({ phase: Schema.Literal('__proto__') })
      const self = Task.make({
        ...options,
        kind: 'toString',
        checkpoint: phase,
        initial: () => ({ phase: '__proto__' as const }),
        phases: { ['__proto__']: () => Effect.void },
      })
      yield* TaskRegistry.validate([self])
      const result = yield* TaskRegistry.inspect(
        { tasks: [binding(self, Context.empty())] },
        {
          ...stored(),
          kind: self.kind,
          state: { status: 'pending', checkpoint: { phase: '__proto__' } },
        },
      )
      assert.strictEqual(result._tag, 'compatible')
      if (result._tag === 'compatible') assert.deepEqual(result.checkpoint, { phase: '__proto__' })
      assertBlocked(
        yield* TaskRegistry.inspect(
          { tasks: [binding(definition, Context.empty())] },
          { ...stored(), kind: 'constructor' },
        ),
        'missingDefinition',
      )
    }),
  )

  it.effect('retains codec service requirements and detaches decoded payloads', () =>
    Effect.gen(function* () {
      class Decoder extends Context.Service<Decoder, number>()('resolution/Decoder') {}
      let decoded = 0
      const number = Schema.Natural.pipe(
        Schema.decodeTo(Schema.Natural, {
          decode: SchemaGetter.transformEffect((value: number) =>
            Decoder.pipe(
              Effect.map(() => {
                decoded++
                return value
              }),
            ),
          ),
          encode: SchemaGetter.transform((value: number) => value),
        }),
      )
      const self = Task.make({ ...options, input: Schema.Struct({ count: number }) })
      const record = stored()
      const result = yield* TaskResolution.inspect(self, record).pipe(
        Effect.provideService(Decoder, 7),
      )
      assert.strictEqual(decoded, 1)
      if (result._tag === 'compatible') {
        assert.deepEqual(result.input, { count: 3 })
        assert.notStrictEqual(result.input, record.input)
        assert.notStrictEqual(result.checkpoint, record.state.checkpoint)
      } else assert.fail('Expected compatible')
    }),
  )

  it.effect(
    'inspects the real canonical registry binding with isolated codecs and disabled denial',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          class Decoder extends Context.Service<Decoder, number>()('resolution/BoundDecoder') {}
          let calls = 0
          const number = Schema.Natural.pipe(
            Schema.decodeTo(Schema.Natural, {
              decode: SchemaGetter.transformEffect((value: number) =>
                Decoder.pipe(
                  Effect.map((seed) => {
                    calls++
                    return value + seed
                  }),
                ),
              ),
              encode: SchemaGetter.transform((value: number) => value),
            }),
          )
          const first = Task.make({ ...options, input: Schema.Struct({ count: number }) })
          const second = Task.make({
            ...options,
            kind: 'resolution.other',
            input: Schema.Struct({ count: number }),
          })
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.provide(
              Registry.withTask(
                Registry.toolkit({ id: 'first', version: 1 }, Toolkit.make()),
                first,
              ),
              Layer.succeed(Decoder, 7),
            ),
          )
          yield* registry.install(
            Registry.provide(
              Registry.withTask(
                Registry.toolkit({ id: 'second', version: 1 }, Toolkit.make()),
                second,
              ),
              Layer.succeed(Decoder, 30),
            ),
          )
          const lease = yield* registry.acquire
          const resolved = yield* TaskRegistry.inspect(lease.snapshot, stored())
          assert.strictEqual(resolved._tag, 'compatible')
          if (resolved._tag === 'compatible') {
            assert.deepEqual(resolved.input, { count: 10 })
            assert.strictEqual(resolved.binding?.owner.id, 'first')
            assert.strictEqual(resolved.definition, resolved.binding?.declaration)
          }
          const other = yield* TaskRegistry.inspect(lease.snapshot, {
            ...stored(),
            kind: second.kind,
          })
          if (other._tag === 'compatible') assert.deepEqual(other.input, { count: 33 })
          else assert.fail('Expected second binding')
          yield* registry.install(
            Registry.provide(
              Registry.withTask(
                Registry.toolkit({ id: 'first', version: 2, enabled: false }, Toolkit.make()),
                first,
              ),
              Layer.succeed(Decoder, 100),
            ),
          )
          const disabled = yield* registry.acquire
          assertBlocked(
            yield* TaskRegistry.inspect(disabled.snapshot, stored()),
            'disabledDefinition',
          )
          assert.strictEqual(calls, 2)
          const pinned = yield* TaskRegistry.inspect(lease.snapshot, stored())
          if (pinned._tag === 'compatible') assert.deepEqual(pinned.input, { count: 10 })
          else assert.fail('Old lease must remain coherent')
        }),
      ),
  )
})

describe('task invocation preparation seams', () => {
  it.effect(
    'checks live run/abort reservations and preserves first-writer-wins owned JSON memos',
    () =>
      Effect.gen(function* () {
        const current = {
          ...stored(),
          state: { status: 'running' as const, checkpoint: { phase: 'work' } },
        }
        const invocation = {
          taskId: current.id,
          conversationId: current.conversationId,
          kind: current.kind,
          version: current.version,
          mode: 'run' as const,
          ended: false,
        }
        yield* TaskTransitions.gate(current, invocation)
        for (const invalid of [
          { ...invocation, ended: true },
          { ...invocation, version: 1 },
          { ...invocation, taskId: Record.TaskId.make(99) },
          { ...invocation, mode: 'abort' as const },
        ])
          assert.strictEqual(
            (yield* TaskTransitions.gate(current, invalid).pipe(Effect.flip)).reason,
            'revoked',
          )
        const aborted = { ...current, abortRequested: true }
        assert.strictEqual(
          (yield* TaskTransitions.gate(aborted, invocation).pipe(Effect.flip)).reason,
          'revoked',
        )
        yield* TaskTransitions.gate(aborted, { ...invocation, mode: 'abort' })
        assert.isTrue(Option.isNone(TaskTransitions.memo(current, 'toString')))
        const caller = { count: 5 }
        const prepared = yield* TaskTransitions.memoize(current, '__proto__', input, caller)
        caller.count = 100
        assert.deepEqual(prepared.value, { count: 5 })
        assert.deepEqual(Option.getOrThrow(TaskTransitions.memo(prepared.record, '__proto__')), {
          count: 5,
        })
        assert.deepEqual(
          (yield* TaskTransitions.memoize(prepared.record, '__proto__', input, { count: 9 })).value,
          { count: 5 },
        )
        const nullMemo = yield* TaskTransitions.memoize(
          prepared.record,
          'toString',
          Schema.Json,
          null,
        )
        assert.deepEqual(TaskTransitions.memo(nullMemo.record, 'toString'), Option.some(null))
        assert.strictEqual(
          (yield* TaskTransitions.memoize(nullMemo.record, 'toString', input, { count: 1 }).pipe(
            Effect.flip,
          ))._tag,
          'SchemaError',
        )
        assert.isTrue(Option.isNone(TaskTransitions.memo(current, '__proto__')))
      }),
  )
})

for (const backend of ['sqlite', 'jsonl'] as const) {
  it.effect(`private migration survives native ${backend} close/reopen`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'task-migration-' })
        const layer =
          backend === 'sqlite'
            ? Storage.layerSql.pipe(
                Layer.provide(SqliteClient.layer({ filename: `${directory}/store.sqlite` })),
              )
            : Storage.layerJsonl({ filePath: `${directory}/store.jsonl` })
        let calls = 0
        const migrating = Task.make({
          ...options,
          migrate: () => {
            calls++
            return Effect.succeed({
              input: { count: 6 },
              checkpoint: { phase: 'work' as const, count: 6 },
            })
          },
        })
        const migrated = yield* Effect.scoped(
          Effect.gen(function* () {
            const session = yield* Session.make()
            return yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.ensureRoot(tx)
                const task = yield* Transaction.createTask(tx, {
                  ...draft(),
                  restartPolicy: 'abandon',
                  abortRequested: true,
                  abortIntent: { _tag: 'request', reason: 'stopped' },
                })
                return yield* TaskRegistry.upgrade(tx, migrating, task.id)
              }),
            )
          }),
        ).pipe(Effect.provide(layer))
        yield* Effect.scoped(
          Effect.gen(function* () {
            const session = yield* Session.make()
            assert.deepEqual(Option.getOrThrow(yield* Session.task(session, migrated.id)), migrated)
            yield* Session.commit(session, (tx) => TaskRegistry.upgrade(tx, migrating, migrated.id))
            assert.strictEqual(calls, 1)
          }),
        ).pipe(Effect.provide(layer))
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )
}

describe('scheduler-private task migration', () => {
  it.effect('caught initial callback failure preserves owned input and all earlier drafts', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.make()
        const caller = { count: 3 }
        const broken = Task.make({
          ...options,
          initial: (owned) => {
            Reflect.set(owned, 'count', 99)
            throw new Error('fixture initial callback')
          },
        })
        const saved = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureRoot(tx)
            const task = yield* Transaction.createTask(tx, draft())
            const entry = yield* Transaction.appendEntry(tx, root, { kind: 'initial.before' })
            const error = yield* Task.initialize(broken, caller).pipe(Effect.flip)
            assert.strictEqual(error._tag, 'TaskError')
            assert.deepEqual(caller, { count: 3 })
            assert.deepEqual(Option.getOrThrow(yield* Transaction.task(tx, task.id)), task)
            assert.isTrue(Option.isSome(yield* Transaction.entry(tx, entry.id)))
            return task
          }),
        )
        assert.deepEqual(Option.getOrThrow(yield* Session.task(session, saved.id)), saved)
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'upgrades version/input/checkpoint while retaining owner, waits, abort intent, policy and memos',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const migrating = Task.make({
            ...options,
            migrate: ({ version, input: previous }) => {
              assert.strictEqual(version, 1)
              assert.deepEqual(previous, { count: 3 })
              return Effect.succeed({
                input: { count: 7 },
                checkpoint: { phase: 'work' as const, count: 7 },
              })
            },
          })
          const task = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureRoot(tx)
              const owner = yield* Transaction.createTask(tx, draft())
              const child = yield* Transaction.createTask(tx, {
                ...draft(),
                owner: owner.id,
                restartPolicy: 'abandon',
                abortRequested: true,
                abortIntent: { _tag: 'restart' },
                state: {
                  status: 'waiting',
                  checkpoint: { phase: 'legacy' },
                  on: [owner.id],
                  policy: 'allSettled',
                },
                memos: { ['__proto__']: 'saved', toString: 3 },
              })
              const upgraded = yield* TaskRegistry.upgrade(tx, migrating, child.id)
              assert.strictEqual(upgraded.version, 2)
              assert.deepEqual(upgraded.input, { count: 7 })
              assert.deepEqual(upgraded.state, {
                status: 'waiting',
                checkpoint: { phase: 'work', count: 7 },
                on: [owner.id],
                policy: 'allSettled',
              })
              assert.deepEqual(upgraded.memos, child.memos)
              assert.strictEqual(upgraded.owner, child.owner)
              assert.deepEqual(upgraded.abortIntent, child.abortIntent)
              assert.strictEqual(upgraded.restartPolicy, child.restartPolicy)
              assert.deepEqual(yield* TaskRegistry.upgrade(tx, migrating, child.id), upgraded)
              assert.strictEqual(
                (yield* Transaction.putTask(tx, { ...upgraded, version: 3 }).pipe(Effect.flip))
                  ._tag,
                'SessionError',
              )
              return upgraded
            }),
          )
          assert.deepEqual(Option.getOrThrow(yield* Session.task(session, task.id)), task)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  for (const failure of [
    'error',
    'invalidInput',
    'invalidCheckpoint',
    'throw',
    'defect',
    'interruption',
  ] as const) {
    it.effect(`caught ${failure} stages no migration and preserves prior task/entry drafts`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const valid = { input: { count: 9 }, checkpoint: { phase: 'work' as const, count: 9 } }
          const migrating = Task.make({
            ...options,
            migrate: ({ input: ownedInput, checkpoint: ownedCheckpoint }) => {
              if (typeof ownedInput === 'object' && ownedInput !== null)
                Reflect.set(ownedInput, 'count', 88)
              if (typeof ownedCheckpoint === 'object' && ownedCheckpoint !== null)
                Reflect.set(ownedCheckpoint, 'phase', 'mutated')
              switch (failure) {
                case 'error':
                  return Effect.fail(
                    new TaskError({
                      reason: 'migrationFailed',
                      operation: 'fixture',
                      message: 'stop',
                    }),
                  )
                case 'invalidInput':
                  return Effect.succeed({ ...valid, input: { count: -1 } })
                case 'invalidCheckpoint':
                  return Effect.succeed({
                    ...valid,
                    checkpoint: { phase: 'work' as const, count: -1 },
                  })
                case 'throw':
                  throw new Error('fixture')
                case 'defect':
                  return Effect.die('fixture')
                case 'interruption':
                  return Effect.interrupt
              }
            },
          })
          const saved = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureRoot(tx)
              const task = yield* Transaction.createTask(tx, draft())
              const entry = yield* Transaction.appendEntry(tx, root, { kind: 'migration.before' })
              const prior = { ...task, memos: { before: 1 } }
              yield* Transaction.putTask(tx, prior)
              const fiber = yield* TaskRegistry.upgrade(tx, migrating, task.id).pipe(
                Effect.forkChild,
              )
              const exit = yield* Fiber.await(fiber)
              assert.isTrue(Exit.isFailure(exit))
              if (Exit.isFailure(exit)) {
                if (failure === 'defect') assert.isTrue(Cause.hasDies(exit.cause))
                else if (failure === 'interruption') assert.isTrue(Cause.hasInterrupts(exit.cause))
                else assert.isTrue(Cause.hasFails(exit.cause))
              }
              assert.deepEqual(Option.getOrThrow(yield* Transaction.task(tx, task.id)), prior)
              assert.isTrue(Option.isSome(yield* Transaction.entry(tx, entry.id)))
              return prior
            }),
          )
          assert.deepEqual(Option.getOrThrow(yield* Session.task(session, saved.id)), saved)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
    )
  }

  it.effect(
    'rejects kind mismatch, newer versions, terminal and held receipts before migration runs',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          let calls = 0
          const migrating = Task.make({
            ...options,
            migrate: () => {
              calls++
              return Effect.succeed({
                input: { count: 1 },
                checkpoint: { phase: 'work' as const, count: 1 },
              })
            },
          })
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureRoot(tx)
              for (const value of [
                { ...draft(), kind: 'other' },
                draft(3),
                {
                  ...draft(),
                  state: {
                    status: 'terminal' as const,
                    outcome: { status: 'completed', result: 1 },
                  },
                },
                {
                  ...draft(),
                  state: {
                    status: 'completing' as const,
                    outcome: { status: 'completed', result: 1 },
                  },
                },
              ]) {
                const task = yield* Transaction.createTask(tx, value)
                const error = yield* TaskRegistry.upgrade(tx, migrating, task.id).pipe(Effect.flip)
                assert.strictEqual(error._tag, 'TaskResolutionBlocked')
              }
              const missingMigration = yield* Transaction.createTask(tx, draft())
              const missing = yield* TaskRegistry.upgrade(tx, definition, missingMigration.id).pipe(
                Effect.flip,
              )
              assert.strictEqual(missing._tag, 'TaskResolutionBlocked')
              if (missing._tag === 'TaskResolutionBlocked')
                assert.strictEqual(missing.reason, 'missingMigration')
            }),
          )
          assert.strictEqual(calls, 0)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
