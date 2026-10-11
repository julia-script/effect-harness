import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Document from '../src/Document.js'
import * as Hook from '../src/Hook.js'
import { HookExecution } from '../src/HookExecution.js'
import * as Record from '../src/Record.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Task from '../src/Task.js'
import { TaskExecution } from '../src/TaskExecution.js'
import * as TaskHook from '../src/TaskHook.js'
import * as TaskSupervision from '../src/TaskSupervision.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Transaction from '../src/Transaction.js'

class Label extends Context.Service<Label, string>()('supervision/hooks/Label') {}
const root = Record.ROOT_CONVERSATION_ID
const contract = {
  kind: 'supervision.hooked',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Struct({ phase: Schema.Literal('work') }),
  success: Schema.Natural,
  failure: Schema.String,
}
const settle = Effect.forEach(Array.from({ length: 100 }), () => Effect.yieldNow, { discard: true })
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const registry = yield* Registry.make
  const scheduler = yield* TaskSupervision.make(session).pipe(
    Effect.provideService(Registry.Registry, registry),
  )
  return { session, registry, scheduler }
})

describe('native supervisor leased hooks', () => {
  it.effect(
    'dispatches real native hooks with isolated captured services, gated writes and revoked handles',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const seen: string[] = []
          let retained: Context.Service.Shape<typeof HookExecution> | undefined
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'work' as const }),
            phases: {
              work: ({ input }) =>
                Effect.gen(function* () {
                  seen.push(yield* Label)
                  const access = yield* TaskExecution
                  yield* access.dispatchHook({ _tag: 'conversationCreated', conversationId: root })
                  yield* access.commit((tx) =>
                    access.progress(tx, contract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: input },
                    }),
                  )
                }),
            },
          }).pipe(Task.provide(Layer.succeed(Label, 'task')))
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'task', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          yield* registry.install(
            Registry.hook(
              { id: 'ordinary-hook', version: 1 },
              Hook.make({
                event: 'conversationCreated',
                execute: () =>
                  Effect.gen(function* () {
                    seen.push(yield* Label)
                    const access = yield* HookExecution
                    yield* access.commit((tx) =>
                      Transaction.appendEntry(tx, root, { kind: 'hook.native', data: null }).pipe(
                        Effect.asVoid,
                      ),
                    )
                  }),
              }).pipe(Hook.provide(Layer.succeed(Label, 'ordinary'))),
            ),
          )
          for (const stage of ['beforePhase', 'afterPhase', 'settled'] as const)
            yield* registry.install(
              Registry.taskHook(
                { id: stage, version: 1 },
                TaskHook.make(contract, {
                  phase: 'work',
                  stage,
                  execute: (event) =>
                    Effect.gen(function* () {
                      seen.push(`${stage}/${yield* Label}/${event.input}`)
                      retained = yield* HookExecution
                      if (stage === 'beforePhase')
                        yield* retained.commit((tx) =>
                          Transaction.appendEntry(tx, root, {
                            kind: 'hook.before',
                            data: event.checkpoint.phase,
                          }).pipe(Effect.asVoid),
                        )
                    }),
                }).pipe(TaskHook.provide(Layer.succeed(Label, stage))),
              ),
            )
          assert.deepStrictEqual(
            yield* scheduler.wait(yield* scheduler.start(definition, 7, { conversationId: root })),
            { status: 'completed', result: 7 },
          )
          yield* settle
          assert.deepStrictEqual(seen, [
            'beforePhase/beforePhase/7',
            'task',
            'ordinary',
            'afterPhase/afterPhase/7',
            'settled/settled/7',
          ])
          const entries = yield* Session.scanEntries(session, { conversationId: root }).pipe(
            Stream.runCollect,
          )
          assert.deepStrictEqual(entries.map((entry) => entry.kind).sort(), [
            'hook.before',
            'hook.native',
          ])
          assert.strictEqual(
            (yield* retained!.commit(() => Effect.void).pipe(Effect.flip))._tag,
            'ExecutionError',
          )
          const document = Document.define({
            kind: 'hook.revoked',
            version: 1,
            scope: 'session',
            schema: Schema.Struct({ n: Schema.Natural }),
            initial: () => ({ n: 0 }),
          })
          assert.strictEqual(
            (yield* retained!.snapshot(document, { scope: { _tag: 'session' } }).pipe(Effect.flip))
              ._tag,
            'ExecutionError',
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect('native failure and fresh abort cleanup dispatch the matching lifecycle stages', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { registry, scheduler } = yield* open
        const entered = yield* Deferred.make<void>()
        const seen: string[] = []
        const definition = Task.make({
          ...contract,
          initial: () => ({ phase: 'work' as const }),
          phases: {
            work: ({ input }) =>
              input === 1
                ? Effect.fail('domain')
                : Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          },
          abort: () =>
            Effect.gen(function* () {
              const access = yield* TaskExecution
              yield* access.commit((tx) =>
                access.progress(tx, contract, {
                  status: 'completing',
                  outcome: { status: 'aborted' },
                }),
              )
            }),
        })
        yield* registry.install(
          Registry.withTask(
            Registry.toolkit({ id: 'task', version: 1 }, Toolkit.make()),
            definition,
          ),
        )
        for (const stage of ['failure', 'abort', 'settled'] as const)
          yield* registry.install(
            Registry.taskHook(
              { id: stage, version: 1 },
              TaskHook.make(contract, {
                phase: 'work',
                stage,
                execute: ({ input }) =>
                  Effect.sync(() => {
                    seen.push(`${input}/${stage}`)
                  }),
              }),
            ),
          )
        assert.strictEqual(
          (yield* scheduler.wait(yield* scheduler.start(definition, 1, { conversationId: root })))
            .status,
          'failed',
        )
        yield* settle
        const ref = yield* scheduler.start(definition, 2, { conversationId: root })
        yield* Deferred.await(entered)
        yield* scheduler.abort(ref.id)
        yield* settle
        assert.strictEqual((yield* scheduler.wait(ref)).status, 'aborted')
        assert.deepStrictEqual(seen, [
          '1/failure',
          '1/settled',
          '2/abort',
          '2/failure',
          '2/settled',
        ])
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
