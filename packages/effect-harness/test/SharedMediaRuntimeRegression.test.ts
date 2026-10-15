import { assert, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Compaction from '../src/Compaction.js'
import * as Record from '../src/Record.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Format from '../src/StorageFormat.js'
import { StorageError } from '../src/StorageError.js'
import * as Task from '../src/Task.js'
import { TaskExecution } from '../src/TaskExecution.js'
import * as TaskScheduler from '../src/internal/TaskScheduler.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Transaction from '../src/Transaction.js'
import * as Protocol from '../src/internal/ContextProtocol.js'
import * as Plan from '../src/internal/CompactionPlan.js'
import * as Run from '../src/internal/RunState.js'
import * as Layer from 'effect/Layer'
import * as PromptUsage from '../src/internal/PromptUsage.js'
import * as Value from '../src/internal/DocumentValue.js'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Hook from '../src/Hook.js'
import * as Settings from '../src/Settings.js'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

for (const data of [new Uint8Array([1, 2, 3]), new URL('https://example.test/a.png'), 'AQID']) {
  it.effect(
    `lossless storage: canonical compaction accepts ${typeof data === 'string' ? 'string' : data.constructor.name} media`,
    () =>
      Effect.gen(function* () {
        const messages = yield* Schema.encodeEffect(Run.Messages)([
          Prompt.userMessage({ content: [Prompt.filePart({ mediaType: 'image/png', data })] }),
        ])
        const entries: Record.Entry[] = [
          {
            id: Record.EntryId.make(2),
            conversationId: Record.ROOT_CONVERSATION_ID,
            kind: 'input',
            model: messages,
          },
          {
            id: Record.EntryId.make(3),
            conversationId: Record.ROOT_CONVERSATION_ID,
            kind: 'input',
            model: [{ role: 'user', content: 'retain me' }],
          },
        ]
        const protocol: Protocol.Protocol = {
          version: 1,
          origin: { conversationId: Record.ROOT_CONVERSATION_ID },
          legacyMode: 'honorAll',
        }
        const view = yield* Protocol.project(Record.ROOT_CONVERSATION_ID, entries, protocol)
        const selection = yield* Compaction.select(
          view,
          { enabled: false, reserveTokens: 20, keepRecentTokens: 0, backgroundTokens: 0 },
          { reason: 'manual' },
        )
        assert.strictEqual(selection._tag, 'Selected')
        if (selection._tag !== 'Selected') return
        const fingerprint = yield* Plan.capture(selection, { entries, view, protocol })
        yield* Plan.validate(fingerprint, { entries, view, protocol })
      }),
  )
}

it.live(
  'lossless storage: mixed native read failure inside invocation commit preserves scheduler diagnostic',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const base = yield* Layer.build(Storage.layerMemory)
        const actual = Context.get(base, Storage.Storage)
        const format = Context.get(base, Format.StorageFormat)
        const critical = new StorageError({
          reason: 'io',
          operation: 'review.read',
          message: 'native read sentinel',
        })
        const full = Cause.combine(Cause.fail(critical), Cause.die('native read cleanup sentinel'))
        let armed = false
        let attempts = 0
        const wrapped: Storage.Storage['Service'] = {
          ...actual,
          conversation: (id) => {
            if (!armed) return actual.conversation(id)
            attempts++
            return Effect.failCause(full)
          },
        }
        const context = base.pipe(
          Context.add(Storage.Storage, wrapped),
          Context.add(Format.StorageFormat, { ...format, storage: wrapped }),
        )
        const session = yield* Session.make().pipe(Effect.provideContext(context))
        yield* Session.commit(session, Transaction.ensureRoot)
        const registry = yield* Registry.make
        const scheduler = yield* TaskScheduler.make(session).pipe(
          Effect.provideService(Registry.Registry, registry),
        )
        const done = yield* Deferred.make<void>()
        const contract = {
          kind: 'review.mixed-read',
          version: 1,
          input: Schema.Void,
          checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
          success: Schema.Void,
          failure: Schema.String,
        } as const
        const definition = Task.make({
          ...contract,
          initial: () => ({ phase: 'run' as const }),
          phases: {
            run: () =>
              Effect.gen(function* () {
                const access = yield* TaskExecution
                yield* access
                  .commit((tx) =>
                    Effect.gen(function* () {
                      armed = true
                      yield* Transaction.conversation(tx, Record.ROOT_CONVERSATION_ID).pipe(
                        Effect.ensuring(
                          Effect.sync(() => {
                            armed = false
                          }),
                        ),
                      )
                    }),
                  )
                  .pipe(Effect.onExit(() => Deferred.succeed(done, undefined)))
              }),
          },
        })
        yield* registry.install(
          Registry.withTask(
            Registry.toolkit({ id: 'review', version: 1 }, Toolkit.make()),
            definition,
          ),
        )
        const ref = yield* scheduler.start(definition, undefined, {
          conversationId: Record.ROOT_CONVERSATION_ID,
        })
        yield* Deferred.await(done)
        const activation = yield* scheduler.activate.pipe(Effect.result)
        assert.strictEqual(activation._tag, 'Failure')
        assert.strictEqual(attempts, 1)
        const health = yield* scheduler.failure
        const saved = yield* Session.task(session, ref.id)
        assert.isTrue(Option.isSome(saved))
        assert.isTrue(
          Option.isSome(health),
          'Native mixed read failure must remain scheduler health',
        )
        if (Option.isSome(health)) assert.strictEqual(health.value.cause, full)
      }),
    ),
)

for (const data of [new Uint8Array([1, 2, 3]), new URL('https://example.test/usage.png')])
  it.effect(
    `lossless storage: persisted usage baseline still matches unchanged ${data.constructor.name} image prompt`,
    () =>
      Effect.gen(function* () {
        const wire = yield* Schema.encodeEffect(Run.Messages)([
          Prompt.userMessage({ content: [Prompt.filePart({ mediaType: 'image/png', data })] }),
        ])
        const entries: Record.Entry[] = [
          {
            id: Record.EntryId.make(2),
            conversationId: Record.ROOT_CONVERSATION_ID,
            kind: 'input',
            model: wire,
          },
          {
            id: Record.EntryId.make(3),
            conversationId: Record.ROOT_CONVERSATION_ID,
            kind: 'input',
            model: [{ role: 'user', content: 'latest' }],
          },
        ]
        const protocol: Protocol.Protocol = {
          version: 1,
          origin: { conversationId: Record.ROOT_CONVERSATION_ID },
          legacyMode: 'honorAll',
        }
        const view = yield* Protocol.project(Record.ROOT_CONVERSATION_ID, entries, protocol)
        const baseline = yield* Value.copy(Schema.toCodecJson(PromptUsage.Baseline), {
          identity: 'test',
          prompt: view.prompt,
          tools: [],
          inputTokens: 9000,
        })
        const result = yield* Compaction.select(
          view,
          { enabled: true, reserveTokens: 2000, keepRecentTokens: 0, backgroundTokens: 0 },
          { reason: 'threshold', contextWindow: 10000, baseline },
        )
        assert.isTrue(
          PromptUsage.applicable(baseline, view.prompt, []),
          'Unchanged durable prompt must retain measured input usage',
        )
        assert.strictEqual(result._tag, 'Selected')
        assert.strictEqual(result.measurement.method, 'usage')
      }),
  )

for (const data of [new Uint8Array([1, 2, 3]), new URL('https://example.test/manual.png'), 'AQID'])
  it.live(
    `lossless storage: public manual compaction accepts stored ${typeof data === 'string' ? 'base64' : data.constructor.name} attachment`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const provider = yield* ScriptedStream.make({
            generateText: () => Effect.die('summary hook supplies summary'),
          })
          const settings = yield* Settings.make({
            compaction: {
              enabled: false,
              reserveTokens: 20,
              keepRecentTokens: 0,
              backgroundTokens: 0,
            },
          })
          let hooks = 0
          const hook = Hook.make({
            event: 'beforeCompact',
            execute: () =>
              Effect.sync(() => {
                hooks++
                return { _tag: 'summary' as const, summary: 'stored summary' }
              }),
          })
          const runtime = yield* HarnessRuntime.make({ hooks: [hook] }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, provider),
            Effect.provideService(Settings.Settings, settings),
          )
          const root = yield* runtime.backend.root
          yield* Session.commit(runtime.session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.appendEntry(tx, root, {
                kind: 'input',
                model: yield* Schema.encodeEffect(Run.Messages)([
                  Prompt.userMessage({
                    content: [Prompt.filePart({ mediaType: 'image/png', data })],
                  }),
                ]),
              })
              yield* Transaction.appendEntry(tx, root, {
                kind: 'input',
                model: [{ role: 'user', content: 'latest' }],
              })
            }),
          )
          const handle = yield* runtime.backend.compact!(root)
          const outcome = yield* runtime.backend.waitCompaction!(handle)
          assert.strictEqual(outcome.status, 'completed')
          assert.strictEqual(hooks, 1)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
