import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Prompt from 'effect/ai/Prompt'
import * as Hook from '../src/Hook.js'
import * as HookReducer from '../src/HookReducer.js'
import * as Registry from '../src/Registry.js'
import * as Task from '../src/Task.js'
import * as TaskHook from '../src/TaskHook.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Record from '../src/Record.js'
import type * as ToolResult from '../src/ToolResult.js'
import { HookExecution } from '../src/HookExecution.js'
import { SessionError } from '../src/SessionError.js'
import { RegistryError } from '../src/RegistryError.js'
import { call, execution, Label, registration, toolkit } from './fixtures/Registry.js'
import { ToolExecution } from '../src/ToolExecution.js'

const hookAccess = Layer.effect(HookExecution, ToolExecution).pipe(Layer.provide(execution))
const result = (text: string): ToolResult.Result => ({
  content: [Prompt.textPart({ text })],
  isError: false,
  diagnostics: [],
})
const text = (prompt: Prompt.Prompt) =>
  prompt.content
    .map((m) =>
      m.role === 'user' ? m.content.map((p) => (p.type === 'text' ? p.text : '')).join('') : '',
    )
    .join('')
const installHook = <N extends Hook.Name, E, R>(
  registry: Registry.Service,
  id: string,
  hook: Hook.Hook<N, E, R>,
) => registry.install(Registry.hook({ id, version: 1 }, hook))

const task = Task.make({
  kind: 'custom',
  version: 1,
  input: Schema.String,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal('ready'), count: Schema.Finite }),
    Schema.Struct({ phase: Schema.Literal('done'), message: Schema.String }),
  ]),
  success: Schema.String,
  failure: Schema.String,
  initial: () => ({ phase: 'ready' as const, count: 1 }),
  phases: { ready: () => Effect.void, done: () => Effect.void },
})

describe('Registry leased hook reducers', () => {
  it.effect(
    'chains accepted request/result replacements and preserves original-event legacy mode',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const seen: string[] = []
          yield* installHook(
            registry,
            'request-one',
            Hook.make({
              event: 'beforeRequest',
              execute: ({ prompt }) =>
                Effect.sync(() => {
                  seen.push(text(prompt))
                  return Prompt.make(text(prompt) + '/one')
                }),
            }),
          )
          yield* installHook(
            registry,
            'request-two',
            Hook.make({
              event: 'beforeRequest',
              execute: ({ prompt }) =>
                Effect.sync(() => {
                  seen.push(text(prompt))
                  return Prompt.make(text(prompt) + '/two')
                }),
            }),
          )
          const lease = yield* registry.acquire
          const chained = yield* lease.dispatchHook({
            _tag: 'beforeRequest',
            prompt: Prompt.make('start'),
          })
          assert.strictEqual(text(chained!), 'start/one/two')
          assert.deepStrictEqual(seen, ['start', 'start/one'])
          seen.length = 0
          const legacy = yield* lease.dispatchHook(
            { _tag: 'beforeRequest', prompt: Prompt.make('start') },
            { composition: 'legacy' },
          )
          assert.strictEqual(text(legacy!), 'start/two')
          assert.deepStrictEqual(seen, ['start', 'start'])
          yield* installHook(
            registry,
            'result-one',
            Hook.make({
              event: 'afterTool',
              execute: ({ result: r }) => Effect.succeed({ ...r, details: 'one' }),
            }),
          )
          yield* installHook(
            registry,
            'result-two',
            Hook.make({
              event: 'afterTool',
              execute: ({ result: r }) =>
                Effect.sync(() => {
                  seen.push(typeof r.details === 'string' ? r.details : JSON.stringify(r.details))
                  return {
                    ...r,
                    details:
                      (typeof r.details === 'string' ? r.details : JSON.stringify(r.details)) +
                      '/two',
                  }
                }),
            }),
          )
          const current = yield* registry.acquire
          const replaced = yield* current.dispatchHook({
            _tag: 'afterTool',
            call,
            result: result('answer'),
          })
          assert.strictEqual(replaced!.details, 'one/two')
          assert.strictEqual(seen.at(-1), 'one')
        }),
      ).pipe(Effect.provide(hookAccess)),
  )

  it.effect(
    'validates each argument transform, chains arguments, and stops at the first block',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const declaration = Tool.make('input', {
            parameters: Schema.Struct({ count: Schema.Finite }),
            success: Schema.Finite,
            failure: Schema.String,
          })
          const inputTools = Toolkit.make(declaration)
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'tool', version: 1 }, inputTools),
              inputTools.toLayer({ input: ({ count }) => Effect.succeed(count) }),
            ),
          )
          const seen: number[] = []
          yield* installHook(
            registry,
            'transform',
            Hook.make({
              event: 'beforeTool',
              execute: ({ call }) =>
                Effect.succeed({
                  _tag: 'replaceArguments',
                  arguments: { count: Number(call.arguments.count) + 1 },
                }),
            }),
          )
          yield* installHook(
            registry,
            'block',
            Hook.make({
              event: 'beforeTool',
              execute: ({ call }) =>
                Effect.sync(() => {
                  seen.push(Number(call.arguments.count))
                  return { _tag: 'block' as const, message: 'denied' }
                }),
            }),
          )
          yield* installHook(
            registry,
            'allow',
            Hook.make({
              event: 'beforeTool',
              execute: () => Effect.die('must not run after block'),
            }),
          )
          const lease = yield* registry.acquire
          assert.deepStrictEqual(
            yield* lease.dispatchHook({
              _tag: 'beforeTool',
              call: { id: 'x', name: 'input', arguments: { count: 1 } },
            }),
            { _tag: 'block', message: 'denied' },
          )
          assert.deepStrictEqual(seen, [2])
          yield* registry.install(
            Registry.hook(
              { id: 'transform', version: 2 },
              Hook.make({
                event: 'beforeTool',
                execute: () =>
                  Effect.succeed({ _tag: 'replaceArguments', arguments: { count: 'invalid' } }),
              }),
            ),
          )
          const invalid = yield* registry.acquire
          assert.isTrue(
            Schema.isSchemaError(
              yield* invalid
                .dispatchHook({
                  _tag: 'beforeTool',
                  call: { id: 'x', name: 'input', arguments: { count: 1 } },
                })
                .pipe(Effect.flip),
            ),
          )
          assert.deepStrictEqual(seen, [2])
        }),
      ).pipe(Effect.provide(hookAccess)),
  )

  it.effect(
    'chooses first continuation and compaction decisions while observers run stable order',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const seen: string[] = []
          for (const id of ['one', 'two']) {
            yield* installHook(
              registry,
              'yield-' + id,
              Hook.make({
                event: 'onYield',
                execute: () =>
                  Effect.sync(() => {
                    seen.push('yield-' + id)
                    return {
                      _tag: 'continue' as const,
                      input: Prompt.userMessage({ content: [Prompt.textPart({ text: id })] }),
                    }
                  }),
              }),
            )
            yield* installHook(
              registry,
              'compact-' + id,
              Hook.make({
                event: 'beforeCompact',
                execute: () =>
                  Effect.sync(() => {
                    seen.push('compact-' + id)
                    return { _tag: 'summary' as const, summary: id }
                  }),
              }),
            )
            yield* installHook(
              registry,
              'observer-' + id,
              Hook.make({
                event: 'afterResponse',
                execute: () =>
                  Effect.sync(() => {
                    seen.push('observer-' + id)
                  }),
              }),
            )
          }
          const lease = yield* registry.acquire
          const message = Prompt.assistantMessage({
            content: [Prompt.textPart({ text: 'answer' })],
          })
          const continuation = yield* lease.dispatchHook({ _tag: 'onYield', message })
          assert.strictEqual(continuation!._tag, 'continue')
          assert.deepStrictEqual(
            yield* lease.dispatchHook({
              _tag: 'beforeCompact',
              reason: 'manual',
              entries: [],
              prompt: Prompt.make('context'),
              firstKept: yield* Schema.decodeEffect(Record.EntryId)(1),
            }),
            { _tag: 'summary', summary: 'one' },
          )
          yield* lease.dispatchHook({ _tag: 'afterResponse', message })
          assert.deepStrictEqual(seen, ['yield-one', 'compact-one', 'observer-one', 'observer-two'])
        }),
      ).pipe(Effect.provide(hookAccess)),
  )

  it.effect(
    'reports ordinary throws, blocks thrown beforeTool decisions, and propagates transaction failures',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const reported: HookReducer.HookFailure[] = []
          const report = (failure: HookReducer.HookFailure) =>
            Effect.sync(() => {
              reported.push(failure)
            })
          yield* registry.install(registration('tool', 'backend'))
          yield* installHook(
            registry,
            'throw-request',
            Hook.make({
              event: 'beforeRequest',
              execute: (): Effect.Effect<void> => {
                throw new TypeError('bad callback')
              },
            }),
          )
          yield* installHook(
            registry,
            'request',
            Hook.make({
              event: 'beforeRequest',
              execute: () => Effect.succeed(Prompt.make('accepted')),
            }),
          )
          yield* installHook(
            registry,
            'throw-tool',
            Hook.make({ event: 'beforeTool', execute: () => Effect.die('bad beforeTool') }),
          )
          let lease = yield* registry.acquire
          assert.strictEqual(
            text(
              (yield* lease.dispatchHook(
                { _tag: 'beforeRequest', prompt: Prompt.make('initial') },
                { report },
              ))!,
            ),
            'accepted',
          )
          assert.strictEqual(
            (yield* lease.dispatchHook({ _tag: 'beforeTool', call }, { report }))!._tag,
            'block',
          )
          assert.strictEqual(reported.length, 2)
          yield* installHook(
            registry,
            'critical',
            Hook.make({
              event: 'afterResponse',
              execute: () =>
                HookExecution.pipe(
                  Effect.flatMap((access) => access.commit<void, never, never>(() => Effect.void)),
                ),
            }),
          )
          lease = yield* registry.acquire
          const failure = new SessionError({
            reason: 'conflict',
            operation: 'commit',
            message: 'transaction rejected',
          })
          const access = yield* HookExecution
          const failed = yield* lease
            .dispatchHook(
              { _tag: 'afterResponse', message: Prompt.assistantMessage({ content: [] }) },
              { report },
            )
            .pipe(
              Effect.provideService(HookExecution, {
                ...access,
                commit: () => Effect.fail(failure),
              }),
              Effect.flip,
            )
          assert.strictEqual(failed._tag, 'HookFailure')
          assert.strictEqual(
            (yield* Schema.decodeUnknownEffect(Schema.toType(HookReducer.HookFailure))(failed))
              .reason,
            'critical',
          )
          assert.strictEqual(reported.length, 2)
        }),
      ).pipe(Effect.provide(hookAccess)),
  )

  it.effect('rejects malformed replacements and checks raw results before schema stripping', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Registry.make
        const bad = Hook.make({
          event: 'afterTool',
          execute: () =>
            Effect.succeed({
              ...result('bad'),
              control: { terminate: true as const, surprise: true },
            }),
        })
        yield* installHook(registry, 'bad', bad)
        const lease = yield* registry.acquire
        const excess = yield* lease
          .dispatchHook({ _tag: 'afterTool', call, result: result('original') })
          .pipe(Effect.flip)
        assert.strictEqual(
          (yield* Schema.decodeUnknownEffect(Schema.toType(HookReducer.HookFailure))(excess))
            .reason,
          'validation',
        )
        const failed = yield* lease
          .dispatchHook(
            { _tag: 'afterTool', call, result: result('original') },
            {
              validateResult: (value) =>
                Effect.gen(function* () {
                  const raw = yield* Schema.decodeUnknownEffect(
                    Schema.Struct({ control: Schema.Struct({ surprise: Schema.Boolean }) }),
                  )(value)
                  assert.isTrue(raw.control.surprise)
                  return yield* new SessionError({
                    reason: 'invalid',
                    operation: 'validate',
                    message: 'unknown key',
                  })
                }),
            },
          )
          .pipe(Effect.flip)
        assert.strictEqual(failed._tag, 'SessionError')
        yield* registry.install(
          Registry.hook(
            { id: 'bad', version: 2 },
            Hook.make({
              event: 'afterTool',
              execute: () =>
                Effect.succeed({
                  ...result('bad'),
                  isError: 'wrong',
                } as unknown as ToolResult.Result),
            }),
          ),
        )
        const current = yield* registry.acquire
        assert.strictEqual(
          (yield* Schema.decodeUnknownEffect(Schema.toType(HookReducer.HookFailure))(
            yield* current
              .dispatchHook({ _tag: 'afterTool', call, result: result('original') })
              .pipe(Effect.flip),
          )).reason,
          'validation',
        )
      }),
    ).pipe(Effect.provide(hookAccess)),
  )

  it.effect(
    'captures independent hook services, survives reload/uninstall, and retains resources until interrupted dispatch settles',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const started = yield* Deferred.make<void>()
          const seen: string[] = []
          let released = 0
          const resource = Layer.effect(
            Label,
            Effect.acquireRelease(Effect.succeed('old'), () =>
              Effect.sync(() => {
                released++
              }),
            ),
          )
          yield* installHook(
            registry,
            'owner',
            Hook.make({
              event: 'beforeRequest',
              execute: () =>
                Effect.gen(function* () {
                  const label = yield* Label
                  const access = yield* HookExecution
                  seen.push(label + '/' + access.conversationId)
                  yield* Deferred.succeed(started, undefined)
                  return yield* Effect.never
                }),
            }).pipe(Hook.provide(resource)),
          )
          const scope = yield* Scope.make()
          const lease = yield* Scope.provide(registry.acquire, scope)
          const fiber = yield* lease
            .dispatchHook({ _tag: 'beforeRequest', prompt: Prompt.make('x') })
            .pipe(Effect.forkChild)
          yield* Deferred.await(started)
          yield* registry.install(
            Registry.hook(
              { id: 'owner', version: 2 },
              Hook.make({
                event: 'beforeRequest',
                execute: () => Label.pipe(Effect.map(Prompt.make)),
              }).pipe(Hook.provide(Layer.succeed(Label, 'new'))),
            ),
          )
          yield* Scope.close(scope, Exit.void)
          yield* registry.uninstall('owner')
          assert.strictEqual(released, 0)
          yield* Fiber.interrupt(fiber)
          const interrupted = yield* Fiber.await(fiber)
          assert.isTrue(Exit.isFailure(interrupted) && Cause.hasInterrupts(interrupted.cause))
          assert.strictEqual(released, 1)
          assert.deepStrictEqual(seen, ['old/1'])
          assert.strictEqual(
            (yield* Schema.decodeUnknownEffect(Schema.toType(RegistryError))(
              yield* lease
                .dispatchHook({ _tag: 'beforeRequest', prompt: Prompt.make('x') })
                .pipe(Effect.flip),
            )).reason,
            'revoked',
          )
        }),
      ).pipe(Effect.provide(hookAccess)),
  )

  it.effect(
    'dispatches typed task phases by kind, version, stage and registration with bound access',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.provide(
              Registry.withTask(Registry.toolkit({ id: 'tasks', version: 1 }, toolkit), task),
              toolkit
                .toLayer({ same: () => Effect.succeed('tool') })
                .pipe(Layer.provideMerge(Layer.succeed(Label, 'tool'))),
            ),
          )
          const seen: string[] = []
          for (const id of ['one', 'two'])
            yield* registry.install(
              Registry.taskHook(
                { id, version: 1 },
                TaskHook.make(task, {
                  phase: 'ready',
                  stage: 'beforePhase',
                  execute: (event) =>
                    Effect.gen(function* () {
                      const label = yield* Label
                      const access = yield* HookExecution
                      assert.strictEqual(event.checkpoint.count, 2)
                      assert.strictEqual(event.input, 'input')
                      assert.strictEqual(access.conversationId, event.conversationId)
                      seen.push(label)
                    }),
                }).pipe(TaskHook.provide(Layer.succeed(Label, id))),
              ),
            )
          const lease = yield* registry.acquire
          const event: TaskHook.Event = {
            kind: 'custom',
            phase: 'ready',
            stage: 'beforePhase',
            taskId: yield* Schema.decodeEffect(Record.TaskId)(1),
            conversationId: yield* Schema.decodeEffect(Record.ConversationId)(1),
            input: 'input',
            checkpoint: { phase: 'ready', count: 2 },
          }
          yield* lease.dispatchTaskHook(event)
          assert.deepStrictEqual(seen, ['one', 'two'])
          yield* lease.dispatchTaskHook({ ...event, stage: 'afterPhase' })
          assert.deepStrictEqual(seen, ['one', 'two'])
          yield* lease.dispatchTaskHook(event, { registrations: ['two'] })
          assert.deepStrictEqual(seen, ['one', 'two', 'two'])
          let reported = 0
          const invalidCheckpoint = yield* lease
            .dispatchTaskHook(
              {
                ...event,
                checkpoint: { phase: 'done', message: 'invalid phase' },
              },
              {
                report: () =>
                  Effect.sync(() => {
                    reported++
                  }),
              },
            )
            .pipe(Effect.flip)
          assert.strictEqual(
            (yield* Schema.decodeUnknownEffect(Schema.toType(HookReducer.HookFailure))(
              invalidCheckpoint,
            )).reason,
            'critical',
          )
          assert.strictEqual(reported, 0)
          assert.deepStrictEqual(seen, ['one', 'two', 'two'])
          const before = yield* registry.snapshot
          const incompatible = TaskHook.make(
            { ...task, version: 2 },
            { phase: 'ready', stage: 'beforePhase', execute: () => Effect.void },
          )
          assert.strictEqual(
            (yield* registry
              .install(Registry.taskHook({ id: 'incompatible', version: 1 }, incompatible))
              .pipe(Effect.flip)).reason,
            'invalidRegistration',
          )
          assert.strictEqual(yield* registry.snapshot, before)
        }),
      ).pipe(Effect.provide(hookAccess)),
  )
})
