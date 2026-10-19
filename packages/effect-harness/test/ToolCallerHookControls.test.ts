import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Layer from 'effect/Layer'
import * as Deferred from 'effect/Deferred'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Registry from '../src/Registry.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as ToolInventory from '../src/ToolInventory.js'
import * as ToolWrapper from '../src/ToolWrapper.js'
import * as Hook from '../src/Hook.js'
import { HookExecution } from '../src/HookExecution.js'
import { execution } from './fixtures/Registry.js'
import { ToolExecution } from '../src/ToolExecution.js'

const call = { id: 'hook-call', name: 'work', arguments: {} } as const
class Codec extends Context.Service<Codec, string>()('test/caller-wrapper/Codec') {}

const checkedString = (expected: string) =>
  Schema.String.pipe(
    Schema.middlewareDecoding((effect) =>
      Effect.flatMap(Codec, (actual) => {
        assert.strictEqual(actual, expected)
        return effect
      }),
    ),
    Schema.middlewareEncoding((effect) =>
      Effect.flatMap(Codec, (actual) => {
        assert.strictEqual(actual, expected)
        return effect
      }),
    ),
  )

describe('caller metadata and controls through leased wrapper/hooks', () => {
  it.effect('typed transformed schemas use outer codecs and keep delegated original services', () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0
        let wrappers = 0
        const original = Toolkit.make(
          Tool.makeResult('work', {
            parameters: Schema.Struct({ payload: checkedString('original') }),
            success: checkedString('original'),
            resultValidation: 'strict',
          }).addDependency(Codec),
        )
        const registry = yield* Registry.make
        yield* registry.install(
          Registry.provide(
            Registry.toolkit({ id: 'owner', version: 1 }, original),
            original
              .toLayer({
                work: ({ payload }) =>
                  Effect.gen(function* () {
                    assert.strictEqual(yield* Codec, 'original')
                    calls++
                    return { content: [], structuredOutput: `original:${payload}` }
                  }),
              })
              .pipe(Layer.provideMerge(Layer.succeed(Codec, 'original'))),
          ),
        )
        const outer = Tool.makeResult('work', {
          callers: ['tools'],
          parameters: Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThan(0)) }).pipe(
            Schema.middlewareDecoding((effect) =>
              Effect.flatMap(Codec, (actual) => {
                assert.strictEqual(actual, 'outer')
                return effect
              }),
            ),
          ),
          success: checkedString('outer'),
          resultValidation: 'strict',
        })
        yield* registry.install(
          Registry.toolWrapper(
            { id: 'wrapper', version: 1 },
            ToolWrapper.bind(outer, (item) =>
              Effect.succeed({
                ...item,
                invoke: (input) =>
                  Effect.gen(function* () {
                    assert.strictEqual(yield* Codec, 'outer')
                    wrappers++
                    const count = yield* Schema.decodeUnknownEffect(Schema.Int)(
                      input.arguments.count,
                    )
                    const result = yield* item.invoke({
                      ...input,
                      arguments: { payload: String(count) },
                    })
                    const value = yield* Schema.decodeUnknownEffect(Schema.String)(
                      result.structuredOutput,
                    )
                    return {
                      ...result,
                      structuredOutput: count === 3 ? 3 : `outer:${value}`,
                    }
                  }),
              }),
            ).pipe(ToolWrapper.provide(Layer.succeed(Codec, 'outer'))),
          ),
        )
        const lease = yield* registry.acquire
        assert.strictEqual(
          (yield* ToolInventory.invoke(lease, { ...call, arguments: { count: 1 } }, 'model').pipe(
            Effect.flip,
          ))._tag,
          'ToolAdmissionDenied',
        )
        for (const arguments_ of [{ count: 0 }, { count: 'bad' }]) {
          assert.isTrue(
            Schema.isSchemaError(
              yield* ToolInventory.invoke(lease, { ...call, arguments: arguments_ }, 'tools').pipe(
                Effect.flip,
              ),
            ),
          )
        }
        assert.strictEqual(calls, 0)
        assert.strictEqual(wrappers, 0)
        const nested = yield* ToolInventory.invokeNested(lease, {
          ...call,
          arguments: { count: 2 },
        })
        assert.strictEqual(nested.structuredOutput, 'outer:original:2')
        assert.strictEqual(calls, 1)
        assert.strictEqual(wrappers, 1)
        assert.notProperty(nested, 'control')
        assert.isTrue(
          Schema.isSchemaError(
            yield* ToolInventory.invokeNested(lease, { ...call, arguments: { count: 3 } }).pipe(
              Effect.flip,
            ),
          ),
        )
        assert.strictEqual(calls, 2)
        assert.strictEqual(wrappers, 2)
      }),
    ).pipe(Effect.provide(execution), Effect.provideService(Codec, 'ambient')),
  )

  it.effect(
    'transformed final codec remains pinned through reload, uninstall and lease closure',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const resume = yield* Deferred.make<void>()
          const releases: string[] = []
          const original = Toolkit.make(
            Tool.makeResult('work', { success: Schema.String, resultValidation: 'strict' }),
          )
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, original),
              original.toLayer({
                work: () => Effect.succeed({ content: [], structuredOutput: 'original' }),
              }),
            ),
          )
          const wrapper = (label: string, version: number) => {
            const outer = Tool.makeResult('work', {
              success: checkedString(label).pipe(
                Schema.middlewareDecoding((effect) =>
                  Effect.gen(function* () {
                    assert.notInclude(releases, label)
                    if (label === 'old') {
                      yield* Deferred.succeed(entered, undefined)
                      yield* Deferred.await(resume)
                    }
                    assert.notInclude(releases, label)
                    return yield* effect
                  }),
                ),
              ),
              resultValidation: 'strict',
            })
            return Registry.toolWrapper(
              { id: 'wrapper', version },
              ToolWrapper.bind(outer, (item) =>
                Effect.succeed({ ...item, invoke: item.invoke }),
              ).pipe(
                ToolWrapper.provide(
                  Layer.effect(
                    Codec,
                    Effect.acquireRelease(Effect.succeed(label), () =>
                      Effect.sync(() => {
                        releases.push(label)
                      }),
                    ),
                  ),
                ),
              ),
            )
          }
          yield* registry.install(wrapper('old', 1))
          const oldScope = yield* Scope.make()
          const lease = yield* Scope.provide(registry.acquire, oldScope)
          const fiber = yield* ToolInventory.invoke(lease, call, 'model').pipe(Effect.forkChild)
          yield* Deferred.await(entered)
          yield* registry.install(wrapper('new', 2))
          const freshScope = yield* Scope.make()
          const fresh = yield* Scope.provide(registry.acquire, freshScope)
          assert.strictEqual(
            (yield* ToolInventory.invoke(fresh, call, 'model')).structuredOutput,
            'original',
          )
          yield* Scope.close(freshScope, Exit.void)
          yield* Scope.close(oldScope, Exit.void)
          yield* registry.uninstall('wrapper')
          assert.notInclude(releases, 'old')
          yield* Deferred.succeed(resume, undefined)
          assert.strictEqual((yield* Fiber.join(fiber)).structuredOutput, 'original')
          assert.deepStrictEqual([...releases].sort(), ['new', 'old'])
        }),
      ).pipe(Effect.provide(execution), Effect.provideService(Codec, 'ambient')),
  )
  it.effect('wrapper caller restrictions apply to canonical and explicit binding admissions', () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0
        let wrappedCalls = 0
        const toolkit = Toolkit.make(Tool.make('work', { success: Schema.String }))
        const registry = yield* Registry.make
        yield* registry.install(
          Registry.provide(
            Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
            toolkit.toLayer({
              work: () =>
                Effect.sync(() => {
                  calls++
                  return 'ok'
                }),
            }),
          ),
        )
        yield* registry.install(
          Registry.toolWrapper(
            { id: 'restriction', version: 1 },
            ToolWrapper.make({
              name: 'work',
              wrap: (item) =>
                Effect.succeed({
                  ...item,
                  declaration: item.declaration.annotate(Tool.Callers, ['tools']),
                  invoke: (call) =>
                    Effect.sync(() => {
                      wrappedCalls++
                    }).pipe(Effect.andThen(item.invoke(call))),
                }),
            }),
          ),
        )
        const lease = yield* registry.acquire
        for (const options of [
          {},
          {
            agent: {
              toolkits: {
                inherit: true,
                registrations: [],
                bindings: { work: { binding: 'owner' } },
              },
            },
          },
        ] as const) {
          assert.strictEqual(
            (yield* ToolInventory.invoke(lease, call, 'model', options).pipe(Effect.flip))._tag,
            'ToolAdmissionDenied',
          )
          assert.strictEqual(calls, 0)
          assert.strictEqual(wrappedCalls, 0)
        }
        yield* ToolInventory.invoke(lease, call, 'tools')
        assert.strictEqual(calls, 1)
        assert.strictEqual(wrappedCalls, 1)
      }),
    ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'real hook replacements revalidate strict programmatic data and registry-bound added controls',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const tool = Tool.makeResult('work', { success: Schema.Int, resultValidation: 'strict' })
          const toolkit = Toolkit.make(tool)
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
              toolkit.toLayer({
                work: () =>
                  Effect.succeed({
                    content: [Prompt.textPart({ text: 'public' })],
                    structuredOutput: 1,
                  }),
              }),
            ),
          )
          yield* registry.install(
            Registry.hook(
              { id: 'after', version: 1 },
              Hook.make({
                event: 'afterTool',
                execute: ({ result }) =>
                  Effect.succeed({ ...result, control: { terminate: true, addTools: ['work'] } }),
              }),
            ),
          )
          const lease = yield* registry.acquire
          const initial = yield* ToolInventory.invoke(lease, call, 'model')
          const output = yield* lease.dispatchHook(
            { _tag: 'afterTool', call, result: initial },
            {
              validateResult: (result) =>
                Toolkit.validateResult(tool, result, ['work']).pipe(Effect.asVoid),
            },
          )
          assert.deepStrictEqual(output && output.control, { terminate: true, addTools: ['work'] })
          yield* registry.install(
            Registry.hook(
              { id: 'after', version: 2 },
              Hook.make({
                event: 'afterTool',
                execute: ({ result }) =>
                  Effect.succeed({ ...result, control: { addTools: ['missing'] } }),
              }),
            ),
          )
          const fresh = yield* registry.acquire
          assert.strictEqual(
            (yield* fresh
              .dispatchHook(
                { _tag: 'afterTool', call, result: initial },
                {
                  validateResult: (result) =>
                    Toolkit.validateResult(tool, result, ['work']).pipe(Effect.asVoid),
                },
              )
              .pipe(Effect.flip))._tag,
            'InvalidToolControl',
          )
          yield* registry.install(
            Registry.hook(
              { id: 'after', version: 3 },
              Hook.make({
                event: 'afterTool',
                execute: ({ result }) => {
                  const { structuredOutput: _data, ...missing } = result
                  return Effect.succeed(missing)
                },
              }),
            ),
          )
          const strictLease = yield* registry.acquire
          assert.isTrue(
            Schema.isSchemaError(
              yield* strictLease
                .dispatchHook(
                  { _tag: 'afterTool', call, result: initial },
                  {
                    validateResult: (result) =>
                      Toolkit.validateResult(tool, result, ['work']).pipe(Effect.asVoid),
                  },
                )
                .pipe(Effect.flip),
            ),
          )
          yield* registry.install(
            Registry.hook(
              { id: 'after', version: 4 },
              Hook.make({
                event: 'afterTool',
                execute: ({ result }) =>
                  Effect.succeed({
                    ...result,
                    control: { terminate: true, unexpected: 'must reject' },
                  }),
              }),
            ),
          )
          const rawLease = yield* registry.acquire
          assert.isTrue(
            Schema.isSchemaError(
              yield* rawLease
                .dispatchHook(
                  { _tag: 'afterTool', call, result: initial },
                  {
                    validateResult: (result) =>
                      Toolkit.validateResult(tool, result, ['work']).pipe(Effect.asVoid),
                  },
                )
                .pipe(Effect.flip),
            ),
          )
        }),
      ).pipe(Effect.provideServiceEffect(HookExecution, ToolExecution), Effect.provide(execution)),
  )
})
