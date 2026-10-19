import { assert, describe, it } from '@effect/vitest'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Registry from '../src/Registry.js'
import * as ToolWrapper from '../src/ToolWrapper.js'
import * as PromptSectionWrapper from '../src/PromptSectionWrapper.js'
import * as PromptSection from '../src/PromptSection.js'
import { HookExecution } from '../src/HookExecution.js'
import { ToolExecution } from '../src/ToolExecution.js'
import { RegistryError } from '../src/RegistryError.js'
import { ExecutionError } from '../src/ExecutionError.js'
import { call, execution, Label, registration } from './fixtures/Registry.js'

const hookAccess = Layer.effect(HookExecution, ToolExecution).pipe(Layer.provide(execution))
class ResourcePath extends Context.Service<ResourcePath, string>()('test/wrapper/ResourcePath') {}

const append = (id: string, label: string, trace: string[]) =>
  Registry.toolWrapper(
    { id, version: 1 },
    ToolWrapper.make({
      name: 'same',
      wrap: (item) =>
        Effect.succeed({
          ...item,
          invoke: (input) =>
            Effect.gen(function* () {
              const label = yield* Label
              trace.push(label + '-before')
              const result = yield* item.invoke(input)
              trace.push(label + '-after')
              return {
                ...result,
                details:
                  (typeof result.details === 'string'
                    ? result.details
                    : JSON.stringify(result.details)) +
                  '/' +
                  label,
              }
            }),
        }),
    }).pipe(ToolWrapper.provide(Layer.succeed(Label, label))),
  )

describe('Registry wrappers', () => {
  it.effect('captures wrapper target and callback before asynchronous Layer acquisition', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Registry.make
        yield* registry.install(registration('source', 'original'))
        const acquired = yield* Deferred.make<void>()
        const resume = yield* Deferred.make<void>()
        const definition = {
          name: 'same',
          wrap: (item: ToolWrapper.Item) =>
            Effect.succeed({
              ...item,
              invoke: (call: Parameters<ToolWrapper.Item['invoke']>[0]) => item.invoke(call),
            }),
        }
        const wrapper = ToolWrapper.make(definition).pipe(
          ToolWrapper.provide(
            Layer.effect(
              Label,
              Deferred.succeed(acquired, undefined).pipe(
                Effect.andThen(Deferred.await(resume)),
                Effect.as('wrapper'),
              ),
            ),
          ),
        )
        const installing = yield* registry
          .install(Registry.toolWrapper({ id: 'capture', version: 1 }, wrapper))
          .pipe(Effect.forkChild)
        yield* Deferred.await(acquired)
        definition.name = 'other'
        definition.wrap = () => Effect.die('mutated callback must not run')
        yield* Deferred.succeed(resume, undefined)
        yield* Fiber.join(installing)
        const lease = yield* registry.acquire
        assert.strictEqual((yield* lease.invokeTool(call)).details, 'original')
      }),
    ).pipe(Effect.provide(execution)),
  )
  it.effect(
    'wraps canonical and explicit bindings in installation order without mixing services',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const trace: string[] = []
          const registry = yield* Registry.make
          yield* registry.install(registration('first', 'root-A'))
          yield* registry.install(registration('second', 'root-B'))
          yield* registry.install(append('inner', 'inner', trace))
          yield* registry.install(append('outer', 'outer', trace))
          const lease = yield* registry.acquire
          const canonical = yield* lease.resolveTool('same')
          assert.strictEqual(canonical.owner.id, 'second')
          assert.strictEqual(Context.get(canonical.context, Label), 'root-B')
          assert.strictEqual((yield* lease.invokeTool(call)).details, 'root-B/inner/outer')
          assert.deepStrictEqual(trace, [
            'outer-before',
            'inner-before',
            'inner-after',
            'outer-after',
          ])
          assert.strictEqual(
            (yield* lease.invokeTool(call, { binding: 'first' })).details,
            'root-A/inner/outer',
          )
          yield* registry.install(registration('second', 'disabled', 2, false))
          const denied = yield* registry.acquire
          const count = trace.length
          assert.strictEqual(
            (yield* Schema.decodeUnknownEffect(Schema.toType(RegistryError))(
              yield* denied.invokeTool(call).pipe(Effect.flip),
            )).reason,
            'disabled',
          )
          assert.strictEqual(trace.length, count)
          yield* registry.uninstall('outer')
          const current = yield* registry.acquire
          assert.strictEqual(
            (yield* current.invokeTool(call, { binding: 'first' })).details,
            'root-A/inner',
          )
          assert.strictEqual((yield* lease.invokeTool(call)).details, 'root-B/inner/outer')
        }),
      ).pipe(Effect.provide(execution)),
  )

  for (const rename of [false, true])
    it.effect(
      `rejects ${rename ? 'renamed' : 'failing'} wrappers atomically and releases candidate resources`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const registry = yield* Registry.make
            yield* registry.install(registration('first', 'shadow'))
            yield* registry.install(registration('second', 'canonical'))
            const before = yield* registry.snapshot
            let released = 0
            let transformedReleased = 0
            const layer = Layer.effect(
              Label,
              Effect.acquireRelease(Effect.succeed('wrapper'), () =>
                Effect.sync(() => {
                  released++
                }),
              ),
            )
            const wrapper = ToolWrapper.make({
              name: 'same',
              wrap: (item) =>
                Effect.gen(function* () {
                  yield* Effect.acquireRelease(Effect.void, () =>
                    Effect.sync(() => {
                      transformedReleased++
                    }),
                  )
                  if (!rename) return yield* Effect.fail('wrapper failed')
                  return { ...item, declaration: { ...item.declaration, name: 'other' } }
                }),
            }).pipe(ToolWrapper.provide(layer))
            const failure = yield* registry
              .install(Registry.toolWrapper({ id: 'broken', version: 1 }, wrapper))
              .pipe(Effect.flip)
            assert.strictEqual(
              (yield* Schema.decodeUnknownEffect(Schema.toType(RegistryError))(failure)).reason,
              'invalidRegistration',
            )
            assert.strictEqual(yield* registry.snapshot, before)
            assert.strictEqual(released, 1)
            assert.strictEqual(transformedReleased, 1)
            const lease = yield* registry.acquire
            assert.strictEqual((yield* lease.invokeTool(call)).details, 'canonical')
          }),
        ).pipe(Effect.provide(execution)),
    )

  it.effect(
    'keeps wrapper Layers and generation resources through reload, uninstall and concurrent lease closure',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const started = yield* Deferred.make<void>()
          const resume = yield* Deferred.make<void>()
          const releases: string[] = []
          yield* registry.install(registration('tool', 'backend'))
          const install = (label: string, version: number) =>
            Registry.toolWrapper(
              { id: 'wrapper', version },
              ToolWrapper.make({
                name: 'same',
                wrap: (item) =>
                  Effect.gen(function* () {
                    const label = yield* Label
                    yield* Effect.acquireRelease(Effect.void, () =>
                      Effect.sync(() => {
                        releases.push(label + '-generation')
                      }),
                    )
                    return {
                      ...item,
                      invoke: (input) =>
                        Effect.gen(function* () {
                          const wrapperLabel = yield* Label
                          yield* Deferred.succeed(started, undefined)
                          yield* Deferred.await(resume)
                          assert.isFalse(releases.includes(wrapperLabel + '-layer'))
                          assert.isFalse(releases.includes(wrapperLabel + '-generation'))
                          const result = yield* item.invoke(input)
                          return {
                            ...result,
                            details:
                              (typeof result.details === 'string'
                                ? result.details
                                : JSON.stringify(result.details)) +
                              '/' +
                              wrapperLabel,
                          }
                        }),
                    }
                  }),
              }).pipe(
                ToolWrapper.provide(
                  Layer.effect(
                    Label,
                    Effect.acquireRelease(Effect.succeed(label), (value) =>
                      Effect.sync(() => {
                        releases.push(value + '-layer')
                      }),
                    ),
                  ),
                ),
              ),
            )
          yield* registry.install(install('old', 1))
          const scope = yield* Scope.make()
          const lease = yield* Scope.provide(registry.acquire, scope)
          const fiber = yield* lease.invokeTool(call).pipe(Effect.forkChild)
          yield* Deferred.await(started)
          yield* registry.install(install('new', 2))
          yield* Scope.close(scope, Exit.void)
          yield* registry.uninstall('wrapper')
          assert.deepStrictEqual(releases, ['new-generation', 'new-layer'])
          yield* Deferred.succeed(resume, undefined)
          assert.strictEqual((yield* Fiber.join(fiber)).details, 'backend/old')
          assert.deepStrictEqual(releases, [
            'new-generation',
            'new-layer',
            'old-generation',
            'old-layer',
          ])
          assert.strictEqual(
            (yield* Schema.decodeUnknownEffect(Schema.toType(RegistryError))(
              yield* lease.invokeTool(call).pipe(Effect.flip),
            )).reason,
            'revoked',
          )
          const current = yield* registry.acquire
          assert.strictEqual((yield* current.invokeTool(call)).details, 'backend')
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'validates ordered prompt wrappers, retains their services, and rejects renamed sections',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const section = PromptSection.make({ key: 'context', render: () => Label })
          yield* registry.install({
            definition: { id: 'section', version: 1 },
            acquire: Effect.succeed({
              tools: [],
              tasks: [],
              hooks: [],
              sections: [{ declaration: section, context: Context.make(Label, 'source') }],
            }),
          })
          const wrap = (id: string) =>
            Registry.promptSectionWrapper(
              { id, version: 1 },
              PromptSectionWrapper.make({
                key: 'context',
                wrap: (item) =>
                  Effect.succeed({
                    ...item,
                    render: (prompt) =>
                      Effect.gen(function* () {
                        const label = yield* Label
                        return String(yield* item.render(prompt)) + '/' + label
                      }),
                  }),
              }).pipe(PromptSectionWrapper.provide(Layer.succeed(Label, id))),
            )
          yield* registry.install(wrap('one'))
          yield* registry.install(wrap('two'))
          const lease = yield* registry.acquire
          assert.strictEqual(
            yield* lease.renderSection('context', Prompt.make('question')),
            'source/one/two',
          )
          const before = yield* registry.snapshot
          const renamed = PromptSectionWrapper.make({
            key: 'context',
            wrap: (item) =>
              Effect.succeed({ ...item, declaration: { ...item.declaration, key: 'renamed' } }),
          })
          assert.strictEqual(
            (yield* registry
              .install(Registry.promptSectionWrapper({ id: 'rename', version: 1 }, renamed))
              .pipe(Effect.flip)).reason,
            'invalidRegistration',
          )
          assert.strictEqual(yield* registry.snapshot, before)
          yield* registry.uninstall('one')
          const current = yield* registry.acquire
          assert.strictEqual(
            yield* current.renderSection('context', Prompt.make('question')),
            'source/two',
          )
          assert.strictEqual(
            yield* lease.renderSection('context', Prompt.make('question')),
            'source/one/two',
          )
        }),
      ).pipe(Effect.provide(hookAccess)),
  )

  it.effect(
    'interrupts unpublished transformations and closes their resources without changing the snapshot',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          yield* registry.install(registration('tool', 'source'))
          const before = yield* registry.snapshot
          const acquired = yield* Deferred.make<void>()
          let released = 0
          const wrapper = ToolWrapper.make({
            name: 'same',
            wrap: () =>
              Effect.gen(function* () {
                yield* Effect.acquireRelease(Effect.void, () =>
                  Effect.sync(() => {
                    released++
                  }),
                )
                yield* Deferred.succeed(acquired, undefined)
                return yield* Effect.never
              }),
          })
          const fiber = yield* registry
            .install(Registry.toolWrapper({ id: 'wrapper', version: 1 }, wrapper))
            .pipe(Effect.forkChild)
          yield* Deferred.await(acquired)
          yield* Fiber.interrupt(fiber)
          assert.strictEqual(released, 1)
          assert.strictEqual(yield* registry.snapshot, before)
        }),
      ),
  )

  it.effect(
    'retains native prompt wrapper files through reload and interrupted leased rendering',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const fs = yield* FileSystem.FileSystem
          yield* registry.install({
            definition: { id: 'section', version: 1 },
            acquire: Effect.succeed({
              tools: [],
              tasks: [],
              hooks: [],
              sections: [
                {
                  declaration: PromptSection.make({
                    key: 'native',
                    render: () => Effect.succeed('source'),
                  }),
                  context: Context.empty(),
                },
              ],
            }),
          })
          const started = yield* Deferred.make<void>()
          const paths: string[] = []
          const wrapper = (version: number) =>
            Registry.promptSectionWrapper(
              { id: 'native-wrapper', version },
              PromptSectionWrapper.make({
                key: 'native',
                wrap: (item) =>
                  Effect.succeed({
                    ...item,
                    render: (prompt) =>
                      Effect.gen(function* () {
                        const path = yield* ResourcePath
                        const fs = yield* FileSystem.FileSystem
                        assert.strictEqual(
                          yield* fs.readFileString(path).pipe(
                            Effect.mapError(
                              (cause) =>
                                new ExecutionError({
                                  reason: 'invalid',
                                  operation: 'wrapper.read',
                                  message: 'Resource read failed',
                                  cause,
                                }),
                            ),
                          ),
                          'live',
                        )
                        assert.strictEqual(yield* item.render(prompt), 'source')
                        yield* Deferred.succeed(started, undefined)
                        return yield* Effect.never
                      }),
                  }),
              }).pipe(
                PromptSectionWrapper.provide(
                  Layer.effect(
                    ResourcePath,
                    Effect.gen(function* () {
                      const fs = yield* FileSystem.FileSystem
                      const path = yield* fs.makeTempFileScoped()
                      yield* fs.writeFileString(path, 'live')
                      paths.push(path)
                      return path
                    }),
                  ).pipe(Layer.provideMerge(NodeServices.layer)),
                ),
              ),
            )
          yield* registry.install(wrapper(1))
          const scope = yield* Scope.make()
          const lease = yield* Scope.provide(registry.acquire, scope)
          const fiber = yield* lease
            .renderSection('native', Prompt.make('x'))
            .pipe(Effect.forkChild)
          yield* Deferred.await(started)
          yield* registry.install(wrapper(2))
          yield* Scope.close(scope, Exit.void)
          yield* registry.uninstall('native-wrapper')
          assert.isTrue(yield* fs.exists(paths[0]!))
          assert.isFalse(yield* fs.exists(paths[1]!))
          yield* Fiber.interrupt(fiber)
          const exit = yield* Fiber.await(fiber)
          assert.isTrue(Exit.isFailure(exit))
          assert.isFalse(yield* fs.exists(paths[0]!))
          const current = yield* registry.acquire
          assert.strictEqual(yield* current.renderSection('native', Prompt.make('x')), 'source')
        }),
      ).pipe(Effect.provide([hookAccess, NodeServices.layer])),
  )

  it.effect('rejects failing prompt transformations and releases their unpublished scopes', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Registry.make
        yield* registry.install({
          definition: { id: 'section', version: 1 },
          acquire: Effect.succeed({
            tools: [],
            tasks: [],
            hooks: [],
            sections: [
              {
                declaration: PromptSection.make({
                  key: 'context',
                  render: () => Effect.succeed('canonical'),
                }),
                context: Context.empty(),
              },
            ],
          }),
        })
        const before = yield* registry.snapshot
        let released = 0
        const failed = PromptSectionWrapper.make({
          key: 'context',
          wrap: () =>
            Effect.gen(function* () {
              yield* Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  released++
                }),
              )
              return yield* Effect.fail('unavailable')
            }),
        })
        yield* registry
          .install(Registry.promptSectionWrapper({ id: 'failed', version: 1 }, failed))
          .pipe(Effect.flip)
        assert.strictEqual(yield* registry.snapshot, before)
        assert.strictEqual(released, 1)
        const lease = yield* registry.acquire
        assert.strictEqual(yield* lease.renderSection('context', Prompt.make('x')), 'canonical')
      }),
    ).pipe(Effect.provide(hookAccess)),
  )
})
