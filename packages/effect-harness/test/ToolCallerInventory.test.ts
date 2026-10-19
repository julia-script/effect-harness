import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Agent from '../src/Agent.js'
import * as Extension from '../src/Extension.js'
import * as Registry from '../src/Registry.js'
import * as Tool from '../src/Tool.js'
import * as ToolInventory from '../src/ToolInventory.js'
import * as Toolkit from '../src/Toolkit.js'
import { execution } from './fixtures/Registry.js'

class Label extends Context.Service<Label, string>()('test/caller-inventory/Label') {}
class Codec extends Context.Service<Codec, string>()('test/caller-inventory/Codec') {}

const call = { id: 'call', name: 'same', arguments: {} } as const

describe('bound caller admission', () => {
  for (const callers of [undefined, [], ['model'], ['tools'], ['model', 'tools']] as const) {
    for (const caller of ['model', 'tools'] as const) {
      it.effect(`${caller} admission with callers=${JSON.stringify(callers)}`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            let calls = 0
            const tool = Tool.make('same', {
              success: Schema.String,
              ...(callers === undefined ? {} : { callers }),
            })
            const toolkit = Toolkit.make(tool)
            const registry = yield* Registry.make
            yield* registry.install(
              Registry.provide(
                Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
                toolkit.toLayer({
                  same: () =>
                    Effect.sync(() => {
                      calls++
                      return 'ok'
                    }),
                }),
              ),
            )
            const lease = yield* registry.acquire
            const allowed = callers === undefined || callers.some((allowed) => allowed === caller)
            const inspected = ToolInventory.inspect(lease.snapshot, 'same', caller)
            assert.isTrue(Schema.is(ToolInventory.InspectionSchema)(inspected))
            const result = yield* Effect.exit(ToolInventory.invoke(lease, call, caller))
            assert.strictEqual(Exit.isSuccess(result), allowed)
            assert.strictEqual(calls, allowed ? 1 : 0)
            assert.strictEqual(
              ToolInventory.inventory(lease.snapshot, caller).length,
              allowed ? 1 : 0,
            )
            if (!allowed) {
              const denied = yield* ToolInventory.admit(lease, 'same', caller).pipe(Effect.flip)
              assert.strictEqual(denied._tag, 'ToolAdmissionDenied')
              assert.isTrue(Schema.is(ToolInventory.Denial)(denied))
            }
          }),
        ).pipe(Effect.provide(execution)),
      )
    }
  }

  it.effect('global tools filter denies both callers; model filter denies only model calls', () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0
        const toolkit = Toolkit.make(Tool.make('same', { success: Schema.String }))
        const registry = yield* Registry.make
        yield* registry.install(
          Registry.provide(
            Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
            toolkit.toLayer({
              same: () =>
                Effect.sync(() => {
                  calls++
                  return 'ok'
                }),
            }),
          ),
        )
        const lease = yield* registry.acquire
        for (const caller of ['model', 'tools'] as const) {
          const denied = yield* ToolInventory.invoke(lease, call, caller, {
            agent: { tools: [] },
          }).pipe(Effect.flip)
          assert.strictEqual(denied._tag, 'ToolAdmissionDenied')
        }
        assert.strictEqual(calls, 0)
        assert.strictEqual(
          (yield* ToolInventory.invoke(lease, call, 'model', {
            agent: { modelTools: { mode: 'explicit', names: [] } },
          }).pipe(Effect.flip))._tag,
          'ToolAdmissionDenied',
        )
        assert.strictEqual(calls, 0)
        yield* ToolInventory.invoke(lease, call, 'tools', {
          agent: { modelTools: { mode: 'explicit', names: [] } },
        })
        assert.strictEqual(calls, 1)
      }),
    ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'last owner is authoritative before all filters; explicit binding and reload stay pinned',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls: string[] = []
          const registry = yield* Registry.make
          const install = (
            id: string,
            label: string,
            version: number,
            enabled: boolean,
            callers: ReadonlyArray<Tool.Caller>,
          ) => {
            const schema = Schema.String.pipe(
              Schema.middlewareEncoding((effect) =>
                Effect.flatMap(Codec, (codec) =>
                  effect.pipe(Effect.map(Option.map((value) => `${codec}:${value}`))),
                ),
              ),
            )
            const toolkit = Toolkit.make(
              Tool.make('same', { success: schema, callers }).addDependency(Label),
            )
            return registry.install(
              Registry.provide(
                Registry.toolkit({ id, version, enabled }, toolkit),
                toolkit
                  .toLayer({
                    same: () =>
                      Effect.gen(function* () {
                        const label = yield* Label
                        calls.push(label)
                        return label
                      }),
                  })
                  .pipe(
                    Layer.provideMerge(
                      Layer.merge(Layer.succeed(Label, label), Layer.succeed(Codec, label)),
                    ),
                  ),
              ),
            )
          }
          yield* install('first', 'first', 1, true, ['model', 'tools'])
          yield* install('last', 'last', 1, false, ['tools'])
          const old = yield* registry.acquire
          for (const caller of ['model', 'tools'] as const) {
            const denial = yield* ToolInventory.invoke(old, call, caller).pipe(Effect.flip)
            assert.strictEqual(denial._tag, 'ToolAdmissionDenied')
            assert.strictEqual(ToolInventory.inspect(old.snapshot, 'same', caller)._tag, 'denied')
          }
          assert.deepStrictEqual(calls, [])
          const explicit = {
            agent: {
              toolkits: {
                inherit: true,
                registrations: [],
                bindings: { same: { binding: 'first', version: 1 } },
              },
            },
          } as const
          assert.strictEqual(
            (yield* ToolInventory.invoke(old, call, 'tools', explicit)).details,
            'first:first',
          )
          yield* install('last', 'new', 2, true, ['tools'])
          const fresh = yield* registry.acquire
          assert.strictEqual(
            (yield* ToolInventory.invoke(old, call, 'tools').pipe(Effect.flip))._tag,
            'ToolAdmissionDenied',
          )
          assert.strictEqual(
            (yield* ToolInventory.invoke(fresh, call, 'model').pipe(Effect.flip))._tag,
            'ToolAdmissionDenied',
          )
          assert.strictEqual((yield* ToolInventory.invoke(fresh, call, 'tools')).details, 'new:new')
          const incompatible = {
            agent: {
              toolkits: {
                inherit: true,
                registrations: [],
                bindings: { same: { binding: 'last', version: 1 } },
              },
            },
          } as const
          assert.strictEqual(
            (yield* ToolInventory.invoke(fresh, call, 'tools', incompatible).pipe(Effect.flip))
              ._tag,
            'ToolAdmissionDenied',
          )
          const missing = {
            agent: {
              toolkits: {
                inherit: true,
                registrations: [],
                bindings: { same: { binding: 'missing' } },
              },
            },
          } as const
          assert.strictEqual(
            (yield* ToolInventory.invoke(fresh, call, 'tools', missing).pipe(Effect.flip))._tag,
            'ToolAdmissionDenied',
          )
          const disabled = {
            agent: { toolkits: { inherit: true, registrations: [], disabled: ['last'] } },
          } as const
          assert.strictEqual(
            (yield* ToolInventory.invoke(fresh, call, 'tools', disabled).pipe(Effect.flip))._tag,
            'ToolAdmissionDenied',
          )
          assert.deepStrictEqual(calls, ['first', 'new'])
          yield* registry.uninstall('last')
          assert.strictEqual((yield* ToolInventory.invoke(fresh, call, 'tools')).details, 'new:new')
          const newest = yield* registry.acquire
          assert.strictEqual(
            (yield* ToolInventory.invoke(newest, call, 'model')).details,
            'first:first',
          )
          assert.deepStrictEqual(calls, ['first', 'new', 'new', 'first'])
        }),
      ).pipe(
        Effect.provide(execution),
        Effect.provideService(Label, 'ambient'),
        Effect.provideService(Codec, 'ambient'),
      ),
  )

  it.effect(
    'selection inheritance, explicit order, fork codecs and released leases remain honest',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls: string[] = []
          const registry = yield* Registry.make
          const toolkit = Toolkit.make(Tool.make('same', { success: Schema.String }))
          for (const id of ['first', 'last'])
            yield* registry.install(
              Registry.provide(
                Registry.toolkit({ id, version: 1 }, toolkit),
                toolkit.toLayer({
                  same: () =>
                    Effect.sync(() => {
                      calls.push(id)
                      return id
                    }),
                }),
              ),
            )
          const scope = yield* Scope.make()
          const lease = yield* Scope.provide(registry.acquire, scope)
          const agent: Agent.State = {
            tools: ['same'],
            modelTools: { mode: 'explicit', names: [] },
            toolkits: { inherit: false, registrations: ['last', 'first'] },
          }
          const fork = yield* Schema.decodeEffect(Schema.toCodecJson(Agent.StateSchema))(
            yield* Schema.encodeEffect(Schema.toCodecJson(Agent.StateSchema))(agent),
          )
          assert.deepStrictEqual(fork, agent)
          assert.strictEqual(
            (yield* ToolInventory.invoke(lease, call, 'tools', { agent: fork })).details,
            'first',
          )
          assert.strictEqual(
            (yield* ToolInventory.invoke(lease, call, 'tools', {
              inheritedRegistrations: ['first'],
              agent: { toolkits: { inherit: true, registrations: [] } },
            })).details,
            'first',
          )
          const unselected = {
            agent: {
              toolkits: {
                inherit: false,
                registrations: ['last'],
                bindings: { same: { binding: 'first' } },
              },
            },
          } as const
          assert.strictEqual(
            (yield* ToolInventory.invoke(lease, call, 'tools', unselected).pipe(Effect.flip))._tag,
            'ToolAdmissionDenied',
          )
          assert.deepStrictEqual(calls, ['first', 'first'])
          yield* Scope.close(scope, Exit.void)
          assert.strictEqual(
            (yield* ToolInventory.invoke(lease, call, 'tools').pipe(Effect.flip))._tag,
            'RegistryError',
          )
          assert.deepStrictEqual(calls, ['first', 'first'])
          assert.deepStrictEqual(
            yield* Schema.decodeEffect(Agent.StateSchema)({ tools: ['same'] }),
            { tools: ['same'] },
          )
          assert.deepStrictEqual(
            yield* Schema.decodeEffect(Agent.ChangeSchema)({
              modelTools: null,
              toolkits: null,
            }),
            { modelTools: null, toolkits: null },
          )
        }),
      ).pipe(Effect.provide(execution)),
  )
  it.effect('extension availability never reveals shadows; registration selection owns order', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls: string[] = []
        const toolkit = Toolkit.make(Tool.make('same', { success: Schema.String }))
        const registry = yield* Registry.make
        for (const name of ['first', 'last'])
          yield* registry.install(
            Registry.extension(
              { id: `owner-${name}`, version: 1 },
              Extension.make({ name, tools: toolkit }).pipe(
                Extension.provide(
                  toolkit.toLayer({
                    same: () =>
                      Effect.sync(() => {
                        calls.push(name)
                        return name
                      }),
                  }),
                ),
              ),
            ),
          )
        const lease = yield* registry.acquire
        for (const caller of ['model', 'tools'] as const)
          assert.strictEqual(
            (yield* ToolInventory.invoke(lease, call, caller, { agent: { extensions: [] } }).pipe(
              Effect.flip,
            ))._tag,
            'ToolAdmissionDenied',
          )
        assert.deepStrictEqual(calls, [])
        assert.strictEqual(
          (yield* ToolInventory.invoke(lease, call, 'tools', {
            agent: { extensions: ['last', 'first'] },
          })).details,
          'last',
        )
        assert.strictEqual(
          ToolInventory.inspect(lease.snapshot, 'same', 'model', {
            agent: { extensions: ['first'] },
          })._tag,
          'denied',
        )
        assert.strictEqual(
          (yield* ToolInventory.invoke(lease, call, 'tools', {
            agent: {
              extensions: ['first', 'last'],
              toolkits: { inherit: false, registrations: ['owner-last', 'owner-first'] },
            },
          })).details,
          'first',
        )
        const filtered = {
          agent: {
            extensions: ['first'],
            toolkits: {
              inherit: true,
              registrations: [],
              bindings: { same: { binding: 'owner-last' } },
            },
          },
        } as const
        assert.strictEqual(
          (yield* ToolInventory.invoke(lease, call, 'model', filtered).pipe(Effect.flip))._tag,
          'ToolAdmissionDenied',
        )
        assert.deepStrictEqual(calls, ['last', 'first'])
        const bindings: NonNullable<Agent.ToolkitSelection['bindings']> = {}
        Object.setPrototypeOf(bindings, { same: { binding: 'missing' } })
        assert.strictEqual(
          (yield* ToolInventory.invoke(lease, call, 'model', {
            agent: { toolkits: { inherit: true, registrations: [], bindings } },
          })).details,
          'last',
        )
      }),
    ).pipe(Effect.provide(execution)),
  )
})
