import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Agent from '../src/Agent.js'
import * as Registry from '../src/Registry.js'
import * as Inventory from '../src/ToolInventory.js'
import * as Catalog from '../src/ToolCatalog.js'
import * as Tool from '../src/Tool.js'
import { ToolExecution } from '../src/ToolExecution.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Search from '../src/ToolSearch.js'
import { execution } from './fixtures/Registry.js'

describe('invocation catalog permission boundary', () => {
  it.effect(
    'never reveals filtered owners, caller-denied tools, incompatible bindings or shadowed duplicates',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const install = (
            id: string,
            name: string,
            callers: ReadonlyArray<Tool.Caller> = ['model', 'tools'],
            enabled = true,
          ) => {
            const toolkit = Toolkit.make(
              Tool.make(name, {
                description: `metadata-${id}`,
                success: Schema.String,
                deferred: true,
                callers,
              }),
            )
            return registry.install(
              Registry.provide(
                Registry.toolkit({ id, version: 1, enabled }, toolkit),
                toolkit.toLayer({ [name]: () => Effect.succeed(id) }),
              ),
            )
          }
          yield* install('allowed', 'visible')
          yield* install('excluded', 'excluded')
          yield* install('caller', 'caller', ['tools'])
          yield* install('old', 'duplicate')
          yield* install('disabled', 'duplicate', ['model'], false)
          yield* install('badVersion', 'version')
          yield* install('agentDenied', 'agentDenied')
          yield* install('muted', 'muted')
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'search', version: 1 }, Search.toolkit),
              Search.layer,
            ),
          )
          const lease = yield* registry.acquire
          const agent: Agent.State = {
            tools: ['tool_search', 'visible', 'caller', 'duplicate', 'version', 'muted'],
            toolkits: {
              inherit: false,
              registrations: [
                'allowed',
                'caller',
                'old',
                'disabled',
                'badVersion',
                'agentDenied',
                'muted',
                'search',
              ],
              disabled: ['muted'],
              bindings: { version: { binding: 'badVersion', version: 2 } },
            },
          }
          const metadata = yield* Inventory.catalog(lease.snapshot, { agent })
          assert.deepStrictEqual(
            metadata.map((item) => item.name),
            ['visible'],
          )
          assert.deepStrictEqual(Object.keys(metadata[0]!).sort(), [
            'description',
            'name',
            'parameters',
          ])
          const result = yield* Inventory.invoke(
            lease,
            { id: 'search', name: 'tool_search', arguments: { query: 'metadata' } },
            'model',
            { agent },
          )
          assert.deepStrictEqual(result.structuredOutput, { loaded: ['visible'] })
          assert.deepStrictEqual(result.details, { loaded: ['visible'] })
          assert.deepStrictEqual(result.control, { addTools: ['visible'] })
          assert.strictEqual(
            (yield* Inventory.invoke(
              lease,
              { id: 'no', name: 'tool_search', arguments: { query: 'metadata' } },
              'tools',
              { agent },
            ).pipe(Effect.flip))._tag,
            'ToolAdmissionDenied',
          )
          assert.deepStrictEqual(
            yield* Inventory.catalog(lease.snapshot, {
              agent: { ...agent, modelTools: { mode: 'explicit', names: ['visible'] } },
            }),
            [],
          )
          assert.deepStrictEqual(
            yield* Inventory.catalog(lease.snapshot, {
              agent: {
                toolkits: {
                  inherit: false,
                  registrations: ['allowed'],
                  bindings: { visible: { binding: 'excluded', version: 1 } },
                },
              },
            }),
            [],
          )
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'pins invocation metadata and revokes captured catalog access when the handler returns',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          let late: ToolExecution['Service']['deferredTools']
          const hidden = Toolkit.make(
            Tool.make('hidden', {
              description: 'old metadata',
              success: Schema.String,
              deferred: true,
            }),
          )
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, hidden),
              hidden.toLayer({ hidden: () => Effect.succeed('old') }),
            ),
          )
          const reader = Toolkit.make(Tool.make('reader', { success: Schema.Array(Schema.String) }))
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'reader', version: 1 }, reader),
              reader.toLayer({
                reader: () =>
                  Effect.gen(function* () {
                    late = (yield* ToolExecution).deferredTools
                    yield* registry.uninstall('owner').pipe(Effect.orDie)
                    return (yield* Catalog.deferred.pipe(Effect.orDie)).map(
                      (item) => item.description,
                    )
                  }),
              }),
            ),
          )
          const lease = yield* registry.acquire
          const result = yield* Inventory.invoke(
            lease,
            { id: 'one', name: 'reader', arguments: {} },
            'model',
          )
          assert.deepStrictEqual(result.details, ['old metadata'])
          assert.deepInclude(yield* late!.pipe(Effect.flip), {
            _tag: 'ExecutionError',
            reason: 'revoked',
          })
          const current = yield* registry.acquire
          assert.deepStrictEqual(yield* Inventory.catalog(current.snapshot), [])
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect(
    'keeps loaded owner/version fences across replacement, disablement, absence and restoration',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          let executions = 0
          const toolkit = Toolkit.make(
            Tool.make('hidden', { success: Schema.String, deferred: true }),
          )
          const install = (id: string, version = 1, enabled = true) =>
            registry.install(
              Registry.provide(
                Registry.toolkit({ id, version, enabled }, toolkit),
                toolkit.toLayer({
                  hidden: () =>
                    Effect.sync(() => {
                      executions++
                      return id
                    }),
                }),
              ),
            )
          yield* install('owner')
          const agent: Agent.State = {
            modelTools: {
              mode: 'default',
              names: ['hidden'],
              bindings: { hidden: { binding: 'owner', version: 1 } },
            },
          }
          const call = { id: 'hidden', name: 'hidden', arguments: {} }
          const first = yield* registry.acquire
          assert.strictEqual(
            (yield* Inventory.invoke(first, call, 'model', { agent })).details,
            'owner',
          )
          yield* install('replacement')
          let lease = yield* registry.acquire
          assert.strictEqual(
            (yield* Inventory.invoke(lease, call, 'model', { agent }).pipe(Effect.flip))._tag,
            'ToolAdmissionDenied',
          )
          assert.deepStrictEqual(yield* Inventory.catalog(lease.snapshot, { agent }), [])
          assert.strictEqual(
            (yield* Inventory.invoke(lease, call, 'tools', { agent })).details,
            'replacement',
          )
          yield* registry.uninstall('replacement')
          for (const [version, enabled] of [
            [2, true],
            [1, false],
          ] as const) {
            yield* install('owner', version, enabled)
            lease = yield* registry.acquire
            assert.strictEqual(
              (yield* Inventory.invoke(lease, call, 'model', { agent }).pipe(Effect.flip))._tag,
              'ToolAdmissionDenied',
            )
          }
          yield* registry.uninstall('owner')
          lease = yield* registry.acquire
          assert.deepStrictEqual(Inventory.inventory(lease.snapshot, 'model', { agent }), [])
          yield* install('owner')
          lease = yield* registry.acquire
          assert.strictEqual(
            (yield* Inventory.invoke(lease, call, 'model', { agent })).details,
            'owner',
          )
          assert.strictEqual(executions, 3)
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect('reports schema conversion failure instead of silently dropping a candidate', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(
          Tool.make('opaque', {
            deferred: true,
            parameters: Schema.Struct({
              value: Schema.suspend((): typeof Schema.String => {
                throw new Error('unavailable schema')
              }),
            }),
            success: Schema.String,
          }),
        )
        const registry = yield* Registry.make
        yield* registry.install(
          Registry.provide(
            Registry.toolkit({ id: 'opaque', version: 1 }, toolkit),
            toolkit.toLayer({ opaque: () => Effect.succeed('unused') }),
          ),
        )
        const lease = yield* registry.acquire
        assert.deepInclude(yield* Inventory.catalog(lease.snapshot).pipe(Effect.flip), {
          _tag: 'ToolCatalogError',
          name: 'opaque',
        })
      }),
    ),
  )
})
