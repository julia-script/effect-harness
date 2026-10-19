import { assert, describe, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Extension from '../src/Extension.js'
import * as Hook from '../src/Hook.js'
import * as PromptSection from '../src/PromptSection.js'
import * as Registry from '../src/Registry.js'
import * as Task from '../src/Task.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as ToolkitFileSystem from '../src/ToolkitFileSystem.js'

import { call, execution, handlers, Label, registration, toolkit } from './fixtures/Registry.js'

const task = (kind: string, callbacks: () => void = () => {}) =>
  Task.make({
    kind,
    version: 1,
    input: Schema.String,
    checkpoint: Schema.Struct({ phase: Schema.Literal('ready') }),
    success: Schema.String,
    failure: Schema.String,
    initial: () => {
      callbacks()
      return { phase: 'ready' as const }
    },
    phases: {
      ready: () => {
        callbacks()
        return Effect.void
      },
    },
    migrate: () => {
      callbacks()
      return Effect.succeed({ input: '', checkpoint: { phase: 'ready' as const } })
    },
  })

describe('Canonical Registry', () => {
  it.effect('decodes parameters and encodes results with the selected binding codec services', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const text = Schema.String.pipe(
          Schema.decodeTo(Schema.String, {
            decode: SchemaGetter.transformEffect((value: string) =>
              Label.pipe(Effect.map((label) => `${label}:${value}`)),
            ),
            encode: SchemaGetter.transformEffect((value: string) =>
              Label.pipe(Effect.map((label) => `${label}:${value}`)),
            ),
          }),
        )
        const declaration = Tool.make('codec', {
          parameters: Schema.Struct({ value: text }),
          success: text,
        })
        const tk = Toolkit.make(declaration)
        const handler = tk.toLayer({ codec: ({ value }) => Effect.succeed(value) })
        const registry = yield* Registry.make
        for (const label of ['A', 'B'])
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: label, version: 1 }, tk),
              handler.pipe(Layer.provideMerge(Layer.succeed(Label, label))),
            ),
          )
        const lease = yield* registry.acquire
        const invoke = { id: 'codec-call', name: 'codec', arguments: { value: 'input' } }
        assert.strictEqual((yield* lease.invokeTool(invoke, { binding: 'A' })).details, 'A:A:input')
        assert.strictEqual((yield* lease.invokeTool(invoke)).details, 'B:B:input')
      }),
    ).pipe(Effect.provide([execution, Layer.succeed(Label, 'ambient-must-not-win')])),
  )

  it.effect('resolves declaration, handler, context and policy from one last-wins owner', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Registry.make
        yield* registry.install(registration('first', 'A'))
        yield* registry.install(registration('second', 'B', 2))
        const lease = yield* registry.acquire
        const canonical = yield* lease.resolveTool('same')
        assert.strictEqual(canonical.owner.id, 'second')
        assert.strictEqual(canonical.owner.version, 2)
        assert.strictEqual(Tool.policy(canonical.declaration).replay, 'unsafe')
        assert.strictEqual(Context.get(canonical.context, Label), 'B')
        assert.strictEqual((yield* lease.invokeTool(call)).details, 'B')
        assert.strictEqual((yield* lease.invokeTool(call, { binding: 'first' })).details, 'A')
        const mismatch = yield* lease.resolveTool('same', { version: 1 }).pipe(Effect.flip)
        assert.strictEqual(mismatch.reason, 'incompatibleVersion')
        const missing = yield* lease.resolveTool('same', { binding: 'missing' }).pipe(Effect.flip)
        assert.strictEqual(missing.reason, 'notFound')
        assert.isTrue(Object.isFrozen(lease.snapshot))
        assert.isTrue(Object.isFrozen(lease.snapshot.tools))
        assert.isTrue(Object.isFrozen(canonical))
        assert.isTrue(Object.isFrozen(canonical.declaration))
      }),
    ).pipe(Effect.provide(execution)),
  )

  it.effect('denies disabled canonical shadows and never silently falls back', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Registry.make
        yield* registry.install(registration('first', 'A'))
        yield* registry.install(registration('second', 'B', 1, false))
        const lease = yield* registry.acquire
        assert.strictEqual((yield* lease.invokeTool(call).pipe(Effect.flip))._tag, 'RegistryError')
        assert.strictEqual((yield* lease.resolveTool('same').pipe(Effect.flip)).reason, 'disabled')
        assert.strictEqual(
          (yield* lease.resolveTool('same', { binding: 'second' }).pipe(Effect.flip)).reason,
          'disabled',
        )
        assert.strictEqual(
          (yield* lease
            .resolveTool('same', { binding: 'first', disabled: ['first'] })
            .pipe(Effect.flip)).reason,
          'disabled',
        )
        // An explicit permitted binding is distinct from accidental fallback.
        assert.strictEqual((yield* lease.invokeTool(call, { binding: 'first' })).details, 'A')
      }),
    ).pipe(Effect.provide(execution)),
  )

  it.effect('keeps identical native filesystem/path tags isolated in two actual roots', () => {
    const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
    const read = Tool.make('read', { success: Schema.String, failure: Schema.String })
      .addDependency(FileSystem.FileSystem)
      .addDependency(Path.Path)
    const tk = Toolkit.make(read)
    const handler = tk.toLayer({
      read: () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          return yield* fs.readFileString(path.resolve('same.txt')).pipe(Effect.mapError(String))
        }),
    })
    return Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const path = yield* Path.Path
        const root = yield* fs.makeTempDirectoryScoped()
        const roots = [path.join(root, 'a'), path.join(root, 'b')]
        yield* Effect.forEach(roots, (root) => fs.makeDirectory(root))
        const registry = yield* Registry.make
        for (const [i, root] of roots.entries()) {
          yield* fs.writeFileString(path.join(root, 'same.txt'), `root-${i}`)
          const services = ToolkitFileSystem.layer({ root }).pipe(Layer.provide(files))
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: `root-${i}`, version: 1 }, tk),
              handler.pipe(Layer.provideMerge(services)),
            ),
          )
        }
        const lease = yield* registry.acquire
        const results = yield* Effect.forEach(
          roots,
          (_, i) => lease.invokeTool({ ...call, name: 'read' }, { binding: `root-${i}` }),
          { concurrency: 'unbounded' },
        )
        assert.deepStrictEqual(
          results.map((r) => r.details),
          ['root-0', 'root-1'],
        )
        yield* registry.uninstall('root-0')
        const next = yield* registry.acquire
        assert.strictEqual((yield* next.invokeTool({ ...call, name: 'read' })).details, 'root-1')
      }),
    ).pipe(Effect.provide([files, execution]))
  })

  it.effect(
    'rejects duplicate task kinds atomically and inspects raw definitions without migrations',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let callbacks = 0
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.withTask(
              registration('first', 'A'),
              task('custom', () => callbacks++),
            ),
          )
          const before = yield* registry.snapshot
          const failed = yield* registry
            .install(Registry.withTask(registration('second', 'B'), task('custom')))
            .pipe(Effect.flip)
          assert.strictEqual(failed.reason, 'duplicateTask')
          assert.strictEqual(yield* registry.snapshot, before)
          const lease = yield* registry.acquire
          const found = yield* lease.resolveTask('custom')
          assert.strictEqual(found.owner.id, 'first')
          assert.strictEqual(found.declaration.version, 1)
          assert.strictEqual(callbacks, 0)
          yield* registry.install(
            Registry.withTask(registration('first', 'reloaded', 2), task('custom')),
          )
          assert.strictEqual(lease.snapshot.generation, 1)
          assert.strictEqual((yield* registry.snapshot).generation, 2)
        }),
      ),
  )

  it.effect(
    'captures extension and hook contexts independently and exposes wrapper/permission seams',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const hook = Hook.make({ event: 'beforeTool', execute: () => Effect.asVoid(Label) }).pipe(
            Hook.provide(Layer.succeed(Label, 'hook-local')),
          )
          const section = PromptSection.make({ key: 'context', render: () => Label })
          const ext = Extension.make({
            name: 'ext',
            tools: toolkit,
            hooks: [hook],
            sections: [section],
          }).pipe(
            Extension.provide(
              handlers.pipe(Layer.provideMerge(Layer.succeed(Label, 'extension-local'))),
            ),
          )
          const registry = yield* Registry.make
          yield* registry.install(Registry.extension({ id: 'extension', version: 4 }, ext))
          const lease = yield* registry.acquire
          const item = yield* lease.resolveTool('same')
          assert.strictEqual(Context.get(item.context, Label), 'extension-local')
          assert.strictEqual(Context.get(lease.snapshot.hooks[0]!.context, Label), 'hook-local')
          assert.strictEqual(
            Context.get(lease.snapshot.sections[0]!.context, Label),
            'extension-local',
          )
          const wrapper = (self: typeof item) =>
            Effect.map(
              lease.invokeTool(call, { binding: self.owner.id, version: self.owner.version }),
              (result) => ({
                ...result,
                details: `${self.owner.id}:${typeof result.details === 'string' ? result.details : JSON.stringify(result.details)}`,
              }),
            )
          assert.strictEqual((yield* wrapper(item)).details, 'extension:extension-local')
          assert.strictEqual(
            (yield* lease.resolveTool('same', { disabled: [item.owner.id] }).pipe(Effect.flip))
              .reason,
            'disabled',
          )
        }),
      ).pipe(Effect.provide(execution)),
  )

  it.effect('rejects invalid, duplicate and reserved registration/section keys', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Registry.make
        for (const id of ['', '__proto__', 'constructor', 'effect-harness/builtin'])
          assert.strictEqual(
            (yield* registry.install(registration(id, 'A')).pipe(Effect.flip)).reason,
            'invalidRegistration',
          )
        for (const keys of [['instructions'], ['Bad'], ['constructor'], ['context', 'context']]) {
          const ext = Extension.make({
            name: 'invalid',
            sections: keys.map((key) =>
              PromptSection.make({ key, render: () => Effect.succeed('') }),
            ),
          })
          assert.strictEqual(
            (yield* registry
              .install(Registry.extension({ id: 'invalid', version: 1 }, ext))
              .pipe(Effect.flip)).reason,
            'invalidSection',
          )
        }
        assert.strictEqual((yield* registry.snapshot).generation, 0)
      }),
    ),
  )
})
