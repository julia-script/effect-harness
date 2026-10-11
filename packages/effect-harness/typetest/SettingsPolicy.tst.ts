import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
// Source imports let this foundation be checked before the integration owner's build.
import * as Settings from 'effect-harness/Settings'
import type { SettingsError } from 'effect-harness/SettingsError'

test('settings requirements and validation errors stay visible', () => {
  expect(Settings.make()).type.toBe<Effect.Effect<Settings.SettingsService, SettingsError>>()
  expect(Settings.get).type.toBe<Effect.Effect<Settings.Snapshot, never, Settings.Settings>>()
  expect(Settings.update({ retry: { maxRetries: 1 } })).type.toBe<
    Effect.Effect<Settings.Snapshot, SettingsError, Settings.Settings>
  >()
  expect(Settings.layer()).type.toBe<Layer.Layer<Settings.Settings, SettingsError>>()
  expect(Settings.layerDefault).type.toBe<Layer.Layer<Settings.Settings>>()
  expect(Settings.getOrDefault).type.toBe<Effect.Effect<Settings.Snapshot>>()
  expect(Effect.runPromise(Settings.get)).type.toRaiseError()
  expect(Settings.get.pipe(Effect.provide(Settings.layerDefault))).type.toBe<
    Effect.Effect<Settings.Snapshot>
  >()
  expect(Settings.get.pipe(Effect.provide(Settings.layer({ maxTurns: 10 })))).type.toBe<
    Effect.Effect<Settings.Snapshot, SettingsError>
  >()
})

test('snapshots and nested request metadata are readonly', () => {
  expect<Settings.Snapshot['revision']>().type.toBe<number>()
  expect<Settings.Snapshot['extensions']>().type.toBe<ReadonlyArray<string> | undefined>()
  expect(Settings.defaults.retry.maxRetries).type.toBe<number>()
  expect((Settings.defaults.retry.maxRetries = 4)).type.toRaiseError()
  expect((Settings.defaults.progress.outputIntervalMs = 4)).type.toRaiseError()
  expect((Settings.defaults.stream.timeoutMs = 4)).type.toRaiseError()
  expect(Settings.defaults.extensions?.push('other')).type.toRaiseError()
  expect((Settings.defaults.stream.headers!.trace = 'other')).type.toRaiseError()
  expect((Settings.defaults.stream.metadata!.nested = [])).type.toRaiseError()
})

test('explicit undefined is allowed in patches and policy modes stay constrained', () => {
  expect(
    Settings.update({
      retry: { maxRetries: undefined },
      progress: { outputIntervalMs: undefined },
      toolExecution: undefined,
      extensions: null,
      stream: { timeoutMs: undefined, transport: 'sse', deferred: { window: '1h' } },
    }),
  ).type.toBe<Effect.Effect<Settings.Snapshot, SettingsError, Settings.Settings>>()
  expect(Settings.update({ toolExecution: 'batch' })).type.toRaiseError()
  expect(Settings.update({ steeringMode: 'one' })).type.toRaiseError()
  expect(Settings.update({ followUpMode: 'latest' })).type.toRaiseError()
  expect(Settings.update({ cwd: '/tmp' })).type.toRaiseError()
  expect(Settings.update({ env: {} })).type.toRaiseError()
  expect(Settings.update({ revision: 1 })).type.toRaiseError()
  expect(Settings.update({ stream: { transport: 'http' } })).type.toRaiseError()
})

class ToolkitBinding extends Context.Service<ToolkitBinding, { readonly cwd: string }>()(
  'test/SettingsToolkitBinding',
) {}

test('providing settings cannot erase toolkit service requirements', () => {
  const program = Effect.gen(function* () {
    const binding = yield* ToolkitBinding
    const policy = yield* Settings.get
    return { cwd: binding.cwd, policy }
  }).pipe(Effect.provide(Settings.layerDefault))
  expect(program).type.toBe<
    Effect.Effect<{ cwd: string; policy: Settings.Snapshot }, never, ToolkitBinding>
  >()
  expect(Effect.runPromise(program)).type.toRaiseError()
  expect(program.pipe(Effect.provideService(ToolkitBinding, { cwd: '/project' }))).type.toBe<
    Effect.Effect<{ cwd: string; policy: Settings.Snapshot }>
  >()
  expect(program.pipe(Effect.provideService(ToolkitBinding, { cwd: 0 }))).type.toRaiseError()
})
