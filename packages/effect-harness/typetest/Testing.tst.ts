import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import { Testing as RootTesting } from 'effect-harness'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'

class FixtureConfig extends Context.Service<FixtureConfig, { readonly path: string }>()(
  'test/FixtureConfig',
) {}
declare const make: Effect.Effect<
  Testing.StorageConformanceFixture<'open failed', FixtureConfig>,
  'factory failed',
  FixtureConfig | Scope.Scope
>
declare const storage: Testing.StorageService
const cases = Testing.storageConformance({ make, capabilities: { history: true, reopen: true } })

test('public cases preserve factory errors and requirements while owning Scope', () => {
  expect(cases).type.toBe<
    ReadonlyArray<Testing.StorageConformanceCase<'factory failed' | 'open failed', FixtureConfig>>
  >()
  expect(cases[0]!.run).type.toBe<
    Effect.Effect<
      void,
      'factory failed' | 'open failed' | Storage.StorageError | Testing.StorageConformanceError,
      FixtureConfig
    >
  >()
  expect(RootTesting.storageConformance).type.toBe<typeof Testing.storageConformance>()
})

test('public layer factories need no runner dependency or outer Scope', () => {
  const cases = Testing.storageConformance({
    make: Effect.succeed({
      open: Layer.build(Storage.layerMemory).pipe(
        Effect.map((context) => Context.get(context, Storage.Storage)),
      ),
    }),
  })
  expect(cases[0]!.run).type.toBe<
    Effect.Effect<void, Storage.StorageError | Testing.StorageConformanceError>
  >()
  expect(
    Context.get(Context.make(Storage.Storage, storage), Storage.Storage),
  ).type.toBe<Testing.StorageService>()
})
