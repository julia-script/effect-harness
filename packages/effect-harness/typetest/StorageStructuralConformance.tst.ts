import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'

class AdapterConfig extends Context.Service<AdapterConfig, { readonly directory: string }>()(
  'testing/StructuralAdapterConfig',
) {}
declare const make: Effect.Effect<
  Testing.StorageConformanceFixture<'open failed', AdapterConfig>,
  'factory failed',
  Scope.Scope | AdapterConfig
>
test('structural cases retain adapter requirements and factory/open errors', () => {
  const cases = Testing.storageConformance({ make, capabilities: { history: true, reopen: true } })
  expect(cases).type.toBe<
    ReadonlyArray<Testing.StorageConformanceCase<'factory failed' | 'open failed', AdapterConfig>>
  >()
  expect(cases[0]!.run).type.toBe<
    Effect.Effect<
      void,
      'factory failed' | 'open failed' | Storage.StorageError | Testing.StorageConformanceError,
      AdapterConfig
    >
  >()
})
test('ordinary current storage runs structural cases without enabling history or reopen', () => {
  const cases = Testing.storageConformance({
    make: Effect.succeed({
      open: Layer.build(Storage.layerMemory).pipe(
        Effect.map((c) => Context.get(c, Storage.Storage)),
      ),
    }),
  })
  expect(cases[0]!.run).type.toBe<
    Effect.Effect<void, Storage.StorageError | Testing.StorageConformanceError>
  >()
})
