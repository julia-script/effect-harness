import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import type * as Registry from 'effect-harness/Registry'
import type { RegistryError } from 'effect-harness/RegistryError'
import type * as RegistryScope from 'effect-harness/RegistryScope'

declare const registry: Registry.Service

// A mutable caller-owned definition remains a valid public structural registration.
const definition = { id: 'valid', version: 1, enabled: true }
const registration: Registry.Registration<never, never> = {
  definition,
  acquire: Effect.sync(() => {
    definition.id = 'effect-harness/reserved'
    definition.version = 0
    definition.enabled = false
    return { tools: [], tasks: [], hooks: [], sections: [] }
  }),
}

test('custom registration with mutable metadata installs without extra requirements or errors', () => {
  expect(registry.install(registration)).type.toBe<
    Effect.Effect<RegistryScope.Snapshot, RegistryError>
  >()
})
