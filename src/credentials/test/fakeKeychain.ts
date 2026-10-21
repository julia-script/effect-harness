/**
 * A keychain in a `Map`, keyed by service and name, standing in for `Bun.secrets` in tests.
 */
import { Effect } from 'effect'
import type { Keychain } from '../bun/internal/keychain.ts'

const key = (service: string, name: string) => JSON.stringify([service, name])

export const makeFakeKeychain = (entries = new Map<string, string>()): Keychain => ({
  get: (service, name) => Effect.sync(() => entries.get(key(service, name))),
  set: (service, name, value) =>
    Effect.sync(() => {
      entries.set(key(service, name), value)
    }),
  remove: (service, name) =>
    Effect.sync(() => {
      entries.delete(key(service, name))
    }),
})
