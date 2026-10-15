import * as Fs from 'node:fs/promises'
import * as Effect from 'effect/Effect'
import { CredentialStore, Resolver } from 'effect-harness/provider-auth'
import { BunSecretsCredentialStore } from 'effect-harness/provider-auth-bun'

const [path, lockPath, id] = process.argv.slice(2)
const other = id === 'a' ? 'b' : 'a'
globalThis.Bun = {
  secrets: {
    get: () => Fs.readFile(path, 'utf8'),
    set: ({ value }) => Fs.writeFile(path, value),
    delete: async () => {
      await Fs.unlink(path)
      return true
    },
  },
}
await Fs.writeFile(`${path}.ready-${id}`, '')
await Effect.runPromise(
  Effect.gen(function* () {
    while (
      !(yield* Effect.promise(() =>
        Fs.access(`${path}.ready-${other}`).then(
          () => true,
          () => false,
        ),
      ))
    )
      yield* Effect.sleep(5)
    const store = yield* BunSecretsCredentialStore.make({
      service: 'test.effect-harness',
      lockPath,
    })
    const { resolve } = yield* Resolver.make({
      binding: { credentialId: 'test', identity: 'login' },
      refresh: (old) =>
        Effect.gen(function* () {
          yield* Effect.promise(() => Fs.appendFile(`${path}.rotations`, 'rotation\n'))
          yield* Effect.sleep(100)
          return { ...old, expiresAt: Date.now() + 600000 }
        }),
    }).pipe(Effect.provideService(CredentialStore.CredentialStore, store))
    yield* resolve
  }),
)
