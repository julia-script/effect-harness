import * as Fs from 'node:fs/promises'
import * as Os from 'node:os'
import * as Path from 'node:path'
import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Providers from '../src/Providers.js'
import * as ProviderStore from '../src/ProviderStore.js'
import * as ProviderStoreNode from '../src/ProviderStoreNode.js'
import * as FileCredentials from '../src/provider-auth-node/FileSystemCredentialStore.js'
import * as CredentialStore from '../src/provider-auth/CredentialStore.js'
import * as Credential from '../src/provider-auth/Credential.js'
import * as Gateway from '../src/provider-openai-compatible/Provider.js'

it.live(
  'separate durable stores reopen typed instances without copying secrets into public state',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.promise(() => Fs.mkdtemp(Path.join(Os.tmpdir(), 'providers-'))),
          (path) => Effect.promise(() => Fs.rm(path, { force: true, recursive: true })),
        )
        const publicPath = Path.join(directory, 'providers.json')
        const secretsPath = Path.join(directory, 'credentials.json')
        const open = Effect.gen(function* () {
          const store = yield* ProviderStoreNode.make({ path: publicPath })
          const credentials = yield* FileCredentials.make({ path: secretsPath })
          const manager = yield* Providers.make({
            definitions: { gateway: Gateway.definition },
          }).pipe(
            Effect.provideService(ProviderStore.ProviderStore, store),
            Effect.provideService(CredentialStore.CredentialStore, credentials),
          )
          return { manager, credentials }
        })
        const first = yield* open
        const second = yield* open
        const create = (manager: typeof first.manager, endpoint: string) =>
          manager.add({
            provider: 'gateway',
            label: 'Same label',
            config: { baseUrl: endpoint, models: [] },
            secrets: { apiKey: Redacted.make(`SECRET-${endpoint}`) },
          })
        const [a, b] = yield* Effect.all(
          [create(first.manager, 'https://a.example'), create(second.manager, 'https://b.example')],
          { concurrency: 'unbounded' },
        )
        const reopened = yield* open
        assert.equal((yield* reopened.manager.list).length, 2)
        const publicText = yield* Effect.promise(() => Fs.readFile(publicPath, 'utf8'))
        assert.notInclude(publicText, 'SECRET')
        assert.notInclude(publicText, 'apiKey')
        assert.include(publicText, a.credential!.credentialId)
        const decoded = yield* CredentialStore.read(a.credential!, Credential.ApiKey).pipe(
          Effect.provideService(CredentialStore.CredentialStore, reopened.credentials),
        )
        assert.equal(Redacted.value(decoded.apiKey), 'SECRET-https://a.example')
        const replacement = yield* reopened.manager.update(a.id, {
          secrets: { apiKey: Redacted.make('SECRET-replaced') },
        })
        assert.notEqual(replacement.credential?.identity, a.credential?.identity)
        assert.isUndefined(yield* first.credentials.read(a.credential!.credentialId))
        yield* reopened.manager.remove(b.id)
        assert.equal((yield* second.manager.list).length, 1)
        assert.equal((yield* second.credentials.list).length, 1)
      }),
    ),
)
