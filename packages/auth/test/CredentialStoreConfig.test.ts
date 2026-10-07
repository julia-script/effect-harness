import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import { assert, describe, it } from '@effect/vitest'
import * as Config from 'effect/Config'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as CredentialStore from '@effect-harness/auth/CredentialStore'

class UpdateInput extends Context.Service<UpdateInput, { readonly value: string }>()(
  'UpdateInput',
) {}

describe('CredentialStoreConfig', () => {
  it.effect(
    'protected file Config uses the injected provider and keeps generic callback requirements',
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const path = `${directory}/private/configured.json`
        const layer = CredentialStore.layerProtectedFileConfig({
          path: Config.String('CREDENTIAL_PATH'),
          lockRetries: Config.Int('LOCK_RETRIES'),
        })
        const context = yield* Layer.build(layer).pipe(
          Effect.provideService(
            ConfigProvider.ConfigProvider,
            ConfigProvider.fromUnknown({ CREDENTIAL_PATH: path, LOCK_RETRIES: 0 }),
          ),
        )
        const store = Context.get(context, CredentialStore.CredentialStore)
        yield* store
          .modify('selected', () =>
            Effect.gen(function* () {
              const input = yield* UpdateInput
              return {
                _tag: 'apiKey',
                provider: 'test',
                apiKey: Redacted.make(input.value),
              } as const
            }),
          )
          .pipe(Effect.provideService(UpdateInput, UpdateInput.of({ value: 'injected-key' })))
        assert.isTrue(yield* fs.exists(path))
        const failure = yield* Layer.build(
          CredentialStore.layerProtectedFileConfig({ path: Config.String('ABSENT') }),
        ).pipe(
          Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({})),
          Effect.flip,
        )
        assert.strictEqual(failure._tag, 'ConfigError')
      }).pipe(Effect.provide(Layer.mergeAll(BunFileSystem.layer, BunPath.layer, BunCrypto.layer))),
  )
})
