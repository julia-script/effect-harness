import * as Fs from 'node:fs/promises'
import * as Os from 'node:os'
import * as Path from 'node:path'
import { execFile } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Redacted from 'effect/Redacted'
import { BunSecretsCredentialStore } from 'effect-harness/provider-auth-bun'
import { Credential, CredentialStore } from 'effect-harness/provider-auth'

const binding = { provider: 'anthropic', profile: 'test', identity: 'login' } as const
const credential: Credential.OAuth = {
  type: 'oauth',
  binding,
  accessToken: Redacted.make('SECRET-access'),
  refreshToken: Redacted.make('SECRET-refresh'),
  expiresAt: 1,
}
const key = Credential.key(binding)
const entries = new Map<string, string>()
const addressKey = (address: { service: string; name: string }) =>
  JSON.stringify([address.service, address.name])
const secrets = {
  get: vi.fn(
    async (address: { service: string; name: string }) => entries.get(addressKey(address)) ?? null,
  ),
  set: vi.fn(async (address: { service: string; name: string; value: string }) => {
    entries.set(addressKey(address), address.value)
  }),
  delete: vi.fn(async (address: { service: string; name: string }) =>
    entries.delete(addressKey(address)),
  ),
}
let directory: string
let options: BunSecretsCredentialStore.Options
beforeEach(async () => {
  entries.clear()
  vi.clearAllMocks()
  vi.stubGlobal('Bun', { secrets })
  directory = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'provider-auth-bun-'))
  options = { service: 'test.effect-harness', lockPath: Path.join(directory, 'credentials.lock') }
})
afterEach(async () => {
  vi.unstubAllGlobals()
  await Fs.rm(directory, { recursive: true, force: true })
})
const save = (store: CredentialStore.Service, value: Credential.Credential = credential) =>
  CredentialStore.save(value).pipe(Effect.provideService(CredentialStore.CredentialStore, store))

it('round-trips redacted credentials, lists only metadata, and deletes the final native entry', async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const first = yield* BunSecretsCredentialStore.make(options)
      expect(yield* first.read(key)).toBeUndefined()
      expect(yield* first.list).toEqual([])
      yield* save(first)
      const apiKey: Credential.ApiKey = {
        type: 'api-key',
        binding: { ...binding, profile: 'second' },
        apiKey: Redacted.make('SECRET-key'),
      }
      yield* save(first, apiKey)
      const reopened = yield* BunSecretsCredentialStore.make(options)
      const loaded = yield* reopened.read(key)
      expect(loaded?.type === 'oauth' && Redacted.value(loaded.refreshToken)).toBe('SECRET-refresh')
      expect(yield* reopened.list).toEqual([
        { type: 'oauth', binding, expiresAt: 1 },
        { type: 'api-key', binding: apiKey.binding },
      ])
      yield* CredentialStore.logout(binding).pipe(
        Effect.provideService(CredentialStore.CredentialStore, reopened),
      )
      expect(yield* first.read(key)).toBeUndefined()
      expect(yield* first.read(Credential.key(apiKey.binding))).toEqual(apiKey)
      yield* CredentialStore.logout(apiKey.binding).pipe(
        Effect.provideService(CredentialStore.CredentialStore, first),
      )
      expect(entries.size).toBe(0)
    }),
  )
  expect(await Fs.readdir(directory)).toEqual([])
})

it('isolates named collections and provides the public layer', async () => {
  const first = await Effect.runPromise(BunSecretsCredentialStore.make(options))
  await Effect.runPromise(save(first))
  await Effect.runPromise(
    CredentialStore.save({ ...credential, accessToken: Redacted.make('OTHER') }).pipe(
      Effect.provide(BunSecretsCredentialStore.layer({ ...options, name: 'other' })),
    ),
  )
  const original = await Effect.runPromise(first.read(key))
  expect(original?.type === 'oauth' && Redacted.value(original.accessToken)).toBe('SECRET-access')
})

it('failed callbacks and unchanged values do not mutate the native entry', async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* BunSecretsCredentialStore.make(options)
      yield* save(store)
      expect(
        yield* Effect.flip(
          store.modify<never, string, never>(key, () => Effect.fail('callback-failed')),
        ),
      ).toBe('callback-failed')
      expect(yield* store.modify(key, (current) => Effect.succeed([42, current] as const))).toBe(42)
      expect(secrets.set).toHaveBeenCalledTimes(1)
      expect(secrets.delete).not.toHaveBeenCalled()
      yield* save(store, { ...credential, expiresAt: 2 })
      expect((yield* store.read(key))?.type).toBe('oauth')
    }),
  )
})

it.each(['not-json SECRET', '{"bad":{"type":"oauth","accessToken":"SECRET"}}'])(
  'rejects malformed persisted data without overwriting or leaking it: %s',
  async (text) => {
    entries.set(addressKey({ service: options.service, name: 'credentials' }), text)
    const store = await Effect.runPromise(BunSecretsCredentialStore.make(options))
    const error = await Effect.runPromise(Effect.flip(save(store)))
    expect(error.reason).toBe('StoreFailure')
    expect(JSON.stringify(error)).not.toContain('SECRET')
    expect(secrets.set).not.toHaveBeenCalled()
    expect(await Fs.readdir(directory)).toEqual([])
  },
)

it('sanitizes native read errors and reports uncertain writes and deletes', async () => {
  const store = await Effect.runPromise(BunSecretsCredentialStore.make(options))
  secrets.get.mockRejectedValueOnce(new Error('SECRET-read'))
  const readError = await Effect.runPromise(Effect.flip(store.read(key)))
  expect(readError.reason).toBe('StoreFailure')
  expect(JSON.stringify(readError)).not.toContain('SECRET')
  secrets.set.mockImplementationOnce(async (address) => {
    entries.set(addressKey(address), address.value)
    throw new Error('SECRET-after-publication')
  })
  const writeError = await Effect.runPromise(Effect.flip(save(store)))
  expect(writeError.reason).toBe('StoreCommitUncertain')
  expect(JSON.stringify(writeError)).not.toContain('SECRET')
  expect(await Effect.runPromise(store.read(key))).toEqual(credential)
  secrets.delete.mockRejectedValueOnce(new Error('SECRET-delete'))
  const deleteError = await Effect.runPromise(
    Effect.flip(
      CredentialStore.logout(binding).pipe(
        Effect.provideService(CredentialStore.CredentialStore, store),
      ),
    ),
  )
  expect(deleteError.reason).toBe('StoreCommitUncertain')
  expect(JSON.stringify(deleteError)).not.toContain('SECRET')
  expect(await Fs.readdir(directory)).toEqual([])
})

it('times out on an owned lock without running the callback or stealing the lock', async () => {
  await Fs.writeFile(options.lockPath, 'owned')
  const store = await Effect.runPromise(
    BunSecretsCredentialStore.make({ ...options, lockTimeoutMs: 30 }),
  )
  const callback = vi.fn(() => Effect.succeed([undefined, credential] as const))
  const error = await Effect.runPromise(Effect.flip(store.modify(key, callback)))
  expect(error.reason).toBe('StoreFailure')
  expect(callback).not.toHaveBeenCalled()
  expect(await Fs.readFile(options.lockPath, 'utf8')).toBe('owned')
})

it('finishes an interrupted transaction before another instance can modify its credential', async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const first = yield* BunSecretsCredentialStore.make(options)
      const second = yield* BunSecretsCredentialStore.make(options)
      const started = yield* Deferred.make<void>()
      const resume = yield* Deferred.make<void>()
      const worker = yield* Effect.forkChild(
        first.modify(key, () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined)
            yield* Deferred.await(resume)
            return [undefined, credential] as const
          }),
        ),
      )
      yield* Deferred.await(started)
      yield* Effect.forkChild(Fiber.interrupt(worker))
      const follower = yield* Effect.forkChild(
        second.modify(key, (current) => {
          expect(current).toEqual(credential)
          return Effect.succeed([undefined, undefined] as const)
        }),
      )
      yield* Deferred.succeed(resume, undefined)
      yield* Fiber.await(worker)
      yield* Fiber.join(follower)
      expect(yield* second.read(key)).toBeUndefined()
    }),
  )
  expect(await Fs.readdir(directory)).toEqual([])
})

it('rejects unsupported runtimes and invalid configuration before native access', async () => {
  for (const invalid of [{ service: '' }, { name: '' }, { lockPath: '' }, { lockTimeoutMs: 0 }]) {
    const error = await Effect.runPromise(
      Effect.flip(BunSecretsCredentialStore.make({ ...options, ...invalid })),
    )
    expect(error.reason).toBe('InvalidConfiguration')
  }
  vi.stubGlobal('Bun', undefined)
  const error = await Effect.runPromise(Effect.flip(BunSecretsCredentialStore.make(options)))
  expect(error.reason).toBe('InvalidConfiguration')
  expect(secrets.get).not.toHaveBeenCalled()
})

it('serializes refresh across OS processes using the same lock (substituted secrets backend)', async () => {
  // Only the native secret boundary is substituted; both processes run the published provider.
  const path = Path.join(directory, 'fake-secret.json')
  const store = await Effect.runPromise(BunSecretsCredentialStore.make(options))
  await Effect.runPromise(save(store))
  await Fs.writeFile(path, [...entries.values()][0]!)
  const fixture = fileURLToPath(new URL('./fixtures/BunSecretsRefreshProcess.mjs', import.meta.url))
  const run = (id: string) =>
    new Promise<void>((resolve, reject) => {
      execFile(
        process.execPath,
        [fixture, path, options.lockPath, id],
        { timeout: 5000 },
        (error) => (error ? reject(error) : resolve()),
      )
    })
  await Promise.all([run('a'), run('b')])
  expect(await Fs.readFile(`${path}.rotations`, 'utf8')).toBe('rotation\n')
})
