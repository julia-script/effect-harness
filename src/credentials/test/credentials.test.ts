/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The stores, and the resolver over them. Every writable store passes the same cases: memory, and
 * memory behind `CredentialsRpcs` across the memory transport, a `KeyValueStore`, and a keychain.
 */
import { describe, expect, test } from 'bun:test'
import {
  Clock,
  ConfigProvider,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Redacted,
  Schema,
} from 'effect'
import type { Scope } from 'effect'
import { KeyValueStore } from 'effect/persistence'
import { HarnessTransport } from '../../client/index.ts'
import { runTest } from '../../task/test/support.ts'
import { Credential, Credentials } from '../index.ts'
import * as keychain from '../internal/keychain.ts'
import { makeFakeKeychain } from './fakeKeychain.ts'

const apiKey = (key: string): Credential.Credential => ({
  type: 'api_key',
  key: Redacted.make(key),
})

const oauth = (access: string, expires: number): Credential.OAuth => ({
  type: 'oauth',
  access: Redacted.make(access),
  refresh: Redacted.make(`${access}-refresh`),
  expires,
})

const accessOf = (credential: Credential.Credential | undefined) =>
  credential?.type === 'oauth' ? Redacted.value(credential.access) : undefined

/** Memory, served over `CredentialsRpcs`, and read through `Credentials.makeRpc`. */
const remote = (
  protocols: typeof HarnessTransport.makeMemory,
): Effect.Effect<Credentials.Service, never, Scope.Scope> =>
  Effect.gen(function* () {
    const transport = yield* protocols
    const store = yield* Credentials.makeMemory()
    yield* Credentials.layerServer.pipe(
      Layer.launch,
      Effect.provideService(Credentials.Credentials, store),
      Effect.provideContext(transport),
      Effect.forkScoped,
    )
    return yield* Effect.provideContext(Credentials.makeRpc, transport)
  })

/** `effect` over an in-memory `KeyValueStore`, open while the scope is. */
const withKeyValue = <A, E>(effect: Effect.Effect<A, E, KeyValueStore.KeyValueStore>) =>
  Effect.flatMap(Layer.build(KeyValueStore.layerMemory), (context) =>
    Effect.provideContext(effect, context),
  )

const stores: ReadonlyArray<
  readonly [string, Effect.Effect<Credentials.Service, never, Scope.Scope>]
> = [
  ['memory', Credentials.makeMemory()],
  ['rpc', remote(HarnessTransport.makeMemory)],
  // Secrets cross as strings and decode back to `Redacted`.
  ['encoded rpc', remote(HarnessTransport.makeMemoryEncoded)],
  ['key-value', withKeyValue(Credentials.makeKeyValueStore())],
  ['keychain', keychain.make('test', makeFakeKeychain())],
]

for (const [name, open] of stores) {
  describe(`${name} store`, () => {
    test('sets, lists and removes credentials', () =>
      runTest(() =>
        Effect.gen(function* () {
          const store = yield* open
          expect(yield* store.get('anthropic')).toBeUndefined()
          yield* store.set('anthropic', apiKey('sk-1'))
          yield* store.set('openai', oauth('a', 0))
          const stored = yield* store.get('anthropic')
          expect(stored?.type === 'api_key' && Redacted.value(stored.key)).toBe('sk-1')
          expect(yield* store.list).toEqual([
            { provider: 'anthropic', type: 'api_key' },
            { provider: 'openai', type: 'oauth' },
          ])
          yield* store.remove('anthropic')
          expect(yield* store.list).toEqual([{ provider: 'openai', type: 'oauth' }])
        }),
      ))

    test('updates only the credential it expected', () =>
      runTest(() =>
        Effect.gen(function* () {
          const store = yield* open
          const first = oauth('a', 0)
          yield* store.set('openai', first)
          // Equal by value, as a credential read back from the store is.
          yield* store.update('openai', oauth('a', 0), oauth('b', 1))
          expect(accessOf(yield* store.get('openai'))).toBe('b')
          const lost = yield* Effect.flip(store.update('openai', first, oauth('c', 2)))
          expect(lost).toMatchObject({ _tag: 'CredentialChanged', provider: 'openai' })
          expect(accessOf(yield* store.get('openai'))).toBe('b')
        }),
      ))

    test('preserves identity and sign-in revision across storage and compare-and-set', () =>
      runTest(() =>
        Effect.gen(function* () {
          const store = yield* open
          const first: Credential.OAuth = {
            ...oauth('same-token', 0),
            identity: {
              issuer: 'https://provider.example',
              subject: 'account-one',
              email: 'person@example.com',
              name: 'Person',
            },
            revision: 'sign-in-one',
          }
          yield* store.set('work', first)
          expect(yield* store.get('work')).toEqual(first)
          const reauthenticated = { ...first, revision: 'sign-in-two' }
          yield* store.update('work', first, reauthenticated)
          expect(yield* store.get('work')).toEqual(reauthenticated)
          const stale = yield* Effect.flip(store.update('work', first, oauth('stale', 1)))
          expect(stale).toMatchObject({ _tag: 'CredentialChanged', provider: 'work' })
          expect(yield* store.get('work')).toEqual(reauthenticated)
        }),
      ))
  })
}

describe('key-value store', () => {
  test('keeps credentials across reopening, with secrets decoded', () =>
    runTest(() =>
      Effect.gen(function* () {
        const first = yield* Credentials.makeKeyValueStore()
        yield* first.set('codex-work', oauth('work', 1))
        yield* first.set('openai', apiKey('sk-1'))
        const reopened = yield* Credentials.makeKeyValueStore()
        expect(accessOf(yield* reopened.get('codex-work'))).toBe('work')
        // A credential read back from the store is the one `update` expects.
        yield* reopened.update('codex-work', oauth('work', 1), oauth('next', 2))
        expect(accessOf(yield* first.get('codex-work'))).toBe('next')
        expect(yield* first.list).toEqual([
          { provider: 'codex-work', type: 'oauth' },
          { provider: 'openai', type: 'api_key' },
        ])
      }).pipe(withKeyValue),
    ))
})

describe('config store', () => {
  const env = ConfigProvider.fromEnvRecord({ ANTHROPIC_API_KEY: 'sk-env', GROQ_API_KEY: 'gk' })

  test('reads API keys from config at every get, and refuses writes', () =>
    runTest(() =>
      Effect.gen(function* () {
        const store = Credentials.makeConfig()
        const stored = yield* store.get('anthropic')
        expect(stored?.type === 'api_key' && Redacted.value(stored.key)).toBe('sk-env')
        expect(yield* store.get('openai')).toBeUndefined()
        expect(yield* store.get('unknown')).toBeUndefined()
        expect(yield* store.list).toEqual([
          { provider: 'anthropic', type: 'api_key' },
          { provider: 'groq', type: 'api_key' },
        ])
        expect(yield* Effect.flip(store.set('anthropic', apiKey('x')))).toMatchObject({
          _tag: 'ReadOnly',
          provider: 'anthropic',
        })
      }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, env)),
    ))

  test('takes the first name that is set', () =>
    runTest(() =>
      Effect.gen(function* () {
        const store = Credentials.makeConfig({ custom: ['FIRST', 'SECOND'] })
        const stored = yield* store.get('custom')
        expect(stored?.type === 'api_key' && Redacted.value(stored.key)).toBe('second')
      }).pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromEnvRecord({ SECOND: 'second' }),
        ),
      ),
    ))
})

const resolverOver = (store: Credentials.Service, refreshes: Array<string>) =>
  Credentials.makeResolver({
    refresh: (provider, credential) =>
      Effect.map(Clock.currentTimeMillis, (now) => {
        refreshes.push(provider)
        return oauth(`${Redacted.value(credential.access)}+`, now + 3_600_000)
      }),
  }).pipe(Effect.provideService(Credentials.Credentials, store))

describe('resolver', () => {
  test('returns API keys and fresh tokens as stored', () =>
    runTest(() =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis
        const store = yield* Credentials.makeMemory([
          ['anthropic', apiKey('sk')],
          ['openai', oauth('a', now + 3_600_000)],
        ])
        const refreshes: Array<string> = []
        const resolve = yield* resolverOver(store, refreshes)
        expect((yield* resolve('anthropic'))?.type).toBe('api_key')
        expect(accessOf(yield* resolve('openai'))).toBe('a')
        expect(yield* resolve('missing')).toBeUndefined()
        expect(refreshes).toEqual([])
      }),
    ))

  test('refreshes expiring tokens once for concurrent requests, and stores them', () =>
    runTest(() =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis
        const store = yield* Credentials.makeMemory([['openai', oauth('a', now + 1000)]])
        const refreshes: Array<string> = []
        const resolve = yield* resolverOver(store, refreshes)
        const resolved = yield* Effect.all([resolve('openai'), resolve('openai')], {
          concurrency: 'unbounded',
        })
        expect(resolved.map((credential) => accessOf(credential))).toEqual(['a+', 'a+'])
        expect(refreshes).toEqual(['openai'])
        expect(accessOf(yield* store.get('openai'))).toBe('a+')
      }),
    ))

  test('returns the tokens another writer stored first', () =>
    runTest(() =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis
        const store = yield* Credentials.makeMemory([['openai', oauth('a', now)]])
        const started = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const resolve = yield* Credentials.makeResolver({
          refresh: () =>
            Deferred.done(started, Exit.void).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(oauth('mine', now + 3_600_000)),
            ),
        }).pipe(Effect.provideService(Credentials.Credentials, store))
        const resolving = yield* Effect.forkChild(resolve('openai'))
        yield* Deferred.await(started)
        // Another process refreshes while this one waits on the provider.
        yield* store.set('openai', oauth('theirs', now + 3_600_000))
        yield* Deferred.done(release, Exit.void)
        expect(accessOf(yield* Fiber.join(resolving))).toBe('theirs')
      }),
    ))
})

describe('credential', () => {
  test('decodes old serialized OAuth without inventing identity or revision', () => {
    const legacy = Schema.decodeSync(Credential.OAuth)({
      type: 'oauth',
      access: 'legacy-access',
      refresh: 'legacy-refresh',
      expires: 1234,
      extra: { identity: 'old-random-id', accountId: 'routing-id' },
    })
    expect(Redacted.value(legacy.access)).toBe('legacy-access')
    expect(Redacted.value(legacy.refresh)).toBe('legacy-refresh')
    expect(legacy.identity).toBeUndefined()
    expect(legacy.revision).toBeUndefined()
    expect(legacy.extra).toEqual({ identity: 'old-random-id', accountId: 'routing-id' })
  })

  test.each([
    { identity: { issuer: '', subject: 'account' } },
    { identity: { issuer: 'provider', subject: '' } },
    { revision: '' },
  ])('rejects empty identity coordinates or revision (%j)', (metadata) => {
    expect(() =>
      Schema.decodeSync(Credential.OAuth)({
        type: 'oauth',
        access: 'a',
        refresh: 'r',
        expires: 0,
        ...metadata,
      }),
    ).toThrow()
  })

  test('keeps secrets out of logs and inspection', () => {
    const credential = oauth('secret-access', 0)
    expect(JSON.stringify(credential)).not.toContain('secret')
    expect(Bun.inspect(credential)).not.toContain('secret')
  })
})
