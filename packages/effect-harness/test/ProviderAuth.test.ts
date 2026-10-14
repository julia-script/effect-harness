import { assert, describe, it } from '@effect/vitest'
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Redacted from 'effect/Redacted'
import * as TestClock from 'effect/testing/TestClock'
import * as Credential from '../src/provider-auth/Credential.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Resolver from '../src/provider-auth/Resolver.js'
import { AuthError } from '../src/provider-auth/AuthError.js'

const binding = { provider: 'openai-codex', profile: 'test', identity: 'login-1' } as const
const credential = (expiresAt: number): Credential.OAuth => ({
  type: 'oauth',
  binding,
  accessToken: Redacted.make('SECRET-access'),
  refreshToken: Redacted.make('SECRET-refresh'),
  expiresAt,
  accountId: 'account',
})

describe('provider credential lifecycle', () => {
  it.effect(
    'logout and account replacement wait for refresh and cannot be overwritten by its late result',
    () =>
      Effect.gen(function* () {
        for (const operation of ['logout', 'replace'] as const) {
          const store = yield* Store.makeMemory
          yield* Store.save(credential(1)).pipe(Effect.provideService(Store.CredentialStore, store))
          const started = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const { resolve } = yield* Resolver.make({
            binding,
            refresh: (old) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(started, undefined)
                yield* Deferred.await(release)
                return { ...old, expiresAt: 600_000 }
              }),
          }).pipe(Effect.provideService(Store.CredentialStore, store))
          const request = yield* Effect.forkChild(resolve)
          yield* Deferred.await(started)
          const modification =
            operation === 'logout'
              ? Store.logout(binding)
              : Store.save({
                  ...credential(600_000),
                  binding: { ...binding, identity: 'replacement' },
                })
          const writer = yield* modification.pipe(
            Effect.provideService(Store.CredentialStore, store),
            Effect.forkChild,
          )
          yield* Effect.yieldNow
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(request)
          yield* Fiber.join(writer)
          const saved = yield* store.read(Credential.key(binding))
          assert.strictEqual(
            saved?.binding.identity,
            operation === 'logout' ? undefined : 'replacement',
          )
          assert.strictEqual(
            (yield* Effect.flip(resolve)).reason,
            operation === 'logout' ? 'MissingCredential' : 'AccountMismatch',
          )
        }
      }),
  )

  it.effect('a failed persistence boundary does not publish the rotated credential', () =>
    Effect.gen(function* () {
      const memory = yield* Store.makeMemory
      yield* Store.save(credential(1)).pipe(Effect.provideService(Store.CredentialStore, memory))
      const store = Store.CredentialStore.of({
        ...memory,
        modify: (key, f) =>
          memory.modify(key, (current) =>
            Effect.andThen(f(current), Effect.fail(new AuthError({ reason: 'StoreFailure' }))),
          ),
      })
      const { resolve } = yield* Resolver.make({
        binding,
        refresh: (old) =>
          Effect.succeed({ ...old, expiresAt: 600_000, refreshToken: Redacted.make('SECRET-new') }),
      }).pipe(Effect.provideService(Store.CredentialStore, store))
      assert.strictEqual((yield* Effect.flip(resolve)).reason, 'StoreFailure')
      const saved = yield* memory.read(Credential.key(binding))
      assert.strictEqual(
        saved?.type === 'oauth' && Redacted.value(saved.refreshToken),
        'SECRET-refresh',
      )
    }),
  )
  it.effect('deduplicates concurrent refresh and publishes only persisted credentials', () =>
    Effect.gen(function* () {
      const store = yield* Store.makeMemory
      const now = yield* Clock.currentTimeMillis
      yield* Store.save(credential(now + 1)).pipe(
        Effect.provideService(Store.CredentialStore, store),
      )
      let rotations = 0
      const { resolve } = yield* Resolver.make({
        binding,
        refresh: (old) =>
          Effect.sync(() => {
            rotations++
            return {
              ...old,
              expiresAt: now + 600_000,
              refreshToken: Redacted.make('SECRET-rotated'),
            }
          }),
      }).pipe(Effect.provideService(Store.CredentialStore, store))
      const results = yield* Effect.all(
        Array.from({ length: 10 }, () => resolve),
        { concurrency: 'unbounded' },
      )
      assert.strictEqual(rotations, 1)
      assert.isTrue(
        results.every((result) => Redacted.value(result.refreshToken) === 'SECRET-rotated'),
      )
      assert.strictEqual((yield* store.read(Credential.key(binding)))?.type, 'oauth')
      assert.notInclude(JSON.stringify(yield* store.list), 'SECRET')
      assert.notInclude(JSON.stringify(results), 'SECRET')
    }),
  )

  it.effect(
    'persists a rotated token despite interruption, then orders logout after the refresh',
    () =>
      Effect.gen(function* () {
        const store = yield* Store.makeMemory
        const now = yield* Clock.currentTimeMillis
        yield* Store.save(credential(now + 1)).pipe(
          Effect.provideService(Store.CredentialStore, store),
        )
        const rotated = yield* Deferred.make<void>()
        const finish = yield* Deferred.make<void>()
        const { resolve } = yield* Resolver.make({
          binding,
          refresh: (old) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(rotated, undefined)
              yield* Deferred.await(finish)
              return {
                ...old,
                expiresAt: now + 600_000,
                refreshToken: Redacted.make('SECRET-rotated'),
              }
            }),
        }).pipe(Effect.provideService(Store.CredentialStore, store))
        const first = yield* Effect.forkChild(resolve)
        yield* Deferred.await(rotated)
        yield* Effect.forkChild(Fiber.interrupt(first))
        yield* Deferred.succeed(finish, undefined)
        yield* Fiber.await(first)
        const persisted = yield* store.read(Credential.key(binding))
        assert.strictEqual(
          persisted?.type === 'oauth' && Redacted.value(persisted.refreshToken),
          'SECRET-rotated',
        )
        yield* Store.logout(binding).pipe(Effect.provideService(Store.CredentialStore, store))
        const error = yield* Effect.flip(resolve)
        assert.strictEqual(error.reason, 'MissingCredential')
        assert.isUndefined(yield* store.read(Credential.key(binding)))
      }),
  )

  it.effect('bounds a hung refresh even inside the masked store transaction', () =>
    Effect.gen(function* () {
      const store = yield* Store.makeMemory
      const now = yield* Clock.currentTimeMillis
      yield* Store.save(credential(now + 1)).pipe(
        Effect.provideService(Store.CredentialStore, store),
      )
      const started = yield* Deferred.make<void>()
      const { resolve } = yield* Resolver.make({
        binding,
        refreshTimeoutMs: 100,
        refresh: () => Effect.andThen(Deferred.succeed(started, undefined), Effect.never),
      }).pipe(Effect.provideService(Store.CredentialStore, store))
      const request = yield* Effect.forkChild(Effect.flip(resolve))
      yield* Deferred.await(started)
      yield* TestClock.adjust(100)
      assert.strictEqual((yield* Fiber.join(request)).reason, 'Timeout')
      yield* Store.logout(binding).pipe(Effect.provideService(Store.CredentialStore, store))
    }),
  )

  it.effect('refuses replacement bindings and expired external-refresh credentials', () =>
    Effect.gen(function* () {
      const store = yield* Store.makeMemory
      yield* Store.save(credential(1)).pipe(Effect.provideService(Store.CredentialStore, store))
      const { resolve } = yield* Resolver.make({ binding }).pipe(
        Effect.provideService(Store.CredentialStore, store),
      )
      yield* TestClock.adjust(2)
      assert.strictEqual((yield* Effect.flip(resolve)).reason, 'ExpiredCredential')
      yield* Store.save({ ...credential(1), binding: { ...binding, identity: 'login-2' } }).pipe(
        Effect.provideService(Store.CredentialStore, store),
      )
      assert.strictEqual((yield* Effect.flip(resolve)).reason, 'AccountMismatch')
    }),
  )
})
