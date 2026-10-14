import * as Fs from 'node:fs/promises'
import { promises as FsMutable } from 'node:fs'
import { syncBuiltinESMExports } from 'node:module'
import * as Os from 'node:os'
import * as Path from 'node:path'
import { execFile } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { assert, it } from '@effect/vitest'
import { vi } from 'vitest'
import * as Net from 'node:net'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Credential from '../src/provider-auth/Credential.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as FileStore from '../src/provider-auth-node/CredentialStore.js'
import * as Resolver from '../src/provider-auth/Resolver.js'
import * as Callback from '../src/provider-auth-node/CallbackServer.js'
import * as Login from '../src/provider-auth/Login.js'

it.live('two OS processes sharing a durable store rotate an expiring credential once', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.promise(() => Fs.mkdtemp(Path.join(Os.tmpdir(), 'provider-auth-process-'))),
        (path) => Effect.promise(() => Fs.rm(path, { recursive: true, force: true })),
      )
      const path = Path.join(directory, 'credentials.json')
      const store = yield* FileStore.make({ path })
      yield* Store.save({
        type: 'oauth',
        binding: { provider: 'anthropic', profile: 'test', identity: 'login' },
        accessToken: Redacted.make('SECRET-access'),
        refreshToken: Redacted.make('SECRET-refresh'),
        expiresAt: 1,
      }).pipe(Effect.provideService(Store.CredentialStore, store))
      const fixture = fileURLToPath(
        new URL('./fixtures/CredentialRefreshProcess.mjs', import.meta.url),
      )
      const run = (id: string) =>
        Effect.promise(
          () =>
            new Promise<void>((resolve, reject) => {
              execFile(process.execPath, [fixture, path, id], { timeout: 5000 }, (error) =>
                error ? reject(error) : resolve(),
              )
            }),
        )
      yield* Effect.all([run('a'), run('b')], { concurrency: 'unbounded' })
      assert.strictEqual(
        yield* Effect.promise(() => Fs.readFile(`${path}.rotations`, 'utf8')),
        'rotation\n',
      )
    }),
  ),
)

it.live(
  'independent durable store instances serialize rotation and reload the committed secret',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.promise(() => Fs.mkdtemp(Path.join(Os.tmpdir(), 'provider-auth-'))),
          (path) => Effect.promise(() => Fs.rm(path, { recursive: true, force: true })),
        )
        const path = Path.join(directory, 'credentials.json')
        const first = yield* FileStore.make({ path })
        const second = yield* FileStore.make({ path })
        const binding = { provider: 'anthropic', profile: 'test', identity: 'login' } as const
        yield* Store.save({
          type: 'oauth',
          binding,
          accessToken: Redacted.make('SECRET-access'),
          refreshToken: Redacted.make('SECRET-refresh'),
          expiresAt: 1,
        }).pipe(Effect.provideService(Store.CredentialStore, first))
        let refreshes = 0
        const refresh = (old: Credential.OAuth) =>
          Effect.gen(function* () {
            refreshes++
            yield* Effect.sleep(10)
            return {
              ...old,
              expiresAt: Date.now() + 600_000,
              accessToken: Redacted.make('SECRET-new'),
            }
          })
        const a = yield* Resolver.make({ binding, refresh }).pipe(
          Effect.provideService(Store.CredentialStore, first),
        )
        const b = yield* Resolver.make({ binding, refresh }).pipe(
          Effect.provideService(Store.CredentialStore, second),
        )
        yield* Effect.all([a.resolve, b.resolve], { concurrency: 'unbounded' })
        assert.strictEqual(refreshes, 1)
        const reopened = yield* FileStore.make({ path })
        const stored = yield* reopened.read(Credential.key(binding))
        assert.strictEqual(
          stored?.type === 'oauth' && Redacted.value(stored.accessToken),
          'SECRET-new',
        )
        assert.notInclude(JSON.stringify(yield* reopened.list), 'SECRET')
        assert.strictEqual((yield* Effect.promise(() => Fs.stat(path))).mode & 0o777, 0o600)
        yield* Store.logout(binding).pipe(Effect.provideService(Store.CredentialStore, second))
        assert.isUndefined(yield* first.read(Credential.key(binding)))
      }),
    ),
)

it.live('contended file locks time out without stealing ownership', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.promise(() => Fs.mkdtemp(Path.join(Os.tmpdir(), 'provider-auth-lock-'))),
        (path) => Effect.promise(() => Fs.rm(path, { recursive: true, force: true })),
      )
      const path = Path.join(directory, 'credentials.json')
      const store = yield* FileStore.make({ path, lockTimeoutMs: 50 })
      yield* Effect.promise(() => Fs.writeFile(`${path}.lock`, 'held'))
      const error = yield* Effect.flip(
        store.modify('key', () => Effect.succeed([undefined, undefined] as const)),
      )
      assert.strictEqual(error.reason, 'StoreFailure')
      assert.strictEqual(yield* Effect.promise(() => Fs.readFile(`${path}.lock`, 'utf8')), 'held')
    }),
  ),
)

it.live('loopback validates state, rejects duplicate callbacks, and closes on scope release', () =>
  Effect.gen(function* () {
    let address = ''
    yield* Effect.scoped(
      Effect.gen(function* () {
        const listener = yield* (yield* Login.CallbackServer).open({
          port: 0,
          path: '/callback',
          state: 'attempt-state',
          fallbackPort: true,
        })
        address = listener.redirectUri.replace('localhost', '127.0.0.1')
        const malformed = yield* Effect.promise(
          () =>
            new Promise<string>((resolve, reject) => {
              const socket = Net.connect(Number(new URL(address).port), '127.0.0.1', () => {
                socket.write(
                  'GET http://[ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n',
                )
              })
              let response = ''
              socket.setEncoding('utf8')
              socket.setTimeout(1000, () => socket.destroy(new Error('Callback did not respond')))
              socket.on('data', (chunk: string) => {
                response += chunk
              })
              socket.once('error', reject)
              socket.once('end', () => resolve(response))
            }),
        )
        assert.match(malformed, /^HTTP\/1\.1 400/)
        const accepted = yield* Effect.promise(() =>
          fetch(`${address}?code=one-time&state=attempt-state`),
        )
        assert.strictEqual(accepted.status, 200)
        assert.include(yield* listener.completion, 'code=one-time')
        const repeated = yield* Effect.promise(() =>
          fetch(`${address}?code=one-time&state=attempt-state`),
        )
        assert.strictEqual(repeated.status, 409)
      }).pipe(Effect.provide(Callback.layer)),
    )
    const reachable = yield* Effect.promise(() =>
      fetch(address).then(
        () => true,
        () => false,
      ),
    )
    assert.isFalse(reachable)
  }),
)

for (const phase of ['open-directory', 'sync-directory', 'unlock'] as const)
  it.live(`file store distinguishes ${phase} failures from a rolled-back write`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.promise(() => Fs.mkdtemp(Path.join(Os.tmpdir(), 'provider-auth-commit-'))),
          (path) => Effect.promise(() => Fs.rm(path, { recursive: true, force: true })),
        )
        const path = Path.join(directory, 'credentials.json')
        const store = yield* FileStore.make({ path })
        const binding = { provider: 'anthropic', profile: 'test', identity: 'before' } as const
        const credential: Credential.OAuth = {
          type: 'oauth',
          binding,
          accessToken: Redacted.make('SECRET-access'),
          refreshToken: Redacted.make('SECRET-refresh'),
          expiresAt: 1,
        }
        yield* Store.save(credential).pipe(Effect.provideService(Store.CredentialStore, store))
        const originalOpen = Fs.open
        const open = vi.spyOn(FsMutable, 'open').mockImplementation(async (...args) => {
          if (args[0] === directory && phase === 'open-directory')
            throw new Error('SECRET-directory-error')
          const file = await originalOpen(...args)
          if (args[0] === directory && phase === 'sync-directory')
            vi.spyOn(file, 'sync').mockRejectedValue(new Error('SECRET-sync-error'))
          return file
        })
        const originalUnlink = Fs.unlink
        const unlink = vi.spyOn(FsMutable, 'unlink').mockImplementation(async (target) => {
          if (target === `${path}.lock` && phase === 'unlock')
            throw new Error('SECRET-unlock-error')
          return originalUnlink(target)
        })
        syncBuiltinESMExports()
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            open.mockRestore()
            unlink.mockRestore()
            syncBuiltinESMExports()
          }),
        )
        const error = yield* Effect.flip(
          Store.save({ ...credential, binding: { ...binding, identity: 'after' } }).pipe(
            Effect.provideService(Store.CredentialStore, store),
          ),
        )
        assert.strictEqual(
          error.reason,
          phase === 'open-directory' ? 'StoreFailure' : 'StoreCommitUncertain',
        )
        assert.notInclude(JSON.stringify(error), 'SECRET')
        const stored = yield* store.read(Credential.key(binding))
        assert.strictEqual(
          stored?.binding.identity,
          phase === 'open-directory' ? 'before' : 'after',
        )
      }),
    ),
  )
