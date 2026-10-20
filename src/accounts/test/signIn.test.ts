/* oxlint-disable max-lines-per-function -- describe blocks and behavioral scenarios */
/** OAuth lifetimes exercise the registry through its public service and encoded RPC transport. */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Fiber, Layer, Queue, Redacted, Stream } from 'effect'
import type { Scope } from 'effect'
import { FetchHttpClient } from 'effect/http'
import { KeyValueStore } from 'effect/persistence'
import { RpcClient } from 'effect/rpc'
import { HarnessTransport } from '../../client/index.ts'
import { Credentials } from '../../credentials/index.ts'
import type { Credential } from '../../credentials/index.ts'
import type { Model } from '../../model/index.ts'
import { AccountError, Accounts, AccountsRpcs, Adapter } from '../index.ts'
import type { Account } from '../index.ts'

const Browser = Adapter.make('browser', { label: 'Browser', auth: Adapter.browserOAuth })
const Device = Adapter.make('device', { label: 'Device', auth: Adapter.deviceCode })
const group = Adapter.group(Browser, Device)
const provider = (account: Adapter.Account): Model.Listable<Adapter.Requires> => ({
  name: account.name,
  models: [],
  stream: () => Stream.empty,
  listModels: Effect.succeed([]),
})
const oauth = (access: string): Credential.OAuth => ({
  type: 'oauth',
  access: Redacted.make(access),
  refresh: Redacted.make('refresh-secret'),
  expires: 4_000_000_000_000,
  identity: { issuer: 'https://identity.test', subject: 'verified-user' },
  revision: access,
  extra: { private: 'not-status' },
})
const services = Layer.mergeAll(
  KeyValueStore.layerMemory,
  Credentials.layerMemory(),
  FetchHttpClient.layer,
)
const run = <A, E>(effect: Effect.Effect<A, E, Layer.Success<typeof services> | Scope.Scope>) =>
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test owns its service graph
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.provide(services)))

type Login = NonNullable<Adapter.Base<Adapter.Any>['login']>
const login: Login = (account, flow) =>
  Effect.gen(function* () {
    yield* flow.onAuthorize('https://identity.test/authorize')
    const completion = yield* flow.completion
    if (completion !== 'accepted') return yield* Effect.fail({ message: `Rejected ${completion}` })
    const store = yield* Credentials.Credentials
    yield* store.set(account.name, oauth('fresh'))
  })
const open = (implementation: Login = login, existing = false) =>
  Accounts.make(group, {
    accounts: existing ? [{ adapter: 'browser', name: 'work', label: 'Work', settings: {} }] : [],
  }).pipe(
    Effect.provide(
      group.toLayer({
        browser: { provider, login: implementation },
        device: {
          provider,
          login: (account, flow) =>
            Effect.gen(function* () {
              yield* flow.onCode({ url: 'https://identity.test/device', code: 'DEVICE' })
              yield* implementation(account, flow)
            }),
        },
      }),
    ),
  )

const observe = Effect.fnUntraced(function* <E>(stream: Stream.Stream<Account.SignInEvent, E>) {
  const events = yield* Queue.unbounded<Account.SignInEvent>()
  const fiber = yield* stream.pipe(
    Stream.tap((event) => Queue.offer(events, event)),
    Stream.runCollect,
    Effect.forkScoped,
  )
  const started = yield* Queue.take(events)
  expect(started.type).toBe('started')
  const authorization = yield* Queue.take(events)
  expect(['authorization', 'code']).toContain(authorization.type)
  expect(authorization.id).toBe(started.id)
  return { id: started.id, fiber, events }
})

describe('OAuth sign-in lifecycle', () => {
  test('commits a browser completion, exposes verified metadata, and never saves redirectUri', () =>
    run(
      Effect.gen(function* () {
        let redirectUri: string | undefined
        const accounts = yield* open((account, flow) => {
          redirectUri = flow.redirectUri
          return login(account, flow)
        })
        const store = yield* Credentials.Credentials
        const attempt = yield* observe(
          accounts.signIn({
            adapter: 'browser',
            name: 'work',
            label: 'Work',
            redirectUri: 'http://127.0.0.1:3000/callback',
          }),
        )
        expect(yield* accounts.list).toEqual([])
        expect(yield* store.get('work')).toBeUndefined()
        expect(redirectUri).toBe('http://127.0.0.1:3000/callback')
        yield* accounts.completeSignIn(attempt.id, Redacted.make('accepted'))
        const events = yield* Fiber.join(attempt.fiber)
        expect(events.map(({ type }) => type)).toEqual(['started', 'authorization', 'done'])
        expect(events.at(-1)).toMatchObject({
          id: attempt.id,
          account: {
            name: 'work',
            status: 'ready',
            identity: { subject: 'verified-user' },
            revision: 'fresh',
          },
        })
        expect(JSON.stringify(events)).not.toContain('refresh-secret')
        expect(JSON.stringify(events)).not.toContain('not-status')
        const reopened = yield* open()
        expect(yield* reopened.list).toMatchObject([
          { settings: {}, identity: { subject: 'verified-user' }, revision: 'fresh' },
        ])
        expect(
          (yield* Effect.flip(accounts.completeSignIn(attempt.id, Redacted.make('accepted'))))
            .reason,
        ).toBeInstanceOf(AccountError.InvalidSignIn)
        expect((yield* Effect.flip(accounts.cancelSignIn(attempt.id))).reason).toBeInstanceOf(
          AccountError.InvalidSignIn,
        )
      }),
    ))

  test('accepts completion once, rejects foreign and device attempts, and reserves each name independently', () =>
    run(
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>()
        const accounts = yield* open((account, flow) =>
          login(account, flow).pipe(Effect.andThen(Deferred.await(gate))),
        )
        const work = yield* observe(accounts.signIn({ adapter: 'browser', name: 'work' }))
        const personal = yield* observe(accounts.signIn({ adapter: 'browser', name: 'personal' }))
        expect(work.id).not.toBe(personal.id)
        expect(
          (yield* Effect.flip(
            Stream.runDrain(accounts.signIn({ adapter: 'browser', name: 'work' })),
          )).reason,
        ).toBeInstanceOf(AccountError.SignInActive)
        expect(
          (yield* Effect.flip(accounts.completeSignIn('unknown', Redacted.make('accepted'))))
            .reason,
        ).toBeInstanceOf(AccountError.InvalidSignIn)
        yield* accounts.completeSignIn(work.id, Redacted.make('accepted'))
        expect(
          (yield* Effect.flip(accounts.completeSignIn(work.id, Redacted.make('accepted')))).reason,
        ).toBeInstanceOf(AccountError.InvalidSignIn)
        yield* accounts.cancelSignIn(personal.id)
        expect((yield* Effect.flip(Fiber.join(personal.fiber))).reason).toBeInstanceOf(
          AccountError.SignInCancelled,
        )
        yield* Deferred.succeed(gate, undefined)
        yield* Fiber.join(work.fiber)
        const device = yield* observe(accounts.signIn({ adapter: 'device', name: 'device' }))
        expect(
          (yield* Effect.flip(accounts.completeSignIn(device.id, Redacted.make('accepted'))))
            .reason,
        ).toBeInstanceOf(AccountError.InvalidSignIn)
        yield* accounts.cancelSignIn(device.id)
        expect((yield* Effect.flip(Fiber.join(device.fiber))).reason).toBeInstanceOf(
          AccountError.SignInCancelled,
        )
        expect((yield* accounts.list).map(({ name }) => name)).toEqual(['work'])
      }),
    ))

  test('stages writes until success and preserves old credentials after explicit cancellation', () =>
    run(
      Effect.gen(function* () {
        const staged = yield* Deferred.make<void>()
        const closed = yield* Deferred.make<void>()
        const accounts = yield* open(
          (account, flow) =>
            login(account, flow).pipe(
              Effect.andThen(Deferred.succeed(staged, undefined)),
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(closed, undefined)),
            ),
          true,
        )
        const store = yield* Credentials.Credentials
        const previous = oauth('previous')
        yield* store.set('work', previous)
        const attempt = yield* observe(accounts.signIn({ name: 'work' }))
        yield* accounts.completeSignIn(attempt.id, Redacted.make('accepted'))
        yield* Deferred.await(staged)
        expect(yield* store.get('work')).toEqual(previous)
        yield* accounts.cancelSignIn(attempt.id)
        expect((yield* Effect.flip(Fiber.join(attempt.fiber))).reason).toBeInstanceOf(
          AccountError.SignInCancelled,
        )
        yield* Deferred.await(closed)
        expect(yield* store.get('work')).toEqual(previous)
        expect((yield* accounts.list)[0]).toMatchObject({ revision: 'previous' })
        expect(
          (yield* Effect.flip(accounts.completeSignIn(attempt.id, Redacted.make('accepted'))))
            .reason,
        ).toBeInstanceOf(AccountError.InvalidSignIn)
      }),
    ))

  test('closing the stream interrupts the adapter and releases its reservation without saving', () =>
    run(
      Effect.gen(function* () {
        const closed = yield* Deferred.make<void>()
        const staged = yield* Deferred.make<void>()
        const accounts = yield* open((account, flow) =>
          login(account, flow).pipe(
            Effect.andThen(Deferred.succeed(staged, undefined)),
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(closed, undefined)),
          ),
        )
        const attempt = yield* observe(accounts.signIn({ adapter: 'browser', name: 'work' }))
        yield* accounts.completeSignIn(attempt.id, Redacted.make('accepted'))
        yield* Deferred.await(staged)
        yield* Fiber.interrupt(attempt.fiber)
        yield* Deferred.await(closed)
        const store = yield* Credentials.Credentials
        expect(yield* store.get('work')).toBeUndefined()
        expect(yield* accounts.list).toEqual([])
        const restarted = yield* observe(accounts.signIn({ adapter: 'browser', name: 'work' }))
        expect(restarted.id).not.toBe(attempt.id)
        yield* accounts.cancelSignIn(restarted.id)
        yield* Effect.flip(Fiber.join(restarted.fiber))
      }),
    ))

  test('provider failures and rejected callbacks preserve credentials and sanitize secrets', () =>
    run(
      Effect.gen(function* () {
        const store = yield* Credentials.Credentials
        const previous = oauth('previous')
        yield* store.set('work', previous)
        const accounts = yield* open(undefined, true)
        const rejected = yield* observe(accounts.signIn({ name: 'work' }))
        yield* accounts.completeSignIn(rejected.id, Redacted.make('callback-secret'))
        const error = yield* Effect.flip(Fiber.join(rejected.fiber))
        expect(error.reason).toBeInstanceOf(AccountError.SignInFailed)
        expect(JSON.stringify(error)).not.toContain('callback-secret')
        expect(yield* store.get('work')).toEqual(previous)
        const failed = yield* open((account, flow) =>
          login(account, flow).pipe(Effect.andThen(Effect.fail({ message: 'access-secret' }))),
        )
        const attempt = yield* observe(failed.signIn({ adapter: 'browser', name: 'new' }))
        yield* failed.completeSignIn(attempt.id, Redacted.make('accepted'))
        expect((yield* Effect.flip(Fiber.join(attempt.fiber))).reason).toBeInstanceOf(
          AccountError.SignInFailed,
        )
        expect(yield* store.get('new')).toBeUndefined()
        expect(yield* failed.list).toEqual([])
      }),
    ))

  test('a login cannot read, write, remove or update another account credential', () =>
    run(
      Effect.gen(function* () {
        const store = yield* Credentials.Credentials
        const other = oauth('other')
        yield* store.set('other', other)
        const accounts = yield* open((account, flow) =>
          Effect.gen(function* () {
            const staging = yield* Credentials.Credentials
            expect(yield* staging.list).toEqual([])
            expect(yield* Effect.flip(staging.get('other')).pipe(Effect.orDie)).toMatchObject({
              _tag: 'CredentialsError',
            })
            expect(
              yield* Effect.flip(staging.set('other', oauth('attacker'))).pipe(Effect.orDie),
            ).toMatchObject({
              _tag: 'CredentialsError',
            })
            expect(yield* Effect.flip(staging.remove('other')).pipe(Effect.orDie)).toMatchObject({
              _tag: 'CredentialsError',
            })
            expect(
              yield* Effect.flip(staging.update('other', other, oauth('attacker'))).pipe(
                Effect.orDie,
              ),
            ).toMatchObject({ _tag: 'CredentialsError' })
            yield* login(account, flow)
          }),
        )
        const attempt = yield* observe(accounts.signIn({ adapter: 'browser', name: 'work' }))
        yield* accounts.completeSignIn(attempt.id, Redacted.make('accepted'))
        yield* Fiber.join(attempt.fiber)
        expect(yield* store.get('other')).toEqual(other)
        expect((yield* store.get('work'))?.type).toBe('oauth')
      }),
    ))

  test('removing a pending or saved account cancels and fences a late completion', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open()
        const pending = yield* observe(accounts.signIn({ adapter: 'browser', name: 'work' }))
        yield* accounts.remove('work')
        expect((yield* Effect.flip(Fiber.join(pending.fiber))).reason).toBeInstanceOf(
          AccountError.SignInCancelled,
        )
        expect(
          (yield* Effect.flip(accounts.completeSignIn(pending.id, Redacted.make('accepted'))))
            .reason,
        ).toBeInstanceOf(AccountError.InvalidSignIn)
        const saved = yield* observe(accounts.signIn({ adapter: 'browser', name: 'work' }))
        yield* accounts.completeSignIn(saved.id, Redacted.make('accepted'))
        yield* Fiber.join(saved.fiber)
        const renewing = yield* observe(accounts.signIn({ name: 'work' }))
        yield* accounts.remove('work')
        expect((yield* Effect.flip(Fiber.join(renewing.fiber))).reason).toBeInstanceOf(
          AccountError.SignInCancelled,
        )
        expect(yield* accounts.list).toEqual([])
        expect(yield* (yield* Credentials.Credentials).get('work')).toBeUndefined()
      }),
    ))

  test('an external credential replacement wins over a stale sign-in', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open(undefined, true)
        const store = yield* Credentials.Credentials
        yield* store.set('work', oauth('previous'))
        const attempt = yield* observe(accounts.signIn({ name: 'work' }))
        const replaced = oauth('replacement')
        yield* store.set('work', replaced)
        yield* accounts.completeSignIn(attempt.id, Redacted.make('accepted'))
        expect((yield* Effect.flip(Fiber.join(attempt.fiber))).reason).toBeInstanceOf(
          AccountError.SignInChanged,
        )
        expect(yield* store.get('work')).toEqual(replaced)
      }),
    ))

  test('relabelled targets cannot be silently overwritten by an earlier attempt', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open()
        const initial = yield* observe(accounts.signIn({ adapter: 'browser', name: 'work' }))
        yield* accounts.completeSignIn(initial.id, Redacted.make('accepted'))
        yield* Fiber.join(initial.fiber)
        const renewing = yield* observe(accounts.signIn({ name: 'work' }))
        yield* accounts.relabel('work', 'Renamed')
        yield* accounts.completeSignIn(renewing.id, Redacted.make('accepted'))
        expect((yield* Effect.flip(Fiber.join(renewing.fiber))).reason).toBeInstanceOf(
          AccountError.SignInChanged,
        )
        expect((yield* accounts.list)[0]?.label).toBe('Renamed')
      }),
    ))

  test('encoded RPC transports completion inward, streams no secrets, and cancels explicitly', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open()
        const transport = yield* HarnessTransport.makeMemoryEncoded
        yield* Accounts.layerServer.pipe(
          Layer.launch,
          Effect.provideService(Accounts.Accounts, accounts),
          Effect.provideContext(transport),
          Effect.forkScoped,
        )
        const client = yield* RpcClient.make(AccountsRpcs.AccountsRpcs).pipe(
          Effect.provideContext(transport),
        )
        const attempt = yield* observe(client.SignIn({ adapter: 'browser', name: 'work' }))
        yield* client.CompleteSignIn({ id: attempt.id, completion: Redacted.make('accepted') })
        const events = yield* Fiber.join(attempt.fiber)
        expect(events.at(-1)).toMatchObject({
          type: 'done',
          account: { identity: { subject: 'verified-user' }, revision: 'fresh' },
        })
        expect(JSON.stringify(events)).not.toContain('refresh-secret')
        expect((yield* client.List())[0]).not.toHaveProperty('extra')
        const cancelled = yield* observe(client.SignIn({ adapter: 'browser', name: 'cancelled' }))
        yield* client.CancelSignIn({ id: cancelled.id })
        expect(yield* Effect.flip(Fiber.join(cancelled.fiber))).toMatchObject({
          _tag: 'AccountError',
          reason: { _tag: 'SignInCancelled' },
        })
        expect((yield* client.List()).map(({ name }) => name)).toEqual(['work'])
      }),
    ))

  test('cancellation fences writes even when an adapter delays interruption', () =>
    run(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const accounts = yield* open(
          (account, flow) =>
            Effect.gen(function* () {
              yield* flow.onAuthorize('https://identity.test/authorize')
              yield* flow.completion
              const store = yield* Credentials.Credentials
              yield* Effect.uninterruptible(
                Effect.gen(function* () {
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                  yield* store.set(account.name, oauth('late'))
                }),
              )
            }),
          true,
        )
        const store = yield* Credentials.Credentials
        const before = oauth('previous')
        yield* store.set('work', before)
        const attempt = yield* observe(accounts.signIn({ name: 'work' }))
        yield* accounts.completeSignIn(attempt.id, Redacted.make('accepted'))
        yield* Deferred.await(entered)
        yield* accounts.cancelSignIn(attempt.id)
        yield* Deferred.succeed(release, undefined)
        expect((yield* Effect.flip(Fiber.join(attempt.fiber))).reason).toBeInstanceOf(
          AccountError.SignInCancelled,
        )
        expect(yield* store.get('work')).toEqual(before)
      }),
    ))

  test('failed account persistence rolls back new credentials or restores an existing credential', () =>
    run(
      Effect.gen(function* () {
        const metadata = yield* KeyValueStore.KeyValueStore
        const credentials = yield* Credentials.Credentials
        const broken = {
          ...metadata,
          set: () =>
            Effect.fail(
              new KeyValueStore.KeyValueStoreError({
                method: 'set',
                message: 'storage-secret',
              }),
            ),
        }
        for (const existing of [false, true]) {
          const before = oauth('previous')
          if (existing) yield* credentials.set('work', before)
          const accounts = yield* open().pipe(
            Effect.provideService(KeyValueStore.KeyValueStore, broken),
          )
          const attempt = yield* observe(accounts.signIn({ adapter: 'browser', name: 'work' }))
          yield* accounts.completeSignIn(attempt.id, Redacted.make('accepted'))
          const error = yield* Effect.flip(Fiber.join(attempt.fiber))
          expect(error.reason).toBeInstanceOf(AccountError.StoreFailed)
          expect(JSON.stringify(error)).not.toContain('storage-secret')
          expect(yield* credentials.get('work')).toEqual(existing ? before : undefined)
          expect(yield* accounts.list).toEqual([])
        }
      }),
    ))
})
