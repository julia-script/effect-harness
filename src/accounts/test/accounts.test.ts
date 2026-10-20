/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The registry over two fake adapters: one that takes an API key and checks it by listing models,
 * and one that signs in with a device code.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Layer, Redacted, Schema, Stream } from 'effect'
import { FetchHttpClient } from 'effect/http'
import { KeyValueStore } from 'effect/persistence'
import { Credentials } from '../../credentials/index.ts'
import { Model, ModelError } from '../../model/index.ts'
import { Accounts, Adapter } from '../index.ts'
import { AccountError } from '../index.ts'

const Keyed = Adapter.make('keyed', {
  label: 'Keyed',
  auth: Adapter.apiKey,
  settings: Schema.Struct({ region: Schema.Literals(['eu', 'us']) }),
})
const Device = Adapter.make('device', { label: 'Device', auth: Adapter.deviceCode })
const adapters = Adapter.group(Keyed, Device)

/** A provider whose listing accepts the key `good`, or any OAuth tokens. */
const fake = (name: string): Model.Listable<Adapter.Requires> => ({
  name,
  models: [Model.define(name, 'fake', { id: 'small' })],
  stream: () => Stream.empty,
  listModels: Effect.gen(function* () {
    const credential = yield* Effect.orDie(
      Effect.flatMap(Credentials.Credentials, (store) => store.get(name)),
    )
    const accepted =
      credential?.type === 'oauth' ||
      (credential?.type === 'api_key' && Redacted.value(credential.key) === 'good')
    if (!accepted) {
      return yield* new ModelError.ModelError({
        message: 'Unauthorized',
        reason: new ModelError.Rejected(),
      })
    }
    return [{ provider: name, id: 'small', name: 'Small', served: true, serviceTiers: [] }]
  }),
})

const Implementations = adapters.toLayer({
  keyed: { provider: (account) => fake(account.name) },
  device: {
    provider: (account) => fake(account.name),
    login: (account, flow) =>
      Effect.gen(function* () {
        yield* flow.onCode({ url: 'https://example.test/device', code: 'ABCD-1234' })
        const store = yield* Credentials.Credentials
        yield* store.set(account.name, {
          type: 'oauth',
          access: Redacted.make('access'),
          refresh: Redacted.make('refresh'),
          expires: Date.now() + 3_600_000,
        })
      }),
  },
})

const Services = Layer.mergeAll(
  KeyValueStore.layerMemory,
  Credentials.layerMemory(),
  FetchHttpClient.layer,
  Implementations,
)

type Env = Layer.Success<typeof Services>

const run = <A, E>(body: Effect.Effect<A, E, Env>) =>
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test is an entry point
  Effect.runPromise(Effect.scoped(body).pipe(Effect.provide(Services)))

const open = (options: Accounts.Options<typeof adapters.adapters> = {}) =>
  Accounts.make(adapters, options)

const work = { adapter: 'keyed', name: 'work', label: 'Work', settings: { region: 'eu' } }

describe('accounts', () => {
  test('adds an API key account only once the key lists models', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open()
        const store = yield* Credentials.Credentials
        const bad = yield* Effect.flip(accounts.add({ ...work, apiKey: Redacted.make('bad') }))
        expect(bad.reason).toBeInstanceOf(AccountError.VerifyFailed)
        expect(yield* accounts.list).toEqual([])
        expect(yield* store.get('work')).toBeUndefined()

        const added = yield* accounts.add({ ...work, apiKey: Redacted.make('good') })
        expect(added).toMatchObject({ name: 'work', status: 'ready', removable: true })
        expect(added.settings).toEqual({ region: 'eu' })

        const again = yield* Effect.flip(accounts.add({ ...work, apiKey: Redacted.make('good') }))
        expect(again.reason).toBeInstanceOf(AccountError.NameTaken)
        const other = { ...work, name: 'other', apiKey: Redacted.make('good') }
        expect(
          (yield* Effect.flip(accounts.add({ ...other, settings: { region: 'mars' } }))).reason,
        ).toBeInstanceOf(AccountError.InvalidSettings)
        expect(
          (yield* Effect.flip(accounts.add({ ...other, adapter: 'nope' }))).reason,
        ).toBeInstanceOf(AccountError.UnknownAdapter)
        expect(
          (yield* Effect.flip(accounts.add({ ...other, adapter: 'device' }))).reason,
        ).toBeInstanceOf(AccountError.Unsupported)
      }),
    ))

  test('signs in with a device code, starting before streaming the code', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open()
        const events = yield* Stream.runCollect(
          accounts.signIn({ adapter: 'device', name: 'laptop', label: 'Laptop' }),
        )
        expect(events.map(({ type }) => type)).toEqual(['started', 'code', 'done'])
        expect(events[0]).toMatchObject({ name: 'laptop' })
        expect(events[1]).toMatchObject({ code: 'ABCD-1234' })
        expect(events[2]).toMatchObject({ account: { name: 'laptop', status: 'ready' } })
        expect(events[2]).not.toHaveProperty('account.identity')
        expect(events[2]).not.toHaveProperty('account.revision')

        const renewed = yield* Stream.runCollect(accounts.signIn({ name: 'laptop' }))
        expect(renewed.at(-1)).toMatchObject({ account: { label: 'Laptop', status: 'ready' } })
        const missing = yield* Effect.flip(Stream.runDrain(accounts.signIn({ name: 'nobody' })))
        expect(missing.reason).toBeInstanceOf(AccountError.NotFound)
      }),
    ))

  test('an account that loses its credential needs auth until a new key is set', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open()
        const store = yield* Credentials.Credentials
        yield* accounts.add({ ...work, apiKey: Redacted.make('good') })
        yield* store.remove('work')
        expect((yield* accounts.list)[0]).toMatchObject({ status: 'needsAuth' })
        expect(yield* accounts.providers).toEqual([])

        const bad = yield* Effect.flip(accounts.setKey('work', Redacted.make('bad')))
        expect(bad.reason).toBeInstanceOf(AccountError.VerifyFailed)
        expect(yield* store.get('work')).toBeUndefined()

        expect(yield* accounts.setKey('work', Redacted.make('good'))).toMatchObject({
          status: 'ready',
        })
      }),
    ))

  test('a refused listing marks the account, and serves its catalog meanwhile', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open()
        const store = yield* Credentials.Credentials
        yield* accounts.add({ ...work, apiKey: Redacted.make('good') })
        yield* store.set('work', { type: 'api_key', key: Redacted.make('revoked') })
        const [listed] = yield* accounts.models
        expect(listed).toMatchObject({
          account: { status: 'needsAuth', error: 'Unauthorized' },
          error: 'Unauthorized',
          models: [{ id: 'small', served: true }],
        })
        expect(yield* accounts.providers).toEqual([])

        yield* accounts.setKey('work', Redacted.make('good'))
        expect((yield* accounts.list)[0]?.status).toBe('ready')
        expect((yield* accounts.providers).map(({ name }) => name)).toEqual(['work'])
      }),
    ))

  test('accounts survive a restart; an unknown adapter is kept as an error', () =>
    run(
      Effect.gen(function* () {
        const first = yield* open()
        yield* first.add({ ...work, apiKey: Redacted.make('good') })
        yield* Stream.runDrain(first.signIn({ adapter: 'device', name: 'laptop', label: 'Laptop' }))
        yield* first.relabel('work', 'Work key')

        const second = yield* open()
        expect(
          (yield* second.list).map(({ name, label, status }) => [name, label, status]),
        ).toEqual([
          ['work', 'Work key', 'ready'],
          ['laptop', 'Laptop', 'ready'],
        ])

        const narrower = yield* Accounts.make(Adapter.group(Keyed))
        const [, laptop] = yield* narrower.list
        expect(laptop).toMatchObject({ name: 'laptop', status: 'error' })
        expect((yield* narrower.providers).map(({ name }) => name)).toEqual(['work'])
      }),
    ))

  test('accounts declared in code are listed, never stored, and cannot be removed', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open({
          accounts: [
            { adapter: 'keyed', name: 'fixed', label: 'Fixed', settings: { region: 'us' } },
          ],
        })
        expect((yield* accounts.list)[0]).toMatchObject({
          name: 'fixed',
          status: 'needsAuth',
          removable: false,
        })
        expect((yield* Effect.flip(accounts.remove('fixed'))).reason).toBeInstanceOf(
          AccountError.Unsupported,
        )
        expect(yield* accounts.setKey('fixed', Redacted.make('good'))).toMatchObject({
          status: 'ready',
        })
        expect(yield* open().pipe(Effect.flatMap(({ list }) => list))).toEqual([])
      }),
    ))

  test('removing an account forgets its credential and its models', () =>
    run(
      Effect.gen(function* () {
        const accounts = yield* open()
        const store = yield* Credentials.Credentials
        yield* accounts.add({ ...work, apiKey: Redacted.make('good') })
        const model = yield* Model.makeDynamic(accounts.providers)
        expect((yield* model.models).map(({ provider }) => provider)).toEqual(['work'])

        yield* accounts.remove('work')
        expect(yield* model.models).toEqual([])
        expect(yield* store.get('work')).toBeUndefined()
        expect((yield* Effect.flip(accounts.remove('work'))).reason).toBeInstanceOf(
          AccountError.NotFound,
        )
      }),
    ))
})
