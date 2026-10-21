/**
 * The registry's state: the entries, stored under one key of a `KeyValueStore` except those declared
 * in code; the names a provider refused; and changes to the entries one at a time.
 */
import { Effect, Option, Schema, Semaphore, SubscriptionRef } from 'effect'
import type { Context } from 'effect'
import { KeyValueStore } from 'effect/persistence'
import { CredentialStore } from '../../credentials/CredentialStore.ts'
import type { Service as Store } from '../../credentials/CredentialStore.ts'
import type { Credential } from '../../credentials/Credential.ts'
import * as AccountError from '../AccountError.ts'
import { Saved } from '../Account.ts'
import type { AccountOf, Any, Requires } from '../Adapter.ts'
import * as entries from './entry.ts'
import type { Entry, Lookup } from './entry.ts'
import type { Attempt } from './signIn.ts'

const Stored = Schema.Array(Saved)

export interface Options {
  readonly accounts?: ReadonlyArray<AccountOf<Record<string, Any>>>
  readonly key?: string
}

export interface State {
  readonly group: Lookup
  readonly implementations: Context.Context<never>
  readonly store: KeyValueStore.SchemaStore<typeof Stored>
  readonly key: string
  readonly credentials: Store
  /** What providers and sign-ins answer with. */
  readonly context: Context.Context<Requires>
  readonly live: SubscriptionRef.SubscriptionRef<ReadonlyArray<Entry>>
  /** Names whose credential a provider refused, and why; cleared when it is replaced. */
  readonly refused: SubscriptionRef.SubscriptionRef<ReadonlyMap<string, string>>
  readonly line: Semaphore.Semaphore
  /** Reservations owned by active sign-in streams, keyed by account name. */
  readonly attempts: Map<string, Attempt>
}

export const storeFailed = (what: string) => (cause: { readonly message: string }) =>
  entries.fail(new AccountError.StoreFailed(), `${what}: ${cause.message}`, cause)

/** The entry of an account declared in code. */
const declared = Effect.fnUntraced(function* (
  state: Omit<State, 'live'>,
  account: AccountOf<Record<string, Any>>,
) {
  const { adapter } = yield* entries.adapterOf(state.group, state.implementations, account.adapter)
  const settings = yield* entries.encode(adapter, account.settings)
  return yield* entries.load(
    state.group,
    state.implementations,
    { adapter: account.adapter, name: account.name, label: account.label, settings },
    true,
  )
})

/** Open the registry over the stored accounts and those declared in `options`. */
export const open = Effect.fnUntraced(function* (group: Lookup, options: Options) {
  const key = options.key ?? 'accounts'
  const store = KeyValueStore.toSchemaStore(yield* KeyValueStore.KeyValueStore, Stored)
  const saved = yield* store
    .get(key)
    .pipe(
      Effect.map(Option.getOrElse((): ReadonlyArray<Saved> => [])),
      Effect.mapError(storeFailed('The stored accounts could not be read')),
    )
  const base = {
    group,
    implementations: yield* Effect.context(),
    store,
    key,
    credentials: yield* CredentialStore,
    context: yield* Effect.context<Requires>(),
    refused: yield* SubscriptionRef.make<ReadonlyMap<string, string>>(new Map()),
    line: yield* Semaphore.make(1),
    attempts: new Map<string, Attempt>(),
  }
  const fixed = yield* Effect.forEach(options.accounts ?? [], (account) => declared(base, account))
  const names = new Set(fixed.map(({ saved: { name } }) => name))
  const loaded = yield* Effect.forEach(
    saved.filter(({ name }) => !names.has(name)),
    (record) => entries.load(group, base.implementations, record, false),
  )
  const state: State = {
    ...base,
    live: yield* SubscriptionRef.make<ReadonlyArray<Entry>>([...fixed, ...loaded]),
  }
  return state
})

export const current = (state: State) => SubscriptionRef.get(state.live)

export const lookup = (state: State, name: string) =>
  Effect.map(current(state), (all) => all.find(({ saved }) => saved.name === name))

/** The entry of `all` named `name`, or `NotFound`. */
export const existingIn = (all: ReadonlyArray<Entry>, name: string) => {
  const found = all.find(({ saved }) => saved.name === name)
  return found === undefined ? Effect.fail(entries.notFound(name)) : Effect.succeed(found)
}

/** Change the entries under the lock, storing them before they change. */
export const change = <A, E>(
  state: State,
  f: (all: ReadonlyArray<Entry>) => Effect.Effect<readonly [A, ReadonlyArray<Entry>], E>,
) =>
  Semaphore.withPermit(
    state.line,
    Effect.gen(function* () {
      const [result, next] = yield* f(yield* current(state))
      yield* state.store
        .set(
          state.key,
          next.filter(({ fixed }) => !fixed).map(({ saved }) => saved),
        )
        .pipe(Effect.mapError(storeFailed('The accounts could not be stored')))
      yield* SubscriptionRef.set(state.live, next)
      return result
    }),
  )

/** Replace the entry named after `entry`, keeping its place. */
export const replace = (all: ReadonlyArray<Entry>, entry: Entry) =>
  all.map((found) => (found.saved.name === entry.saved.name ? entry : found))

/** Mark `name`'s credential as refused, for `why`; a change only when it was not already. */
export const refuse = (state: State, name: string, why: string) =>
  SubscriptionRef.updateSome(state.refused, (all) =>
    all.get(name) === why ? Option.none() : Option.some(new Map([...all, [name, why]])),
  )

/** Clear the mark on `name`'s credential: it was replaced, or accepted. */
export const accept = (state: State, name: string) =>
  SubscriptionRef.updateSome(state.refused, (all) => {
    if (!all.has(name)) return Option.none()
    const next = new Map(all)
    next.delete(name)
    return Option.some(next)
  })

/** The names with a stored credential. */
export const stored = (state: State) =>
  state.credentials.list.pipe(
    Effect.map((all) => new Set(all.map(({ provider }) => provider))),
    Effect.mapError(storeFailed('The credentials could not be listed')),
  )

/** How `all` stand now. */
export const statuses = Effect.fnUntraced(function* (state: State, all: ReadonlyArray<Entry>) {
  const names = yield* stored(state)
  const refused = yield* SubscriptionRef.get(state.refused)
  return yield* Effect.forEach(all, (entry) =>
    Effect.map(
      state.credentials
        .get(entry.saved.name)
        .pipe(Effect.mapError(storeFailed('The credential could not be read'))),
      (credential) => entries.status(entry, names, refused, credential),
    ),
  )
})

export const statusOf = (state: State, entry: Entry) =>
  Effect.map(
    statuses(state, [entry]),
    ([found]) => found ?? entries.status(entry, new Set(), new Map()),
  )

/** The providers of the accounts that are `ready`; all of them when credentials can't be listed. */
export const providers = Effect.fnUntraced(function* (state: State) {
  const all = yield* current(state)
  const found = yield* Effect.option(statuses(state, all))
  return all.flatMap((entry, index) => {
    const ready = Option.match(found, {
      onNone: () => true,
      onSome: (listed) => listed[index]?.status === 'ready',
    })
    return ready && entry.provider !== undefined ? [entry.provider] : []
  })
})

/**
 * Store `credential` under `name` and run `check`; when it fails, put back what was stored before.
 */
export const withCredential = Effect.fnUntraced(function* <A>(
  state: State,
  name: string,
  credential: Credential,
  check: Effect.Effect<A, AccountError.AccountError>,
) {
  const failed = storeFailed('The credential could not be stored')
  const before = yield* state.credentials.get(name).pipe(Effect.mapError(failed))
  yield* state.credentials.set(name, credential).pipe(Effect.mapError(failed))
  const restore =
    before === undefined ? state.credentials.remove(name) : state.credentials.set(name, before)
  return yield* check.pipe(Effect.onError(() => Effect.ignore(restore)))
})
