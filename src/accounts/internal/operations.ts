/**
 * What the registry does: add an account after checking its credential, replace a key, sign in
 * with a device code, relabel, remove, and list every account's models.
 */
import { Effect, Queue, Stream } from 'effect'
import type { Cause, Redacted } from 'effect'
import type { Listable, Listed } from '../../model/Model.ts'
import { Rejected } from '../../model/ModelError.ts'
import * as AccountError from '../AccountError.ts'
import type { Add, Models, Saved, SignIn, SignInEvent } from '../Account.ts'
import type { Account, Any, Base, Requires } from '../Adapter.ts'
import * as entries from './entry.ts'
import type { Entry } from './entry.ts'
import * as registry from './registry.ts'
import type { State } from './registry.ts'

const unsupported = (message: string) => entries.fail(new AccountError.Unsupported(), message)

/** The models `provider` lists, which checks its credential. */
const check = (state: State, provider: Listable<Requires>) =>
  provider.listModels.pipe(
    Effect.provideContext(state.context),
    Effect.mapError((error) =>
      entries.fail(
        new AccountError.VerifyFailed(),
        `The provider refused the account: ${error.message}`,
        error,
      ),
    ),
  )

/** The entry of `account`, with the models it lists now and its settings encoded for storage. */
const checked = Effect.fnUntraced(function* (
  state: State,
  found: { readonly adapter: Any; readonly implementation: Base<Any> },
  account: Account,
  fixed: boolean,
  apiKey?: Redacted.Redacted,
) {
  const listing = check(state, found.implementation.provider(account))
  const listed =
    apiKey === undefined
      ? yield* listing
      : yield* registry.withCredential(
          state,
          account.name,
          { type: 'api_key', key: apiKey },
          listing,
        )
  const models = listed.map(({ id }) => id)
  const saved: Saved = {
    adapter: account.adapter,
    name: account.name,
    label: account.label,
    settings: yield* entries.encode(found.adapter, account.settings),
    models,
  }
  return entries.ready(found.adapter, found.implementation, { ...account, models }, saved, fixed)
})

const taken = (all: ReadonlyArray<Entry>, name: string) =>
  all.some(({ saved }) => saved.name === name)
    ? Effect.fail(entries.fail(new AccountError.NameTaken(), `${name} is already an account`))
    : Effect.void

export const add = (state: State) =>
  Effect.fnUntraced(function* (input: Add) {
    const entry = yield* registry.change(state, (all) =>
      Effect.gen(function* () {
        yield* taken(all, input.name)
        const found = yield* entries.adapterOf(state.group, state.implementations, input.adapter)
        if (found.adapter.auth === 'deviceCode') {
          return yield* unsupported(`${found.adapter.label} accounts sign in with a device code`)
        }
        if (found.adapter.auth === 'apiKey' && input.apiKey === undefined) {
          return yield* unsupported(`${found.adapter.label} accounts need an API key`)
        }
        const settings = yield* entries.decode(found.adapter, input.settings ?? {})
        const account = { adapter: input.adapter, name: input.name, label: input.label, settings }
        const added = yield* checked(state, found, account, false, input.apiKey)
        return [added, [...all, added]] as const
      }),
    )
    yield* registry.accept(state, input.name)
    return yield* registry.statusOf(state, entry)
  })

export const setKey = (state: State) =>
  Effect.fnUntraced(function* (name: string, apiKey: Redacted.Redacted) {
    const entry = yield* registry.change(state, (all) =>
      Effect.gen(function* () {
        const found = all.find(({ saved }) => saved.name === name)
        if (found === undefined) return yield* entries.notFound(name)
        if (found.problem !== undefined) return yield* found.problem
        const { adapter, account } = found
        if (adapter?.auth !== 'apiKey' || account === undefined) {
          return yield* unsupported(`${name} does not take an API key`)
        }
        const resolved = yield* entries.adapterOf(state.group, state.implementations, adapter.id)
        const next = yield* checked(state, resolved, account, found.fixed, apiKey)
        return [next, registry.replace(all, next)] as const
      }),
    )
    yield* registry.accept(state, name)
    return yield* registry.statusOf(state, entry)
  })

/** The account a sign-in is for: the existing one named, or a new one from `input`. */
const signInTarget = Effect.fnUntraced(function* (state: State, input: SignIn) {
  const existing = yield* registry.lookup(state, input.name)
  if (existing?.problem !== undefined) return yield* existing.problem
  const id = existing?.saved.adapter ?? input.adapter
  if (id === undefined) return yield* entries.notFound(input.name)
  const found = yield* entries.adapterOf(state.group, state.implementations, id)
  if (found.implementation.login === undefined || found.adapter.auth !== 'deviceCode') {
    return yield* unsupported(`${found.adapter.label} accounts do not sign in with a device code`)
  }
  const account: Account = existing?.account ?? {
    adapter: id,
    name: input.name,
    label: input.label ?? found.adapter.label,
    settings: yield* entries.decode(found.adapter, input.settings ?? {}),
  }
  return { found, account, login: found.implementation.login, isNew: existing === undefined }
})

/** Add the account a sign-in made, unless its name was taken meanwhile. */
const added = (
  state: State,
  found: { readonly adapter: Any; readonly implementation: Base<Any> },
  account: Account,
) =>
  registry.change(state, (all) =>
    Effect.gen(function* () {
      yield* taken(all, account.name)
      const saved: Saved = {
        adapter: account.adapter,
        name: account.name,
        label: account.label,
        settings: yield* entries.encode(found.adapter, account.settings),
      }
      const entry = entries.ready(found.adapter, found.implementation, account, saved, false)
      return [entry, [...all, entry]] as const
    }),
  )

/** The entry named `name`, publishing a change: its status follows its new credential. */
const touched = (state: State, name: string) =>
  registry.change(state, (all) =>
    Effect.map(registry.existingIn(all, name), (entry) => [entry, all] as const),
  )

const signInto = Effect.fnUntraced(function* (
  state: State,
  input: SignIn,
  queue: Queue.Queue<SignInEvent, AccountError.AccountError | Cause.Done>,
) {
  const { found, account, login, isNew } = yield* signInTarget(state, input)
  yield* login(account, (code) =>
    Effect.asVoid(Queue.offer(queue, { type: 'code', ...code })),
  ).pipe(
    Effect.provideContext(state.context),
    Effect.mapError((error) =>
      entries.fail(new AccountError.SignInFailed(), `Signing in failed: ${error.message}`, error),
    ),
  )
  const entry = isNew ? yield* added(state, found, account) : yield* touched(state, account.name)
  yield* registry.accept(state, account.name)
  yield* Queue.offer(queue, { type: 'done', account: yield* registry.statusOf(state, entry) })
})

export const signIn =
  (state: State) =>
  (input: SignIn): Stream.Stream<SignInEvent, AccountError.AccountError> =>
    Stream.callback<SignInEvent, AccountError.AccountError>((queue) =>
      signInto(state, input, queue).pipe(
        Effect.andThen(Queue.end(queue)),
        Effect.catch((error) => Queue.fail(queue, error)),
      ),
    )

/** The entry named `name`, if it may be changed. */
const removable = (all: ReadonlyArray<Entry>, name: string) => {
  const found = all.find(({ saved }) => saved.name === name)
  if (found === undefined) return Effect.fail(entries.notFound(name))
  return found.fixed
    ? Effect.fail(unsupported(`${name} is declared in code`))
    : Effect.succeed(found)
}

export const relabel = (state: State) => (name: string, label: string) =>
  registry
    .change(state, (all) =>
      Effect.map(removable(all, name), (found) => {
        const next: Entry = {
          ...found,
          saved: { ...found.saved, label },
          ...(found.account === undefined ? {} : { account: { ...found.account, label } }),
        }
        return [next, registry.replace(all, next)] as const
      }),
    )
    .pipe(Effect.flatMap((entry) => registry.statusOf(state, entry)))

export const remove = (state: State) => (name: string) =>
  registry
    .change(state, (all) =>
      Effect.map(
        removable(all, name),
        () => [name, all.filter(({ saved }) => saved.name !== name)] as const,
      ),
    )
    .pipe(
      Effect.andThen(registry.accept(state, name)),
      Effect.andThen(
        state.credentials
          .remove(name)
          .pipe(Effect.mapError(registry.storeFailed('The credential could not be removed'))),
      ),
    )

/** The models `provider` serves, as a listing shows them. */
const served = (provider: Listable<Requires>): ReadonlyArray<Listed> =>
  provider.models.map((info) => ({
    provider: info.provider,
    id: info.id,
    name: info.name,
    served: true,
    serviceTiers: [],
    contextWindow: info.contextWindow,
  }))

/** What listing `entry`'s models came to: the models, and why listing failed if it did. */
interface Listing {
  readonly models: ReadonlyArray<Listed>
  readonly error?: string
}

/** `entry`'s models now, marking its credential refused when the provider refuses the listing. */
const modelsOf = (state: State, entry: Entry): Effect.Effect<Listing> => {
  const { provider, problem } = entry
  if (provider === undefined) {
    return Effect.succeed(
      problem === undefined ? { models: [] } : { models: [], error: problem.message },
    )
  }
  return provider.listModels.pipe(
    Effect.provideContext(state.context),
    Effect.tap(() => registry.accept(state, entry.saved.name)),
    Effect.map((models): Listing => ({ models })),
    Effect.catch((error) =>
      Effect.as(
        error.reason instanceof Rejected
          ? registry.refuse(state, entry.saved.name, error.message)
          : Effect.void,
        { models: served(provider), error: error.message },
      ),
    ),
  )
}

export const models = (state: State) =>
  Effect.gen(function* () {
    const all = yield* registry.current(state)
    const listed = yield* Effect.forEach(all, (entry) => modelsOf(state, entry), {
      concurrency: 'unbounded',
    })
    const statuses = yield* registry.statuses(state, all)
    return statuses.flatMap((account, index): ReadonlyArray<Models> => {
      const found = listed[index]
      return found === undefined ? [] : [{ account, ...found }]
    })
  })
