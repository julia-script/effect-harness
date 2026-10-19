/**
 * One account in the registry: its stored record, and what it decodes to with the host's adapters.
 * A record whose adapter or settings no longer decode is kept, with the problem, and serves nothing.
 */
import { Context, Effect, Option, Schema } from 'effect'
import type { Listable } from '../../model/Model.ts'
import { JsonObject } from '../../storage/schema/Json.ts'
import * as AccountError from '../AccountError.ts'
import type { Saved, Status } from '../Account.ts'
import type { Account, Any, Base, Requires } from '../Adapter.ts'
import { implementation } from '../Adapter.ts'

/** Where the host's adapters are found. */
export interface Lookup {
  readonly get: (id: string) => Any | undefined
}

export interface Entry {
  readonly saved: Saved
  /** Declared in code: never stored, renamed or removed. */
  readonly fixed: boolean
  readonly adapter?: Any
  readonly account?: Account
  readonly provider?: Listable<Requires>
  /** Why the record cannot be used. */
  readonly problem?: AccountError.AccountError
}

export const fail = (reason: AccountError.Reason, message: string, cause?: unknown) =>
  new AccountError.AccountError({
    message,
    reason,
    ...(cause === undefined ? {} : { cause }),
  })

export const notFound = (name: string) =>
  fail(new AccountError.NotFound(), `No account is named ${name}`)

/** The adapter `id` of `group`, with its implementation from `implementations`. */
export const adapterOf = Effect.fnUntraced(function* <Services>(
  group: Lookup,
  implementations: Context.Context<Services>,
  id: string,
) {
  const adapter = group.get(id)
  const implemented =
    adapter === undefined ? Option.none() : Context.getOption(implementations, implementation(id))
  if (adapter === undefined || Option.isNone(implemented)) {
    return yield* fail(
      new AccountError.UnknownAdapter({ adapter: id }),
      `This host does not support ${id} accounts`,
    )
  }
  return { adapter, implementation: implemented.value }
})

const invalid = (adapter: Any, why: string) =>
  fail(new AccountError.InvalidSettings(), `Invalid ${adapter.label} settings: ${why}`)

/** `settings`, decoded with `adapter`'s Schema. */
export const decode = (adapter: Any, settings: unknown) =>
  Schema.decodeUnknownEffect(adapter.settings)(settings).pipe(
    Effect.mapError((issue) => invalid(adapter, issue.message)),
  )

const isJsonObject = Schema.is(JsonObject)

/** `settings`, encoded with `adapter`'s Schema for storage. */
export const encode = (adapter: Any, settings: unknown) =>
  Schema.encodeUnknownEffect(adapter.settings)(settings).pipe(
    Effect.mapError((issue) => invalid(adapter, issue.message)),
    Effect.filterOrFail(isJsonObject, () => invalid(adapter, 'they are not a JSON object')),
  )

/** The entry of a decoded account. */
export const ready = (
  adapter: Any,
  base: Base<Any>,
  account: Account,
  saved: Saved,
  fixed: boolean,
): Entry => ({ saved, fixed, adapter, account, provider: base.provider(account) })

/** The entry of `saved`, decoding it with the host's adapters. */
export const load = <Services>(
  group: Lookup,
  implementations: Context.Context<Services>,
  saved: Saved,
  fixed: boolean,
): Effect.Effect<Entry> =>
  adapterOf(group, implementations, saved.adapter).pipe(
    Effect.flatMap(({ adapter, implementation: base }) =>
      Effect.map(decode(adapter, saved.settings), (settings) =>
        ready(adapter, base, { ...toAccount(saved), settings }, saved, fixed),
      ),
    ),
    Effect.catch((problem) => Effect.succeed<Entry>({ saved, fixed, problem })),
  )

const toAccount = (saved: Saved): Account => ({
  adapter: saved.adapter,
  name: saved.name,
  label: saved.label,
  settings: saved.settings,
  ...(saved.models === undefined ? {} : { models: saved.models }),
})

/** How `entry` stands, given the names with a stored credential and those a provider refused. */
export const status = (
  entry: Entry,
  stored: ReadonlySet<string>,
  refused: ReadonlyMap<string, string>,
): Status => {
  const { saved } = entry
  const base = {
    adapter: saved.adapter,
    name: saved.name,
    label: saved.label,
    settings: saved.settings,
    removable: !entry.fixed,
  }
  if (entry.problem !== undefined) {
    return { ...base, status: 'error', error: entry.problem.message }
  }
  const why = refused.get(saved.name)
  if (why !== undefined) return { ...base, status: 'needsAuth', error: why }
  if (entry.adapter?.auth !== 'none' && !stored.has(saved.name)) {
    return { ...base, status: 'needsAuth', error: 'No credential is stored' }
  }
  return { ...base, status: 'ready' }
}
