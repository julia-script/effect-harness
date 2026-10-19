/**
 * Accounts as data: what the registry stores, what it reports, and what a client sends to add one.
 * An account is one instance of an adapter. Its name is the provider name requests route by and the
 * key its credential is stored under. Settings cross storage and transports encoded, as JSON, and
 * decode with the adapter's Schema. Secrets only travel inward: nothing here carries one out.
 */
import { Schema } from 'effect'
import { Secret } from '../credentials/Credential.ts'
import { Listed } from '../model/Model.ts'
import { JsonObject } from '../storage/schema/Json.ts'

/** The name requests route by and the credential is stored under: `codex-work`, `openrouter`. */
export const Name = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]{0,63}$/u))
export type Name = typeof Name.Type

/** An account as the registry stores it. */
export const Saved = Schema.Struct({
  adapter: Schema.String,
  name: Name,
  label: Schema.String,
  /** Encoded with the adapter's settings Schema. */
  settings: JsonObject,
  /** The model IDs the account listed when its credential was last checked. */
  models: Schema.optionalKey(Schema.Array(Schema.String)),
})
export type Saved = typeof Saved.Type

/**
 * `ready`: its models are served. `needsAuth`: it has no credential, or the provider refused it;
 * sign in again or set a new key. `error`: the record cannot be used, see `error`.
 */
export const State = Schema.Literals(['ready', 'needsAuth', 'error'])
export type State = typeof State.Type

/** An account and whether it can serve now. */
export const Status = Schema.Struct({
  adapter: Schema.String,
  name: Name,
  label: Schema.String,
  settings: JsonObject,
  status: State,
  /** False for an account declared in code. */
  removable: Schema.Boolean,
  /** Why the account is not `ready`, when known. */
  error: Schema.optionalKey(Schema.String),
})
export type Status = typeof Status.Type

/** Add an account of an `apiKey` or `none` adapter. */
export const Add = Schema.Struct({
  adapter: Schema.String,
  name: Name,
  label: Schema.String,
  /** Encoded; checked with the adapter's settings Schema. Default `{}`. */
  settings: Schema.optionalKey(JsonObject),
  /** Required by an `apiKey` adapter. */
  apiKey: Schema.optionalKey(Secret),
})
export type Add = typeof Add.Type

/**
 * Sign in with a device code. Naming an existing account renews its tokens; a new name needs
 * `adapter`, and takes `label` and `settings` as `Add` does.
 */
export const SignIn = Schema.Struct({
  name: Name,
  adapter: Schema.optionalKey(Schema.String),
  label: Schema.optionalKey(Schema.String),
  settings: Schema.optionalKey(JsonObject),
})
export type SignIn = typeof SignIn.Type

/** What the person signing in opens, and the code they enter there. */
export const DeviceCode = Schema.Struct({ url: Schema.String, code: Schema.String })
export type DeviceCode = typeof DeviceCode.Type

/** What a sign-in streams: the code to enter, then the account once approved. */
export const SignInEvent = Schema.Union([
  Schema.Struct({ type: Schema.Literal('code'), ...DeviceCode.fields }),
  Schema.Struct({ type: Schema.Literal('done'), account: Status }),
])
export type SignInEvent = typeof SignInEvent.Type

/** One account's models now, or why they could not be listed and what it serves anyway. */
export const Models = Schema.Struct({
  account: Status,
  models: Schema.Array(Listed),
  /** Listing failed; `models` are the ones the account serves without a listing. */
  error: Schema.optionalKey(Schema.String),
})
export type Models = typeof Models.Type
