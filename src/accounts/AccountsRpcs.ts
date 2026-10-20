/**
 * The protocol a client manages accounts over: what `Accounts.layerServer` serves. Secrets only
 * travel inward: `Add` and `SetKey` take a key, and nothing returns one, so a browser can use it.
 */
import { Schema } from 'effect'
import { Rpc, RpcGroup } from 'effect/rpc'
import { Secret } from '../credentials/Credential.ts'
import { Add, Models, Name, SignIn, SignInEvent, Status } from './Account.ts'
import { AccountError } from './AccountError.ts'

export class AccountsRpcs extends RpcGroup.make(
  Rpc.make('List', { success: Schema.Array(Status), error: AccountError }),
  /** The accounts now, then again at each change. */
  Rpc.make('Changes', { success: Schema.Array(Status), error: AccountError, stream: true }),
  /** Check the credential by listing models with it, then store it and add the account. */
  Rpc.make('Add', { payload: Add, success: Status, error: AccountError }),
  /** Replace an account's API key, after checking the new one. */
  Rpc.make('SetKey', {
    payload: { name: Name, apiKey: Secret },
    success: Status,
    error: AccountError,
  }),
  /** Sign in with a device code: the code to show, then the account. */
  Rpc.make('SignIn', { payload: SignIn, success: SignInEvent, error: AccountError, stream: true }),
  Rpc.make('Relabel', {
    payload: { name: Name, label: Schema.String },
    success: Status,
    error: AccountError,
  }),
  /** Forget the account and its credential. */
  Rpc.make('Remove', { payload: { name: Name }, error: AccountError }),
  /** Every account's models, listed live over its own credential. */
  Rpc.make('Models', { success: Schema.Array(Models), error: AccountError }),
) {}
