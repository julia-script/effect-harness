/**
 * The protocol of a remote credential store: what `Credentials.layerRpc` sends, and
 * `Credentials.layerServer` serves from the store in its context.
 */
import { Schema } from 'effect'
import { Rpc, RpcGroup } from 'effect/rpc'
import { Credential, Info } from './Credential.ts'
import { CredentialChanged, CredentialsError, ReadOnly } from './CredentialsError.ts'

const provider = { provider: Schema.String }
const writeError = Schema.Union([CredentialsError, ReadOnly])

export class CredentialsRpcs extends RpcGroup.make(
  Rpc.make('Get', {
    payload: provider,
    success: Schema.NullOr(Credential),
    error: CredentialsError,
  }),
  Rpc.make('List', { success: Schema.Array(Info), error: CredentialsError }),
  Rpc.make('Set', { payload: { ...provider, credential: Credential }, error: writeError }),
  Rpc.make('Remove', { payload: provider, error: writeError }),
  Rpc.make('Update', {
    payload: { ...provider, expected: Credential, next: Credential },
    error: Schema.Union([CredentialsError, ReadOnly, CredentialChanged]),
  }),
) {}
