import type * as DateTime from 'effect/DateTime'
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Redacted from 'effect/Redacted'
import type { AuthError } from './Credential.ts'

export interface Identity {
  readonly sub: string
  readonly iss: string
  readonly exp: DateTime.Utc
  readonly nonce?: string | undefined
  readonly email?: string | undefined
}
export interface VerifyOptions {
  readonly issuer: string
  readonly audience: string
  readonly jwksUrl: string
  readonly nonce?: string | undefined
  readonly algorithms?: ReadonlyArray<string> | undefined
}
export class Jwt extends Context.Service<
  Jwt,
  {
    readonly verify: (
      token: Redacted.Redacted<string>,
      options: VerifyOptions,
    ) => Effect.Effect<Identity, AuthError>
  }
>()('@effect-harness/auth/Jwt') {}
