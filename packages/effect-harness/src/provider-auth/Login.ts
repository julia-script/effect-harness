/** Host-owned login UI. Every callback is cancellable Effect work under the login deadline. */
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Scope from 'effect/Scope'
import type { AuthError } from './AuthError.js'

export interface Interaction {
  readonly authorize: (url: string) => Effect.Effect<void, AuthError>
  /** Return a redirect URL or code#state. Bare codes require allowBareCode on the login operation. */
  readonly complete: Effect.Effect<string, AuthError>
  readonly device?: (instructions: {
    readonly url: string
    readonly code: string
  }) => Effect.Effect<void, AuthError>
}

/** Optional loopback capability, acquired only during explicit browser login. */
export class CallbackServer extends Context.Service<
  CallbackServer,
  {
    readonly open: (options: {
      readonly port: number
      readonly path: string
      readonly state: string
      readonly fallbackPort: boolean
    }) => Effect.Effect<
      {
        readonly redirectUri: string
        readonly completion: Effect.Effect<string, AuthError>
      },
      AuthError,
      Scope.Scope
    >
  }
>()('effect-harness/provider-auth/CallbackServer') {}

export interface Options {
  readonly interaction: Interaction
  /** Defaults to ten minutes (device login is also bounded by provider expiry). */
  readonly timeoutMs?: number
  /** Bare codes cannot independently validate state; they remain bound to this live PKCE attempt. */
  readonly allowBareCode?: boolean
}
