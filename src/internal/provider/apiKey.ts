/**
 * The API key of a stock provider: the one it was given, or the provider's stored credential, read
 * fresh for each request.
 */
import { Effect } from 'effect'
import type { Redacted } from 'effect'
import { CredentialStore } from '../../credentials/CredentialStore.ts'
import { ModelError, Unavailable } from '../../model/ModelError.ts'

/** No usable key: another model may still answer. */
const unavailable = (message: string, cause?: unknown) =>
  new ModelError({
    message,
    reason: new Unavailable({}),
    ...(cause === undefined ? {} : { cause }),
  })

/** The stored API key of `provider`. */
export const stored = (
  provider: string,
): Effect.Effect<Redacted.Redacted, ModelError, CredentialStore> =>
  Effect.flatMap(CredentialStore, (store) => store.get(provider)).pipe(
    Effect.mapError((cause) => unavailable(cause.message, cause)),
    Effect.flatMap((credential) => {
      if (credential?._tag === 'ApiKey') return Effect.succeed(credential.key)
      return Effect.fail(
        unavailable(
          credential === undefined
            ? `No ${provider} credential is stored`
            : `The ${provider} provider takes an API key, not OAuth tokens`,
        ),
      )
    }),
  )
