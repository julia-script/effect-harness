/**
 * The `Config` store: API keys read from the `ConfigProvider` in context, such as the
 * environment or a `.env` file, at every `get`. It cannot be written.
 */
import { Config, Effect, Option } from 'effect'
import type { Credential, Info } from '../Credential.ts'
import type { Names, Service } from '../Credentials.ts'
import { CredentialsError, ReadOnly } from '../CredentialsError.ts'

const readOnly = (provider: string) =>
  Effect.fail(
    new ReadOnly({
      message: `The ${provider} credential comes from config and cannot change`,
      provider,
    }),
  )

/** The first of `names` that is set, as an API key. */
const read = (names: ReadonlyArray<string>) =>
  Effect.map(
    Effect.forEach(names, (name) => Config.option(Config.Redacted(name))),
    (found): Credential | undefined => {
      const key = Option.firstSomeOf(found)
      return Option.isSome(key) ? { type: 'api_key', key: key.value } : undefined
    },
  ).pipe(
    Effect.mapError(
      (cause) => new CredentialsError({ message: 'Reading credentials from config failed', cause }),
    ),
  )

export const make = (names: Names): Service => {
  const get: Service['get'] = (provider) => {
    const candidates = names[provider]
    return candidates === undefined ? Effect.undefined : read(candidates)
  }
  return {
    get,
    list: Effect.map(
      Effect.forEach(Object.keys(names), (provider) =>
        Effect.map(get(provider), (credential): ReadonlyArray<Info> =>
          credential === undefined ? [] : [{ provider, type: credential.type }],
        ),
      ),
      (found) => found.flat(),
    ),
    set: readOnly,
    remove: readOnly,
    update: readOnly,
  }
}
