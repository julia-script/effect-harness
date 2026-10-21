/**
 * A store over an OS keychain: one entry per provider, named after it, holding the credential as
 * JSON, and one index entry listing what is stored without secrets, since a keychain cannot list
 * its entries. Writes run one at a time, so `update` compares and sets atomically within the
 * process. A credential is written before the index names it and dropped from the index before it
 * is deleted, so the index never names a missing entry; `get` reads the entry itself.
 */
import { Effect, Equal, Schema, Semaphore } from 'effect'
import { Credential, Info } from '../Credential.ts'
import type { Service } from '../CredentialStore.ts'
import { CredentialChanged, CredentialsError } from '../CredentialsError.ts'

/** The keychain operations a store needs; `get` is `undefined` for a missing entry. */
export interface Keychain {
  readonly get: (
    service: string,
    name: string,
  ) => Effect.Effect<string | undefined, CredentialsError>
  readonly set: (
    service: string,
    name: string,
    value: string,
  ) => Effect.Effect<void, CredentialsError>
  readonly remove: (service: string, name: string) => Effect.Effect<void, CredentialsError>
}

const Entry = Schema.fromJsonString(Credential)
const Index = Schema.fromJsonString(Schema.Array(Info))
type Index = ReadonlyArray<Info>

const malformed = (what: string) => (cause: unknown) =>
  new CredentialsError({ message: `The keychain holds a malformed ${what}`, cause })

/** The index lives in its own service, so no provider name can collide with it. */
export const indexService = (service: string) => `${service}:index`
const indexName = 'providers'

/** Reads and writes of one service's entries and index, without locking. */
const entries = (service: string, keychain: Keychain) => {
  const readIndex: Effect.Effect<Index, CredentialsError> = Effect.flatMap(
    keychain.get(indexService(service), indexName),
    (value) =>
      value === undefined
        ? Effect.succeed([])
        : Effect.mapError(Schema.decodeEffect(Index)(value), malformed('index')),
  )
  const writeIndex = (index: Index) =>
    Effect.flatMap(
      Effect.mapError(Schema.encodeEffect(Index)(index), malformed('index')),
      (value) => keychain.set(indexService(service), indexName, value),
    )
  const read = (provider: string): Effect.Effect<Credential | undefined, CredentialsError> =>
    Effect.flatMap(keychain.get(service, provider), (value) =>
      value === undefined
        ? Effect.undefined
        : Effect.mapError(Schema.decodeEffect(Entry)(value), malformed(`${provider} credential`)),
    )
  const write = Effect.fnUntraced(function* (provider: string, credential: Credential) {
    const value = yield* Effect.mapError(
      Schema.encodeEffect(Entry)(credential),
      malformed(`${provider} credential`),
    )
    yield* keychain.set(service, provider, value)
    const index = yield* readIndex
    if (!index.some((entry) => entry.provider === provider && entry.type === credential._tag)) {
      const others = index.filter((entry) => entry.provider !== provider)
      yield* writeIndex([...others, Info.make({ provider, type: credential._tag })])
    }
  })
  const remove = Effect.fnUntraced(function* (provider: string) {
    const index = yield* readIndex
    if (index.some((entry) => entry.provider === provider)) {
      yield* writeIndex(index.filter((entry) => entry.provider !== provider))
    }
    yield* keychain.remove(service, provider)
  })
  return { readIndex, read, write, remove }
}

export const make = (service: string, keychain: Keychain) =>
  Effect.map(Semaphore.make(1), (line): Service => {
    const { readIndex, read, write, remove } = entries(service, keychain)
    const locked = Semaphore.withPermit(line)
    return {
      get: read,
      list: readIndex,
      set: (provider, credential) => locked(write(provider, credential)),
      remove: (provider) => locked(remove(provider)),
      update: (provider, expected, next) =>
        locked(
          Effect.flatMap(read(provider), (stored) =>
            Equal.equals(stored, expected)
              ? write(provider, next)
              : Effect.fail<CredentialChanged | CredentialsError>(
                  new CredentialChanged({
                    message: `The ${provider} credential changed since it was read`,
                    provider,
                  }),
                ),
          ),
        ),
    }
  })
