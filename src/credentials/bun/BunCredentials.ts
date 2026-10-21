/**
 * A `CredentialStore` in the operating system's keychain, through `Bun.secrets`: the macOS
 * Keychain, libsecret on Linux, or the Windows Credential Manager. Bun only.
 *
 * Each credential is one keychain entry under `service`, named after its provider. A keychain
 * cannot list its entries, so an index entry under `${service}:index` records which providers are
 * stored and their credential types, without secrets. Writes run one at a time in this process;
 * two processes must not write one service at once.
 */
import { Effect, Layer } from 'effect'
import { CredentialStore } from '../CredentialStore.ts'
import type { Service } from '../CredentialStore.ts'
import { CredentialsError } from '../CredentialsError.ts'
import * as keychain from '../internal/keychain.ts'

export interface Options {
  /** The keychain service the entries are stored under. Default: `effect-harness`. */
  readonly service?: string
}

const failed = (cause: unknown) =>
  new CredentialsError({ message: 'The keychain could not be read or written', cause })

const secrets: keychain.Keychain = {
  get: (service, name) =>
    Effect.tryPromise({
      try: () => Bun.secrets.get({ service, name }),
      catch: failed,
    }).pipe(Effect.map((value) => value ?? undefined)),
  set: (service, name, value) =>
    Effect.tryPromise({ try: () => Bun.secrets.set({ service, name, value }), catch: failed }),
  remove: (service, name) =>
    Effect.asVoid(
      Effect.tryPromise({ try: () => Bun.secrets.delete({ service, name }), catch: failed }),
    ),
}

export const make = (options: Options = {}): Effect.Effect<Service> =>
  keychain.make(options.service ?? 'effect-harness', secrets)

export const layer = (options: Options = {}): Layer.Layer<CredentialStore> =>
  Layer.effect(CredentialStore, make(options))
