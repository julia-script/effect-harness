import { expect, test } from 'tstyche'
import * as Credential from 'effect-harness/auth/Credential'
import * as CredentialStore from 'effect-harness/auth/CredentialStore'
// effect-nit-allow P9-namespace-alias-equals-module: effect-harness/auth/Duration and effect/Duration both bind Duration; DurationInput distinguishes the concepts.
import * as DurationInput from 'effect-harness/auth/Duration'
import * as JoseJwt from 'effect-harness/auth/JoseJwt'
import * as Jwt from 'effect-harness/auth/Jwt'
import * as Pkce from 'effect-harness/auth/Pkce'
import * as Token from 'effect-harness/auth/Token'
import * as Context from 'effect/Context'
import type * as Crypto from 'effect/Crypto'
import type * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import type * as Layer from 'effect/Layer'
import type * as Option from 'effect/Option'
import type * as Path from 'effect/Path'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import type * as HttpClient from 'effect/http/HttpClient'

class Caller extends Context.Service<Caller, { readonly token: string }>()(
  'typetest/Auth/Caller',
) {}
class CallbackError extends Schema.TaggedError<CallbackError>()('CallbackError', {}) {}
declare const store: CredentialStore.CredentialStore.Service
declare const registration: Credential.Registration
declare const oauth: Credential.OAuth
declare const opaque: Credential.OpaqueOAuth
declare const unknownValue: unknown

test('credential layers and transaction callbacks preserve exact channels', () => {
  expect(CredentialStore.layerMemory).type.toBe<
    Layer.Layer<CredentialStore.CredentialStore, never, Crypto.Crypto>
  >()
  expect(CredentialStore.layerProtectedFile({ path: 'credentials' })).type.toBe<
    Layer.Layer<
      CredentialStore.CredentialStore,
      AuthError.AuthError,
      Crypto.Crypto | FileSystem.FileSystem | Path.Path
    >
  >()
  expect(store.get('key')).type.toBe<
    Effect.Effect<Option.Option<Credential.Credential>, AuthError.AuthError>
  >()
  const changed = store.modify('key', () => Caller.pipe(Effect.as(undefined)))
  expect(changed).type.toBe<Effect.Effect<undefined, AuthError.AuthError, Caller>>()
  expect(store.modify('key', () => Effect.succeed(apiKey))).type.toBe<
    Effect.Effect<Credential.ApiKey, AuthError.AuthError>
  >()
  expect(store.modify('key', () => Effect.succeed(registration))).type.toBe<
    Effect.Effect<Credential.Registration, AuthError.AuthError>
  >()
  expect(store.modify('key', () => Effect.succeed(oauth))).type.toBe<
    Effect.Effect<Credential.OAuth, AuthError.AuthError>
  >()
  expect(store.modify('key', () => Effect.succeed(opaque))).type.toBe<
    Effect.Effect<Credential.OpaqueOAuth, AuthError.AuthError>
  >()
  expect(store.modify('key', () => Caller.pipe(Effect.andThen(CallbackError.make({}))))).type.toBe<
    Effect.Effect<never, CallbackError | AuthError.AuthError, Caller>
  >()
  expect(CredentialStore.layerProtectedFile).type.not.toBeCallableWith({ path: 1 })
  expect(store.modify).type.not.toBeCallableWith('key', () =>
    Effect.succeed('unvalidated credential'),
  )
})

test('portable JWT, crypto and token boundaries retain native service errors', () => {
  expect(JoseJwt.layer).type.toBe<Layer.Layer<Jwt.Jwt, never, HttpClient.HttpClient>>()
  expect(Pkce.make).type.toBe<Effect.Effect<Pkce.Challenge, AuthError.AuthError, Crypto.Crypto>>()
  expect(Token.request('https://fixture.invalid', { code: Redacted.make('secret') })).type.toBe<
    Effect.Effect<Token.TokenResponse, AuthError.AuthError, HttpClient.HttpClient>
  >()
  expect(DurationInput.fromInput('1 second', 'invalid')).type.toBe<
    Effect.Effect<Duration.Duration, AuthError.AuthError>
  >()
  expect(DurationInput.fromInput).type.not.toBeCallableWith('forever', 'invalid')
  expect(Jwt.isIdentity(unknownValue)).type.toBe<boolean>()
  if (Jwt.isIdentity(unknownValue)) expect(unknownValue).type.toBe<Jwt.Identity>()
  if (Credential.isCredential(unknownValue)) expect(unknownValue).type.toBe<Credential.Credential>()
  if (Token.isTokenResponse(unknownValue)) expect(unknownValue).type.toBe<Token.TokenResponse>()
})

declare const apiKey: Credential.ApiKey
declare const encodedApiKey: typeof Credential.ApiKey.Encoded
test('credential tags and redacted tokens expose exact public types', () => {
  expect(apiKey._tag).type.toBe<'apiKey'>()
  expect(encodedApiKey._tag).type.toBe<'apiKey'>()
  expect(apiKey.apiKey).type.toBe<Redacted.Redacted<string>>()
  expect(Credential.accountKey).type.not.toBeCallableWith({ provider: 'x', subject: 'x' })
})

import type * as AuthError from 'effect-harness/auth/AuthError'
