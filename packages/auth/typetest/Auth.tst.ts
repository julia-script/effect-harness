import { expect, test } from 'tstyche'
import * as Credential from '@effect-harness/auth/Credential'
import * as Store from '@effect-harness/auth/CredentialStore'
import * as DurationInput from '@effect-harness/auth/Duration'
import * as JoseJwt from '@effect-harness/auth/JoseJwt'
import * as Jwt from '@effect-harness/auth/Jwt'
import * as Pkce from '@effect-harness/auth/Pkce'
import * as Token from '@effect-harness/auth/Token'
import * as Context from 'effect/Context'
import type * as Crypto from 'effect/Crypto'
import type * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import type * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import type * as Path from 'effect/Path'
import * as Redacted from 'effect/Redacted'
import type * as HttpClient from 'effect/http/HttpClient'

class Caller extends Context.Service<Caller, { readonly token: string }>()(
  'typetest/Auth/Caller',
) {}
declare const store: Store.Service
declare const unknownValue: unknown

test('credential layers and transaction callbacks preserve exact channels', () => {
  expect(Store.layerMemory).type.toBe<Layer.Layer<Store.CredentialStore, never, Crypto.Crypto>>()
  expect(Store.layerProtectedFile({ path: 'credentials' })).type.toBe<
    Layer.Layer<
      Store.CredentialStore,
      Credential.AuthError,
      Crypto.Crypto | FileSystem.FileSystem | Path.Path
    >
  >()
  expect(store.get('key')).type.toBe<
    Effect.Effect<Option.Option<Credential.Credential>, Credential.AuthError>
  >()
  const changed = store.modify('key', () => Caller.pipe(Effect.as(undefined)))
  expect(changed).type.toBe<
    Effect.Effect<Credential.Credential | undefined, Credential.AuthError, Caller>
  >()
  expect(Store.layerProtectedFile).type.not.toBeCallableWith({ path: 1 })
  expect(store.modify).type.not.toBeCallableWith('key', () =>
    Effect.succeed('unvalidated credential'),
  )
})

test('portable JWT, crypto and token boundaries retain native service errors', () => {
  expect(JoseJwt.layer).type.toBe<Layer.Layer<Jwt.Jwt, never, HttpClient.HttpClient>>()
  expect(Pkce.make).type.toBe<Effect.Effect<Pkce.Challenge, Credential.AuthError, Crypto.Crypto>>()
  expect(Token.request('https://fixture.invalid', { code: Redacted.make('secret') })).type.toBe<
    Effect.Effect<Token.TokenResponse, Credential.AuthError, HttpClient.HttpClient>
  >()
  expect(DurationInput.fromInput('1 second', 'invalid')).type.toBe<
    Effect.Effect<Duration.Duration, Credential.AuthError>
  >()
  expect(DurationInput.fromInput).type.not.toBeCallableWith('forever', 'invalid')
  expect(Jwt.isIdentity(unknownValue)).type.toBe<boolean>()
  if (Jwt.isIdentity(unknownValue)) expect(unknownValue).type.toBe<Jwt.Identity>()
  if (Credential.isCredential(unknownValue)) expect(unknownValue).type.toBe<Credential.Credential>()
  if (Token.isTokenResponse(unknownValue)) expect(unknownValue).type.toBe<Token.TokenResponse>()
})
