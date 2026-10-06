import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import {
  AuthIdentityError,
  AuthNetworkError,
  makeAuthErrorReason,
  AuthError,
  accountKey,
} from '@effect-harness/auth/Credential'
import * as Store from '@effect-harness/auth/CredentialStore'
import { Jwt } from '@effect-harness/auth/Jwt'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Stream from 'effect/Stream'
import * as NativeLanguageModel from 'effect/ai/LanguageModel'
import * as AiError from 'effect/ai/AiError'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as TestClock from 'effect/testing/TestClock'
import * as ChatGpt from '../src/ChatGpt.ts'
import * as Provider from '../src/LanguageModel.ts'

const responseBody = {
  id: 'resp-1',
  model: 'account-model',
  created_at: 0,
  output: [
    {
      type: 'message',
      id: 'message-1',
      role: 'assistant',
      status: 'completed',
      content: [{ type: 'output_text', text: 'Hello', annotations: [] }],
    },
  ],
  usage: {
    input_tokens: 7,
    output_tokens: 2,
    total_tokens: 9,
    input_tokens_details: { cached_tokens: 0 },
    output_tokens_details: { reasoning_tokens: 0 },
  },
}
const sse = (type: string) =>
  `data: ${JSON.stringify({ type, sequence_number: 1, response: responseBody })}\n\n`
const makeFixture = () => {
  const requests: Array<HttpClientRequest.HttpClientRequest> = []
  const options = {
    tokenStatus: 200,
    subject: 'account-1',
    scopes: 'openid profile email offline_access resource.invoke chatgpt.tokens.use.direct',
    clientId: 'issued-1',
    stream: sse('response.completed'),
    refreshIdToken: true,
    expiresIn: 3600,
    earliestRefreshAt: undefined as number | undefined,
  }
  let tokenRequests = 0
  const http = HttpClient.make((request) => {
    requests.push(request)
    let body: unknown
    let status = 200
    let contentType = 'application/json'
    if (request.url.endsWith('/oauth/token')) {
      tokenRequests++
      status = options.tokenStatus
      body = {
        access_token: `access-${tokenRequests}`,
        refresh_token: `refresh-${tokenRequests}`,
        ...(options.refreshIdToken ? { id_token: 'id-token' } : {}),
        expires_in: options.expiresIn,
        ...(options.earliestRefreshAt === undefined
          ? {}
          : { earliest_refresh_at: options.earliestRefreshAt }),
        token_type: 'Bearer',
        scope: options.scopes,
      }
    } else if (request.url.endsWith('/models'))
      body = {
        models: [
          { slug: 'first', display_name: 'First', visibility: 'list' },
          { slug: 'hidden', display_name: 'Hidden', visibility: 'hidden' },
          { slug: 'last', display_name: 'Last', visibility: 'list' },
        ],
      }
    else if (request.url.endsWith('/responses')) {
      body = options.stream
      contentType = 'text/event-stream'
    } else body = {}
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        new Response(typeof body === 'string' ? body : JSON.stringify(body), {
          status,
          headers: { 'content-type': contentType },
        }),
      ),
    )
  })
  const jwtOptions: Array<{
    readonly issuer: string
    readonly audience: string
    readonly nonce?: string | undefined
  }> = []
  const jwt = Jwt.of({
    verify: (_token, verifyOptions) => {
      jwtOptions.push(verifyOptions)
      if (verifyOptions.audience !== options.clientId)
        return Effect.fail(
          new AuthError({ reason: new AuthIdentityError({ message: 'audience mismatch' }) }),
        )
      return Effect.succeed({
        sub: options.subject,
        iss: verifyOptions.issuer,
        exp: 3600,
        email: 'same@email.test',
        ...(verifyOptions.nonce === undefined ? {} : { nonce: verifyOptions.nonce }),
      })
    },
  })
  const layer = ChatGpt.layer({ appName: 'Effect Harness' }).pipe(
    Layer.provideMerge(Store.layerMemory.pipe(Layer.provide(BunCrypto.layer))),
    Layer.provide(Layer.succeed(Jwt, jwt)),
    Layer.provideMerge(Layer.succeed(HttpClient.HttpClient, http)),
    Layer.provide(BunCrypto.layer),
  )
  return { requests, options, jwtOptions, layer, tokenRequests: () => tokenRequests }
}
const callback = (authorization: ChatGpt.Authorization, clientId?: string) =>
  `${authorization.redirectUri}?${new URLSearchParams({ state: authorization.state, code: 'authorization-code', ...(clientId === undefined ? {} : { client_id: clientId }) }).toString()}`
const login = Effect.fnUntraced(function* () {
  const auth = yield* ChatGpt.ChatGpt
  const authorization = yield* auth.begin({ redirectUri: 'http://127.0.0.1:12345/auth/callback' })
  return yield* auth.complete(callback(authorization, 'issued-1'))
})

describe('ChatGPT account', () => {
  it.effect(
    'native client HTTP field retains public URL and refreshed account authentication',
    () => {
      const f = makeFixture()
      return Effect.gen(function* () {
        const credential = yield* login()
        const client = yield* OpenAiClient.OpenAiClient.pipe(
          Effect.provide(Provider.layerChatGptClient({ account: accountKey(credential) })),
        )
        yield* TestClock.adjust('61 minutes')
        yield* client.client.get('/models')
        const request = f.requests.find((request) => request.url.endsWith('/models'))
        assert.strictEqual(request?.url, 'https://api.openai.com/v1/models')
        assert.strictEqual(request?.headers['authorization'], 'Bearer access-2')
      }).pipe(Effect.provide(f.layer))
    },
  )
  it.effect(
    'raw native account HTTP client preserves the exact AuthError inside TransportError',
    () => {
      const f = makeFixture()
      return Effect.gen(function* () {
        const original = new AuthError({
          reason: new AuthNetworkError({
            message: 'Sanitized credential failure',
            cause: new Error('private-token-diagnostic'),
          }),
        })
        const auth = yield* ChatGpt.ChatGpt
        const client = yield* OpenAiClient.OpenAiClient.pipe(
          Effect.provide(
            Provider.layerChatGptClient({ account: 'selected-account' }).pipe(
              Layer.provide(
                Layer.succeed(ChatGpt.ChatGpt, {
                  ...auth,
                  accessToken: () => Effect.fail(original),
                }),
              ),
            ),
          ),
        )
        const error = yield* client.client.get('/models').pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, 'TransportError')
        assert.strictEqual(error.reason.cause, original)
        assert.strictEqual(original.cause instanceof Error, true)
        assert.isFalse(JSON.stringify(error).includes('private-token-diagnostic'))
        assert.strictEqual(f.requests.length, 0)
      }).pipe(Effect.provide(f.layer))
    },
  )

  it.effect(
    'registers dynamically, exchanges issued ID, validates nonce and persists separate identity',
    () => {
      const f = makeFixture()
      return Effect.gen(function* () {
        const auth = yield* ChatGpt.ChatGpt
        const authorization = yield* auth.begin({
          redirectUri: 'http://127.0.0.1:12345/auth/callback',
        })
        const url = new URL(Redacted.value(authorization.url))
        assert.strictEqual(url.searchParams.get('client_id'), 'dynamic_agent_client')
        assert.strictEqual(url.searchParams.get('agent_name_hint'), 'Effect Harness')
        assert.match(url.searchParams.get('ext_agent_host_id') ?? '', /^urn:uuid:/)
        assert.strictEqual(url.searchParams.get('code_challenge_method'), 'S256')
        const credential = yield* auth.complete(callback(authorization, 'issued-1'))
        assert.strictEqual(credential.subject, 'account-1')
        assert.strictEqual(f.jwtOptions[0]?.nonce, url.searchParams.get('nonce'))
        const request = f.requests.find((req) => req.url.endsWith('/oauth/token'))
        assert.isDefined(request)
        if (request?.body._tag === 'Uint8Array') {
          const form = new URLSearchParams(new TextDecoder().decode(request.body.body))
          assert.strictEqual(form.get('client_id'), 'issued-1')
          assert.strictEqual(form.get('redirect_uri'), authorization.redirectUri)
          assert.strictEqual(form.get('resource'), ChatGpt.resource)
          assert.isFalse(form.has('client_secret'))
          assert.match(form.get('code_verifier') ?? '', /^[\w-]{43}$/)
        }
        assert.strictEqual(
          Redacted.value(yield* auth.accessToken(accountKey(credential))),
          'access-1',
        )
        assert.strictEqual(f.tokenRequests(), 1)
        assert.strictEqual(
          (yield* auth.complete(callback(authorization, 'issued-1')).pipe(Effect.flip)).code,
          'callback',
        )
      }).pipe(Effect.provide(f.layer))
    },
  )

  it.effect(
    'rejects invalid state, callback identity, duplicate parameters and dynamic token-exchange ID',
    () => {
      const f = makeFixture()
      return Effect.gen(function* () {
        const auth = yield* ChatGpt.ChatGpt
        assert.strictEqual(
          (yield* auth
            .complete(
              'http://127.0.0.1:12345/auth/callback?state=invalid&code=x&client_id=issued-1',
            )
            .pipe(Effect.flip)).code,
          'callback',
        )
        const a = yield* auth.begin({ redirectUri: 'http://127.0.0.1:12345/auth/callback' })
        for (const invalid of [
          callback(a, 'issued-1').replace('12345', '12346'),
          `${callback(a, 'issued-1')}&state=duplicate`,
          callback(a, 'dynamic_agent_client'),
        ])
          assert.strictEqual((yield* auth.complete(invalid).pipe(Effect.flip)).code, 'callback')
        assert.strictEqual(f.tokenRequests(), 0)
      }).pipe(Effect.provide(f.layer))
    },
  )

  it.effect('denial and expired attempts do not exchange tokens', () => {
    const f = makeFixture()
    return Effect.gen(function* () {
      const auth = yield* ChatGpt.ChatGpt
      const denied = yield* auth.begin({ redirectUri: 'http://127.0.0.1:12345/auth/callback' })
      assert.strictEqual(
        (yield* auth
          .complete(`${denied.redirectUri}?state=${denied.state}&error=access_denied`)
          .pipe(Effect.flip)).code,
        'denied',
      )
      const expired = yield* auth.begin({ redirectUri: 'http://127.0.0.1:12345/auth/callback' })
      yield* TestClock.adjust('11 minutes')
      assert.strictEqual(
        (yield* auth.complete(callback(expired, 'issued-1')).pipe(Effect.flip)).code,
        'expired',
      )
      assert.strictEqual(f.tokenRequests(), 0)
    }).pipe(Effect.provide(f.layer))
  })

  it.effect('rejects non-loopback redirects and missing issued ID', () => {
    const f = makeFixture()
    return Effect.gen(function* () {
      const auth = yield* ChatGpt.ChatGpt
      for (const redirectUri of [
        'http://localhost:12345/auth/callback',
        'https://127.0.0.1/auth/callback',
        'http://127.0.0.1/callback',
        'http://127.0.0.1/auth/callback?q=x',
      ])
        assert.strictEqual(
          (yield* auth.begin({ redirectUri }).pipe(Effect.flip)).code,
          'configuration',
        )
      const missing = yield* auth.begin({ redirectUri: 'http://127.0.0.1:12345/auth/callback' })
      assert.strictEqual(
        (yield* auth.complete(callback(missing)).pipe(Effect.flip)).code,
        'callback',
      )
      assert.strictEqual(f.tokenRequests(), 0)
    }).pipe(Effect.provide(f.layer))
  })

  it.effect('requires granted direct scope and keeps unvalidated account inactive', () => {
    const f = makeFixture()
    f.options.scopes = 'openid profile'
    return Effect.gen(function* () {
      const auth = yield* ChatGpt.ChatGpt
      const a = yield* auth.begin({ redirectUri: 'http://127.0.0.1:12345/auth/callback' })
      assert.strictEqual(
        (yield* auth.complete(callback(a, 'issued-1')).pipe(Effect.flip)).code,
        'permission',
      )
      assert.deepStrictEqual(yield* (yield* Store.CredentialStore).list, [])
    }).pipe(Effect.provide(f.layer))
  })

  it.effect(
    'returning sign-in uses issued ID and hints, rejects another account or issued ID',
    () => {
      const f = makeFixture()
      return Effect.gen(function* () {
        const auth = yield* ChatGpt.ChatGpt
        const credential = yield* login()
        const key = accountKey(credential)
        const returning = yield* auth.begin({
          account: key,
          redirectUri: 'http://127.0.0.1:54321/auth/callback',
        })
        const query = new URL(Redacted.value(returning.url)).searchParams
        assert.strictEqual(query.get('client_id'), 'issued-1')
        assert.isFalse(query.has('agent_name_hint'))
        assert.strictEqual(query.get('id_token_hint'), 'id-token')
        assert.strictEqual(query.get('ext_agent_host_id'), credential.hostId)
        assert.isFalse(JSON.stringify(returning).includes('id-token'))
        assert.strictEqual(
          (yield* auth.complete(callback(returning, 'different')).pipe(Effect.flip)).code,
          'identity',
        )
        const second = yield* auth.begin({
          account: key,
          redirectUri: 'http://127.0.0.1:54321/auth/callback',
        })
        f.options.subject = 'other-account'
        assert.strictEqual(
          (yield* auth.complete(callback(second)).pipe(Effect.flip)).code,
          'identity',
        )
        assert.deepStrictEqual(
          yield* (yield* Store.CredentialStore).get(key),
          Option.some(credential),
        )
        f.options.subject = 'account-1'
        const third = yield* auth.begin({
          account: key,
          redirectUri: 'http://127.0.0.1:54321/auth/callback',
        })
        assert.strictEqual((yield* auth.complete(callback(third))).subject, 'account-1')
      }).pipe(Effect.provide(f.layer))
    },
  )

  it.effect(
    'serializes expiry refresh, replaces rotating credentials, preserves prior values on failure',
    () => {
      const f = makeFixture()
      return Effect.gen(function* () {
        const auth = yield* ChatGpt.ChatGpt
        const credential = yield* login()
        const key = accountKey(credential)
        yield* TestClock.adjust('61 minutes')
        const refreshed = yield* Effect.all([auth.accessToken(key), auth.accessToken(key)], {
          concurrency: 'unbounded',
        })
        assert.deepStrictEqual(refreshed.map(Redacted.value), ['access-2', 'access-2'])
        assert.strictEqual(f.tokenRequests(), 2)
        const store = yield* Store.CredentialStore
        const previous = yield* store.get(key)
        f.options.tokenStatus = 500
        assert.strictEqual(
          (yield* auth.refresh(key, { force: true }).pipe(Effect.flip)).code,
          'token',
        )
        assert.deepStrictEqual(yield* store.get(key), previous)
        f.options.tokenStatus = 200
        f.options.subject = 'other-account'
        assert.strictEqual(
          (yield* auth.refresh(key, { force: true }).pipe(Effect.flip)).code,
          'identity',
        )
        assert.deepStrictEqual(yield* store.get(key), previous)
        f.options.subject = 'account-1'
        f.options.refreshIdToken = false
        const latest = yield* auth.refresh(key, { force: true })
        assert.strictEqual(Redacted.value(latest.idToken), 'id-token')
        const lastRequest = f.requests
          .filter((request) => request.url.endsWith('/oauth/token'))
          .at(-1)
        if (lastRequest?.body._tag === 'Uint8Array') {
          const fields = new URLSearchParams(new TextDecoder().decode(lastRequest.body.body))
          assert.strictEqual(fields.get('client_id'), 'issued-1')
          assert.strictEqual(fields.get('refresh_token'), 'refresh-2')
          assert.isFalse(fields.has('scope'))
        }
      }).pipe(Effect.provide(f.layer))
    },
  )

  it.effect(
    'lists current account models in server order and revokes while keeping registration',
    () => {
      const f = makeFixture()
      return Effect.gen(function* () {
        const auth = yield* ChatGpt.ChatGpt
        const credential = yield* login()
        const key = accountKey(credential)
        assert.deepStrictEqual(
          (yield* auth.models(key)).map((model) => model.slug),
          ['first', 'last'],
        )
        assert.strictEqual(
          f.requests.find((req) => req.url.endsWith('/models'))?.headers['authorization'],
          'Bearer access-1',
        )
        yield* auth.signOut(key)
        const registration = yield* (yield* Store.CredentialStore).get(key)
        assert.isTrue(Option.isSome(registration))
        if (Option.isSome(registration)) assert.strictEqual(registration.value.kind, 'registration')
        assert.strictEqual((yield* auth.accessToken(key).pipe(Effect.flip)).code, 'missing')
        const a = yield* auth.begin({
          account: key,
          redirectUri: 'http://127.0.0.1:12346/auth/callback',
        })
        assert.strictEqual(
          new URL(Redacted.value(a.url)).searchParams.get('client_id'),
          credential.clientId,
        )
      }).pipe(Effect.provide(f.layer))
    },
  )

  it.effect(
    'native generated text uses refreshed public streaming Responses with store false',
    () => {
      const f = makeFixture()
      return Effect.gen(function* () {
        const credential = yield* login()
        yield* TestClock.adjust('61 minutes')
        const response = yield* NativeLanguageModel.generateText({ prompt: 'Hello' }).pipe(
          Effect.provide(
            Provider.layerChatGpt({
              account: accountKey(credential),
              model: 'account-model',
              config: { store: true },
            }),
          ),
          Effect.provideService(OpenAiClient.OpenAiSocket, {
            createResponseStream: () => Effect.die('Account layer must use HTTP Responses'),
          }),
        )
        assert.strictEqual(response.text, 'Hello')
        assert.strictEqual(response.usage.inputTokens.total, 7)
        const request = f.requests.find((req) => req.url.endsWith('/responses'))
        assert.strictEqual(request?.url, 'https://api.openai.com/v1/responses')
        assert.strictEqual(request?.headers['authorization'], 'Bearer access-2')
        if (request?.body._tag === 'Uint8Array') {
          const body: unknown = JSON.parse(new TextDecoder().decode(request.body.body))
          assert.propertyVal(body, 'stream', true)
          assert.propertyVal(body, 'store', false)
        }
      }).pipe(Effect.provide(f.layer))
    },
  )

  it.effect('native streaming preserves text deltas and final token usage', () => {
    const f = makeFixture()
    const events = [
      { type: 'response.created', response: { ...responseBody, output: [] } },
      { type: 'response.output_item.added', output_index: 0, item: responseBody.output[0] },
      {
        type: 'response.output_text.delta',
        item_id: 'message-1',
        output_index: 0,
        content_index: 0,
        delta: 'Hello',
        logprobs: [],
      },
      { type: 'response.output_item.done', output_index: 0, item: responseBody.output[0] },
      { type: 'response.completed', response: responseBody },
    ]
    f.options.stream = events
      .map((event, index) => `data: ${JSON.stringify({ ...event, sequence_number: index })}\n\n`)
      .join('')
    return Effect.gen(function* () {
      const credential = yield* login()
      const parts = yield* NativeLanguageModel.streamText({ prompt: 'Hello' }).pipe(
        Stream.runCollect,
        Effect.provide(
          Provider.layerChatGpt({ account: accountKey(credential), model: 'account-model' }),
        ),
      )
      assert.strictEqual(
        parts
          .filter((part) => part.type === 'text-delta')
          .map((part) => part.delta)
          .join(''),
        'Hello',
      )
      const finish = parts.find((part) => part.type === 'finish')
      assert.strictEqual(finish?.usage.inputTokens.total, 7)
      assert.strictEqual(finish?.usage.outputTokens.total, 2)
      assert.strictEqual(finish?.reason, 'stop')
    }).pipe(Effect.provide(f.layer))
  })

  for (const terminal of ['response.failed', 'response.incomplete', 'missing'] as const)
    it.effect(`streaming rejects ${terminal} instead of reporting success`, () => {
      const f = makeFixture()
      f.options.stream = terminal === 'missing' ? '' : sse(terminal)
      return Effect.gen(function* () {
        const credential = yield* login()
        const failure = yield* NativeLanguageModel.streamText({ prompt: 'Hello' }).pipe(
          Stream.runCollect,
          Effect.provide(
            Provider.layerChatGpt({ account: accountKey(credential), model: 'account-model' }),
          ),
          Effect.flip,
        )
        assert.strictEqual(failure._tag, 'AiError')
      }).pipe(Effect.provide(f.layer))
    })

  for (const field of ['expiresIn', 'earliestRefreshAt'] as const) {
    it.effect(
      `rejects derived ${field} overflow before sign-in or refresh can write a grant`,
      () => {
        const f = makeFixture()
        return Effect.gen(function* () {
          const auth = yield* ChatGpt.ChatGpt
          const store = yield* Store.CredentialStore
          f.options[field] = Number.MAX_VALUE
          const invalid = yield* login().pipe(Effect.flip)
          assert.strictEqual(invalid.code, 'protocol')
          assert.isFalse(JSON.stringify(invalid).includes('access-'))
          assert.deepStrictEqual(yield* store.list, [])
          f.options.expiresIn = 3600
          f.options.earliestRefreshAt = undefined
          const credential = yield* login()
          const key = accountKey(credential)
          const previous = yield* store.get(key)
          f.options[field] = Number.MAX_VALUE
          assert.strictEqual(
            (yield* auth.refresh(key, { force: true }).pipe(Effect.flip)).code,
            'protocol',
          )
          assert.deepStrictEqual(yield* store.get(key), previous)
        }).pipe(Effect.provide(f.layer))
      },
    )
  }
  it.effect('accepts finite fractional derived timestamps using the persisted OAuth policy', () => {
    const f = makeFixture()
    f.options.expiresIn = 0.00025
    f.options.earliestRefreshAt = 0.00025
    return Effect.gen(function* () {
      const credential = yield* login()
      assert.strictEqual(credential.expiresAt % 1, 0.25)
      assert.strictEqual(credential.earliestRefreshAt, 0.25)
      assert.strictEqual(
        (yield* (yield* Store.CredentialStore).get(accountKey(credential)))._tag,
        'Some',
      )
    }).pipe(Effect.provide(f.layer))
  })

  for (const [reason, status, expected, retryable] of [
    ['network', undefined, 'NetworkError', true],
    ['token', 429, 'RateLimitError', true],
    ['token', 503, 'InternalProviderError', true],
    ['busy', undefined, 'InternalProviderError', true],
    ['identity', undefined, 'AuthenticationError', false],
    ['permission', 403, 'AuthenticationError', false],
    ['permission', 503, 'AuthenticationError', false],
    ['storage', 503, 'AuthenticationError', false],
    ['token', 401, 'AuthenticationError', false],
    ['token', 600, 'AuthenticationError', false],
  ] as const) {
    it.effect(
      `preserves ${reason}/${status ?? 'none'} inference refresh semantics in generation and streaming`,
      () => {
        const f = makeFixture()
        return Effect.gen(function* () {
          const auth = yield* ChatGpt.ChatGpt
          const failing = Layer.succeed(ChatGpt.ChatGpt, {
            ...auth,
            accessToken: () =>
              Effect.fail(
                new AuthError({
                  reason: makeAuthErrorReason({
                    reason,
                    status,
                    message: 'Sanitized credential failure',
                  }),
                }),
              ),
          })
          const layer = Provider.layerChatGpt({ account: 'selected', model: 'account-model' }).pipe(
            Layer.provide(failing),
          )
          for (const effect of [
            NativeLanguageModel.generateText({ prompt: 'Hello' }).pipe(Effect.asVoid),
            NativeLanguageModel.streamText({ prompt: 'Hello' }).pipe(Stream.runDrain),
          ]) {
            const failure = yield* effect.pipe(Effect.provide(layer), Effect.flip)
            assert.strictEqual(failure.reason._tag, expected)
            assert.strictEqual(failure.isRetryable, retryable)
            if (failure.reason._tag === 'NetworkError') {
              assert.deepStrictEqual(failure.reason.request.headers, {})
              assert.deepStrictEqual(failure.reason.request.urlParams, [])
            }
            assert.isFalse(JSON.stringify(failure).includes('refresh-'))
          }
        }).pipe(Effect.provide(f.layer))
      },
    )
  }

  for (const [event, expected, retryable, code] of [
    [
      { type: 'error', code: 'insufficient_quota', message: 'billing diagnostic', param: null },
      'QuotaExhaustedError',
      false,
      'insufficient_quota',
    ],
    [
      {
        type: 'error',
        error: { code: 'rate_limit_exceeded', message: 'throttle diagnostic', param: null },
      },
      'RateLimitError',
      true,
      'rate_limit_exceeded',
    ],
    [
      {
        type: 'response.failed',
        response: {
          ...responseBody,
          error: { code: 'insufficient_quota', message: 'quota diagnostic' },
        },
      },
      'QuotaExhaustedError',
      false,
      'insufficient_quota',
    ],
    [
      {
        type: 'response.failed',
        response: {
          ...responseBody,
          error: { code: 'context_length_exceeded', message: 'context diagnostic' },
        },
      },
      'InvalidRequestError',
      false,
      'context_length_exceeded',
    ],
    [
      {
        type: 'response.failed',
        response: {
          ...responseBody,
          error: { code: 'server_error', message: 'server diagnostic' },
        },
      },
      'InternalProviderError',
      true,
      'server_error',
    ],
    [
      {
        type: 'response.incomplete',
        response: { ...responseBody, incomplete_details: { reason: 'content_filter' } },
      },
      'ContentPolicyError',
      false,
      null,
    ],
    [
      {
        type: 'response.incomplete',
        response: { ...responseBody, incomplete_details: { reason: 'max_output_tokens' } },
      },
      'InvalidRequestError',
      false,
      null,
    ],
    [
      {
        type: 'response.failed',
        response: {
          ...responseBody,
          error: { code: 'new_provider_code', message: 'original diagnostic' },
        },
      },
      'UnknownError',
      false,
      'new_provider_code',
    ],
  ] as const) {
    it.effect(
      `retains semantic ${event.type}/${code ?? expected} SSE diagnostics for generation and streaming`,
      () => {
        const f = makeFixture()
        f.options.stream = `data: ${JSON.stringify(event)}\n\n`
        return Effect.gen(function* () {
          const credential = yield* login()
          const layer = Provider.layerChatGpt({
            account: accountKey(credential),
            model: 'account-model',
          })
          for (const effect of [
            NativeLanguageModel.generateText({ prompt: 'Hello' }).pipe(Effect.asVoid),
            NativeLanguageModel.streamText({ prompt: 'Hello' }).pipe(Stream.runDrain),
          ]) {
            const failure = yield* effect.pipe(Effect.provide(layer), Effect.flip)
            assert.isTrue(AiError.isAiError(failure))
            assert.strictEqual(failure.reason._tag, expected)
            assert.strictEqual(failure.isRetryable, retryable)
            if ('metadata' in failure.reason)
              assert.propertyVal(failure.reason.metadata.openai, 'code', code)
            assert.isFalse(failure.message.includes('without a completed response'))
          }
        }).pipe(Effect.provide(f.layer))
      },
    )
  }

  it.effect('API-key provider remains a standard native model layer', () => {
    const requests: Array<HttpClientRequest.HttpClientRequest> = []
    const client = HttpClient.make((request) => {
      requests.push(request)
      return Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(JSON.stringify(responseBody), {
            headers: { 'content-type': 'application/json' },
          }),
        ),
      )
    })
    return Effect.gen(function* () {
      const result = yield* NativeLanguageModel.generateText({ prompt: 'Hello' })
      assert.strictEqual(result.text, 'Hello')
      assert.strictEqual(requests[0]?.headers['authorization'], 'Bearer api-key')
    }).pipe(
      Effect.provide(
        Provider.layerApiKey({ apiKey: Redacted.make('api-key'), model: 'account-model' }).pipe(
          Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
        ),
      ),
    )
  })
})
