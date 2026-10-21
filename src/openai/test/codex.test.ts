/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Codex against local stand-ins for ChatGPT's auth server and backend: device sign-in, the
 * request it sends with the stored tokens, and refreshing tokens near expiry.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Redacted, Ref } from 'effect'
import { Base64Url } from 'effect/encoding'
import { FetchHttpClient } from 'effect/http'
import type { HttpClient } from 'effect/http'
import { Credentials } from '../../credentials/index.ts'
import { Model } from '../../model/index.ts'
import type { Message } from '../../model/index.ts'
import { serve } from '../../model/test/fakeApi.ts'
import type { Recorded } from '../../model/test/fakeApi.ts'
import { OpenAiCodex } from '../index.ts'
import { refresh } from '../internal/codex/auth.ts'
import { events } from './support.ts'

/** An access token whose claims carry `account`. */
const jwt = (account: string) =>
  [
    Base64Url.encode(JSON.stringify({ alg: 'none' })),
    Base64Url.encode(
      JSON.stringify({ 'https://api.openai.com/auth': { chatgpt_account_id: account } }),
    ),
    'signature',
  ].join('.')

const pendingPolls = new Map<string, number>()

/** The auth server's device flow and token endpoint, and the Codex backend's responses. */
const respond = (request: Recorded) => {
  if (request.path === '/api/accounts/deviceauth/usercode') {
    return { json: { device_auth_id: 'dev_1', user_code: 'ABCD-1234', interval: '0' } }
  }
  if (request.path === '/api/accounts/deviceauth/token') {
    const seen = pendingPolls.get('dev_1') ?? 0
    pendingPolls.set('dev_1', seen + 1)
    return seen === 0
      ? { status: 403, json: { error: 'pending' } }
      : { json: { authorization_code: 'code_1', code_verifier: 'verifier_1' } }
  }
  if (request.path === '/oauth/token') {
    return {
      json: { access_token: jwt('acct_new'), refresh_token: 'refresh_new', expires_in: 3600 },
    }
  }
  if (request.path === '/codex/models') return { json: codexModels }
  return events
}

/** A model list as Codex's backend sends it, trimmed to a few fields and models. */
const codexModels = {
  models: [
    {
      slug: 'gpt-new',
      display_name: 'GPT New',
      description: 'Not in the catalog yet.',
      default_reasoning_level: 'none',
      supported_reasoning_levels: [
        { effort: 'none', description: 'No reasoning' },
        { effort: 'ultra', description: 'A level the harness has no name for' },
      ],
      visibility: 'list',
      priority: 2,
      context_window: 400_000,
      shell_type: 'shell_command',
    },
    { slug: 'gpt-hidden', display_name: 'Hidden', visibility: 'hide', priority: 0 },
    {
      slug: 'gpt-5.5',
      display_name: 'GPT-5.5',
      description: null,
      default_reasoning_level: 'medium',
      supported_reasoning_levels: [
        { effort: 'low', description: 'Fast' },
        { effort: 'medium', description: 'Balanced' },
        { effort: 'xhigh', description: 'Deepest' },
      ],
      visibility: 'list',
      priority: 1,
      service_tiers: [{ id: 'priority', name: 'Fast', description: 'Faster, at more usage' }],
    },
  ],
}

const user = (text: string): Message.Message => ({ role: 'user', content: text, timestamp: 0 })
const system: Message.Message = {
  role: 'system',
  content: [{ type: 'text', text: 'Be brief.' }],
  timestamp: 0,
}

/** Ask Codex model `modelId` through a provider over the stand-in at `url`. */
const ask = (url: string, modelId: string) =>
  Effect.gen(function* () {
    const model = yield* Model.make([OpenAiCodex.provider({ baseUrl: url, authUrl: url })])
    const info = yield* model.info(OpenAiCodex.ref(modelId))
    if (info === undefined) return yield* Effect.die(`no ${modelId}`)
    const request = {
      model: OpenAiCodex.ref(modelId),
      messages: [system, user('hi')],
      options: { reasoning: 'minimal' as const, maxTokens: 500, sessionId: 'conv_1' },
    }
    return yield* Model.collect(model.stream(request), info)
  })

const run = <A, E>(
  body: (url: string) => Effect.Effect<A, E, HttpClient.HttpClient | Credentials.CredentialStore>,
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.flatMap(serve(respond), (api) =>
        Effect.map(body(api.url), (value) => ({ value, requests: api.requests })),
      ),
    ).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test is an entry point
      Effect.provide([FetchHttpClient.layer, Credentials.layerMemory()]),
      Effect.orDie,
    ),
  )

const stored = (expires: number) => ({
  type: 'oauth' as const,
  access: Redacted.make(jwt('acct_1')),
  refresh: Redacted.make('refresh_1'),
  expires,
})

describe('OpenAiCodex', () => {
  test('login shows the device code and stores the tokens once approved', async () => {
    pendingPolls.clear()
    const { value } = await run((url) =>
      Effect.gen(function* () {
        const shown = yield* Ref.make<ReadonlyArray<OpenAiCodex.DeviceCode>>([])
        const login = OpenAiCodex.login({
          authUrl: url,
          onCode: (code) => Ref.update(shown, (all) => [...all, code]),
        })
        yield* login
        const store = yield* Credentials.CredentialStore
        const first = yield* store.get('openai-codex')
        yield* login
        return {
          shown: yield* Ref.get(shown),
          first,
          credential: yield* store.get('openai-codex'),
        }
      }),
    )
    expect(value.shown.map(({ code }) => code)).toEqual(['ABCD-1234', 'ABCD-1234'])
    expect(value.shown[0]?.url.endsWith('/codex/device')).toBe(true)
    expect(value.credential).toMatchObject({
      type: 'oauth',
      extra: { accountId: 'acct_new' },
      identity: { issuer: 'https://auth.openai.com', subject: 'acct_new' },
    })
    if (value.first?.type !== 'oauth' || value.credential?.type !== 'oauth')
      throw new Error('Missing signed-in credentials')
    expect(value.credential.identity).toEqual(value.first.identity)
    expect(value.first.revision).toEqual(expect.any(String))
    expect(value.credential.revision).toEqual(expect.any(String))
    expect(value.credential.revision).not.toBe(value.first.revision)
    expect(Redacted.value(value.credential.access)).toBe(Redacted.value(value.first.access))
    expect(Object.keys(value.credential.identity ?? {}).toSorted()).toEqual(['issuer', 'subject'])
  }, 10_000)

  test('sends the stored token, account, instructions and no output limit', async () => {
    const { value, requests } = await run((url) =>
      Effect.gen(function* () {
        const store = yield* Credentials.CredentialStore
        yield* store.set('openai-codex', stored(Date.now() + 3_600_000))
        return yield* ask(url, 'gpt-5.5')
      }),
    )
    expect(value).toMatchObject({
      content: [{ type: 'text', text: 'Hello' }],
      provider: 'openai-codex',
    })
    expect(requests).toHaveLength(1)
    expect(requests[0]).toMatchObject({
      path: '/codex/responses',
      headers: {
        authorization: `Bearer ${jwt('acct_1')}`,
        'chatgpt-account-id': 'acct_1',
        originator: 'effect-harness',
        'session-id': 'conv_1',
      },
      body: {
        model: 'gpt-5.5',
        store: false,
        instructions: 'Be brief.',
        prompt_cache_key: 'conv_1',
        // Codex maps `minimal` to `low` for gpt-5.5.
        reasoning: { effort: 'low' },
      },
    })
    expect(requests[0]?.body).not.toHaveProperty('max_output_tokens')
    expect(JSON.stringify(requests[0]?.body)).not.toContain('"role":"system"')
  })

  test('refreshes tokens near expiry, writes them back, and uses them', async () => {
    const { value, requests } = await run((url) =>
      Effect.gen(function* () {
        const store = yield* Credentials.CredentialStore
        yield* store.set('openai-codex', {
          ...stored(Date.now() + 1000),
          access: Redacted.make(jwt('acct_new')),
          identity: { issuer: 'https://auth.openai.com', subject: 'acct_new' },
          revision: 'sign-in-one',
          extra: { accountId: 'acct_new', hostSetting: 'preserved' },
        })
        yield* ask(url, 'gpt-5.5')
        return yield* store.get('openai-codex')
      }),
    )
    expect(requests.map((request) => request.path)).toEqual(['/oauth/token', '/codex/responses'])
    expect(requests[0]?.body).toMatchObject({
      grant_type: 'refresh_token',
      refresh_token: 'refresh_1',
    })
    expect(requests[1]?.headers['chatgpt-account-id']).toBe('acct_new')
    expect(value).toMatchObject({
      type: 'oauth',
      identity: { issuer: 'https://auth.openai.com', subject: 'acct_new' },
      revision: 'sign-in-one',
      extra: { accountId: 'acct_new', hostSetting: 'preserved' },
    })
  })

  test('refresh rejects another account before persisting tokens or sending inference', async () => {
    const { value, requests } = await run((url) =>
      Effect.gen(function* () {
        const store = yield* Credentials.CredentialStore
        const expired = {
          ...stored(0),
          identity: { issuer: 'https://auth.openai.com', subject: 'acct_1' },
          revision: 'original-sign-in',
        }
        yield* store.set('openai-codex', expired)
        const result = yield* Effect.result(ask(url, 'gpt-5.5'))
        expect(result).toMatchObject({ _tag: 'Failure' })
        expect(yield* store.get('openai-codex')).toEqual(expired)
      }),
    )
    expect(value).toBeUndefined()
    expect(requests.map((request) => request.path)).toEqual(['/oauth/token'])
  })

  test('legacy refresh preserves absent identity and revision, and rejects account changes', async () => {
    const { value } = await run((url) =>
      Effect.gen(function* () {
        const legacy = { ...stored(0), access: Redacted.make(jwt('acct_new')) }
        const next = yield* refresh(url, legacy)
        expect(next.identity).toBeUndefined()
        expect(next.revision).toBeUndefined()
        expect(Redacted.value(next.refresh)).toBe('refresh_new')
        return yield* Effect.result(refresh(url, stored(0)))
      }),
    )
    expect(value).toMatchObject({ _tag: 'Failure', failure: { _tag: 'LoginError' } })
  })

  test('two accounts sign in and answer under their own names', async () => {
    pendingPolls.clear()
    const { value, requests } = await run((url) =>
      Effect.gen(function* () {
        yield* OpenAiCodex.login({ name: 'codex-work', authUrl: url, onCode: () => Effect.void })
        const store = yield* Credentials.CredentialStore
        yield* store.set('codex-home', stored(Date.now() + 3_600_000))
        const model = yield* Model.make(
          ['codex-work', 'codex-home'].map((name) =>
            OpenAiCodex.provider({ name, baseUrl: url, authUrl: url }),
          ),
        )
        const askOn = (name: string) => {
          const ref = OpenAiCodex.ref('gpt-5.5', name)
          return Effect.flatMap(model.info(ref), (info) =>
            info === undefined
              ? Effect.die(`no ${name}`)
              : Model.collect(model.stream({ model: ref, messages: [user('hi')] }), info),
          )
        }
        const answers = [yield* askOn('codex-home'), yield* askOn('codex-work')]
        return {
          providers: answers.map((answer) => answer.provider),
          stored: (yield* store.list).map((info) => info.provider),
        }
      }),
    )
    expect(value.providers).toEqual(['codex-home', 'codex-work'])
    expect(value.stored.toSorted()).toEqual(['codex-home', 'codex-work'])
    const sent = requests.filter((request) => request.path === '/codex/responses')
    expect(sent.map((request) => request.headers['chatgpt-account-id'])).toEqual([
      'acct_1',
      'acct_new',
    ])
  }, 10_000)

  test('listModels lists the account models with their efforts and tiers', async () => {
    const { value, requests } = await run((url) =>
      Effect.gen(function* () {
        const store = yield* Credentials.CredentialStore
        yield* store.set('codex-work', stored(Date.now() + 3_600_000))
        const codex = OpenAiCodex.provider({ name: 'codex-work', baseUrl: url, authUrl: url })
        return yield* codex.listModels
      }),
    )
    expect(requests[0]).toMatchObject({
      path: '/codex/models',
      search: '?client_version=0.163.0',
      headers: {
        authorization: `Bearer ${jwt('acct_1')}`,
        'chatgpt-account-id': 'acct_1',
        originator: 'effect-harness',
      },
    })
    expect(value).toEqual([
      {
        provider: 'codex-work',
        id: 'gpt-5.5',
        name: 'GPT-5.5',
        served: true,
        thinkingLevels: ['low', 'medium', 'xhigh'],
        defaultThinkingLevel: 'medium',
        serviceTiers: [{ id: 'priority', name: 'Fast', description: 'Faster, at more usage' }],
        contextWindow: 272_000,
      },
      {
        provider: 'codex-work',
        id: 'gpt-new',
        name: 'GPT New',
        description: 'Not in the catalog yet.',
        served: false,
        thinkingLevels: ['off'],
        defaultThinkingLevel: 'off',
        serviceTiers: [],
        contextWindow: 400_000,
      },
    ])
  })

  test('without stored tokens, a request fails asking to sign in', async () => {
    const { value } = await run((url) => Effect.flip(ask(url, 'gpt-5.5')))
    expect(value.message).toContain('Sign in to Codex first')
  })
})
