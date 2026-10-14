import { expect, test } from 'tstyche'
import type * as Layer from 'effect/Layer'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as AiError from 'effect/ai/AiError'
import type * as HttpClient from 'effect/http/HttpClient'
import type { CredentialStore } from 'effect-harness/provider-auth'
import { CodexLanguageModel } from 'effect-harness/provider-openai-codex'
import { AnthropicLanguageModel } from 'effect-harness/provider-anthropic'

const binding = { provider: 'openai-codex', profile: 'personal', identity: 'login' } as const
test('subscription layers require live HTTP and credential services, and expose only LanguageModel', () => {
  expect(CodexLanguageModel.layer({ model: 'gpt-5.4', binding })).type.toBe<
    Layer.Layer<
      LanguageModel.LanguageModel,
      AiError.AiError,
      HttpClient.HttpClient | CredentialStore.CredentialStore
    >
  >()
  expect(
    AnthropicLanguageModel.layerOAuth({
      model: 'claude-sonnet-4-6',
      binding: { ...binding, provider: 'anthropic' },
    }),
  ).type.toBe<
    Layer.Layer<
      LanguageModel.LanguageModel,
      AiError.AiError,
      HttpClient.HttpClient | CredentialStore.CredentialStore
    >
  >()
})
test('unsupported transports and ordinary OpenAI generation settings are excluded', () => {
  expect(CodexLanguageModel.layer).type.not.toBeCallableWith({
    model: 'gpt-5.4',
    binding,
    transport: 'websocket',
  })
  expect(CodexLanguageModel.layer).type.not.toBeCallableWith({
    model: 'gpt-5.4',
    binding,
    config: { max_output_tokens: 100 },
  })
  expect(CodexLanguageModel.layer).type.not.toBeCallableWith({ model: 'gpt-5.4' })
  expect(AnthropicLanguageModel.layerOAuth).type.not.toBeCallableWith({
    model: 'claude-sonnet-4-6',
    binding: { ...binding, provider: 'anthropic' },
    transport: 'websocket',
  })
})
