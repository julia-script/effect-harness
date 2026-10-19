/**
 * The providers people can add accounts of, declared once: Codex through a ChatGPT sign-in, and
 * OpenAI and Anthropic with an API key. A client can import this; it holds no implementation.
 */
import { Adapter } from 'effect-harness/accounts'
import { Anthropic } from 'effect-harness/anthropic/adapter'
import { OpenAi, OpenAiCodex } from 'effect-harness/openai/adapter'

export const adapters = Adapter.group(OpenAiCodex.adapter, OpenAi.adapter, Anthropic.adapter)
