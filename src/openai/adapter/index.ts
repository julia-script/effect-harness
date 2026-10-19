/**
 * The OpenAI adapters' declarations, safe to import in a client.
 */

/** OpenAI and OpenAI-compatible servers, with an API key. */
export * as OpenAi from './OpenAi.ts'

/** Codex through a ChatGPT subscription, signed in with a device code. */
export * as OpenAiCodex from './OpenAiCodex.ts'
