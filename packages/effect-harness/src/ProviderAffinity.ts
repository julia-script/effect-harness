/** Conversation affinity is a routing hint, never a provider session handle or an authorization token. */
import * as Context from 'effect/Context'

/**
 * Request-scoped durable identity, supplied to Model.configure and generation.
 * Custom adapters map it only to options their provider supports. Unsupported providers ignore it.
 * Harness OpenAI adapters default prompt_cache_key at the captured client boundary;
 * explicit caller values win, including construction defaults and per-request overrides.
 * Anthropic's installed API exposes no session/cache affinity key; metadata.user_id is
 * user attribution and is not set automatically. Unwrapped native models need a custom adapter.
 * New low-level Session creations initialize identity atomically before user initializers.
 * Legacy conversations acquire identity once in a separate commit before their first request;
 * reopening alone does not change them. Identity allocation failures use SessionError.
 * Requires Web Crypto randomUUID in the supported Node/Bun/browser host.
 * Identities survive reopen, differ across durable sessions, and are fresh on every fork.
 * There is no reset/handoff API here; ordinary configuration changes retain the identity.
 */
export class ProviderAffinity extends Context.Service<ProviderAffinity, { readonly id: string }>()(
  'effect-harness/ProviderAffinity',
) {}
