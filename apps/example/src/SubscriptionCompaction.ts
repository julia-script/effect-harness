/** Compact and resume a persisted subscription conversation using live credentials. */
import * as Effect from 'effect/Effect'
import type * as CompactionTask from 'effect-harness/CompactionTask'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type * as Record from 'effect-harness/Record'
import * as Settings from 'effect-harness/Settings'
import type * as Credential from 'effect-harness/provider-auth/Credential'
import * as Codex from 'effect-harness/provider-openai-codex/Model'
import * as Claude from 'effect-harness/provider-anthropic/OAuthModel'

/**
 * Supply Storage (for example layerJsonl), HttpClient and CredentialStore. The existing
 * conversation must contain a safe cut. No login or credential bytes enter its history.
 */
export const compactAndContinue = Effect.fnUntraced(function* (
  provider: 'openai-codex' | 'anthropic',
  binding: Credential.Binding,
  conversationId: Record.ConversationId,
  contextWindow: number,
) {
  const capabilities = { tools: true, images: false, reasoning: true, structuredOutput: false }
  const model =
    provider === 'openai-codex'
      ? yield* Codex.make({
          binding,
          model: 'gpt-5.4',
          definition: {
            ref: { provider: 'openai-codex', modelId: 'gpt-5.4' },
            capabilities,
            maxOutputTokens: 4096,
            contextWindow,
          },
        })
      : yield* Claude.make({
          binding,
          model: 'claude-sonnet-5-5',
          thinkingModes: ['adaptive'],
          config: { thinking: { type: 'adaptive' }, maxTokens: 4096 },
          definition: {
            ref: { provider: 'anthropic', modelId: 'claude-sonnet-5-5' },
            capabilities,
            maxOutputTokens: 4096,
            contextWindow,
          },
        })
  const settings = yield* Settings.make({
    compaction: {
      enabled: true,
      reserveTokens: 8192,
      keepRecentTokens: 16000,
      backgroundTokens: 8192,
    },
  })
  const runtime = yield* HarnessRuntime.make({ models: [model] }).pipe(
    Effect.provideService(Settings.Settings, settings),
  )
  const handle: CompactionTask.Handle = yield* runtime.backend.compact!(conversationId, {
    instructions: 'Preserve decisions and unfinished work.',
  })
  const outcome = yield* runtime.backend.waitCompaction!(handle)
  if (outcome.status !== 'completed') return { compaction: outcome }
  if (outcome.result._tag === 'Queued') yield* runtime.backend.wait(outcome.result.submissionId)
  const submission = yield* runtime.backend.submit({
    conversationId,
    draft: { type: 'input', content: 'Continue with the next unfinished step.' },
  })
  return { compaction: outcome, continuation: yield* runtime.backend.wait(submission.id) }
})
