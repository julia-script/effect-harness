/* oxlint-disable effecttsgo/any-unknown-in-error-context -- extension code may fail with anything; a failure is reported */
/**
 * What `select` decides: the summarization request, or what a `beforeCompact` hook decided.
 */
import { Effect, Option } from 'effect'
import type * as Model from '../../../model/Model.ts'
import type { EntryId } from '../../../storage/schema/Ids.ts'
import * as Hook from '../../../task/Hook.ts'
import * as Agent from '../../Agent.ts'
import type { Resolved } from '../../Agent.ts'
import { Hooks } from '../../Compaction.ts'
import type { Checkpoint, Pending } from '../../Compaction.ts'
import { compactionFor } from '../../Settings.ts'
import type { View } from '../context.ts'
import type { Env } from '../generation/env.ts'

type Request = Omit<Extract<Checkpoint, { readonly phase: 'summarize' }>, 'phase'>

/** The summarization request over the range `select` chose. */
export const fixRequest = (
  env: Env,
  agent: Resolved,
  info: Model.Info,
  view: View,
  firstKept: EntryId,
) =>
  Effect.gen(function* () {
    const settings = yield* env.settings
    const model = { provider: info.provider, modelId: info.id }
    const policy = compactionFor(settings, model)
    // A deferred summary would leave the generation waiting on a batch.
    const { deferred: _deferred, ...stream } = settings.stream
    const { reasoning } = agent
    const maxTokens = Math.min(
      Math.floor(0.8 * policy.reserveTokens),
      info.maxTokens > 0 ? info.maxTokens : Number.POSITIVE_INFINITY,
    )
    const options = {
      ...stream,
      cacheRetention: 'none' as const,
      maxTokens,
      ...(reasoning === 'off' ? {} : { reasoning }),
    }
    const tail = view.entries.reduce((max, entry) => (entry.id > max ? entry.id : max), firstKept)
    const request: Request = { attempt: 1, model, options, tail, firstKept }
    return request
  })

/** Ask `beforeCompact` in order; the first decision wins. */
export const beforeCompact = (agent: Resolved, pending: Pending, api: Hook.Api) =>
  Effect.gen(function* () {
    for (const hook of Agent.hooks(agent, Hooks)) {
      if (hook.beforeCompact === undefined) continue
      const decision = yield* Hook.ask(hook.beforeCompact(pending, api), 'beforeCompact')
      if (Option.isSome(decision)) return decision
    }
    return Option.none<{ readonly decline: true } | { readonly summary: string }>()
  })
