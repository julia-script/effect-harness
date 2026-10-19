/**
 * What the phases of the generation task close over, and helpers they share.
 */
import { Clock, Effect } from 'effect'
import type * as Model from '../../../model/Model.ts'
import type { EntryId } from '../../../storage/schema/Ids.ts'
import type { NextState, Runtime } from '../../../task/Task.ts'
import type { Service as Extensions } from '../../Extensions.ts'
import type { Checkpoint, Result } from '../../Generation.ts'
import type { Settings } from '../../Settings.ts'
import { current as currentAgent } from '../agent.ts'
import { read } from '../context.ts'
import type { View } from '../context.ts'

export interface Env {
  readonly model: Model.Service
  readonly settings: Effect.Effect<Settings>
  readonly extensions: Extensions
}

/** The conversation's agent now, resolved once per decision. */
export const agentOf = <I, S, R>(env: Env, runtime: Runtime<I, S, R>) =>
  currentAgent(runtime, runtime.conversationId, env.extensions, env.settings)

export type GenerationRuntime = Runtime<Record<string, never>, Checkpoint, Result>
export type Next = NextState<Checkpoint, Result>

/** The committed context, now or cut off at `at`, read in a commit of its own. */
export const readView = <I, S, R>(runtime: Runtime<I, S, R>, at?: EntryId) =>
  Effect.suspend(() => {
    let view: View = { head: undefined, entries: [], contributions: [], messages: [] }
    return runtime
      .commit((tx) =>
        Effect.map(read(tx, runtime.conversationId, at), (current) => {
          view = current
        }),
      )
      .pipe(Effect.map(() => view))
  })

/** Sleep until the epoch millisecond `at`. */
export const sleepUntil = (at: number) =>
  Effect.flatMap(Clock.currentTimeMillis, (now) => Effect.sleep(Math.max(0, at - now)))
