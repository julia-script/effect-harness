/**
 * The generation task: its phases and abort handler.
 */
import { Clock, Effect } from 'effect'
import * as ToolResult from '../../../tool/ToolResult.ts'
import type { Definition, RunningTask } from '../../../task/Task.ts'
import type { Checkpoint, Result } from '../../Generation.ts'
import * as Live from '../../Live.ts'
import { convertPartial, endRun, RUN_KIND } from '../run.ts'
import { sleepUntil } from './env.ts'
import type { Env, GenerationRuntime, Next } from './env.ts'
import { preparePhase } from './prepare.ts'
import { pollPhase, requestPhase } from './request.ts'
import { readCalls, toolsPhase } from './round.ts'

type Phase<P extends Checkpoint['phase']> = {
  readonly state: { readonly checkpoint: Extract<Checkpoint, { readonly phase: P }> }
}

/** The `retry` phase: wait out the backoff, then prepare the next attempt. */
const retryPhase = (task: Phase<'retry'>, runtime: GenerationRuntime) =>
  Effect.gen(function* () {
    const { attempt, until, compacted } = task.state.checkpoint
    yield* sleepUntil(until)
    yield* runtime.commit((tx) =>
      Effect.gen(function* () {
        const live = yield* tx.doc(Live.document, runtime.conversationId)
        live.generation = { attempt: attempt + 1 }
        const next: Next = {
          status: 'running',
          checkpoint: {
            phase: 'prepare',
            attempt: attempt + 1,
            ...(compacted === undefined ? {} : { compacted }),
          },
        }
        return next
      }),
    )
  })

/**
 * Abort: cancel a deferred response, answer the calls of the round not started yet `aborted`,
 * keep a partial as an aborted entry, and end the run `unanswered` with `aborted`. It runs after
 * the round's tool tasks, which the generation owns, are terminal.
 */
const abort =
  (env: Env) =>
  (task: RunningTask<Record<string, never>, Checkpoint>, runtime: GenerationRuntime) =>
    Effect.gen(function* () {
      const { checkpoint } = task.state
      if (checkpoint.phase === 'poll')
        yield* Effect.ignore(env.model.cancel(checkpoint.model, checkpoint.handle))
      const { conversationId } = runtime
      const timestamp = yield* Clock.currentTimeMillis
      yield* runtime.commit((tx) =>
        Effect.gen(function* () {
          const unstarted =
            checkpoint.phase === 'tools'
              ? yield* readCalls(tx, checkpoint.assistant, checkpoint.pending)
              : []
          const live = yield* tx.doc(Live.document, conversationId)
          yield* convertPartial(tx, live, conversationId)
          for (const call of unstarted) {
            const aborted = ToolResult.failure('aborted', `Tool ${call.name} was aborted`)
            yield* ToolResult.append(tx, conversationId, call, aborted, { timestamp })
          }
          yield* endRun(tx, live, runtime.taskId, { status: 'unanswered', reason: 'aborted' })
          const next: Next = { status: 'terminal', outcome: { status: 'aborted' } }
          return next
        }),
      )
    })

export const definition = (env: Env): Definition<Record<string, never>, Checkpoint, Result> => ({
  name: RUN_KIND,
  version: 1,
  initial: () => ({ phase: 'prepare', attempt: 1 }),
  phases: {
    prepare: preparePhase(env),
    request: requestPhase(env),
    retry: retryPhase,
    poll: pollPhase(env),
    tools: toolsPhase(env),
  },
  abort: abort(env),
})
