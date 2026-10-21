import { Deferred, Effect } from 'effect'
import type { TaskId } from '../../storage/schema/Ids.ts'
import { loadScopes } from './chains.ts'
import { terminate } from './invocation.ts'
import { resolve } from './resolve.ts'
import { withAbortMark } from './state.ts'
import { readsFirst } from '../../internal/readsFirst.ts'
import type { State } from './state.ts'
import { hasOwnedLive } from './walk.ts'
import { notFound } from './wait.ts'

/**
 * Commit the abort mark, or settle a task no definition can take as `orphaned` when no invocation
 * runs it and nothing it owns is live; then wait for the run invocation the observer interrupted.
 * The abort invocation starts once the task's ordinary owned work is gone. A `completing` task is
 * only marked. A request replaces a `restart` mark; with `keepRestart`, as for an owner's own
 * cleanup, a task abandoned after a restart keeps its mark and waits on for its definition.
 */
export const abort = Effect.fnUntraced(function* (state: State, id: TaskId, keepRestart = false) {
  const marked = yield* readsFirst(
    state.session.commit((tx) =>
      Effect.gen(function* () {
        const current = yield* tx.task(id)
        if (current === undefined) return yield* notFound(id)
        if (current.state.status === 'terminal') return { result: 'terminal' as const }
        const invocation = state.invocations.get(id)
        // A restart-marked task has no run invocation; it only waits for its abort.
        if (keepRestart && current.abortReason === 'restart') return { result: 'marked' as const }
        if (invocation === undefined && current.state.status !== 'completing') {
          yield* loadScopes(state)
          const resolution = hasOwnedLive(state, id)
            ? undefined
            : yield* resolve(state, current, yield* state.registry.snapshot)
          if (resolution?.kind === 'blocked') {
            const outcome = { status: 'orphaned', reason: resolution.reason } as const
            yield* terminate(state, tx, current, outcome)
            return { result: 'marked' as const }
          }
        }
        if (!current.abortRequested || current.abortReason !== undefined) {
          yield* tx.setTask(withAbortMark(current))
        }
        return {
          result: 'marked' as const,
          run: invocation?.mode === 'run' ? invocation : undefined,
        }
      }),
    ),
  )
  if ('run' in marked && marked.run !== undefined) yield* Deferred.await(marked.run.done)
  return marked.result
})
