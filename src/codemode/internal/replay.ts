/**
 * The machine's event handlers: the fingerprint gate, calls checked against the journal while
 * replaying, acknowledged counts, the release of held calls, and the journal commits.
 *
 * A rerun refuses to replay, ending the call with an error result, when the VM's fingerprint
 * changed or the script diverges: it makes another call, or calls in another order, than the
 * journal says.
 */
import type { CodemodeHostCall } from '@earendil-works/pi-codemode/portable'
import { Effect, Equal, Option, Schema } from 'effect'
import { NestedCalls, NestedInput } from '../../tool/ToolTask.ts'
import { TaskId } from '../../storage/schema/Ids.ts'
import type { RuntimeError } from '../../task/Task.ts'
import type { Event, Machine } from './machine.ts'
import { deliverReplayed, diverge, end, finishRow, started, stopped } from './machine.ts'
import { launch, replayTool } from './launch.ts'
import { describeCall, toSandbox } from './outcome.ts'
import { draft, journal, run } from './state.ts'

/** Launch the waiting calls whose issued count is committed. */
const launchCovered = Effect.fnUntraced(function* (m: Machine) {
  if (!m.released || stopped(m)) return
  const covered = m.waiting.filter((call) => call.id <= m.durableIssued)
  m.waiting = m.waiting.filter((call) => call.id > m.durableIssued)
  for (const call of covered) yield* launch(m, call)
})

const decodeNested = Schema.decodeUnknownOption(NestedInput)

/** The call an earlier attempt made under this call's key, when it is another one. */
const earlier = Effect.fnUntraced(function* (m: Machine, call: CodemodeHostCall) {
  const member = yield* m.call.snapshot(NestedCalls, m.call.taskId, `c${call.id}`)
  if (member?.taskId === undefined) return Option.none<string>()
  const record = yield* m.call.getTask(TaskId.make(member.taskId))
  return Option.flatMap(decodeNested(record?.input), ({ call: made }) => {
    const same =
      call.target === 'tool' &&
      made.name === call.name &&
      Equal.equals(made.arguments, call.args ?? {})
    return same
      ? Option.none()
      : Option.some(`tools.${made.name}(${JSON.stringify(made.arguments)})`)
  })
})

/**
 * Let the held calls launch, and later live calls, each once its issued count is committed. On a
 * rerun, an earlier attempt may have admitted some of them under their keys as other calls: the
 * replay releases once its counts matched every recorded one, so it has made every call an
 * earlier attempt could have admitted. All held calls are checked first, and a divergence
 * launches none.
 */
const release = Effect.fnUntraced(function* (m: Machine) {
  if (stopped(m) || m.released) return
  if (m.rerun) {
    for (const call of m.held) {
      const other = yield* earlier(m, call)
      if (Option.isSome(other)) {
        yield* diverge(m, `call ${call.id} is ${describeCall(call)}, but was ${other.value}`)
        return
      }
    }
  }
  m.released = true
  m.waiting.push(...m.held.splice(0))
  yield* launchCovered(m)
})

/** Check the VM's fingerprint, or record it; nothing is delivered or admitted before. */
const checkFingerprint = Effect.fnUntraced(function* (m: Machine, fingerprint: string) {
  const recorded = m.start.fingerprint
  if (recorded !== undefined && recorded !== fingerprint) {
    const change = `${recorded} -> ${fingerprint}`
    yield* end(m, `The script cannot resume: the sandbox changed since it started (${change}).`)
    return
  }
  if (recorded === undefined)
    yield* m.call.commit((tx) =>
      Effect.map(tx.doc(run, m.call.taskId), (state) => {
        if (state.start !== undefined) state.start.fingerprint = fingerprint
      }),
    )
  // A rerun without a journal releases its calls at the script's first acknowledgment.
  if (m.replaying) deliverReplayed(m)
  else if (!m.rerun) yield* release(m)
})

/** A journaled call made again: it must be the same call. */
const replayCall = Effect.fnUntraced(function* (m: Machine, call: CodemodeHostCall, index: number) {
  const entry = m.replayed[index]
  if (entry === undefined) return
  const global = entry.global
  if (global !== undefined) {
    const same =
      call.target === 'global' && call.name === global.name && Equal.equals(call.args, global.args)
    if (!same) {
      yield* diverge(
        m,
        `call ${call.id} is ${describeCall(call)}, but was a call to ${global.name}`,
      )
      return
    }
    m.ready.set(call.id, global.outcome)
    deliverReplayed(m)
    return
  }
  if (call.target === 'tool') yield* replayTool(m, call)
  else yield* diverge(m, `call ${call.id} is ${describeCall(call)}, but was a tool call`)
})

const onCall = Effect.fnUntraced(function* (m: Machine, call: CodemodeHostCall) {
  // Before the check: the VM made the call before its run finished, whenever it is handled.
  if (call.target === 'tool') started(m, call)
  if (stopped(m)) return
  m.issued = Math.max(m.issued, call.id)
  const index = m.replaying ? m.replayIndex.get(call.id) : undefined
  if (index !== undefined) {
    yield* replayCall(m, call, index)
    return
  }
  if (!m.released) {
    m.held.push(call)
    return
  }
  m.waiting.push(call)
  yield* launchCovered(m)
})

/** A count an earlier attempt recorded for result `seq`, which must match. */
const recordedCount = (m: Machine, seq: number) =>
  seq === 0 ? m.firstIssued : m.replayed[seq - 1]?.issuedAfter

const mismatch = (seq: number, issued: number, recorded: number) =>
  seq === 0
    ? `its first run made ${issued} calls, where it had made ${recorded}`
    : `after result ${seq} it had made ${issued} calls, where it had made ${recorded}`

/** The VM processed result `seq` and waits, having issued `issued` calls. */
const onIdle = Effect.fnUntraced(function* (m: Machine, seq: number, issued: number) {
  if (stopped(m)) return
  const recorded = m.rerun && seq <= m.replayed.length ? recordedCount(m, seq) : undefined
  if (recorded !== undefined && recorded !== issued) {
    yield* diverge(m, mismatch(seq, issued, recorded))
    return
  }
  m.acks.set(seq, issued)
  if (!m.replaying) {
    // A rerun without a journal: every call an earlier attempt could have admitted is made.
    if (seq === 0 && m.rerun) yield* release(m)
    // At once when the VM processed everything delivered: a restart usually finds it here.
    if (seq === m.journalLength) m.wantCommit = true
    return
  }
  if (seq !== m.nextReplayed) return
  const next = m.replayed[seq]
  if (next !== undefined) {
    if (next.id > issued)
      yield* diverge(m, `it waits without making call ${next.id}, which it made before`)
    return
  }
  // Caught up with the journal: continue live.
  m.replaying = false
  m.wantCommit = true
  yield* release(m)
})

/**
 * Journal the finished calls and the acknowledged counts in one commit, then deliver the calls
 * and launch those the counts cover.
 */
export const commitJournal = Effect.fnUntraced(function* (m: Machine) {
  if (stopped(m) || (m.queue.length === 0 && m.acks.size === 0)) return
  const batch = m.queue.splice(0)
  const acks = [...m.acks]
  m.acks.clear()
  yield* m.call.commit((tx) =>
    Effect.map(tx.doc(journal, m.call.taskId), (state) => {
      for (const [seq, issued] of acks) {
        if (seq === 0) state.firstIssued ??= issued
        else {
          const entry = state.entries[seq - 1]
          if (entry !== undefined && entry.issuedAfter === undefined) entry.issuedAfter = issued
        }
      }
      for (const { id, at, global } of batch)
        state.entries.push(draft(global === undefined ? { id, at } : { id, at, global }))
    }),
  )
  for (const [, issued] of acks) m.durableIssued = Math.max(m.durableIssued, issued)
  for (const item of batch) {
    m.journalLength += 1
    if (!m.run.finished) m.run.deliver(m.journalLength, item.at, item.id, toSandbox(item.outcome))
  }
  yield* launchCovered(m)
})

/** A call's outcome arrived. */
const onResult = (m: Machine, event: Extract<Event, { readonly type: 'replayed' | 'done' }>) => {
  if (stopped(m)) return
  finishRow(m, event.done, event.nested)
  if (event.type === 'replayed') {
    m.ready.set(event.done.id, event.done.outcome)
    deliverReplayed(m)
  } else {
    m.queue.push(event.done)
    m.wantCommit = true
  }
}

/** Handle one event. */
export const handle = (m: Machine, event: Event): Effect.Effect<void, RuntimeError> => {
  if (event.type === 'started') return checkFingerprint(m, event.fingerprint)
  if (event.type === 'call') return onCall(m, event.call)
  if (event.type === 'idle') return onIdle(m, event.seq, event.issued)
  if (event.type === 'diverged') return diverge(m, event.message)
  if (event.type === 'failed') return stopped(m) ? Effect.void : Effect.fail(event.error)
  if (event.type === 'finished') return Effect.void
  return Effect.sync(() => {
    onResult(m, event)
  })
}
