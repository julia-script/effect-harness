/**
 * The host side of one script run, ported from pi's `CodemodeCall`: the VM's callbacks arrive as
 * events on a queue, and one fiber handles them in order, so a check, a commit or a delivery never
 * interleaves with another. Calls run in forked fibers that report back as events.
 *
 * A live result is delivered only after its journal commit, and each result's acknowledged call
 * count is committed with the next journal commit, or at once when the VM has processed
 * everything delivered. A call launches only once the count of calls issued when it was made is
 * committed, so every key an earlier attempt admitted is within its last recorded count.
 */
import type {
  CodemodeHostCall,
  CodemodeResult,
  CodemodeRun,
} from '@earendil-works/pi-codemode/portable'
import { Effect, Queue, Schema } from 'effect'
import type { Tx } from '../../session/Session.ts'
import type * as Call from '../../tool/Call.ts'
import type { RuntimeError } from '../../task/Task.ts'
import type { Global } from './launch.ts'
import { cut, errorResult, toSandbox } from './outcome.ts'
import type { Details, JournalEntry, Outcome, Row, Start, Stored } from './state.ts'
import { draft, run as runDocument, Stored as StoredSchema } from './state.ts'

/** A finished live call waiting for its journal commit. */
export interface Done {
  readonly id: number
  readonly at: number
  readonly outcome: Outcome
  readonly global?: JournalEntry['global']
}

export type Event =
  | { readonly type: 'started'; readonly fingerprint: string }
  | { readonly type: 'call'; readonly call: CodemodeHostCall }
  | { readonly type: 'idle'; readonly seq: number; readonly issued: number }
  | { readonly type: 'replayed'; readonly done: Done; readonly nested?: Call.Outcome }
  | { readonly type: 'done'; readonly done: Done; readonly nested?: Call.Outcome }
  | { readonly type: 'diverged'; readonly message: string }
  | { readonly type: 'failed'; readonly error: RuntimeError }
  | { readonly type: 'finished'; readonly result: CodemodeResult }

export interface Machine {
  readonly call: Call.Service
  readonly start: Start
  readonly run: CodemodeRun
  readonly controller: AbortController
  readonly globals: ReadonlyArray<Global>
  readonly offer: (event: Event) => void
  readonly fork: (effect: Effect.Effect<void>) => Effect.Effect<void>
  /** The journal as this attempt found it; replay delivers exactly these. */
  readonly replayed: ReadonlyArray<JournalEntry>
  /** Journal index of each replayed call ID. */
  readonly replayIndex: ReadonlyMap<number, number>
  /** Whether an earlier attempt ran this call: its nested call keys may already exist. */
  readonly rerun: boolean
  /** The count an earlier attempt recorded after the script's first run. */
  readonly firstIssued: number | undefined
  /** Entries in the journal, replayed and live; the next delivery is `length + 1`. */
  journalLength: number
  /** Replayed entries delivered so far. */
  nextReplayed: number
  replaying: boolean
  /** Whether live calls may launch; before, they are held (see `release`). */
  released: boolean
  /** The highest issued count known to be committed: calls up to it may launch. */
  durableIssued: number
  /** Calls the VM issued so far; its call IDs are 1, 2, ... */
  issued: number
  /** Live calls waiting for their issued count to be committed. */
  waiting: Array<CodemodeHostCall>
  /** Calls made during replay that the journal does not have, admitted once it is done. */
  readonly held: Array<CodemodeHostCall>
  /** Replay: results of journaled calls, ready to deliver in journal order. */
  readonly ready: Map<number, Outcome>
  /** Finished live calls waiting for their journal commit. */
  readonly queue: Array<Done>
  /** Acknowledged counts to record, by result number (0: the script's first run). */
  readonly acks: Map<number, number>
  /** A journal commit was asked for. */
  wantCommit: boolean
  /** The outcome the call decided instead of the script's own. */
  ended: Stored | undefined
  /** The script's tool calls for the details, by VM call ID. */
  readonly rows: Map<number, Row>
}

/** Whether nothing more may happen: the run has its outcome, or the call decided one. */
export const stopped = (m: Machine) => m.ended !== undefined || m.run.finished

/** The script's tool calls in call order; those still running were cancelled. */
export const details = (m: Machine): Details => ({
  calls: [...m.rows.values()]
    .toSorted((a, b) => a.id - b.id)
    .map((row) =>
      row.status === 'running' ? Object.assign({}, row, { status: 'cancelled' as const }) : row,
    ),
})

/**
 * Write `stored` as the outcome, with `also` in the same commit, unless an outcome was decided
 * before; the one that stands.
 */
export const settle = <E = never>(
  call: Call.Service,
  stored: Stored,
  also: (tx: Tx) => Effect.Effect<void, E> = () => Effect.void,
) =>
  call
    .commit((tx) =>
      Effect.gen(function* () {
        const state = yield* tx.doc(runDocument, call.taskId)
        if (state.result === undefined) {
          state.result = draft(stored)
          yield* also(tx)
        }
        return JSON.stringify(state.result)
      }),
    )
    .pipe(Effect.flatMap((text) => Effect.orDie(decodeStored(text))))

/** Decide `stored` as the call's outcome, with the script's calls so far as its details. */
export const decide = (m: Machine, stored: Stored) =>
  settle(m.call, {
    isError: stored.isError,
    output: Object.assign({}, stored.output, { details: details(m) }),
  })

const decodeStored = Schema.decodeEffect(Schema.fromJsonString(StoredSchema))

/** End the call with `message`, then stop the VM with an error the script cannot catch. */
export const end = Effect.fnUntraced(function* (m: Machine, message: string) {
  m.ended = errorResult(message)
  m.ended = yield* decide(m, m.ended)
  m.controller.abort(new Error(message))
})

/** The script took another path than the journal says: end the call. */
export const diverge = (m: Machine, message: string) =>
  stopped(m)
    ? Effect.void
    : end(m, `The script took a different path when resumed after a restart: ${message}`)

/** Replay: deliver journaled results in journal order, each once the VM has issued its call. */
export const deliverReplayed = (m: Machine) => {
  while (m.nextReplayed < m.replayed.length && !stopped(m)) {
    const entry = m.replayed[m.nextReplayed]
    const outcome = entry === undefined ? undefined : m.ready.get(entry.id)
    if (entry === undefined || outcome === undefined || entry.id > m.issued) return
    m.ready.delete(entry.id)
    m.nextReplayed += 1
    m.run.deliver(m.nextReplayed, entry.at, entry.id, toSandbox(outcome))
  }
}

const ARGS_PREVIEW_CHARS = 200
const ERROR_PREVIEW_CHARS = 500

/** The script made a tool call: its row appears, running, also while it waits to launch. */
export const started = (m: Machine, call: CodemodeHostCall) => {
  if (m.rows.has(call.id)) return
  const args = call.args === undefined ? '' : (JSON.stringify(call.args) ?? '')
  m.rows.set(call.id, {
    id: call.id,
    name: call.name,
    args: cut(args, ARGS_PREVIEW_CHARS),
    status: 'running',
  })
}

/** A tool call ended: an error result is an error row, with its error text. */
export const finishRow = (m: Machine, done: Done, nested: Call.Outcome | undefined) => {
  const row = m.rows.get(done.id)
  if (row === undefined || nested === undefined) return
  const errors = nested.diagnostics.filter((each) => each.severity === 'error')
  const text = done.outcome.ok
    ? errors.map((each) => each.message).join('\n')
    : done.outcome.message
  const error = nested.isError && text !== '' ? { error: cut(text, ERROR_PREVIEW_CHARS) } : {}
  m.rows.set(done.id, { ...row, status: nested.isError ? 'error' : 'ok', ...error })
}

/** Take the queued events, in order, until the run has its result. */
export const loop = Effect.fnUntraced(function* (
  m: Machine,
  events: Queue.Dequeue<Event>,
  handle: (m: Machine, event: Event) => Effect.Effect<void, RuntimeError>,
  commit: (m: Machine) => Effect.Effect<void, RuntimeError>,
): Effect.fn.Return<CodemodeResult, RuntimeError> {
  for (;;) {
    const batch = yield* Queue.takeAll(events)
    for (const event of batch) {
      if (event.type === 'finished') return event.result
      yield* handle(m, event)
    }
    if (m.wantCommit) {
      m.wantCommit = false
      yield* commit(m)
    }
  }
})
