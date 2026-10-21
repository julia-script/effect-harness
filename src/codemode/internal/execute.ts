/**
 * One execution of the code mode tool: a first run, or a rerun after a restart that replays the
 * journal. A rerun refuses to replay when the version changed or the deadline passed; a decided
 * outcome is returned without starting the VM.
 */
import type {
  CodemodeResult,
  CodemodeRun,
  CodemodeSandboxBase,
} from '@earendil-works/pi-codemode/portable'
import { parseCodemodeSource } from '@earendil-works/pi-codemode/source'
import { Clock, Effect, FiberSet, Option, Queue, Result, Schema } from 'effect'
import * as Agent from '../../harness/Agent.ts'
import * as Call from '../../tool/Call.ts'
import type { Output } from '../../tool/ToolResult.ts'
import { output } from '../../tool/ToolResult.ts'
import type { Namespace } from './describe.ts'
import type { Files } from './files.ts'
import { writer } from './files.ts'
import type { Global } from './launch.ts'
import type { Event, Machine } from './machine.ts'
import { decide, details, loop, settle } from './machine.ts'
import { errorResult } from './outcome.ts'
import { format, OUTPUT_LIMITS } from './output.ts'
import { commitJournal, handle } from './replay.ts'
import type { Found } from './start.ts'
import { fresh, load } from './start.ts'
import type { Stored } from './state.ts'
import { draft, store, VERSION } from './state.ts'

export interface Deps {
  readonly sandbox: CodemodeSandboxBase
  readonly namespaces: ReadonlyArray<Namespace>
  readonly globals: ReadonlyArray<Global>
  readonly files: Files
}

type Attempt = Extract<Found, { readonly start: unknown }>

const RESUMED = 'The script took a different path when resumed after a restart'

/** Whether a rerun's script ended where the live run went on before. */
const endedEarly = (m: Machine, result: CodemodeResult) => {
  const during = result.during
  if (!m.rerun || during === undefined || during.seq > m.replayed.length) return false
  const acknowledged =
    during.seq === 0
      ? m.replayed.length > 0 || m.firstIssued !== undefined
      : m.replayed[during.seq - 1]?.issuedAfter !== undefined
  return acknowledged || m.replayed.slice(during.seq).some((entry) => entry.id > during.issued)
}

/** The script's own outcome, stored with its store writes in one commit. */
const complete = Effect.fnUntraced(function* (m: Machine, deps: Deps, result: CodemodeResult) {
  const shown = details(m)
  const formatted = yield* format(result, {
    wallMs: (yield* Clock.currentTimeMillis) - m.start.startedAt,
    maxOutputTokens: m.start.maxOutputTokens,
    calls: shown.calls,
    limits: OUTPUT_LIMITS,
    write: yield* writer(m.call, deps.files),
  })
  const full = formatted.fullOutputPath
  const stored: Stored = {
    isError: !result.ok,
    output: output(formatted.content, {
      details: full === undefined ? shown : { ...shown, fullOutputPath: full },
    }),
  }
  const writes = result.ok ? result.storeWrites : undefined
  return yield* settle(m.call, stored, (tx) =>
    Effect.gen(function* () {
      if (writes === undefined) return
      const values = (yield* tx.doc(store, m.call.conversationId)).values
      for (const key of writes.delete) delete values[key]
      for (const [key, value] of Object.entries(writes.set)) {
        const json = toJson(value)
        if (Option.isSome(json)) values[key] = draft(json.value)
      }
    }),
  )
})

const toJson = Schema.decodeUnknownOption(Schema.Json)

const sandboxFailure = (message: string): CodemodeResult => ({
  ok: false,
  error: { kind: 'sandbox', message },
  output: [],
  calls: [],
})

/** The machine of one attempt, over the VM `run` it starts. */
const makeMachine = (
  call: Call.Service,
  found: Attempt,
  parts: Pick<Machine, 'run' | 'controller' | 'globals' | 'offer' | 'fork'>,
): Machine => ({
  ...parts,
  call,
  start: found.start,
  replayed: found.journal.entries,
  replayIndex: new Map(found.journal.entries.map((entry, index) => [entry.id, index])),
  rerun: found.rerun,
  firstIssued: found.journal.firstIssued,
  journalLength: found.journal.entries.length,
  nextReplayed: 0,
  replaying: found.journal.entries.length > 0,
  released: false,
  durableIssued: found.journal.entries.reduce(
    (max, entry) => Math.max(max, entry.issuedAfter ?? 0),
    found.journal.firstIssued ?? 0,
  ),
  issued: 0,
  waiting: [],
  held: [],
  ready: new Map(),
  queue: [],
  acks: new Map(),
  wantCommit: false,
  ended: undefined,
  rows: new Map(),
})

/** Start the VM on the start data; its callbacks become events. */
const startVm = (
  deps: Deps,
  code: string,
  start: Attempt['start'],
  signal: AbortSignal,
  offer: (event: Event) => void,
  remaining: number | undefined,
) =>
  Effect.sync(() =>
    deps.sandbox.start(code, {
      startedAt: start.startedAt,
      seed: start.seed,
      tools: start.tools.map(({ name, jsName, description }) => ({ name, jsName, description })),
      globals: start.globals.map((global) => ({ ...global })),
      store: start.store,
      signal,
      ...(remaining === undefined ? {} : { timeoutMs: remaining }),
      onStarted: (fingerprint) => {
        offer({ type: 'started', fingerprint })
      },
      onCall: (made) => {
        offer({ type: 'call', call: made })
      },
      onIdle: (seq, issued) => {
        offer({ type: 'idle', seq, issued })
      },
    }),
  )

/** The VM's result as the last event. */
const watchResult = (vm: CodemodeRun, offer: (event: Event) => void) =>
  Effect.forkScoped(
    Effect.tryPromise(() => vm.result).pipe(
      Effect.catch((error) => Effect.succeed(sandboxFailure(error.message))),
      Effect.flatMap((result) =>
        Effect.sync(() => {
          offer({ type: 'finished', result })
        }),
      ),
    ),
  )

/** Start the VM and run the machine until the run has its result; the machine and the result. */
const runScript = Effect.fnUntraced(function* (
  deps: Deps,
  call: Call.Service,
  code: string,
  found: Attempt,
  remaining?: number,
) {
  const events = yield* Queue.unbounded<Event>()
  const offer = (event: Event) => {
    Queue.offerUnsafe(events, event)
  }
  // The VM's own signal: aborted on a divergence as well as when this attempt ends.
  // oxlint-disable-next-line effecttsgo/abort-controller-in-effect
  const controller = new AbortController()
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      controller.abort()
    }),
  )
  const fibers = yield* FiberSet.make<void, never>()
  const vm = yield* startVm(deps, code, found.start, controller.signal, offer, remaining)
  yield* watchResult(vm, offer)
  const m = makeMachine(call, found, {
    run: vm,
    controller,
    globals: deps.globals,
    offer,
    fork: (effect) => Effect.asVoid(FiberSet.run(fibers, effect)),
  })
  return { m, result: yield* loop(m, events, handle, commitJournal) }
})

/** Run an attempt to its outcome. */
const attempt = Effect.fnUntraced(function* (
  deps: Deps,
  call: Call.Service,
  code: string,
  found: Attempt,
) {
  if (found.start.version !== VERSION) {
    const message = `The script cannot resume: it started under code mode version ${found.start.version}, this is ${VERSION}.`
    return yield* settle(call, errorResult(message))
  }
  const now = yield* Clock.currentTimeMillis
  const remaining = found.start.deadline === undefined ? undefined : found.start.deadline - now
  if (remaining !== undefined && remaining <= 0)
    return yield* settle(
      call,
      errorResult('Script timed out: its deadline passed while it was interrupted.'),
    )
  const { m, result } = yield* runScript(deps, call, code, found, remaining)
  if (m.ended !== undefined) return m.ended
  // Aborted, but not by this call: the host closed the sandbox, say while shutting down. That
  // decides nothing about the script: store nothing and wait to be interrupted, so a later
  // attempt replays it.
  if (!result.ok && result.error.kind === 'aborted' && !m.controller.signal.aborted)
    return yield* Effect.never
  if (endedEarly(m, result)) {
    const at = result.during?.seq ?? 0
    return yield* decide(
      m,
      errorResult(`${RESUMED}: it ended after result ${at}, where it went on before.`),
    )
  }
  return yield* complete(m, deps, result)
}, Effect.scoped)

const refuse = (message: string): Stored => errorResult(message)

/** The outcome of one call of the code mode tool for `source`. */
const outcome = Effect.fnUntraced(function* (deps: Deps, source: string) {
  const parsed = Result.try(() => parseCodemodeSource(source))
  if (Result.isFailure(parsed)) return refuse(`Invalid script: ${String(parsed.failure)}`)
  const agent = yield* Agent.Current
  const call = yield* Call.Current
  if (agent === undefined || call === undefined)
    return refuse('codemode runs only for the calls of a conversation')
  const start = yield* fresh(agent, deps, parsed.success.options)
  const found = yield* load(call, start)
  if ('done' in found) return found.done
  return yield* attempt(deps, call, parsed.success.code, found)
})

/** The handler of the code mode tool. */
export const execute =
  (deps: Deps) =>
  ({ code }: { readonly code: string }): Effect.Effect<Output, Output> =>
    Effect.flatMap(Effect.orDie(outcome(deps, code)), (stored) =>
      stored.isError ? Effect.fail(stored.output) : Effect.succeed(stored.output),
    )
