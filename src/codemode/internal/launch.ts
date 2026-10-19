/**
 * Running a script's calls: tools as nested calls keyed `c<VM call id>`, which reattach to their
 * tasks after a restart, and globals, whose outcomes the journal keeps. Each runs in a forked
 * fiber and reports back to the machine as an event.
 */
import type { CodemodeHostCall } from '@earendil-works/pi-codemode/portable'
import { Clock, Effect, Option, Schema } from 'effect'
import { JsonObject } from '../../storage/schema/Json.ts'
import type * as Call from '../../tool/Call.ts'
import { NestedCallRefused } from '../../tool/ToolError.ts'
import { discover, GLOBALS as DISCOVERY } from './discover.ts'
import type { Done, Event, Machine } from './machine.ts'
import { describeCall, outcomeOf } from './outcome.ts'
import type { Outcome } from './state.ts'

/** A function scripts call as a top-level global; its outcome is journaled. */
export interface Global {
  /** An identifier, or `namespace.member`. */
  readonly name: string
  /** `execute` gets the argument list instead of the first argument. */
  readonly spread?: boolean
  /** Fails with the message the script's promise rejects with. */
  readonly execute: (args: unknown) => Effect.Effect<Schema.Json | undefined, string>
}

const decodeArgs = Schema.decodeUnknownOption(JsonObject)

/** A tool call's outcome, and the nested call's own for the details. */
const callTool = (m: Machine, call: CodemodeHostCall) =>
  Effect.gen(function* () {
    const args = decodeArgs(call.args ?? {})
    if (Option.isNone(args)) {
      const outcome: Outcome = { ok: false, message: `tools.${call.name}() expects an object` }
      return { outcome }
    }
    const nested = yield* m.call.execute(call.name, args.value, { key: `c${call.id}` })
    const structured = m.start.tools.find((tool) => tool.name === call.name)?.structured === true
    return { outcome: outcomeOf(call.name, structured, nested), nested }
  })

const callGlobal = (m: Machine, call: CodemodeHostCall): Effect.Effect<Outcome> => {
  if (DISCOVERY.includes(call.name)) return Effect.succeed(discover(m.start, call.name, call.args))
  const global = m.globals.find((candidate) => candidate.name === call.name)
  if (global === undefined)
    return Effect.succeed({ ok: false, message: `Unknown global ${call.name}` })
  return global.execute(call.args).pipe(
    Effect.map((value): Outcome => (value === undefined ? { ok: true } : { ok: true, value })),
    Effect.catch((message) => Effect.succeed<Outcome>({ ok: false, message })),
  )
}

/** Report what `running` came to; a refused key means this attempt took another path. */
const report = (m: Machine, call: CodemodeHostCall, running: Effect.Effect<Event, Call.Failure>) =>
  running.pipe(
    Effect.catch((error) =>
      Effect.succeed<Event>(
        error instanceof NestedCallRefused
          ? {
              type: 'diverged',
              message: `call ${call.id} is ${describeCall(call)}: ${error.message}`,
            }
          : { type: 'failed', error },
      ),
    ),
    Effect.flatMap((event) =>
      Effect.sync(() => {
        m.offer(event)
      }),
    ),
  )

const doneOf = (call: CodemodeHostCall, outcome: Outcome, global: boolean) =>
  Effect.map(Clock.currentTimeMillis, (at): Done => {
    if (!global) return { id: call.id, at, outcome }
    const args = Option.getOrUndefined(Schema.decodeUnknownOption(Schema.Json)(call.args))
    const record =
      args === undefined ? { name: call.name, outcome } : { name: call.name, args, outcome }
    return { id: call.id, at, outcome, global: record }
  })

/** Run one live call; its result goes to the journal. */
export const launch = (m: Machine, call: CodemodeHostCall) =>
  m.fork(
    report(
      m,
      call,
      call.target === 'tool'
        ? Effect.flatMap(callTool(m, call), ({ outcome, nested }) =>
            Effect.map(doneOf(call, outcome, false), (done): Event => ({
              type: 'done',
              done,
              ...(nested === undefined ? {} : { nested }),
            })),
          )
        : Effect.flatMap(callGlobal(m, call), (outcome) =>
            Effect.map(doneOf(call, outcome, true), (done): Event => ({ type: 'done', done })),
          ),
    ),
  )

/** Reattach a journaled tool call to its nested call, for its stored result. */
export const replayTool = (m: Machine, call: CodemodeHostCall) =>
  m.fork(
    report(
      m,
      call,
      Effect.map(callTool(m, call), ({ outcome, nested }): Event => ({
        type: 'replayed',
        done: { id: call.id, at: 0, outcome },
        ...(nested === undefined ? {} : { nested }),
      })),
    ),
  )
