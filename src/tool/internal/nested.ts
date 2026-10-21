/**
 * Nested calls of a running tool call (see `Call`): admission, waiting, and the outcome a caller
 * gets; and settling a nested call into its caller's `NestedCalls` member.
 */
import { Effect, Option, Schema } from 'effect'
import type { ToolCall } from '../../model/Message.ts'
import type { Tx } from '../../session/Session.ts'
import { TaskId } from '../../storage/schema/Ids.ts'
import type { JsonObject } from '../../storage/schema/Json.ts'
import type { TaskOutcome } from '../../storage/schema/Task.ts'
import type { Runtime } from '../../task/Task.ts'
import type * as Call from '../Call.ts'
import { NestedCallRefused } from '../ToolError.ts'
import * as ToolResult from '../ToolResult.ts'
import type { Checkpoint, Input, NestedInput, Result as TaskResult } from '../ToolTask.ts'
import { kind, NestedCalls, NestedInput as NestedInputSchema } from '../ToolTask.ts'
import type { Ran } from './execute.ts'

type ToolRuntime = Runtime<Input, Checkpoint, TaskResult>

const KEY = /^[\w.:-]+$/u

export const isNested = (input: Input): input is NestedInput => 'kind' in input

/** Lazy: `ToolTask` imports this module before it defines the Schema. */
const decodeNested = (input: unknown) => Schema.decodeUnknownOption(NestedInputSchema)(input)

const sameCall = (input: unknown, name: string, args: JsonObject) => {
  const nested = decodeNested(input)
  return (
    Option.isSome(nested) &&
    nested.value.call.name === name &&
    JSON.stringify(nested.value.call.arguments) === JSON.stringify(args)
  )
}

interface Request {
  readonly key: string
  readonly name: string
  readonly args: JsonObject
}

/** In one commit: the task already made under the key, or a new one with its member. */
const admitIn = Effect.fnUntraced(function* (
  tx: Tx,
  runtime: ToolRuntime,
  call: ToolCall,
  { key, name, args }: Request,
  abandonOnRestart: boolean,
) {
  const state = yield* tx.doc(NestedCalls, runtime.taskId, key, null)
  if (state.taskId !== undefined) {
    const id = TaskId.make(state.taskId)
    const record = yield* tx.task(id)
    if (sameCall(record?.input, name, args)) return id
    return yield* new NestedCallRefused({
      message: `Nested call ${key} of ${call.id} was already made with another tool or other arguments`,
    })
  }
  const input: NestedInput = {
    kind: 'nested',
    parent: runtime.taskId,
    parentCallId: call.id,
    key,
    call: { type: 'toolCall', id: `${call.id}/${key}`, name, arguments: args },
  }
  const id = yield* tx.createTask(kind, input, {
    ownership: { kind: 'task', taskId: runtime.taskId },
    ...(abandonOnRestart ? { abandonOnRestart: true } : {}),
  })
  state.taskId = id
  return id
})

/**
 * The task of nested call `request.key`: the one already made under it, which must be of the same
 * tool and arguments, or a new one the caller owns.
 */
const admit = (runtime: ToolRuntime, call: ToolCall, request: Request, abandonOnRestart: boolean) =>
  Effect.suspend(() => {
    let admitted: TaskId | undefined
    return runtime
      .commit((tx) =>
        Effect.map(admitIn(tx, runtime, call, request, abandonOnRestart), (id) => {
          admitted = id
        }),
      )
      .pipe(Effect.flatMap(() => Effect.orDie(Effect.fromNullishOr(admitted))))
  })

/** Commit `change` bound to the task, leaving its state; what `change` returned. */
const commitWith = <A, E>(runtime: ToolRuntime, change: (tx: Tx) => Effect.Effect<A, E>) =>
  Effect.suspend(() => {
    let value: Option.Option<A> = Option.none()
    return runtime
      .commit((tx) =>
        Effect.map(change(tx), (result) => {
          value = Option.some(result)
        }),
      )
      .pipe(Effect.flatMap(() => Effect.orDie(Effect.fromOption(value))))
  })

/** What a caller gets for a nested call that stored no outcome: one the scheduler ended. */
const fallback = (name: string, outcome: TaskOutcome): Call.Outcome => {
  const message =
    outcome.status === 'aborted'
      ? `Tool ${name} was aborted`
      : `Tool ${name} ended ${outcome.status} without a result`
  return { isError: true, diagnostics: [ToolResult.error(outcome.status, message)] }
}

/** The nested calls of one run of `call`; `end` refuses later ones. */
export const make = (runtime: ToolRuntime, call: ToolCall, replay: 'safe' | 'unsafe') => {
  let sequence = 0
  let ended = false
  const execute: Call.Service['execute'] = Effect.fnUntraced(function* (name, args, options = {}) {
    if (ended) return yield* new NestedCallRefused({ message: `Tool call ${call.id} has settled` })
    const key = options.key ?? String(++sequence)
    if (!KEY.test(key))
      return yield* new NestedCallRefused({ message: `Invalid nested call key ${key}` })
    // A caller that is not replay-safe never resumes, so a restart abandons its calls.
    const id = yield* admit(runtime, call, { key, name, args }, replay === 'unsafe')
    const settled = yield* runtime.waitForTask(id)
    const state = yield* runtime.snapshot(NestedCalls, runtime.taskId, key)
    return state?.result ?? fallback(name, settled.state.outcome)
  })
  const service: Call.Service = {
    taskId: runtime.taskId,
    conversationId: runtime.conversationId,
    callId: call.id,
    execute,
    commit: (change) => commitWith(runtime, change),
    snapshot: runtime.snapshot,
    getTask: runtime.getTask,
  }
  return {
    service,
    /** Refuse later calls, and abort the calls still running, before the caller settles. */
    end: Effect.gen(function* () {
      ended = true
      const owned = yield* runtime.ownedTasks
      for (const record of owned) {
        if (record.kind === kind.name && Option.isSome(decodeNested(record.input)))
          yield* runtime.abortOwned(record.id)
      }
    }),
  }
}

/** The outcome a nested call stores, from what it ran to. */
export const outcomeOf = (ran: Ran): Call.Outcome => {
  const { result } = ran
  return {
    isError: result.isError,
    ...(ran.value === undefined ? {} : { value: ran.value }),
    diagnostics: result.diagnostics,
    ...(result.details === undefined ? {} : { details: result.details }),
  }
}

/** In the nested call's terminal commit: its outcome, in its caller's member. */
export const settleNested = (tx: Tx, input: NestedInput, ran: Ran) =>
  Effect.map(tx.doc(NestedCalls, input.parent, input.key, null), (state) => {
    const outcome = outcomeOf(ran)
    state.result = {
      ...outcome,
      diagnostics: outcome.diagnostics.map((each) => Object.assign({}, each)),
    }
  })
