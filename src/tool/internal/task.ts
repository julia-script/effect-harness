/* oxlint-disable effecttsgo/any-unknown-in-error-context -- the loop's resolver may fail with anything, and a failure faults the task */
/**
 * The phases of the tool task.
 */
import { Clock, Effect, Exit, Option, Result, Schema } from 'effect'
import { AssistantMessage } from '../../model/Message.ts'
import type { ToolCall } from '../../model/Message.ts'
import type { Tx } from '../../session/Session.ts'
import { JsonObject } from '../../storage/schema/Json.ts'
import type * as Hook from '../../task/Hook.ts'
import type { Definition, NextState, Runtime } from '../../task/Task.ts'
import * as Call from '../Call.ts'
import type { Any, Caller } from '../Tool.ts'
import { CallNotFound } from '../ToolError.ts'
import * as ToolResult from '../ToolResult.ts'
import type {
  Checkpoint,
  Hooks,
  Input,
  Lifecycle,
  Resolution,
  Resolve,
  Result as TaskResult,
} from '../ToolTask.ts'
import { COMPLETED, execute, rebound } from './execute.ts'
import { afterTool, beforeTool } from './hooks.ts'
import type { Ending, Installed, Ran } from './execute.ts'
import * as Nested from './nested.ts'
import { isNested, settleNested } from './nested.ts'

type ToolRuntime = Runtime<Input, Checkpoint, TaskResult>

/** Who made the call: the model, or a running tool. */
const callerOf = (input: Input): Caller => (isNested(input) ? 'tools' : 'model')

export const NAME = '$tool'

const decodeAssistant = Schema.decodeUnknownOption(AssistantMessage)

/** The call `input` names: a nested call's own, or read from its assistant entry. */
const readCall = Effect.fnUntraced(function* (tx: Tx, input: Input) {
  if (isNested(input)) return input.call
  const entry = yield* tx.entry(input.assistant)
  const message = Option.flatMap(Option.fromNullishOr(entry?.model?.[0]), decodeAssistant)
  const call = Option.flatMap(message, ({ content }) =>
    Option.fromNullishOr(
      content.find(
        (block): block is ToolCall => block.type === 'toolCall' && block.id === input.callId,
      ),
    ),
  )
  if (Option.isSome(call)) return call.value
  return yield* new CallNotFound({
    message: `Entry ${input.assistant} has no tool call ${input.callId}`,
    assistant: input.assistant,
    callId: input.callId,
  })
})

/** The terminal state of `ending`, with the call's result entry as the task's result. */
const terminal = (ending: Ending, result: TaskResult): NextState<Checkpoint, TaskResult> => {
  if (ending.status === 'failed')
    return {
      status: 'terminal',
      outcome: { status: 'failed', error: { message: ending.message }, result },
    }
  return { status: 'terminal', outcome: { status: ending.status, result } }
}

/** The task as its lifecycle sees it. */
const callOf = (runtime: ToolRuntime) => ({
  taskId: runtime.taskId,
  conversationId: runtime.conversationId,
})

/**
 * In one commit: append the call's result entry, or store a nested call's outcome with its
 * caller, and end the task.
 */
const settleIn = Effect.fnUntraced(function* (
  tx: Tx,
  runtime: ToolRuntime,
  lifecycle: Lifecycle,
  input: Input,
  call: ToolCall,
  ran: Ran,
) {
  if (isNested(input)) {
    yield* settleNested(tx, input, ran)
    return terminal(ran.ending, { kind: 'nested' })
  }
  const timestamp = yield* Clock.currentTimeMillis
  const timing =
    ran.durationMs === undefined ? { timestamp } : { timestamp, durationMs: ran.durationMs }
  const entry = yield* ToolResult.append(tx, runtime.conversationId, call, ran.result, timing)
  if (lifecycle.settled !== undefined) yield* lifecycle.settled(tx, callOf(runtime), entry.id)
  const { control } = ran.result
  return terminal(ran.ending, {
    entryId: entry.id,
    ...(control === undefined ? {} : { control }),
  })
})

/** Settle with the call read on the line. */
const settle = (
  runtime: ToolRuntime,
  lifecycle: Lifecycle,
  input: Input,
  build: (call: ToolCall) => Ran,
) =>
  runtime.commit((tx) =>
    Effect.flatMap(readCall(tx, input), (call) =>
      settleIn(tx, runtime, lifecycle, input, call, build(call)),
    ),
  )

/** Settle at once with `result`: the call never ran. */
const answer = (
  runtime: ToolRuntime,
  lifecycle: Lifecycle,
  input: Input,
  result: ToolResult.Result,
) => settle(runtime, lifecycle, input, () => ({ result, ending: COMPLETED }))

/** The call, read in a commit of its own. */
const currentCall = (runtime: ToolRuntime, input: Input) =>
  Effect.suspend(() => {
    let call: ToolCall | undefined
    return runtime
      .commit((tx) =>
        Effect.map(readCall(tx, input), (read) => {
          call = read
        }),
      )
      .pipe(Effect.flatMap(() => Effect.fromNullishOr(call)))
  })

/**
 * Run the handler on the arguments of intent, with its nested calls; abort those still running,
 * ask `afterTool`, then settle.
 */
const run = Effect.fnUntraced(function* (
  runtime: ToolRuntime,
  lifecycle: Lifecycle,
  hooks: ReadonlyArray<Partial<Hooks>>,
  input: Input,
  call: ToolCall,
  installed: Installed,
  args: JsonObject,
) {
  const nested = Nested.make(runtime, call, installed.tool.replay)
  const ran = yield* Effect.provideService(
    execute(call, installed, args),
    Call.Current,
    nested.service,
  )
  yield* nested.end
  const asked = { ...call, arguments: args }
  const result = yield* afterTool(hooks, asked, ran.result, runtime)
  const final = result === ran.result ? ran : { ...ran, result: rebound(installed.tool, result) }
  yield* runtime.commit((tx) => settleIn(tx, runtime, lifecycle, input, call, final))
})

const encodeArguments = (tool: Any, params: unknown) =>
  Effect.map(Effect.exit(Schema.encodeUnknownEffect(tool.parameters)(params)), (exit) =>
    Exit.isSuccess(exit) ? Schema.decodeUnknownOption(JsonObject)(exit.value) : Option.none(),
  )

/** Why the call cannot run on `args`, or the arguments it runs with, encoded for the checkpoint. */
const prepare = Effect.fnUntraced(function* (
  call: ToolCall,
  installed: Installed | undefined,
  args: JsonObject,
) {
  if (installed === undefined)
    return ToolResult.failure('tool_unavailable', `Tool ${call.name} is not available`)
  const decoded = yield* Effect.result(Schema.decodeEffect(installed.tool.parameters)(args))
  if (Result.isFailure(decoded)) {
    const message = `Invalid arguments for ${call.name}: ${decoded.failure.message}`
    return ToolResult.failure('invalid_arguments', message)
  }
  const encoded = yield* encodeArguments(installed.tool, decoded.success)
  if (Option.isNone(encoded))
    return ToolResult.failure(
      'invalid_arguments',
      `Arguments of ${call.name} are not a JSON object`,
    )
  return { args: encoded.value, installed }
})

/** Validate, ask `beforeTool`, and validate what it leaves: the call runs, or its answer. */
const admit = Effect.fnUntraced(function* (
  call: ToolCall,
  caller: Caller,
  resolution: Resolution,
  api: Hook.Api,
) {
  const installed = resolution.tool(call.name, caller)
  const checked = yield* prepare(call, installed, call.arguments)
  if (!('args' in checked)) return checked
  const decided = yield* beforeTool(resolution.hooks, { ...call, arguments: checked.args }, api)
  if ('block' in decided)
    return ToolResult.failure('blocked', `Tool call blocked: ${decided.block}`)
  return decided.args === checked.args ? checked : yield* prepare(call, installed, decided.args)
})

/**
 * The `call` phase: resolve, validate, and ask `beforeTool`, settling at once when the call
 * cannot run; then record intent and run the handler.
 */
const callPhase = (resolve: Resolve, lifecycle: Lifecycle) =>
  Effect.fnUntraced(function* (task: { readonly input: Input }, runtime: ToolRuntime) {
    const call = yield* currentCall(runtime, task.input)
    const resolution = yield* resolve(runtime)
    const admitted = yield* admit(call, callerOf(task.input), resolution, runtime)
    if (!('args' in admitted)) return yield* answer(runtime, lifecycle, task.input, admitted)
    const { installed, args } = admitted
    yield* runtime.commit((tx) =>
      Effect.gen(function* () {
        if (lifecycle.started !== undefined && !isNested(task.input))
          yield* lifecycle.started(tx, callOf(runtime))
        return {
          status: 'running' as const,
          checkpoint: {
            phase: 'execute' as const,
            arguments: args,
            replay: installed.tool.replay,
          },
        }
      }),
    )
    return yield* run(runtime, lifecycle, resolution.hooks, task.input, call, installed, args)
  })

/** The `execute` phase, reached only by recovery: rerun only when both policies say `safe`. */
const executePhase = (resolve: Resolve, lifecycle: Lifecycle) =>
  Effect.fnUntraced(function* (
    task: {
      readonly input: Input
      readonly state: { readonly checkpoint: Extract<Checkpoint, { phase: 'execute' }> }
    },
    runtime: ToolRuntime,
  ) {
    const { arguments: args, replay } = task.state.checkpoint
    const call = yield* currentCall(runtime, task.input)
    const resolution = yield* resolve(runtime)
    const installed = resolution.tool(call.name, callerOf(task.input))
    if (replay === 'safe' && installed?.tool.replay === 'safe')
      return yield* run(runtime, lifecycle, resolution.hooks, task.input, call, installed, args)
    const message = `Tool ${call.name} was interrupted and may have partially run`
    // `failed` records cancellation intent for what the call owns, now left unsupervised.
    return yield* settle(runtime, lifecycle, task.input, () => ({
      result: ToolResult.failure('interrupted', message),
      ending: { status: 'failed', message },
    }))
  })

/** Why the call was aborted: on an abandon after a restart, whether it may have run. */
const abortCode = (
  task: { readonly abortReason?: 'restart'; readonly state: { readonly checkpoint: Checkpoint } },
  name: string,
): readonly [string, string] => {
  if (task.abortReason !== 'restart') return ['aborted', `Tool ${name} was aborted`]
  if (task.state.checkpoint.phase === 'execute')
    return ['interrupted', `Tool ${name} was interrupted by a restart and may have partially run`]
  return ['abandoned', `Tool ${name} was not started: its caller ended with a restart`]
}

export const definition = (
  resolve: Resolve,
  lifecycle: Lifecycle,
): Definition<Input, Checkpoint, TaskResult> => ({
  name: NAME,
  version: 1,
  initial: () => ({ phase: 'call' }),
  phases: { call: callPhase(resolve, lifecycle), execute: executePhase(resolve, lifecycle) },
  abort: (task, runtime) =>
    settle(runtime, lifecycle, task.input, (call) => {
      const [code, message] = abortCode(task, call.name)
      return { result: ToolResult.failure(code, message), ending: { status: 'aborted' } }
    }),
})
