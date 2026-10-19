/**
 * The built-in tool task, ported from pi's `ToolTask` (`harness/tool.ts`): one durable task per
 * model-issued tool call.
 *
 * Its `call` phase reads the call from its assistant entry, resolves the tool, validates the
 * arguments against the tool's parameters Schema, records intent (the arguments it runs with and
 * the replay policy), runs the handler, and settles: a `pi.tool-result` entry and the task's
 * outcome in one commit. Every way a call can go wrong still answers it with a result entry, so
 * the model always sees one:
 *
 * - `tool_unavailable`: no tool of that name, or no implementation for it.
 * - `invalid_arguments`: the arguments do not decode; `invalid_config`: its config does not.
 * - The tool's declared failure: an error result with the encoded failure; the task completes.
 * - `tool_error`: the handler died, or returned a value its Schema cannot encode; the task ends
 *   `failed`.
 * - `interrupted`: a crash stopped the call after intent and the tool is not replay-safe; on
 *   recovery the `execute` phase answers it instead of running it again. A tool declared with
 *   `replay: 'safe'`, both when intent was recorded and now, runs again.
 * - `aborted`, or on an abandon after restart `interrupted` and `abandoned`: the abort handler.
 *
 * Extensions answer its hooks: `beforeTool` may replace a call's arguments or block it (`blocked`),
 * and `afterTool` may replace its result.
 *
 * The task needs no services: `makeWith` resolves each call's tools and hooks through a function,
 * as the harness does from the conversation's agent; `make` closes over a toolkit's
 * implementations, and `layer` installs that in the `Registry`. Config is given encoded.
 */
import { Effect, Layer, Schema } from 'effect'
import type { Context } from 'effect'
import type { TaskKind, Tx } from '../session/Session.ts'
import type { TxDocumentError } from '../session/SessionError.ts'
import { EntryId, TaskId } from '../storage/schema/Ids.ts'
import type { ConversationId } from '../storage/schema/Ids.ts'
import { JsonObject } from '../storage/schema/Json.ts'
import { ToolCall as ToolCallSchema } from '../model/Message.ts'
import type { ToolCall } from '../model/Message.ts'
import * as Document from '../session/Document.ts'
import type { Outcome } from './Call.ts'
import * as Hook from '../task/Hook.ts'
import { Registry } from '../task/Registry.ts'
import * as Task from '../task/Task.ts'
import type * as ToolResult from './ToolResult.ts'
import { Control as ToolResultControl } from './ToolResult.ts'
import * as internalExecute from './internal/execute.ts'
import * as internal from './internal/task.ts'
import type * as Tool from './Tool.ts'
import type * as Toolkit from './Toolkit.ts'

/** The task kind. */
export const name = internal.NAME

/** A model-issued call: the assistant entry that holds it, and its ID there. */
export const ModelInput = Schema.Struct({ assistant: EntryId, callId: Schema.String })
export type ModelInput = typeof ModelInput.Type

/**
 * A nested call a running tool made (see `Call`): the calling task and its call, the call's key
 * there, and the call itself, which no entry holds.
 */
export const NestedInput = Schema.Struct({
  kind: Schema.Literal('nested'),
  parent: TaskId,
  parentCallId: Schema.String,
  key: Schema.String,
  call: ToolCallSchema,
})
export type NestedInput = typeof NestedInput.Type

export const Input = Schema.Union([ModelInput, NestedInput])
export type Input = typeof Input.Type

export const Checkpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal('call') }),
  /** Durable intent: the arguments the call runs with, and the replay policy at that time. */
  Schema.Struct({
    phase: Schema.Literal('execute'),
    arguments: JsonObject,
    replay: Schema.Literals(['safe', 'unsafe']),
  }),
])
export type Checkpoint = typeof Checkpoint.Type

/** A model-issued call's result entry, and what the call asks of its generation. */
export const ModelResult = Schema.Struct({
  entryId: EntryId,
  control: Schema.optionalKey(ToolResultControl),
})
export type ModelResult = typeof ModelResult.Type

/** A nested call's outcome is in its caller's `NestedCalls` member, so the receipt stays small. */
export const NestedResult = Schema.Struct({ kind: Schema.Literal('nested') })

export const Result = Schema.Union([ModelResult, NestedResult])
export type Result = typeof Result.Type

/** One nested call of a running tool: its task, and once it ended, its outcome. */
export interface NestedCallState {
  [key: string]: Schema.Json
  /** Absent only inside the commit that creates the call. */
  taskId?: number
  result?: Outcome
}

/**
 * The nested calls of a tool call, by key: a task-scoped family of the calling task, so a rerun
 * finds the calls it made, and retired with it. The nested call's own terminal commit writes its
 * outcome.
 */
export const NestedCalls = Document.makeFamily<NestedCallState, null>({
  kind: 'pi.tool.nested-call',
  version: 1,
  family: true,
  scope: 'task',
  initial: () => Effect.succeed({}),
})

export type ToolTask = Task.Task<Input, Checkpoint, Result>

/** What `Tx.createTask()` needs to create a call, without the handlers. */
export const kind: TaskKind<Input> = {
  name,
  version: 1,
  initial: () => ({ phase: 'call' }),
}

/** Each tool's config, encoded, by name; a tool left out gets `{}`. */
export type Config<Tools extends Record<string, Tool.Any>> = {
  readonly [Name in keyof Tools]?: Tools[Name]['config']['Encoded']
}

/** The tool task of a call. */
export interface Call {
  readonly taskId: TaskId
  readonly conversationId: ConversationId
}

/**
 * Run in the tool task's own commits, so the loop can present the call: `started` when the call
 * records intent and its handler is about to run, `settled` with its result entry.
 */
export interface Lifecycle {
  readonly started?: (tx: Tx, call: Call) => Effect.Effect<void, TxDocumentError>
  readonly settled?: (tx: Tx, call: Call, entry: EntryId) => Effect.Effect<void, TxDocumentError>
}

/** A model-issued call as hooks see it, with the arguments it would run with. */
export type HookCall = ToolCall

/** The hooks the tool task asks (spec §7.2), for extensions to answer. */
export interface Hooks {
  /**
   * Before intent: replace the arguments, or block the call with error text; the first block
   * wins. A failing hook blocks the call with its failure.
   */
  readonly beforeTool: (
    call: HookCall,
    api: Hook.Api,
  ) => Effect.Effect<
    { readonly arguments?: JsonObject; readonly block?: string } | undefined,
    unknown
  >
  /** After execution, before the result is committed: replace the result. */
  readonly afterTool: (
    call: HookCall,
    result: ToolResult.Result,
    api: Hook.Api,
  ) => Effect.Effect<ToolResult.Result | undefined, unknown>
}
export const Hooks: Hook.Kind<Hooks> = Hook.kind<Hooks>(name)

/** A tool the task can run: its declaration, its implementation if any, and its config, encoded. */
export type Installed = internalExecute.Installed

/**
 * What a call resolves against: the tool of a name that `caller` may call, and the hooks it asks,
 * in order.
 */
export interface Resolution {
  readonly tool: (name: string, caller: Tool.Caller) => Installed | undefined
  readonly hooks: ReadonlyArray<Partial<Hooks>>
}

/** Resolve the tools and hooks of a call, once per phase, with the task's reads. */
export type Resolve = (api: Hook.Api) => Effect.Effect<Resolution, unknown>

export interface Options<Tools extends Record<string, Tool.Any>> {
  readonly config?: Config<Tools>
  readonly lifecycle?: Lifecycle
}

/** The tool task over a resolver: the loop picks each conversation's tools and hooks. */
export const makeWith = (resolve: Resolve, lifecycle: Lifecycle = {}): ToolTask =>
  Task.make(internal.definition(resolve, lifecycle))

/** The tool task over the tools of `toolkit` and their `implementations`, with no hooks. */
export const make = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
  implementations: Context.Context<Toolkit.Implementation<Toolkit.Names<Tools>>>,
  options: Options<Tools> = {},
): ToolTask => {
  const tools = internalExecute.install(
    Object.values(toolkit.tools),
    implementations,
    options.config ?? {},
  )
  const resolution: Resolution = {
    tool: (toolName, caller) => {
      const found = tools.get(toolName)
      return found?.tool.callers.includes(caller) === true ? found : undefined
    },
    hooks: [],
  }
  return makeWith(() => Effect.succeed(resolution), options.lifecycle)
}

/** Install the tool task in the `Registry`, over the implementations `toolkit.toLayer()` provides. */
export const layer = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
  options: Options<Tools> = {},
): Layer.Layer<never, never, Registry | Toolkit.Implementation<Toolkit.Names<Tools>>> =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const implementations = yield* Effect.context<Toolkit.Implementation<Toolkit.Names<Tools>>>()
      const registry = yield* Registry
      yield* registry.install(make(toolkit, implementations, options))
    }),
  )

/** Each of `tools` by name, with its implementation from `implementations` and its config. */
export const install: <Services>(
  tools: ReadonlyArray<Tool.Any>,
  implementations: Context.Context<Services>,
  config: Readonly<Record<string, unknown>>,
) => ReadonlyMap<string, Installed> = internalExecute.install
