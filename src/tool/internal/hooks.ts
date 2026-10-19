/* oxlint-disable effecttsgo/any-unknown-in-error-context -- extension code may fail with anything; a failure is reported or blocks the call */
/**
 * Asking the tool task's hooks: `beforeTool` before intent, `afterTool` before the result commits.
 */
import { Cause, Effect, Option } from 'effect'
import type { ToolCall } from '../../model/Message.ts'
import type { JsonObject } from '../../storage/schema/Json.ts'
import * as Hook from '../../task/Hook.ts'
import type * as ToolResult from '../ToolResult.ts'
import type { Hooks } from '../ToolTask.ts'

type Decision = { readonly block: string } | { readonly args: JsonObject }

/** Ask one `beforeTool` about `args`. */
const askBefore = (
  beforeTool: Hooks['beforeTool'],
  call: ToolCall,
  args: JsonObject,
  api: Hook.Api,
): Effect.Effect<Decision> =>
  Effect.catchCause(
    Effect.map(beforeTool({ ...call, arguments: args }, api), (answered): Decision =>
      answered?.block === undefined
        ? { args: answered?.arguments ?? args }
        : { block: answered.block },
    ),
    (cause) =>
      Cause.hasInterrupts(cause)
        ? Effect.interrupt
        : Effect.succeed<Decision>({ block: Hook.failureMessage(cause) }),
  )

/**
 * Ask `beforeTool` in order: each sees the arguments the earlier ones left, and the first block
 * wins. A hook that fails blocks the call with its failure.
 */
export const beforeTool = (hooks: ReadonlyArray<Partial<Hooks>>, call: ToolCall, api: Hook.Api) =>
  Effect.gen(function* () {
    let decided: Decision = { args: call.arguments }
    for (const hook of hooks) {
      if (hook.beforeTool === undefined || 'block' in decided) continue
      decided = yield* askBefore(hook.beforeTool, call, decided.args, api)
    }
    return decided
  })

/** Ask `afterTool` in order, each replacing the result the earlier ones left. */
export const afterTool = (
  hooks: ReadonlyArray<Partial<Hooks>>,
  call: ToolCall,
  result: ToolResult.Result,
  api: Hook.Api,
) =>
  Effect.gen(function* () {
    let current = result
    for (const hook of hooks) {
      if (hook.afterTool === undefined) continue
      const replaced = yield* Hook.ask(hook.afterTool(call, current, api), 'afterTool')
      if (Option.isSome(replaced)) current = replaced.value
    }
    return current
  })
