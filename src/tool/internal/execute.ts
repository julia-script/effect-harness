/**
 * Running one call, and turning what it comes to into the result the model sees.
 */
import { Clock, Context, Effect, Option, Schema } from 'effect'
import type { ToolCall } from '../../model/Message.ts'
import type { JsonObject } from '../../storage/schema/Json.ts'
import type { Any, Content } from '../Tool.ts'
import { implementation } from '../Toolkit.ts'
import * as ToolResult from '../ToolResult.ts'
import { boundContent } from './bound.ts'
import type { Outcome, Runner } from './runner.ts'

/** A tool, its implementation if a toolkit layer provided one, and its encoded config. */
export interface Installed {
  readonly tool: Any
  readonly runner: Option.Option<Runner>
  readonly config: unknown
}

/** Each of `tools` by name, with its implementation in `implementations`. */
export const install = <Services>(
  tools: ReadonlyArray<Any>,
  implementations: Context.Context<Services>,
  config: Readonly<Record<string, unknown>>,
): ReadonlyMap<string, Installed> =>
  new Map(
    tools.map((tool) => [
      tool.name,
      {
        tool,
        runner: Context.getOption(implementations, implementation(tool.name)),
        config: Object.hasOwn(config, tool.name) ? config[tool.name] : {},
      },
    ]),
  )

/**
 * How a call ends besides its result: `failed` when the tool broke, which records cancellation
 * intent for what the call owns.
 */
export type Ending =
  | { readonly status: 'completed' | 'aborted' }
  | { readonly status: 'failed'; readonly message: string }

export const COMPLETED: Ending = { status: 'completed' }

/** What a run produced. */
export interface Ran {
  readonly result: ToolResult.Result
  readonly ending: Ending
  readonly durationMs?: number
  /** The tool's success or declared failure, encoded: what a nested call returns. */
  readonly value?: Schema.Json
}

/** An encoded value as model content: a string as itself, anything else as JSON. */
const contentOf = (encoded: unknown): Content => {
  const text = typeof encoded === 'string' ? encoded : JSON.stringify(encoded ?? null)
  return text === '' ? [] : [{ type: 'text', text }]
}

const isOutput = Schema.is(ToolResult.Output)
const isJson = Schema.is(Schema.Json)

/** An encoded success or failure as the result the model sees, within the tool's limits. */
const resultOf = (tool: Any, encoded: unknown, isError: boolean): ToolResult.Result => {
  if (!isOutput(encoded)) return bounded(tool, contentOf(encoded), isError)
  const { content, diagnostics = [], details, control } = encoded
  return rebound(tool, {
    output: content,
    isError,
    diagnostics,
    ...(details === undefined ? {} : { details }),
    ...(control === undefined ? {} : { control }),
  })
}

/** `output` within the tool's limits, with a `truncated` warning when they dropped text. */
const bounded = (tool: Any, output: Content, isError: boolean): ToolResult.Result => {
  const limits = tool.outputLimits
  const { content, droppedBytes, droppedLines } = boundContent(output, limits)
  if (droppedBytes === 0) return { output: content, isError, diagnostics: [] }
  const kept = limits.retain === 'head' ? 'beginning' : 'end'
  const message = `Output truncated to its ${kept}: ${droppedLines} lines, ${droppedBytes} bytes dropped`
  return {
    output: content,
    isError,
    diagnostics: [{ severity: 'warn', code: 'truncated', message }],
  }
}

/** A result a hook replaced, within the tool's limits again. */
export const rebound = (tool: Any, result: ToolResult.Result): ToolResult.Result => {
  const limited = bounded(tool, result.output, result.isError)
  return {
    ...result,
    output: limited.output,
    diagnostics: [...result.diagnostics, ...limited.diagnostics],
  }
}

/** What an outcome means for the call. */
const ranOf = (call: ToolCall, tool: Any, outcome: Outcome): Omit<Ran, 'durationMs'> => {
  if (outcome.type === 'success' || outcome.type === 'failure') {
    const isError = outcome.type === 'failure'
    const result = resultOf(tool, outcome.value, isError)
    return isJson(outcome.value)
      ? { result, ending: COMPLETED, value: outcome.value }
      : { result, ending: COMPLETED }
  }
  if (outcome.type === 'invalidArguments') {
    const message = `Invalid arguments for ${call.name}: ${outcome.message}`
    return { result: ToolResult.failure('invalid_arguments', message), ending: COMPLETED }
  }
  if (outcome.type === 'invalidConfig') {
    const message = `Invalid config for ${call.name}: ${outcome.message}`
    return { result: ToolResult.failure('invalid_config', message), ending: COMPLETED }
  }
  return {
    result: ToolResult.failure('tool_error', outcome.message),
    ending: { status: 'failed', message: `Tool ${call.name} failed` },
  }
}

/** Run the call on `args`, the arguments of intent. */
export const execute = Effect.fnUntraced(function* (
  call: ToolCall,
  installed: Installed,
  args: JsonObject,
) {
  const { tool, runner, config } = installed
  if (Option.isNone(runner)) {
    const message = `Tool ${call.name} has no implementation`
    return { result: ToolResult.failure('tool_unavailable', message), ending: COMPLETED }
  }
  const started = yield* Clock.currentTimeMillis
  const outcome = yield* runner.value.run(args, { callId: call.id, config })
  const durationMs = (yield* Clock.currentTimeMillis) - started
  return { ...ranOf(call, tool, outcome), durationMs }
})
