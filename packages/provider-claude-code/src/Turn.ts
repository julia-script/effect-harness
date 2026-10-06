import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Predicate from 'effect/Predicate'
import * as Stream from 'effect/Stream'
import type * as AiError from 'effect/ai/AiError'
import type * as Response from 'effect/ai/Response'
import * as Tool from 'effect/ai/Tool'
import { authentication, processError, protocol, unsupported } from './Error.ts'
import * as Protocol from './Protocol.ts'

type Part = Response.StreamPartEncoded
interface Block {
  readonly source: Protocol.Block
  readonly id: string
  text: string
  signature: string
  json: string
  closed: boolean
}
const sameJson = (left: unknown, right: unknown): boolean => {
  if (left === right) return true
  if (left === null || right === null || typeof left !== 'object' || typeof right !== 'object')
    return false
  if (Array.isArray(left))
    return (
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => {
        const other = right[index]
        return other !== undefined && sameJson(value, other)
      })
    )
  if (!Predicate.isReadonlyObject(left) || !Predicate.isReadonlyObject(right)) return false
  const keys = Object.keys(left)
  return (
    keys.length === Object.keys(right).length &&
    keys.every((key) => {
      const a = left[key]
      const b = right[key]
      return a !== undefined && b !== undefined && sameJson(a, b)
    })
  )
}
const usage = (value: Protocol.Usage): typeof Response.Usage.Encoded => ({
  inputTokens: {
    ...(value.input_tokens === undefined ? {} : { uncached: value.input_tokens }),
    ...(value.cache_read_input_tokens === undefined
      ? {}
      : { cacheRead: value.cache_read_input_tokens }),
    ...(value.cache_creation_input_tokens === undefined
      ? {}
      : { cacheWrite: value.cache_creation_input_tokens }),
    ...(value.input_tokens === undefined
      ? {}
      : {
          total:
            value.input_tokens +
            (value.cache_read_input_tokens ?? 0) +
            (value.cache_creation_input_tokens ?? 0),
        }),
  },
  outputTokens: value.output_tokens === undefined ? {} : { total: value.output_tokens },
})
const reason = (stop: string | null | undefined): Response.FinishReason => {
  switch (stop) {
    case 'end_turn':
    case 'stop_sequence':
      return 'stop'
    case 'max_tokens':
      return 'length'
    case 'tool_use':
      return 'tool-calls'
    case 'refusal':
      return 'content-filter'
    case 'pause_turn':
      return 'pause'
    case undefined:
    case null:
      return 'unknown'
    default:
      return 'other'
  }
}

/** One model turn. MCP handlers remain blocked; all tool calls are emitted only after the complete turn. */
export const translate = (
  events: Stream.Stream<Protocol.Event, AiError.AiError>,
  aliases: ReadonlyMap<string, string>,
): Stream.Stream<Part, AiError.AiError> =>
  Stream.suspend(() => {
    const blocks = new Map<number, Block>()
    const completedTools = new Map<string, Response.ToolCallPartEncoded>()
    let initialized = false
    let partial = false
    let messageId: string | undefined
    let model: string | undefined
    let tokenUsage: Protocol.Usage = {}
    let modelUsage: typeof Response.Usage.Encoded | undefined
    let stopReason: string | null | undefined
    let terminal: Extract<Protocol.Event, { type: 'result' }> | undefined
    let toolDone = false
    let messageStopped = false
    let assistantSeen = false

    const start = Effect.fnUntraced(function* (
      index: number,
      source: Protocol.Block,
    ): Effect.fn.Return<Array<Part>, AiError.AiError> {
      if (blocks.has(index)) return yield* protocol('Duplicate Claude Code content block')
      const id = source.type === 'tool_use' ? source.id : `${messageId ?? 'message'}:${index}`
      const block: Block = { source, id, text: '', signature: '', json: '', closed: false }
      blocks.set(index, block)
      switch (source.type) {
        case 'text':
          block.text = source.text
          return [
            { type: 'text-start', id },
            ...(source.text === ''
              ? []
              : [{ type: 'text-delta' as const, id, delta: source.text }]),
          ]
        case 'thinking':
          block.text = source.thinking
          block.signature = source.signature ?? ''
          return [
            { type: 'reasoning-start', id },
            ...(source.thinking === ''
              ? []
              : [{ type: 'reasoning-delta' as const, id, delta: source.thinking }]),
          ]
        case 'redacted_thinking':
          return [
            {
              type: 'reasoning-start',
              id,
              metadata: { claudeCode: { redactedThinking: source.data } },
            },
          ]
        case 'tool_use': {
          const name = aliases.get(source.name)
          if (name === undefined)
            return yield* protocol('Claude Code requested a tool outside the supplied toolkit')
          if ([...blocks.values()].filter((b) => b.id === id).length > 1)
            return yield* protocol('Duplicate Claude Code tool call ID')
          return [{ type: 'tool-params-start', id, name }]
        }
      }
    })
    const close = Effect.fnUntraced(function* (index: number) {
      const block = blocks.get(index)
      if (block === undefined || block.closed)
        return yield* protocol('Claude Code ended an unknown or closed block')
      block.closed = true
      switch (block.source.type) {
        case 'text':
          return [{ type: 'text-end', id: block.id }] satisfies Array<Part>
        case 'thinking':
          return [
            {
              type: 'reasoning-end',
              id: block.id,
              metadata: { claudeCode: { signature: block.signature } },
            },
          ] satisfies Array<Part>
        case 'redacted_thinking':
          return [{ type: 'reasoning-end', id: block.id }] satisfies Array<Part>
        case 'tool_use': {
          const raw =
            block.json === ''
              ? block.source.input
              : yield* Effect.try({
                  try: () => Tool.unsafeSecureJsonParse(block.json),
                  catch: () => protocol('Malformed Claude Code tool parameters'),
                })
          const params = yield* Schema.decodeUnknownEffect(Schema.JsonObject)(raw).pipe(
            Effect.mapError(() => protocol('Claude Code tool parameters must be a JSON object')),
          )
          const name = aliases.get(block.source.name)
          if (name === undefined) return yield* protocol('Unknown Claude Code tool alias')
          completedTools.set(block.id, {
            type: 'tool-call',
            id: block.id,
            name,
            params,
            providerExecuted: false,
          })
          return [{ type: 'tool-params-end', id: block.id }] satisfies Array<Part>
        }
      }
    })
    const toolFinish = () =>
      Effect.gen(function* () {
        if ([...blocks.values()].some((b) => !b.closed) || completedTools.size === 0)
          return yield* protocol('Incomplete Claude Code tool turn')
        toolDone = true
        return [
          ...completedTools.values(),
          {
            type: 'finish',
            reason: 'tool-calls',
            usage: usage(tokenUsage),
            metadata: { claudeCode: { interruptedAfterToolIntent: true, costUnavailable: true } },
          },
        ] satisfies Array<Part>
      })
    const consume = Effect.fnUntraced(function* (
      event: Protocol.Event,
    ): Effect.fn.Return<Array<Part>, AiError.AiError> {
      if (event.type === 'system') {
        if (event.subtype !== 'init')
          return yield* unsupported(
            'CLI system events outside initialization (including host hooks)',
          )
        if (initialized || event.tools === undefined)
          return yield* protocol('Missing or duplicate Claude Code initialization manifest')
        const actual = new Set(event.tools)
        if (actual.size !== aliases.size || [...actual].some((name) => !aliases.has(name)))
          return yield* protocol(
            'Claude Code loaded tools outside the exact intent-only MCP manifest',
          )
        initialized = true
        return []
      }
      if (!initialized)
        return yield* protocol('Claude Code emitted model output before its tool manifest')
      if (event.type === 'rate_limit_event') return []
      if (event.type === 'auth_status')
        return event.error === undefined ? [] : yield* authentication()
      if (event.type === 'result') {
        if (terminal !== undefined || event.is_error || event.subtype !== 'success')
          return yield* processError('Claude Code did not complete a successful turn')
        if ((event.num_turns ?? 1) !== 1) return yield* unsupported('multiple autonomous CLI turns')
        if ((event.permission_denials?.length ?? 0) !== 0)
          return yield* protocol('Claude Code attempted a denied tool execution')
        if (event.structured_output !== undefined)
          return yield* unsupported('unexpected CLI structured output')
        if (event.modelUsage !== undefined) {
          const models = yield* Schema.decodeUnknownEffect(
            Schema.Record(Schema.String, Protocol.ModelUsage),
          )(event.modelUsage).pipe(
            Effect.mapError(() => protocol('Malformed Claude Code model accounting')),
          )
          const values = Object.values(models)
          if (values.some((value) => (value.thinkingTokens ?? 0) > value.outputTokens))
            return yield* protocol('Invalid Claude Code reasoning token count')
          if (values.length > 0) {
            const total = (select: (value: typeof Protocol.ModelUsage.Type) => number) =>
              values.reduce((sum, value) => sum + select(value), 0)
            const uncached = total((value) => value.inputTokens)
            const cacheRead = total((value) => value.cacheReadInputTokens)
            const cacheWrite = total((value) => value.cacheCreationInputTokens)
            const output = total((value) => value.outputTokens)
            const thinking = total((value) => value.thinkingTokens ?? 0)
            if (
              ![
                uncached,
                cacheRead,
                cacheWrite,
                uncached + cacheRead + cacheWrite,
                output,
                thinking,
              ].every(Number.isSafeInteger)
            )
              return yield* protocol('Claude Code token totals exceed safe integer accounting')
            modelUsage = {
              inputTokens: {
                uncached,
                cacheRead,
                cacheWrite,
                total: uncached + cacheRead + cacheWrite,
              },
              outputTokens: {
                total: output,
                ...(values.every((value) => value.thinkingTokens !== undefined)
                  ? { reasoning: thinking, text: output - thinking }
                  : {}),
              },
            }
          }
        }
        terminal = event
        tokenUsage = event.usage
        stopReason = event.stop_reason ?? stopReason
        return []
      }
      if (event.parent_tool_use_id !== undefined && event.parent_tool_use_id !== null)
        return yield* unsupported('nested CLI agent output')
      if (event.type === 'assistant') {
        if (event.error !== undefined)
          return yield* processError('Claude Code model request failed')
        assistantSeen = true
        if (messageId !== undefined && messageId !== event.message.id)
          return yield* unsupported('multiple CLI assistant turns')
        messageId = event.message.id
        model = event.message.model
        tokenUsage = { ...tokenUsage, ...event.message.usage }
        stopReason = event.message.stop_reason ?? stopReason
        if (partial) {
          for (const source of event.message.content) {
            const matches = [...blocks.values()].some((block) => {
              if (!block.closed || block.source.type !== source.type) return false
              if (source.type === 'text') return block.text === source.text
              if (source.type === 'thinking')
                return (
                  block.text === source.thinking && block.signature === (source.signature ?? '')
                )
              if (source.type === 'redacted_thinking')
                return (
                  block.source.type === 'redacted_thinking' && block.source.data === source.data
                )
              const call = completedTools.get(source.id)
              return (
                call !== undefined &&
                call.name === aliases.get(source.name) &&
                sameJson(call.params, source.input)
              )
            })
            if (!matches)
              return yield* protocol(
                'Claude Code complete assistant content conflicts with its stream',
              )
          }
          return []
        }
        const output: Array<Part> = [{ type: 'response-metadata', id: messageId, modelId: model }]
        for (const source of event.message.content) {
          const index = blocks.size
          output.push(...(yield* start(index, source)), ...(yield* close(index)))
        }
        if (stopReason === 'tool_use') output.push(...(yield* toolFinish()))
        return output
      }
      partial = true
      const frame = event.event
      switch (frame.type) {
        case 'ping':
          return []
        case 'error':
          return yield* processError('Claude Code model stream failed')
        case 'message_start': {
          if (messageId !== undefined) return yield* protocol('Duplicate Claude Code message start')
          messageId = frame.message.id
          model = frame.message.model
          tokenUsage = frame.message.usage
          return [{ type: 'response-metadata', id: messageId, modelId: model }]
        }
        case 'content_block_start': {
          if (messageId === undefined || messageStopped)
            return yield* protocol('Claude Code content outside an active message')
          return yield* start(frame.index, frame.content_block)
        }
        case 'content_block_delta': {
          const block = blocks.get(frame.index)
          if (block === undefined || block.closed)
            return yield* protocol('Claude Code delta outside an active block')
          const delta = frame.delta
          if (delta.type === 'text_delta' && block.source.type === 'text') {
            block.text += delta.text
            return [{ type: 'text-delta', id: block.id, delta: delta.text }]
          }
          if (delta.type === 'thinking_delta' && block.source.type === 'thinking') {
            block.text += delta.thinking
            return [{ type: 'reasoning-delta', id: block.id, delta: delta.thinking }]
          }
          if (delta.type === 'signature_delta' && block.source.type === 'thinking') {
            block.signature += delta.signature
            return []
          }
          if (delta.type === 'input_json_delta' && block.source.type === 'tool_use') {
            block.json += delta.partial_json
            return [{ type: 'tool-params-delta', id: block.id, delta: delta.partial_json }]
          }
          return yield* protocol('Claude Code delta has the wrong block type')
        }
        case 'content_block_stop':
          return yield* close(frame.index)
        case 'message_delta':
          stopReason = frame.delta.stop_reason ?? stopReason
          tokenUsage = { ...tokenUsage, ...frame.usage }
          return []
        case 'message_stop': {
          if (
            messageId === undefined ||
            messageStopped ||
            [...blocks.values()].some((b) => !b.closed)
          )
            return yield* protocol('Incomplete Claude Code message')
          messageStopped = true
          return stopReason === 'tool_use' ? yield* toolFinish() : []
        }
      }
    })
    return events.pipe(
      Stream.mapEffect((event) =>
        consume(event).pipe(
          Effect.filterOrFail(
            () =>
              Number.isSafeInteger(
                (tokenUsage.input_tokens ?? 0) +
                  (tokenUsage.cache_read_input_tokens ?? 0) +
                  (tokenUsage.cache_creation_input_tokens ?? 0),
              ),
            () => protocol('Claude Code input token total exceeds safe integer accounting'),
          ),
        ),
      ),
      Stream.takeUntil(() => toolDone),
      Stream.flatMap((parts) => Stream.fromIterable(parts)),
      Stream.concat(
        Stream.fromEffect(
          Effect.suspend((): Effect.Effect<Array<Part>, AiError.AiError> => {
            if (toolDone) return Effect.succeed([] as Array<Part>)
            if (
              terminal === undefined ||
              (!assistantSeen && !messageStopped) ||
              completedTools.size !== 0 ||
              [...blocks.values()].some((b) => !b.closed)
            )
              return Effect.fail(
                protocol('Claude Code ended without a complete successful response'),
              )
            return Effect.succeed([
              {
                type: 'finish',
                reason: reason(stopReason),
                usage: modelUsage ?? usage(tokenUsage),
                metadata: {
                  claudeCode: {
                    ...(terminal.total_cost_usd === undefined
                      ? {}
                      : { totalCostUsd: terminal.total_cost_usd }),
                    ...(terminal.modelUsage === undefined
                      ? {}
                      : { modelUsage: terminal.modelUsage }),
                  },
                },
              },
            ] satisfies Array<Part>)
          }),
        ).pipe(Stream.flatMap((parts) => Stream.fromIterable(parts))),
      ),
    )
  })

/** Consolidates the same validated stream used by streaming generation. */
export const collect = (stream: Stream.Stream<Part, AiError.AiError>) =>
  Effect.gen(function* () {
    const parts: Array<Response.PartEncoded> = []
    const positions = new Map<string, number>()
    yield* stream.pipe(
      Stream.runForEach((part) =>
        Effect.sync(() => {
          if (part.type === 'text-start' || part.type === 'reasoning-start') {
            positions.set(part.id, parts.length)
            parts.push({
              type: part.type === 'text-start' ? 'text' : 'reasoning',
              text: '',
              metadata: { claudeCode: part.metadata?.claudeCode ?? {} },
            })
          } else if (part.type === 'text-delta' || part.type === 'reasoning-delta') {
            const index = positions.get(part.id)
            const current = index === undefined ? undefined : parts[index]
            if (
              index !== undefined &&
              current !== undefined &&
              (current.type === 'text' || current.type === 'reasoning')
            )
              parts[index] = { ...current, text: current.text + part.delta }
          } else if (part.type === 'text-end' || part.type === 'reasoning-end') {
            const index = positions.get(part.id)
            const current = index === undefined ? undefined : parts[index]
            if (
              index !== undefined &&
              current !== undefined &&
              (current.type === 'text' || current.type === 'reasoning')
            )
              parts[index] = {
                ...current,
                metadata: {
                  claudeCode: part.metadata?.claudeCode ?? current.metadata?.claudeCode ?? {},
                },
              }
          } else if (
            part.type !== 'tool-params-start' &&
            part.type !== 'tool-params-delta' &&
            part.type !== 'tool-params-end' &&
            part.type !== 'error'
          )
            parts.push(part)
        }),
      ),
    )
    return parts
  })
