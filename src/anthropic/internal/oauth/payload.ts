/** Claude subscription request projection, without traversing opaque tool JSON. */
import type { AnthropicClient, Generated } from '@effect/ai-anthropic'
import { Effect } from 'effect'
import { AiError } from 'effect/ai'
import type { Request } from '../../../model/Model.ts'
import type { ImageContent, TextContent } from '../../../model/Message.ts'

const canonical: Readonly<Record<string, string>> = {
  read: 'Read',
  write: 'Write',
  edit: 'Edit',
  bash: 'Bash',
  grep: 'Grep',
  glob: 'Glob',
  askuserquestion: 'AskUserQuestion',
  enterplanmode: 'EnterPlanMode',
  exitplanmode: 'ExitPlanMode',
  killshell: 'KillShell',
  notebookedit: 'NotebookEdit',
  skill: 'Skill',
  task: 'Task',
  taskoutput: 'TaskOutput',
  todowrite: 'TodoWrite',
  webfetch: 'WebFetch',
  websearch: 'WebSearch',
}
const wireName = (name: string) =>
  Object.hasOwn(canonical, name.toLowerCase()) ? (canonical[name.toLowerCase()] ?? name) : name
const invalid = (description: string) =>
  new AiError.AiError({
    module: 'AnthropicOAuth',
    method: 'messages',
    reason: new AiError.InvalidUserInputError({ description }),
  })
type Input = Parameters<AnthropicClient.Service['createMessage']>[0]

interface Projection {
  readonly clean: <A extends object>(block: A) => A
  readonly results: ReadonlyMap<string, ReadonlyArray<TextContent | ImageContent>>
  readonly names: ReadonlyMap<string, string>
  readonly register: (name: string) => Effect.Effect<string, AiError.AiError>
}

const media = (parts: ReadonlyArray<TextContent | ImageContent>) =>
  Effect.forEach(
    parts,
    (
      part,
    ): Effect.Effect<
      Generated.BetaRequestTextBlock | Generated.BetaRequestImageBlock,
      AiError.AiError
    > => {
      if (part.type === 'text') return Effect.succeed({ type: 'text', text: part.text })
      const mime = part.mimeType
      if (
        mime !== 'image/jpeg' &&
        mime !== 'image/png' &&
        mime !== 'image/gif' &&
        mime !== 'image/webp'
      )
        return Effect.fail(invalid('Unsupported Claude tool-result image media type'))
      return Effect.succeed({
        type: 'image',
        source: {
          type: 'base64',
          media_type: mime,
          data: part.data.replace(/^data:[^;]+;base64,/u, ''),
        },
      })
    },
  )

const projection = (request: Request): Projection => {
  const clean = <A extends object>(block: A): A => {
    if (request.options?.cacheRetention !== 'none') return block
    const copy = { ...block }
    Reflect.deleteProperty(copy, 'cache_control')
    return copy
  }
  const results = new Map(
    request.messages.flatMap((message) =>
      message.role === 'toolResult' && message.content.some((part) => part.type === 'image')
        ? [[message.toolCallId, message.content] as const]
        : [],
    ),
  )
  const names = new Map<string, string>()
  const register = (name: string) => {
    const wire = wireName(name)
    const previous = names.get(wire)
    if (previous !== undefined && previous !== name)
      return Effect.fail(invalid('OAuth tool names collide after normalization'))
    names.set(wire, name)
    return Effect.succeed(wire)
  }
  return { clean, results, names, register }
}

const messages = (input: Input, view: Projection) =>
  Effect.forEach(
    input.payload.messages,
    Effect.fnUntraced(function* (message) {
      if (typeof message.content === 'string') return message
      const content = yield* Effect.forEach(
        message.content,
        Effect.fnUntraced(function* (block) {
          if (block.type === 'tool_result') {
            const parts = view.results.get(block.tool_use_id)
            if (parts !== undefined) return { ...view.clean(block), content: yield* media(parts) }
            return {
              ...view.clean(block),
              ...(block.content !== undefined && typeof block.content !== 'string'
                ? { content: block.content.map((part) => view.clean(part)) }
                : {}),
            }
          }
          if (block.type !== 'tool_use') return view.clean(block)
          return { ...view.clean(block), name: yield* view.register(block.name) }
        }),
      )
      return { ...message, content }
    }),
  )

const thinking = Effect.fnUntraced(function* (input: Input) {
  const serverDrop = /^claude-(?:fable-5-1|opus-5-5|sonnet-5-5|haiku-5-5)(?:-\d{8})?$/u.test(
    input.payload.model,
  )
  const requested =
    input.payload.thinking ?? (serverDrop ? { type: 'adaptive' as const } : undefined)
  if (serverDrop && requested?.type !== 'adaptive')
    return yield* invalid('This Claude model requires adaptive thinking for safe prefix changes')
  // Native methods serialize encoded payloads directly, retaining subscription-only fields.
  const wire =
    requested === undefined || requested === null || requested.type === 'disabled'
      ? undefined
      : {
          ...requested,
          display: 'summarized' as const,
          ...(serverDrop
            ? { block_binding: { prefix_mismatch_behavior: 'drop_block' as const } }
            : {}),
        }
  const params = {
    ...input.params,
    'anthropic-beta': [
      ...new Set([
        ...(input.params?.['anthropic-beta']?.split(',').filter(Boolean) ?? []),
        ...(serverDrop ? ['thinking-binding-controls-2026-08-01'] : []),
      ]),
    ].join(','),
  }
  return { wire, params }
})

const system = (input: Input, view: Projection) => {
  const value = input.payload.system
  const blocks =
    typeof value === 'string' ? [{ type: 'text' as const, text: value }] : (value ?? [])
  return [
    { type: 'text' as const, text: "You are Claude Code, Anthropic's official CLI for Claude." },
    ...blocks.map((block) => view.clean(block)),
  ]
}

export const prepare = Effect.fnUntraced(function* (input: Input, request: Request) {
  const view = projection(request)
  const tools = yield* Effect.forEach(
    input.payload.tools ?? [],
    Effect.fnUntraced(function* (tool) {
      if (tool.type !== undefined && tool.type !== 'custom')
        return yield* invalid('Claude OAuth supports application-defined tools only')
      return { ...view.clean(tool), name: yield* view.register(tool.name) }
    }),
  )
  const translated = yield* messages(input, view)
  const thought = yield* thinking(input)
  const choice = input.payload.tool_choice
  const retention = request.options?.cacheRetention
  const prepared: Input = {
    ...input,
    params: thought.params,
    payload: {
      ...view.clean(input.payload),
      messages: translated,
      tools,
      system: system(input, view),
      ...(choice?.type === 'tool'
        ? { tool_choice: { ...choice, name: wireName(choice.name) } }
        : {}),
      ...(thought.wire === undefined ? {} : { thinking: thought.wire }),
      ...(retention === 'short' || retention === 'long'
        ? {
            cache_control: { type: 'ephemeral', ...(retention === 'long' ? { ttl: '1h' } : {}) },
          }
        : {}),
    },
  }
  return { prepared, originalName: (name: string) => view.names.get(name) ?? name }
})
