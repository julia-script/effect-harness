/**
 * What a demo shows of a conversation, folded from its agent events: the committed entries as
 * bubbles, the response streaming in, and the tools running now. Pure, so the views and tests
 * share it.
 */
import { Array, Match, Number, Option, pipe, Schema } from 'effect'
import type { AgentEvent } from 'effect-harness/protocol'
import { Message } from 'effect-harness/model'
import type { Entry } from 'effect-harness/storage/schema'
import { defineTaggedUnion } from 'foldkit/schema'

// MODEL

export const Block = defineTaggedUnion({
  Text: { text: Schema.String },
  Thinking: { text: Schema.String },
  ToolCall: { name: Schema.String, args: Schema.String },
})
export type Block = typeof Block.Type

export const Bubble = defineTaggedUnion({
  User: { id: Schema.Finite, text: Schema.String },
  Assistant: { id: Schema.Finite, blocks: Schema.Array(Block) },
  ToolResult: {
    id: Schema.Finite,
    toolName: Schema.String,
    text: Schema.String,
    isError: Schema.Boolean,
  },
  System: { id: Schema.Finite, tools: Schema.Array(Schema.String) },
  Note: { id: Schema.Finite, kind: Schema.String },
})
export type Bubble = typeof Bubble.Type

export const RunningTool = Schema.Struct({ callId: Schema.String, name: Schema.String })
export type RunningTool = typeof RunningTool.Type

export const Activity = defineTaggedUnion({ Idle: {}, Running: {} })
export type Activity = typeof Activity.Type

export const Transcript = Schema.Struct({
  bubbles: Schema.Array(Bubble),
  maybeDraft: Schema.Option(Schema.Array(Block)),
  runningTools: Schema.Array(RunningTool),
  activity: Activity,
})
export type Transcript = typeof Transcript.Type

export const LogLine = Schema.Struct({
  seq: Schema.Finite,
  type: Schema.String,
  detail: Schema.String,
})
export type LogLine = typeof LogLine.Type

export const empty: Transcript = {
  bubbles: [],
  maybeDraft: Option.none(),
  runningTools: [],
  activity: Activity.Idle(),
}

// ENTRIES

const decodeMessage = Schema.decodeUnknownOption(Message.Message)

const textOf = (content: string | ReadonlyArray<{ readonly type: string }>): string =>
  typeof content === 'string'
    ? content
    : Array.join(
        Array.flatMap(content, (part) =>
          'text' in part && typeof part.text === 'string' ? [part.text] : [],
        ),
        '',
      )

const blockOf = (part: Message.AssistantContent): Block =>
  Match.value(part).pipe(
    Match.withReturnType<Block>(),
    Match.discriminatorsExhaustive('type')({
      text: ({ text }) => Block.Text({ text }),
      thinking: ({ thinking }) => Block.Thinking({ text: thinking }),
      toolCall: ({ name, arguments: args }) => Block.ToolCall({ name, args: JSON.stringify(args) }),
    }),
  )

const blocksOf = (content: ReadonlyArray<Message.AssistantContent>): ReadonlyArray<Block> =>
  Array.map(content, blockOf)

export const bubbleOf = (entry: Entry.EntryRecord): Bubble =>
  pipe(
    Option.fromNullishOr(entry.model?.[0]),
    Option.flatMap(decodeMessage),
    Option.map((message) =>
      Match.value(message).pipe(
        Match.withReturnType<Bubble>(),
        Match.discriminatorsExhaustive('role')({
          user: ({ content }) => Bubble.User({ id: entry.id, text: textOf(content) }),
          assistant: ({ content }) => Bubble.Assistant({ id: entry.id, blocks: blocksOf(content) }),
          toolResult: ({ toolName, content, isError }) =>
            Bubble.ToolResult({ id: entry.id, toolName, text: textOf(content), isError }),
          system: ({ toolsAdded }) =>
            Bubble.System({
              id: entry.id,
              tools: Array.map(toolsAdded ?? [], (tool) => tool.name),
            }),
        }),
      ),
    ),
    Option.getOrElse(() => Bubble.Note({ id: entry.id, kind: entry.kind })),
  )

// STREAMING

type Change = Extract<AgentEvent.AgentEvent, { type: 'message_update' }>['changes'][number]

const blockAt = (
  draft: ReadonlyArray<Block>,
  index: number,
  update: (block: Block) => Block,
): ReadonlyArray<Block> =>
  pipe(
    Array.modify(draft, index, update),
    Option.getOrElse(() => draft),
  )

const placeAt = (draft: ReadonlyArray<Block>, index: number, block: Block): ReadonlyArray<Block> =>
  index < draft.length ? blockAt(draft, index, () => block) : Array.append(draft, block)

const appendText = (delta: string) => (block: Block) =>
  Match.value(block).pipe(
    Match.withReturnType<Block>(),
    Match.tag('Text', ({ text }) => Block.Text({ text: text + delta })),
    Match.tag('Thinking', ({ text }) => Block.Thinking({ text: text + delta })),
    Match.orElse(() => block),
  )

const applyChange = (draft: ReadonlyArray<Block>, change: Change): ReadonlyArray<Block> =>
  Match.value(change).pipe(
    Match.withReturnType<ReadonlyArray<Block>>(),
    Match.discriminators('type')({
      text_start: ({ contentIndex, block }) => placeAt(draft, contentIndex, blockOf(block)),
      thinking_start: ({ contentIndex, block }) => placeAt(draft, contentIndex, blockOf(block)),
      toolcall_start: ({ contentIndex, block }) => placeAt(draft, contentIndex, blockOf(block)),
      text_delta: ({ contentIndex, delta }) => blockAt(draft, contentIndex, appendText(delta)),
      thinking_delta: ({ contentIndex, delta }) => blockAt(draft, contentIndex, appendText(delta)),
      block: ({ contentIndex, block }) => placeAt(draft, contentIndex, blockOf(block)),
      message: ({ message }) => blocksOf(message.content),
    }),
    Match.orElse(() => draft),
  )

// EVENTS

const withBubble = (transcript: Transcript, bubble: Bubble): Transcript => ({
  ...transcript,
  bubbles: Array.some(transcript.bubbles, (existing) => existing.id === bubble.id)
    ? transcript.bubbles
    : Array.append(transcript.bubbles, bubble),
})

/** Fold one event into the transcript. */
export const apply = (transcript: Transcript, event: AgentEvent.AgentEvent): Transcript =>
  Match.value(event).pipe(
    Match.withReturnType<Transcript>(),
    Match.discriminators('type')({
      snapshot: ({ entries, run, generation, tools }) => ({
        bubbles: Array.map(entries, bubbleOf),
        maybeDraft: pipe(
          Option.fromNullishOr(generation?.message),
          Option.map(({ content }) => blocksOf(content)),
        ),
        runningTools: Array.flatMap(tools, (tool) =>
          tool.status === 'running' ? [{ callId: tool.callId, name: tool.name }] : [],
        ),
        activity: run === undefined ? Activity.Idle() : Activity.Running(),
      }),
      run_start: () => ({ ...transcript, activity: Activity.Running() }),
      run_end: () => ({ ...transcript, activity: Activity.Idle(), maybeDraft: Option.none() }),
      message_start: ({ message }) =>
        message['role'] === 'assistant'
          ? { ...transcript, maybeDraft: Option.some([]) }
          : transcript,
      message_update: ({ changes }) => ({
        ...transcript,
        maybeDraft: Option.map(transcript.maybeDraft, (draft) =>
          Array.reduce(changes, draft, applyChange),
        ),
      }),
      message_end: ({ entry }) => ({
        ...withBubble(transcript, bubbleOf(entry)),
        maybeDraft: entry.kind === '$assistant' ? Option.none() : transcript.maybeDraft,
      }),
      entry_appended: ({ entry }) => withBubble(transcript, bubbleOf(entry)),
      tool_execution_start: ({ toolCallId, toolName }) => ({
        ...transcript,
        runningTools: Array.append(transcript.runningTools, {
          callId: toolCallId,
          name: toolName,
        }),
      }),
      tool_execution_end: ({ toolCallId }) => ({
        ...transcript,
        runningTools: Array.filter(transcript.runningTools, (tool) => tool.callId !== toolCallId),
      }),
    }),
    Match.orElse(() => transcript),
  )

/** The transcript as storage holds it: what a crash leaves. */
export const settle = (transcript: Transcript): Transcript => ({
  ...transcript,
  maybeDraft: Option.none(),
  runningTools: [],
  activity: Activity.Idle(),
})

// LOG

const LOG_LIMIT = 80

const detailOf = (event: AgentEvent.AgentEvent): string =>
  Match.value(event).pipe(
    Match.withReturnType<string>(),
    Match.discriminators('type')({
      snapshot: ({ entries }) => `${entries.length} entries`,
      message_update: ({ changes }) =>
        pipe(
          changes,
          Array.flatMap((change) => ('delta' in change ? [change.delta] : [])),
          Array.join(''),
          (text) =>
            text === ''
              ? Array.join(
                  Array.map(changes, (change) => change.type),
                  ', ',
                )
              : JSON.stringify(text),
        ),
      message_end: ({ entry }) => entry.kind,
      entry_appended: ({ entry }) => entry.kind,
      tool_execution_start: ({ toolName, args }) => `${toolName} ${JSON.stringify(args)}`,
      tool_execution_end: ({ toolName, entry }) =>
        entry === undefined ? `${toolName}, no result` : toolName,
      submission: ({ record }) => record.status,
      usage_changed: ({ usage }) =>
        `${Array.reduce(Object.values(usage.models), 0, (sum, { totalTokens }) => sum + totalTokens)} tokens`,
      task_failed: ({ kind, message }) => `${kind}: ${message}`,
    }),
    Match.orElse(() => ''),
  )

const nextSeq = (lines: ReadonlyArray<LogLine>): number =>
  pipe(
    Array.last(lines),
    Option.match({ onNone: () => 0, onSome: ({ seq }) => Number.increment(seq) }),
  )

/** Add a line per event, keeping the latest few. */
export const log = (
  lines: ReadonlyArray<LogLine>,
  events: ReadonlyArray<AgentEvent.AgentEvent>,
): ReadonlyArray<LogLine> =>
  Array.takeRight(
    Array.appendAll(
      lines,
      Array.map(events, (event, index) => ({
        seq: nextSeq(lines) + index,
        type: event.type,
        detail: detailOf(event),
      })),
    ),
    LOG_LIMIT,
  )

/** Add a line the demo itself writes, such as a crash. */
export const note = (
  lines: ReadonlyArray<LogLine>,
  type: string,
  detail: string,
): ReadonlyArray<LogLine> =>
  Array.takeRight(Array.append(lines, { seq: nextSeq(lines), type, detail }), LOG_LIMIT)
