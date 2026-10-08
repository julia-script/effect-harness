/** Compact the active model context, reset it, and still search the full saved history. */
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as Conversation from 'effect-harness/Conversation'
import * as Record from 'effect-harness/Record'
import * as Task from 'effect-harness/Task'
import * as Runtime from './Runtime.ts'

export const HistoryMatch = Schema.Struct({ entryId: Record.EntryId, text: Schema.String })
export const Result = Schema.Struct({
  beforeCount: Schema.Int,
  afterCompactCount: Schema.Int,
  afterResetCount: Schema.Int,
  summaryCalls: Schema.Int,
  summary: Schema.String,
  compactionEntryId: Record.EntryId,
  resetEntryId: Record.EntryId,
  historyMatches: Schema.Array(HistoryMatch),
  afterCompactionPrompt: Schema.String,
  afterResetPrompt: Schema.String,
  finalReply: Schema.String,
})

const textOf = (prompt: Prompt.Prompt): string =>
  prompt.content
    .flatMap((message) =>
      message.role === 'system'
        ? [message.content]
        : message.content.flatMap((part) => (part.type === 'text' ? [part.text] : [])),
    )
    .join('\n')

export const run = Effect.scoped(
  Effect.gen(function* () {
    const calls = yield* Ref.make(0)
    const prompts = yield* Ref.make<ReadonlyArray<string>>([])
    const summary = 'Earlier work: the launch checklist was drafted and reviewed.'
    const { harness } = yield* Runtime.open({
      settings: { compaction: { enabled: false, keepRecentTokens: 1, reserveTokens: 1000 } },
      provider: {
        generateText: () =>
          Ref.update(calls, (count) => count + 1).pipe(
            Effect.as([{ type: 'text' as const, text: summary }, Runtime.finish('stop')]),
          ),
        streamText: ({ prompt }) =>
          Stream.unwrap(
            Ref.update(prompts, (values) => [...values, textOf(prompt)]).pipe(
              Effect.as(Runtime.answer(`Reply: ${Runtime.lastUserText(prompt)}`)),
            ),
          ),
      },
    })
    const root = yield* harness.root
    yield* Runtime.ask(root, 'Draft the launch checklist')
    yield* Runtime.ask(root, 'Review the launch checklist')
    yield* Runtime.ask(root, 'Keep the latest decision available')
    const before = yield* Conversation.snapshot(root)
    const taskId = yield* Conversation.compact(root, 'Retain the launch decisions')
    const task = yield* harness.awaitTask(taskId)
    const outcome = yield* Schema.decodeUnknownEffect(Task.Outcome)(task.state.outcome)
    yield* Runtime.check(outcome.status === 'completed', 'Manual compaction must finish')
    const compacted = yield* Conversation.snapshot(root)
    const marker = compacted.entries.find((entry) => entry.kind === 'harness.compaction')
    if (marker === undefined)
      return yield* new Runtime.ExampleError({ message: 'Compaction did not place its summary' })
    yield* Runtime.check(
      before.entries.every((entry) => compacted.entries.some((stored) => stored.id === entry.id)),
      'Compaction must keep every historical entry',
    )
    yield* Runtime.ask(root, 'Continue from the summary')
    const afterCompactionPrompt = (yield* Ref.get(prompts)).at(-1) ?? ''
    yield* Runtime.check(
      afterCompactionPrompt.includes(summary),
      'The next native request must receive the placed summary',
    )

    const reset = yield* Conversation.reset(root)
    yield* Conversation.append(root, {
      kind: 'tour.reset-note',
      data: { note: 'Start a fresh discussion' },
    })
    const reply = yield* Runtime.ask(root, 'Start a fresh discussion')
    const afterResetPrompt = (yield* Ref.get(prompts)).at(-1) ?? ''
    yield* Runtime.check(
      !afterResetPrompt.includes('launch') && !afterResetPrompt.includes(summary),
      'Reset must remove earlier context from the next model request',
    )
    const saved = yield* harness.transaction((tx) =>
      tx.scanEntries({ conversationId: root.id }).pipe(Stream.runCollect),
    )
    const historyMatches = (yield* Effect.forEach(
      saved,
      Effect.fn('tour.context.history')(function* (entry) {
        const messages = yield* Schema.decodeEffect(
          Schema.toCodecJson(Schema.Array(Prompt.Message)),
        )(entry.model ?? [])
        const text = messages
          .flatMap((message) =>
            message.role === 'system'
              ? []
              : message.content.flatMap((part) => (part.type === 'text' ? [part.text] : [])),
          )
          .join('\n')
        return { entryId: entry.id, text }
      }),
    )).filter((entry) => entry.text.includes('launch'))
    yield* Runtime.check(
      historyMatches.length >= 2,
      'Historical search must still find messages preceding compaction and reset',
    )
    return yield* Schema.decodeEffect(Result)({
      beforeCount: before.entries.length,
      afterCompactCount: compacted.entries.length,
      afterResetCount: saved.length,
      summaryCalls: yield* Ref.get(calls),
      summary,
      compactionEntryId: marker.id,
      resetEntryId: reset.id,
      historyMatches,
      afterCompactionPrompt,
      afterResetPrompt,
      finalReply: reply.text,
    })
  }),
)
