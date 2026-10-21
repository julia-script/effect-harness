/**
 * Tool handlers can require the Harness: the tool task provides the one it runs under, so a tool
 * delegates to a conversation its call owns without the Harness in its layer.
 */
import { describe, expect, test } from 'bun:test'
import { Context, Effect, Layer, Option, Schema } from 'effect'
import { Faux, Model } from '../../model/index.ts'
import * as Storage from '../../storage/Storage.ts'
import { Call, Tool, Toolkit } from '../../tool/index.ts'
import { Extension, Harness } from '../index.ts'
import { runTest } from '../../test/harness.ts'

const Delegate = Tool.make('delegate', {
  description: 'Delegate a task to a subagent.',
  parameters: Schema.Struct({ task: Schema.String }),
  success: Schema.Struct({ conversationId: Schema.Finite, answer: Schema.String }),
})
const toolkit = Toolkit.make(Delegate)
const delegate = Extension.make('delegate', { toolkit })

const answerText = (model: ReadonlyArray<Record<string, unknown>> | undefined) => {
  const content: unknown = model?.[0]?.['content']
  if (!Array.isArray(content)) return ''
  return content
    .flatMap((block: unknown) =>
      typeof block === 'object' && block !== null && 'text' in block ? [String(block.text)] : [],
    )
    .join('')
}

const DelegateLive = toolkit.toLayer({
  delegate: ({ task }) =>
    Effect.gen(function* () {
      const harness = yield* Harness.Harness
      const call = yield* Call.Current
      if (call === undefined) return yield* Effect.die('not in a tool call')
      const child = yield* harness.createConversation({
        ownership: { kind: 'task', taskId: call.taskId },
      })
      const submission = yield* child.submit({ type: 'input', content: task })
      const settled = yield* submission.wait
      if (settled.status !== 'done' || settled.type !== 'input')
        return yield* Effect.die('child unanswered')
      const page = yield* child.entries(
        { minEntryId: settled.answer, maxEntryId: settled.answer },
        1,
      )
      return { conversationId: child.id, answer: answerText(page.items[0]?.model) }
    }).pipe(Effect.orDie),
})

const loop = (faux: Faux.Faux, storage: Storage.Service) =>
  Harness.layer({
    extensions: [delegate],
    settings: { model: faux.ref(), retry: { baseDelayMs: 1, maxDelayMs: 1 } },
  }).pipe(
    Layer.provide([
      DelegateLive,
      Model.layer([faux.provider]),
      Layer.succeed(Storage.Storage, storage),
    ]),
  )

describe('the Harness in tool handlers', () => {
  test('is missing outside a Harness, so the call breaks', () =>
    runTest(() =>
      Effect.gen(function* () {
        const context = yield* Layer.build(DelegateLive)
        const found = Context.getOption(context, Toolkit.implementation('delegate'))
        if (Option.isNone(found)) return yield* Effect.die('delegate has no implementation')
        const outcome = yield* found.value.run(
          { task: 'Anything.' },
          { callId: 'call-1', config: {} },
        )
        expect(outcome).toMatchObject({ type: 'broken' })
      }),
    ))

  test('lets a tool handler run a conversation its call owns', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const faux = yield* Faux.make()
        yield* faux.setResponses([
          Faux.message(Faux.toolCall('delegate', { task: 'Name three primes.' }), {
            stopReason: 'toolUse',
          }),
          Faux.message('2, 3 and 5.'),
          Faux.message('The subagent said 2, 3 and 5.'),
        ])
        const context = yield* Layer.build(loop(faux, storage))
        const harness = Context.get(context, Harness.Harness)
        const root = yield* harness.root()
        const submission = yield* root.submit({ type: 'input', content: 'Delegate it.' })
        expect(yield* submission.wait).toMatchObject({ status: 'done' })

        const { items } = yield* root.entries({ order: 'ascending' }, 50)
        const result = items.find((entry) => entry.kind === '$tool-result')
        expect(JSON.stringify(result?.model)).toContain('2, 3 and 5.')
        const ownerTaskId = result?.byTaskId
        if (ownerTaskId === undefined) return yield* Effect.die('tool result without a task')

        const owned = yield* harness.session.commit((tx) =>
          tx.scanConversations({ ownerTaskId }, 10),
        )
        expect(owned.items).toHaveLength(1)
        const child = yield* harness.conversation(owned.items[0]!.id)
        const transcript = yield* child!.entries({ order: 'ascending' }, 50)
        expect(transcript.items.map((entry) => entry.kind)).toContain('$assistant')
      }),
    ))
})
