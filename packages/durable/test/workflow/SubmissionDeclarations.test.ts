import { assertSome } from '@effect/vitest/utils'
import * as Identity from '@effect-harness/durable/Identity'
import { assert, describe, it } from '@effect/vitest'
import * as Prompt from 'effect/ai/Prompt'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Exit from 'effect/Exit'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Record from '@effect-harness/durable/Record'
import { Submission, Result } from '@effect-harness/durable/workflow/Submission'
import { Generation } from '@effect-harness/durable/workflow/Generation'
import { ToolCall } from '@effect-harness/durable/workflow/ToolCall'
import { Compaction } from '@effect-harness/durable/workflow/Compaction'
import { Abort } from '@effect-harness/durable/workflow/Abort'

const receipt = Schema.decodeSync(Result)({
  id: 2,
  conversationId: 1,
  type: 'write',
  status: 'done',
  entry: 3,
})
const payload = {
  sessionId: Identity.SessionId.make('test'),
  conversationId: Record.ROOT_CONVERSATION_ID,
  requestId: Identity.RequestId.make('request-1'),
  submission: {
    _tag: 'write' as const,
    type: 'write' as const,
    entry: { kind: 'example', data: { text: 'first' } },
  },
}

describe('SubmissionDeclarations', () => {
  it.effect('uses the normal executor Layer, execute and poll API', () =>
    Effect.gen(function* () {
      const count = yield* Ref.make(0)
      const executor = Submission.toLayer(() =>
        Ref.update(count, (n) => n + 1).pipe(Effect.as(receipt)),
      )
      const runtime = executor.pipe(Layer.provideMerge(WorkflowEngine.layerMemory))
      yield* Effect.gen(function* () {
        const first = yield* Submission.execute(payload)
        const second = yield* Submission.execute({
          ...payload,
          submission: {
            _tag: 'write' as const,
            type: 'write',
            entry: { kind: 'example', data: { text: 'changed' } },
          },
        })
        assert.deepStrictEqual(first, receipt)
        assert.deepStrictEqual(second, receipt)
        const id = yield* Submission.executionId(payload)
        const polled = yield* Submission.poll(id)
        assertSome(polled, new Workflow.Complete({ exit: Exit.succeed(receipt) }))
        assert.strictEqual(yield* Ref.get(count), 1)
      }).pipe(Effect.provide(runtime))
    }),
  )

  it.effect('separates input/write executions so domain request conflict can be checked', () =>
    Effect.gen(function* () {
      const writeId = yield* Submission.executionId(payload)
      const inputId = yield* Submission.executionId({
        ...payload,
        submission: {
          _tag: 'input' as const,
          type: 'input',
          message: Prompt.userMessage({ content: [Prompt.textPart({ text: 'Hello' })] }),
        },
      })
      const conversationId = yield* Schema.decodeEffect(Record.ConversationId)(4)
      const anotherConversation = yield* Submission.executionId({
        ...payload,
        conversationId,
      })
      assert.notStrictEqual(writeId, inputId)
      assert.notStrictEqual(writeId, anotherConversation)
    }),
  )

  it.effect('validates persistent numeric identities and structured payloads', () =>
    Effect.gen(function* () {
      const codec = new TestSchema.Asserts(Submission.payloadSchema)
      yield* codec
        .decoding()
        .succeedEffect(
          { ...payload, submission: { type: 'write', entry: payload.submission.entry } },
          payload,
        )
      yield* codec.encoding().succeedEffect(payload, {
        ...payload,
        submission: { type: 'write', entry: payload.submission.entry },
      })
      yield* codec
        .decoding()
        .failEffect(
          { ...payload, conversationId: Number.MAX_SAFE_INTEGER + 1 },
          'Expected an integer\n  at ["conversationId"]',
        )
      yield* codec
        .decoding()
        .failEffect(
          { ...payload, submission: { type: 'input', message: 'unvalidated string' } },
          'Expected UserMessage\n  at ["submission"]["message"]',
        )
      for (const workflow of [Submission, Generation, ToolCall, Compaction, Abort]) {
        assert.strictEqual(typeof workflow.execute, 'function')
        assert.strictEqual(typeof workflow.toLayer, 'function')
        assert.strictEqual(typeof workflow.resume, 'function')
        assert.strictEqual(typeof workflow.interrupt, 'function')
      }
    }),
  )

  it.effect('distinguishes separator-like identity strings without normalization', () =>
    Effect.gen(function* () {
      const first = yield* Submission.executionId({
        ...payload,
        sessionId: Identity.SessionId.make('a/1'),
        requestId: Identity.RequestId.make('b'),
      })
      const second = yield* Submission.executionId({
        ...payload,
        sessionId: Identity.SessionId.make('a'),
        requestId: Identity.RequestId.make('1/b'),
      })
      assert.notStrictEqual(first, second)
    }),
  )
})
