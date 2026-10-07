import * as Identity from '../../src/Identity.ts'
import { assert, describe, it } from '@effect/vitest'
import * as Prompt from 'effect/ai/Prompt'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Record from '../../src/Record.ts'
import { Submission, Result } from '../../src/workflow/Submission.ts'
import { Generation } from '../../src/workflow/Generation.ts'
import { ToolCall } from '../../src/workflow/ToolCall.ts'
import { Compaction } from '../../src/workflow/Compaction.ts'
import { Abort } from '../../src/workflow/Abort.ts'

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
  submission: { type: 'write' as const, entry: { kind: 'example', data: { text: 'first' } } },
}

describe('native Workflow declarations', () => {
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
          submission: { type: 'write', entry: { kind: 'example', data: { text: 'changed' } } },
        })
        assert.deepStrictEqual(first, receipt)
        assert.deepStrictEqual(second, receipt)
        const id = yield* Submission.executionId(payload)
        const polled = yield* Submission.poll(id)
        assert.strictEqual(polled._tag, 'Some')
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

  it('validates persistent numeric identities and structured payloads', () => {
    assert.strictEqual(
      Schema.decodeOption(Submission.payloadSchema)({
        ...payload,
        conversationId: Number.MAX_SAFE_INTEGER + 1,
      })._tag,
      'None',
    )
    assert.strictEqual(
      Schema.decodeUnknownOption(Submission.payloadSchema)({
        ...payload,
        submission: { type: 'input', message: 'unvalidated string' },
      })._tag,
      'None',
    )
    for (const workflow of [Submission, Generation, ToolCall, Compaction, Abort]) {
      assert.strictEqual(typeof workflow.execute, 'function')
      assert.strictEqual(typeof workflow.toLayer, 'function')
      assert.strictEqual(typeof workflow.resume, 'function')
      assert.strictEqual(typeof workflow.interrupt, 'function')
    }
  })

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
