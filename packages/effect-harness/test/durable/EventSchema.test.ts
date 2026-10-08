import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as TestSchema from 'effect/testing/TestSchema'

import * as Prompt from 'effect/ai/Prompt'

import * as Event from 'effect-harness/durable/Event'

import * as Inspection from 'effect-harness/durable/Inspection'

import * as View from 'effect-harness/durable/View'

describe('EventSchema', () => {
  it.effect('round-trips native message events and validates structural/graph frames', () =>
    Effect.gen(function* () {
      const batch: Event.Batch = [
        {
          _tag: 'message_start' as const,
          message: Prompt.assistantMessage({ content: [Prompt.textPart({ text: 'hello' })] }),
        },
      ]
      const codec = Schema.fromJsonString(Event.BatchJson)
      const wire =
        '[{"_tag":"message_start","message":{"options":{},"role":"assistant","content":"hello"}}]'
      const assertions = new TestSchema.Asserts(codec)
      yield* assertions.encoding().succeedEffect(batch, wire)
      yield* assertions.decoding().succeedEffect(wire, [
        {
          _tag: 'message_start',
          message: Prompt.assistantMessage({ content: [Prompt.textPart({ text: 'hello' })] }),
        },
      ])
      assert.isFalse(
        Schema.is(Event.AgentEvent)({
          _tag: 'tool_execution_start' as const,
          toolCallId: 'x',
          toolName: 'x',
          args: undefined,
        }),
      )
      assert.isFalse(Schema.is(View.Op)(['splice', [], 1.5, 0, []]))
      assert.isFalse(
        Schema.is(Inspection.GraphChange)({
          seq: -1,
          before: { tasks: {} },
          value: { tasks: {} },
          ops: [],
          reset: false,
        }),
      )
    }),
  )
})
