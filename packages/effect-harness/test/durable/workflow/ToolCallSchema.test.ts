import * as Ownership from 'effect-harness/durable/Ownership'

import { ToolCall } from 'effect-harness/durable/workflow/ToolCall'

import { assert, describe, it } from '@effect/vitest'

import * as Ref from 'effect/Ref'

import * as HashSet from 'effect/HashSet'

import * as Effect from 'effect/Effect'

import * as Event from 'effect-harness/durable/Event'

import * as Identity from 'effect-harness/durable/Identity'

import * as Record from 'effect-harness/durable/Record'

import type * as View from 'effect-harness/durable/View'

import * as Outcome from 'effect-harness/durable/workflow/Outcome'

describe('ToolCallSchema', () => {
  it.effect(
    'uses declared tool payload and pinned checkpoint schemas for tool-start arguments',
    () =>
      Effect.gen(function* () {
        const conversation = { id: Record.ROOT_CONVERSATION_ID }
        const taskId = Record.TaskId.make(2)
        const payload = {
          sessionId: Identity.SessionId.make('schema'),
          conversationId: conversation.id,
          taskId,
          generationTaskId: taskId,
          assistantId: Record.EntryId.make(3),
          callId: 'call',
          name: 'tool',
          arguments: { original: true },
        }
        const binding = Ownership.Binding.make({
          workflow: ToolCall._tag,
          executionId: 'external-native-execution',
          payload,
        })
        const task: Record.Task = {
          id: taskId,
          conversationId: conversation.id,
          kind: 'harness.tool',
          version: 1,
          input: binding,
          background: false,
          abortRequested: false,
          state: {
            status: 'running',
            checkpoint: Outcome.ToolCheckpoint.make({ arguments: { pinned: true } }),
          },
        }
        const before: View.Value = { conversation, entries: [], docs: {} }
        const value: View.Value = {
          ...before,
          docs: {
            'harness.live': {
              tools: [{ taskId, callId: 'call', name: 'tool', status: 'running' }],
            },
          },
        }
        const change: View.Change = {
          seq: Record.Seq.make(1),
          before,
          value,
          ops: [],
          reset: false,
          publication: {
            seq: Record.Seq.make(1),
            writes: [{ _tag: 'task' as const, value: task }],
            documents: [],
          },
        }
        const events = yield* Event.translate(
          conversation.id,
          change,
          yield* Ref.make(HashSet.empty<Record.TaskId>()),
        )
        assert.deepStrictEqual(events[0], {
          _tag: 'tool_execution_start' as const,
          toolCallId: 'call',
          toolName: 'tool',
          args: { pinned: true },
        })
        const invalid = {
          ...change,
          publication: {
            ...change.publication!,
            writes: [
              {
                _tag: 'task' as const,
                value: { ...task, input: { ...binding, payload: { arguments: { wrong: true } } } },
              },
            ],
          },
        }
        assert.deepStrictEqual(
          (yield* Event.translate(
            conversation.id,
            invalid,
            yield* Ref.make(HashSet.empty<Record.TaskId>()),
          ))[0],
          {
            _tag: 'tool_execution_start' as const,
            toolCallId: 'call',
            toolName: 'tool',
            args: {},
          },
        )
      }),
  )
})
