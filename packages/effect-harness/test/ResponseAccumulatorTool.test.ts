import { assertNone, assertSome } from '@effect/vitest/utils'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Prompt from 'effect/ai/Prompt'

import * as Response from 'effect/ai/Response'

import * as Tool from 'effect/ai/Tool'

import * as Toolkit from 'effect/ai/Toolkit'

import * as Agent from 'effect-harness/Agent'

import * as ResponseAccumulator from 'effect-harness/ResponseAccumulator'

import * as ToolRegistration from 'effect-harness/ToolRegistration'

import { Invocation, ToolCall } from 'effect-harness/Invocation'

import { ToolError, ToolExecutionError } from 'effect-harness/ToolError'

describe('ResponseAccumulatorTool', () => {
  describe('native response reduction and tool controls', () => {
    it.effect('unsupported parameter JSON schemas fail through SchemaError lazily', () =>
      Effect.gen(function* () {
        const symbolic = Tool.providerDefined({
          id: 'test.symbolic',
          providerName: 'symbolic',
          customName: 'Symbolic',
          args: Schema.Struct({}),
          parameters: Schema.Struct({ [Symbol('parameter')]: Schema.String }),
          success: Schema.String,
        })({})
        const registrations = yield* ToolRegistration.bind(Toolkit.make(symbolic))
        const registration = registrations[0]
        if (registration === undefined) return yield* Effect.die('Missing symbolic tool')
        const declaration = ToolRegistration.declaration(registration)
        assert.isTrue(Effect.isEffect(declaration))
        const error = yield* declaration.pipe(Effect.flip)
        assert.isTrue(Schema.isSchemaError(error))
      }),
    )
    it('partial copies, tool argument fragments, final block order and final native Prompt', () => {
      let state = ResponseAccumulator.append(
        ResponseAccumulator.make(),
        Response.makePart('text-start', { id: 'text' }),
      )
      assertNone(ResponseAccumulator.partial(state))
      state = ResponseAccumulator.append(
        state,
        Response.makePart('text-delta', { id: 'text', delta: 'hello' }),
      )
      const previous = ResponseAccumulator.partial(state)
      const expectedPrevious = Prompt.assistantMessage({
        content: [Prompt.textPart({ text: 'hello' })],
      })
      assertSome(previous, expectedPrevious)
      state = ResponseAccumulator.append(
        state,
        Response.makePart('tool-params-start', {
          id: 'call',
          name: 'test',
          providerExecuted: false,
        }),
      )
      state = ResponseAccumulator.append(
        state,
        Response.makePart('tool-params-delta', { id: 'call', delta: '{"x":' }),
      )
      const incomplete = ResponseAccumulator.partial(state)
      assertSome(
        incomplete,
        Prompt.assistantMessage({
          content: [
            Prompt.textPart({ text: 'hello' }),
            Prompt.toolCallPart({
              id: 'call',
              name: 'test',
              params: '{"x":',
              providerExecuted: false,
              options: { harness: { partial: true } },
            }),
          ],
        }),
      )
      state = ResponseAccumulator.append(
        state,
        Response.toolCallPart({
          id: 'call',
          name: 'test',
          params: { x: 1 },
          providerExecuted: false,
        }),
      )
      state = ResponseAccumulator.append(state, Response.makePart('text-end', { id: 'text' }))
      assert.deepStrictEqual(
        state.parts.map((part) => part.type),
        ['text', 'tool-call'],
      )
      assertSome(previous, expectedPrevious)
      assert.strictEqual(expectedPrevious.content.length, 1)
      const message = ResponseAccumulator.message(state).content[0]
      assert.strictEqual(message?.role, 'assistant')
      assert.deepStrictEqual(
        message?.role === 'assistant' ? message.content.map((part) => part.type) : [],
        ['text', 'tool-call'],
      )
    })
    it('native append deltas and root fallback preserve text and argument updates', () => {
      const before = Prompt.assistantMessage({
        content: [
          Prompt.textPart({ text: 'a' }),
          Prompt.toolCallPart({ id: 'c', name: 'tool', params: '{', providerExecuted: false }),
        ],
      })
      const after = Prompt.assistantMessage({
        content: [
          Prompt.textPart({ text: 'ab' }),
          Prompt.toolCallPart({ id: 'c', name: 'tool', params: '{"x":', providerExecuted: false }),
        ],
      })
      assert.deepStrictEqual(
        ResponseAccumulator.delta(before, after).map((change) => [
          change.type,
          change.path,
          change.value,
        ]),
        [
          ['append', ['content', 0, 'text'], 'b'],
          ['append', ['content', 1, 'params'], '"x":'],
        ],
      )
      assert.deepStrictEqual(
        ResponseAccumulator.delta(after, Prompt.assistantMessage({ content: [] }))[0]?.path,
        [],
      )
    })
    it('only completed controls count, terminate requires every slot and reset last call wins', () => {
      const completed: ToolRegistration.Execution = {
        outcome: 'completed',
        result: {
          isError: true,
          control: { terminate: true, reset: { note: 'first' }, addTools: ['a', 'b'] },
        },
      }
      const last: ToolRegistration.Execution = {
        outcome: 'completed',
        result: { control: { terminate: true, reset: { note: 'last' }, addTools: ['b', 'c'] } },
      }
      assert.deepStrictEqual(ToolRegistration.controls([completed, last]), {
        terminate: true,
        reset: { note: 'last' },
        addTools: ['a', 'b', 'c'],
      })
      assert.strictEqual(
        ToolRegistration.controls([
          completed,
          { outcome: 'unavailable', result: { control: { terminate: true } } },
        ]).terminate,
        false,
      )
      assert.strictEqual(ToolRegistration.controls([]).terminate, false)
      assert.deepStrictEqual(
        ToolRegistration.controls([
          {
            outcome: 'failed',
            result: { control: { reset: { note: 'ignored' }, addTools: ['ignored'] } },
          },
        ]),
        { terminate: false, addTools: [] },
      )
    })
    it('addTools appends exact offers, removes exclusions and leaves unrestricted config unchanged', () => {
      assert.deepStrictEqual(Agent.addTools({ tools: ['a'] }, ['b', 'a']), { tools: ['a', 'b'] })
      assert.deepStrictEqual(Agent.addTools({ tools: { remove: ['a', 'b'] } }, ['b']), {
        tools: { remove: ['a'] },
      })
      const all = {}
      assert.strictEqual(Agent.addTools(all, ['b']), all)
    })
    it('execution mode any sequential tool forces round and final bounds preserve files without sanitizing explicit text', () => {
      const file = Prompt.filePart({ mediaType: 'image/png', data: 'AA==' })
      const result = ToolRegistration.boundResult(
        {
          content: [file, Prompt.textPart({ text: 'x\0long' }), Prompt.textPart({ text: 'tail' })],
        },
        { maxBytes: 3, maxLines: 10, retain: 'head' },
      )
      assert.strictEqual(result.content?.[1]?.type === 'text' ? result.content[1].text : '', 'x\0l')
      assert.strictEqual(result.content?.[0], file)
      assert.strictEqual(result.diagnostics?.[0]?.kind, 'truncated')
    })
    it.effect(
      'native return-mode failure uses declared failure codec and stays a result rather than throwing',
      () =>
        Effect.gen(function* () {
          const FailureTool = Tool.make('failure', {
            parameters: Schema.Struct({}),
            success: Schema.String,
            failure: ToolError,
            failureMode: 'return',
          })
          const kit = Toolkit.make(FailureTool)
          const registrations = yield* ToolRegistration.bind(kit).pipe(
            Effect.provide(
              kit.toLayer({
                failure: () =>
                  Effect.fail(
                    new ToolError({
                      reason: new ToolExecutionError({ name: 'failure', message: 'expected' }),
                    }),
                  ),
              }),
            ),
          )
          const registration = registrations[0]
          if (registration === undefined) return yield* Effect.die('Missing bound tool')
          const result = yield* registration.execute({}, 'c').pipe(
            Effect.provideService(
              ToolCall,
              ToolCall.of({
                id: 'c',
                output: () => Effect.void,
                details: () => Effect.void,
                diagnostic: () => Effect.void,
              }),
            ),
          )
          assert.strictEqual(result.isFailure, true)
          assert.strictEqual(typeof result.encoded, 'object')
        }).pipe(
          Effect.provideService(
            Invocation,
            Invocation.of({
              cwd: '.',
              report: () => Effect.void,
              progress: () => Effect.void,
            }),
          ),
        ),
    )
    it.effect(
      'native provider-defined no-handler tools bind without local handlers and retain provider identity in declaration',
      () =>
        Effect.gen(function* () {
          const remote = Tool.providerDefined({
            id: 'test.search',
            providerName: 'search',
            customName: 'Search',
            args: Schema.Struct({ query: Schema.String }),
            parameters: Schema.Unknown,
            success: Schema.Unknown,
          })({ query: 'x' })
          const registrations = yield* ToolRegistration.bind(Toolkit.make(remote))
          const registration = registrations[0]
          if (registration === undefined) return yield* Effect.die('Missing provider tool')
          assert.deepStrictEqual((yield* ToolRegistration.declaration(registration)).provider, {
            id: 'test.search',
            name: 'search',
            args: { query: 'x' },
          })
        }),
    )
  })
})
