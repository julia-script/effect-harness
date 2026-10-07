import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as AiResponse from 'effect/ai/Response'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Agent from '../../src/Agent.ts'
import * as Response from '../../src/Response.ts'
import * as Tool from '../../src/Tool.ts'
import { Invocation, ToolCall } from '../../src/Invocation.ts'
import { ToolError, ToolExecution } from '../../src/Error.ts'

describe('native response reduction and tool controls', () => {
  it('partial copies, tool argument fragments, final block order and final native Prompt', () => {
    let state = Response.append(Response.empty(), AiResponse.makePart('text-start', { id: 'text' }))
    assert.strictEqual(Response.partial(state), undefined)
    state = Response.append(
      state,
      AiResponse.makePart('text-delta', { id: 'text', delta: 'hello' }),
    )
    const previous = Response.partial(state)
    state = Response.append(
      state,
      AiResponse.makePart('tool-params-start', {
        id: 'call',
        name: 'test',
        providerExecuted: false,
      }),
    )
    state = Response.append(
      state,
      AiResponse.makePart('tool-params-delta', { id: 'call', delta: '{"x":' }),
    )
    const incomplete = Response.partial(state)
    assert.strictEqual(
      incomplete?.content[1]?.type === 'tool-call' ? incomplete.content[1].params : null,
      '{"x":',
    )
    state = Response.append(
      state,
      AiResponse.toolCallPart({
        id: 'call',
        name: 'test',
        params: { x: 1 },
        providerExecuted: false,
      }),
    )
    state = Response.append(state, AiResponse.makePart('text-end', { id: 'text' }))
    assert.deepStrictEqual(
      state.parts.map((part) => part.type),
      ['text', 'tool-call'],
    )
    assert.strictEqual(previous?.content.length, 1)
    const message = Response.message(state).content[0]
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
      Response.delta(before, after).map((change) => [change.type, change.path, change.value]),
      [
        ['append', ['content', 0, 'text'], 'b'],
        ['append', ['content', 1, 'params'], '"x":'],
      ],
    )
    assert.deepStrictEqual(
      Response.delta(after, Prompt.assistantMessage({ content: [] }))[0]?.path,
      [],
    )
  })
  it('only completed controls count, terminate requires every slot and reset last call wins', () => {
    const completed: Tool.Execution = {
      outcome: 'completed',
      result: {
        isError: true,
        control: { terminate: true, reset: { note: 'first' }, addTools: ['a', 'b'] },
      },
    }
    const last: Tool.Execution = {
      outcome: 'completed',
      result: { control: { terminate: true, reset: { note: 'last' }, addTools: ['b', 'c'] } },
    }
    assert.deepStrictEqual(Tool.controls([completed, last]), {
      terminate: true,
      reset: { note: 'last' },
      addTools: ['a', 'b', 'c'],
    })
    assert.strictEqual(
      Tool.controls([
        completed,
        { outcome: 'unavailable', result: { control: { terminate: true } } },
      ]).terminate,
      false,
    )
    assert.strictEqual(Tool.controls([]).terminate, false)
    assert.deepStrictEqual(
      Tool.controls([
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
    const result = Tool.boundResult(
      { content: [file, Prompt.textPart({ text: 'x\0long' }), Prompt.textPart({ text: 'tail' })] },
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
        const FailureTool = AiTool.make('failure', {
          parameters: Schema.Struct({}),
          success: Schema.String,
          failure: ToolError,
          failureMode: 'return',
        })
        const kit = Toolkit.make(FailureTool)
        const registrations = yield* Tool.bind(kit).pipe(
          Effect.provide(
            kit.toLayer({
              failure: () =>
                Effect.fail(
                  new ToolError({
                    reason: new ToolExecution({ name: 'failure', message: 'expected' }),
                  }),
                ),
            }),
          ),
        )
        const registration = registrations[0]
        if (registration === undefined) return yield* Effect.die('Missing bound tool')
        const result = yield* registration.execute({}, 'c').pipe(
          Effect.provideService(ToolCall, {
            id: 'c',
            output: () => Effect.void,
            details: () => Effect.void,
            diagnostic: () => Effect.void,
          }),
        )
        assert.strictEqual(result.isFailure, true)
        assert.strictEqual(typeof result.encoded, 'object')
      }).pipe(
        Effect.provideService(Invocation, {
          cwd: '.',
          report: () => Effect.void,
          progress: () => Effect.void,
        }),
      ),
  )
  it.effect(
    'native provider-defined no-handler tools bind without local handlers and retain provider identity in declaration',
    () =>
      Effect.gen(function* () {
        const remote = AiTool.providerDefined({
          id: 'test.search',
          providerName: 'search',
          customName: 'Search',
          args: Schema.Struct({ query: Schema.String }),
          parameters: Schema.Unknown,
          success: Schema.Unknown,
        })({ query: 'x' })
        const registrations = yield* Tool.bind(Toolkit.make(remote))
        const registration = registrations[0]
        if (registration === undefined) return yield* Effect.die('Missing provider tool')
        assert.deepStrictEqual((yield* Tool.declaration(registration)).provider, {
          id: 'test.search',
          name: 'search',
          args: { query: 'x' },
        })
      }),
  )
})
