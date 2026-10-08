import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as ToolRegistration from 'effect-harness/ToolRegistration'

import * as Invocation from 'effect-harness/Invocation'

import * as Toolkit from 'effect/ai/Toolkit'

import * as Tool from 'effect/ai/Tool'

describe('ToolRegistrationBinding', () => {
  it.effect(
    'data-last binding retains codec services and metadata through actual tool execution',
    () =>
      Effect.gen(function* () {
        const echo = Tool.make('cleanupEcho', {
          parameters: Schema.String,
          success: Schema.String,
          failure: Schema.Never,
        })
        const toolkit = Toolkit.make(echo)
        const registrations = yield* ToolRegistration.bind({ cleanupEcho: { replay: 'safe' } })(
          toolkit,
        ).pipe(
          Effect.provide(toolkit.toLayer({ cleanupEcho: (value) => Effect.succeed(value + '!') })),
        )
        const registration = registrations[0]
        assert.ok(registration)
        assert.strictEqual(registration.metadata.replay, 'safe')
        const intent = yield* ToolRegistration.makeIntent({ id: 'call', decoded: 'value' })(
          registration,
        )
        assert.strictEqual(intent.replay, 'safe')
        const output = yield* registration.execute(intent.args, intent.id).pipe(
          Effect.provide(Invocation.layerSilent),
          Effect.provideService(
            Invocation.ToolCall,
            Invocation.ToolCall.of({
              id: 'call',
              output: () => Effect.void,
              details: () => Effect.void,
              diagnostic: () => Effect.void,
            }),
          ),
        )
        assert.deepStrictEqual(output, { result: 'value!', encoded: 'value!', isFailure: false })
      }),
  )
})
