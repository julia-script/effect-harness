import { Effect } from 'effect'
import { expect, test } from 'vitest'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import * as DemoModel from '../../example/src/DemoModel.js'
import { withModel } from '../src/examples/Tools.js'
import { readAnswer } from '../src/examples/Observe.js'

test('the tool and observation guide modules compose into a working conversation', async () => {
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const harness = yield* Harness.Harness
      const conversation = yield* harness.root
      const submission = yield* Conversation.submit(conversation, {
        type: 'input',
        content: 'hello from the docs',
      })
      return yield* readAnswer(conversation, submission)
    }).pipe(Effect.provide(withModel(DemoModel.layer))),
  )
  expect(result).toEqual({ status: 'InputDone', text: 'HELLO FROM THE DOCS' })
})
