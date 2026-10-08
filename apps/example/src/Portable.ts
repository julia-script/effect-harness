/** A memory-backed tour using standard JavaScript and Effect, without platform services. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Harness from 'effect-harness'
import * as Storage from 'effect-harness/storage'
import * as Env from 'effect-harness/env'
import * as OpenAi from 'effect-harness/provider-openai'
import * as Anthropic from 'effect-harness/provider-anthropic'
import * as Forks from './tour/Forks.ts'
import * as Extensions from './tour/Extensions.ts'
import * as Subagent from './tour/Subagent.ts'
import * as Checkout from './tour/Checkout.ts'
import * as Reminder from './tour/Reminder.ts'
import * as Context from './tour/Context.ts'
import * as Documents from './tour/Documents.ts'
import * as Multiplayer from './tour/Multiplayer.ts'

export const Result = Schema.Struct({
  imports: Schema.Array(Schema.String),
  forks: Forks.Result,
  extensions: Extensions.Result,
  subagent: Subagent.Result,
  checkout: Checkout.Result,
  reminder: Reminder.Result,
  context: Context.Result,
  documents: Documents.Result,
  multiplayer: Multiplayer.Result,
})

export const run = Effect.gen(function* () {
  // Enumerate every portable barrel so bundling also checks its complete import closure.
  const imports = [Harness, Storage, Env, OpenAi, Anthropic].flatMap((module) =>
    Object.keys(module),
  )
  return yield* Schema.decodeEffect(Result)({
    imports,
    forks: yield* Forks.run,
    extensions: yield* Extensions.run,
    subagent: yield* Subagent.run,
    checkout: yield* Checkout.run,
    reminder: yield* Reminder.run,
    context: yield* Context.run,
    documents: yield* Documents.run,
    multiplayer: yield* Multiplayer.run,
  })
})
