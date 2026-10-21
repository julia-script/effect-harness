/** The two live demos, each a loop of its own in this tab. */
import { Effect, Layer, Ref } from 'effect'
import * as Release from './demo/agents/release.ts'
import * as Wordsmith from './demo/agents/wordsmith.ts'
import * as Demo from './demo/demo.ts'
import * as Lab from './demo/lab.ts'

export const release = Demo.make({
  id: 'release',
  acquire: Effect.gen(function* () {
    const releases = yield* Ref.make<ReadonlyArray<string>>([])
    return yield* Lab.make({
      loop: (model) =>
        Release.loop(model).pipe(Layer.provide(Layer.succeed(Release.Production, { releases }))),
      respond: Release.respond,
      tokensPerSecond: 60,
    })
  }),
  composer: Demo.Composer.Action({ label: '🚀 Ship it', content: 'Ship it' }),
  crashable: true,
})

export const wordsmith = Demo.make({
  id: 'wordsmith',
  acquire: Lab.make({
    loop: Wordsmith.loop,
    respond: Wordsmith.respond,
    tokensPerSecond: 40,
  }),
  composer: Demo.Composer.Chat({ suggestions: Wordsmith.suggestions }),
  crashable: true,
})
