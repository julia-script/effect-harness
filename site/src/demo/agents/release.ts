/**
 * The agent behind the "Kill the loop" demo: a release pipeline whose steps take a while, so
 * there is time to crash the loop in the middle of one. `Production` stands for the world outside
 * the loop. It survives crashes, like a real deploy target would.
 */
import { Array, Context, Effect, Layer, Option, pipe, Ref, Schema, String } from 'effect'
import { Extension, HarnessServer } from 'effect-harness/harness'
import type { Model, Message } from 'effect-harness/model'
import { Faux } from 'effect-harness/model'
import { Tool, Toolkit } from 'effect-harness/tool'

export class Production extends Context.Service<
  Production,
  { readonly releases: Ref.Ref<ReadonlyArray<string>> }
>()('site/Production') {}

const version = Schema.Struct({ version: Schema.String })

// Building and testing again is harmless, so after a crash the loop simply runs them again.
const Build = Tool.make('build', {
  description: 'Build a release.',
  parameters: version,
  success: Schema.String,
  replay: 'safe',
})

const Test = Tool.make('test', {
  description: 'Run the test suite against a build.',
  parameters: version,
  success: Schema.String,
  replay: 'safe',
})

// Deploying twice is not harmless. A crash mid-deploy answers `interrupted` instead, and the
// model decides what to do.
const Deploy = Tool.make('deploy', {
  description: 'Deploy a release to production.',
  parameters: version,
  success: Schema.String,
})

const Status = Tool.make('status', {
  description: 'Check whether a release is live in production.',
  parameters: version,
  success: Schema.String,
  replay: 'safe',
})

const toolkit = Toolkit.make(Build, Test, Deploy, Status)

const ToolsLive = toolkit.toLayer(
  Effect.gen(function* () {
    const { releases } = yield* Production
    return {
      build: ({ version }) => Effect.as(Effect.sleep('1600 millis'), `Built ${version}.`),
      test: () => Effect.as(Effect.sleep('2 seconds'), '128 tests passed.'),
      deploy: ({ version }) =>
        Effect.sleep('2400 millis').pipe(
          Effect.andThen(Ref.update(releases, Array.append(version))),
          Effect.as(`${version} is live.`),
        ),
      status: ({ version }) =>
        Effect.map(Ref.get(releases), (live) =>
          Array.contains(live, version) ? `${version} is live.` : `${version} is not live.`,
        ),
    }
  }),
)

export const loop = (model: Model.Ref) =>
  HarnessServer.layer({
    extensions: [Extension.make('release', { toolkit })],
    settings: { model },
  }).pipe(Layer.provide(ToolsLive))

// The scripted model: one step of the pipeline per request, chosen from the last message.

const textOf = (result: Message.ToolResultMessage): string =>
  Array.join(
    Array.flatMap(result.content, (part) => (part.type === 'text' ? [part.text] : [])),
    '',
  )

const call = (name: string, version: string, thought?: string) =>
  Faux.message(
    [...(thought === undefined ? [] : [Faux.thinking(thought)]), Faux.toolCall(name, { version })],
    { stopReason: 'toolUse' },
  )

const next = (version: string, result: Message.ToolResultMessage) => {
  if (result.toolName === 'build') {
    return call('test', version)
  }
  if (result.toolName === 'test') {
    return call('deploy', version)
  }
  if (result.toolName === 'deploy' && result.isError) {
    return call(
      'status',
      version,
      'The deploy was interrupted. Deploying twice is not safe, so check what landed first.',
    )
  }
  if (result.toolName === 'status' && !String.includes('not live')(textOf(result))) {
    return Faux.message(`${version} landed before the crash, so I did not deploy it again.`)
  }
  if (result.toolName === 'status') {
    return call('deploy', version, `${version} never landed. Deploying it now.`)
  }
  return Faux.message(`Shipped ${version}.`)
}

export const respond: Faux.Respond = (request) => {
  const version = `v1.${Array.filter(request.messages, (message) => message.role === 'user').length}`
  return Effect.succeed(
    pipe(
      Array.last(request.messages),
      Option.flatMap((message) =>
        message.role === 'toolResult' ? Option.some(message) : Option.none(),
      ),
      Option.match({
        onNone: () => call('build', version, `Release ${version}: build, test, then deploy.`),
        onSome: (result) => next(version, result),
      }),
    ),
  )
}
