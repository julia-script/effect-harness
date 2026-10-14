import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolSearch from 'effect-harness/ToolSearch'

const application = Toolkit.make(
  Tool.make('findAppointments', {
    description: 'Find calendar appointments by person',
    deferred: true,
    replay: 'safe',
    parameters: Schema.Struct({ person: Schema.String }),
    success: Schema.Array(Schema.String),
  }),
  Tool.make('findDocuments', {
    description: 'Find project documents by title',
    deferred: true,
    replay: 'safe',
    parameters: Schema.Struct({ title: Schema.String }),
    success: Schema.Array(Schema.String),
  }),
  Tool.make('findMessages', {
    description: 'Find email messages by sender',
    deferred: true,
    replay: 'safe',
    parameters: Schema.Struct({ sender: Schema.String }),
    success: Schema.Array(Schema.String),
  }),
)
export const toolkit = Toolkit.merge(ToolSearch.toolkit, application)
export const handlers = Layer.merge(
  ToolSearch.layer,
  application.toLayer({
    findAppointments: ({ person }) => Effect.succeed([`Review with ${person} at noon`]),
    findDocuments: ({ title }) => Effect.succeed([`/documents/${title}`]),
    findMessages: ({ sender }) => Effect.succeed([`Message from ${sender}: review scheduled`]),
  }),
)

/** Supply a LanguageModel and Storage Layer. The runtime offers only tool_search initially. */
export const program = Effect.scoped(
  Effect.gen(function* () {
    const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(Effect.provide(handlers))
    const conversationId = yield* runtime.backend.root
    const job = yield* runtime.backend.submit({
      conversationId,
      draft: { type: 'input', content: 'Find my calendar appointment with Sam.' },
    })
    yield* runtime.backend.wait(job.id)
    return yield* runtime.backend.agent(conversationId)
  }),
)
