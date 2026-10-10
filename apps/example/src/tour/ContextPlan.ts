import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Plan from 'effect-harness/ContextPlan'
import * as Projection from 'effect-harness/ContextProjection'
import * as Extension from 'effect-harness/Extension'
import { HookExecution } from 'effect-harness/HookExecution'
import * as PromptSection from 'effect-harness/PromptSection'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'

// After integration build: node apps/example/dist/tour/ContextPlan.js
// This example explicitly persists read-only plans. Runtime activation remains a separate integration.
class Instructions extends Context.Service<Instructions, string>()(
  'example/ContextPlan/Instructions',
) {}
const instructions = (text: string) =>
  Extension.make({
    name: 'instructions',
    sections: [PromptSection.make({ key: 'guidance', render: () => Instructions })],
  }).pipe(Extension.provide(Layer.succeed(Instructions, text)))
const lookup = Tool.make('lookup', {
  description: 'Look up a label',
  parameters: Schema.Struct({ label: Schema.String }),
  success: Schema.String,
})
const toolkit = Toolkit.make(lookup)

NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const session = yield* Session.make()
      const root = yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
      yield* Session.commit(session, (tx) =>
        Transaction.appendEntry(tx, root.id, {
          kind: 'input',
          model: [{ role: 'user', content: 'Find a label' }],
        }),
      )
      const protocol = yield* Plan.activateProtocol(
        (yield* Projection.read(session, root.id)).bounds,
      )
      // A runtime integrator persists this protocol once and inherits it unchanged on forks.
      const access = Layer.succeed(HookExecution, {
        conversationId: root.id,
        snapshot: (document, target) => Session.snapshot(session, document, target),
        watch: (document, target) => Session.watch(session, document, target),
        commit: (change) => Session.commit(session, change, { conversationId: root.id }),
      })
      const registry = yield* Registry.make
      yield* registry.install(
        Registry.provide(
          Registry.toolkit({ id: 'tools', version: 1 }, toolkit),
          toolkit.toLayer({ lookup: () => Effect.succeed('unused: planning never invokes tools') }),
        ),
      )
      yield* registry.install(
        Registry.extension({ id: 'instructions', version: 1 }, instructions('Answer briefly.')),
      )
      for (const version of [1, 2]) {
        if (version === 2)
          yield* registry.install(
            Registry.extension(
              { id: 'instructions', version },
              instructions('Include the source.'),
            ),
          )
        // The caller keeps this lease through render and the serialized plan commit.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const lease = yield* registry.acquire
            const view = yield* Plan.readWithProtocol(session, root.id, protocol)
            const desired = yield* Plan.render(view, lease).pipe(Effect.provide(access))
            const planned = yield* Plan.plan(view, desired)
            yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                for (const draft of planned.drafts)
                  yield* Transaction.appendEntry(tx, root.id, draft)
              }),
            )
          }),
        )
        if (version === 1)
          yield* Session.commit(session, (tx) =>
            Transaction.appendEntry(tx, root.id, {
              kind: 'assistant',
              model: [{ role: 'assistant', content: 'A first answer.' }],
            }),
          )
      }
      const view = yield* Plan.readWithProtocol(session, root.id, protocol)
      const positioned = yield* Plan.translate(view, { sections: 'positional', tools: 'latest' })
      const fallback = yield* Plan.translate(view, { sections: 'leading', tools: 'latest' })
      yield* Effect.log({
        positioned: positioned.prompt.content.map((message) =>
          message.role === 'system' ? message.content : message.role,
        ),
        fallback: fallback.prompt.content.map((message) =>
          message.role === 'system' ? message.content : message.role,
        ),
        tools: positioned.tools.map((tool) => tool.name),
        protocol,
      })
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)
