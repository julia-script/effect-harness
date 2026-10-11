import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Prompt from 'effect/ai/Prompt'
import * as Extension from 'effect-harness/Extension'
import * as Hook from 'effect-harness/Hook'
import { HookExecution } from 'effect-harness/HookExecution'
import * as PromptSection from 'effect-harness/PromptSection'
import * as PromptSectionWrapper from 'effect-harness/PromptSectionWrapper'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

// After integration build: node apps/example/dist/tour/ScopedHooks.js
// This explicitly invokes leased dispatch. Static Harness composition remains a separate consumer.
class Label extends Context.Service<Label, string>()('example/ScopedHooks/Label') {}
const section = Extension.make({
  name: 'context',
  sections: [PromptSection.make({ key: 'context', render: () => Label })],
}).pipe(Extension.provide(Layer.succeed(Label, 'original')))
const wrapper = (version: number, label: string) =>
  Registry.promptSectionWrapper(
    { id: 'wrapper', version },
    PromptSectionWrapper.make({
      key: 'context',
      wrap: (item) =>
        Effect.succeed({
          ...item,
          render: (prompt) =>
            Effect.gen(function* () {
              const outer = yield* Label
              return `${yield* item.render(prompt)}/${outer}`
            }),
        }),
    }).pipe(PromptSectionWrapper.provide(Layer.succeed(Label, label))),
  )
const asText = (prompt: Prompt.Prompt) =>
  prompt.content
    .flatMap((message) =>
      typeof message.content === 'string'
        ? [message.content]
        : message.content.flatMap((part) => (part.type === 'text' ? [part.text] : [])),
    )
    .join(' ')

NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const session = yield* Session.make()
      const root = yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
      const access = Layer.succeed(HookExecution, {
        conversationId: root.id,
        snapshot: (document, target) => Session.snapshot(session, document, target),
        watch: (document, target) => Session.watch(session, document, target),
        commit: (change) => Session.commit(session, change, { conversationId: root.id }),
      })
      const registry = yield* Registry.make
      yield* registry.install(Registry.extension({ id: 'source', version: 1 }, section))
      yield* registry.install(wrapper(1, 'old'))
      for (const suffix of ['one', 'two'])
        yield* registry.install(
          Registry.hook(
            { id: suffix, version: 1 },
            Hook.make({
              event: 'beforeRequest',
              execute: ({ prompt }) =>
                Effect.gen(function* () {
                  const invocation = yield* HookExecution
                  yield* invocation.commit((tx) =>
                    Transaction.appendEntry(tx, invocation.conversationId, {
                      kind: 'example.hook',
                      data: { prompt: asText(prompt), suffix },
                    }),
                  )
                  return Prompt.make(`${asText(prompt)}/${suffix}`)
                }),
            }),
          ),
        )
      const previous = yield* registry.acquire
      yield* registry.install(wrapper(2, 'new'))
      const current = yield* registry.acquire
      yield* registry.uninstall('wrapper')
      yield* Effect.gen(function* () {
        yield* Effect.log({
          previous: yield* previous.renderSection('context', Prompt.make('question')),
          current: yield* current.renderSection('context', Prompt.make('question')),
        })
        const replacement = yield* current.dispatchHook({
          _tag: 'beforeRequest',
          prompt: Prompt.make('start'),
        })
        if (replacement !== undefined) yield* Effect.log(asText(replacement))
      }).pipe(Effect.provide(access))
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)
