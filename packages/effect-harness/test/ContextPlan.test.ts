import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as Prompt from 'effect/ai/Prompt'
import * as NativeTool from 'effect/ai/Tool'
import * as Plan from 'effect-harness/ContextPlan'
import * as Projection from 'effect-harness/ContextProjection'
import * as Document from 'effect-harness/Document'
import * as Extension from 'effect-harness/Extension'
import * as PromptSection from 'effect-harness/PromptSection'
import * as PromptSectionWrapper from 'effect-harness/PromptSectionWrapper'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolWrapper from 'effect-harness/ToolWrapper'
import { HookExecution } from 'effect-harness/HookExecution'
import { ToolExecution } from 'effect-harness/ToolExecution'
import { RegistryError } from 'effect-harness/RegistryError'
import { execution, Label } from './fixtures/Registry.js'

const root = Record.ROOT_CONVERSATION_ID
const id = Schema.decodeSync(Record.EntryId)
const entry = (n: number, rest: Partial<Record.Entry> = {}): Record.Entry => ({
  id: id(n),
  conversationId: root,
  kind: 'custom',
  ...rest,
})
const section = (key: string, text = key): Projection.Section => ({ key, text })
const tool = (name: string, description = name): Projection.ToolDeclaration => ({
  name,
  description,
  parameters: { type: 'object' },
})
const apply = (entries: ReadonlyArray<Record.Entry>, planned: Plan.Plan) => {
  const next = (entries.at(-1)?.id ?? 0) + 1
  return [...entries, ...planned.drafts.map((draft, index) => entry(next + index, draft))]
}
const baseline = Effect.fnUntraced(function* (desired: Plan.Desired) {
  return apply([], yield* Plan.plan(yield* Projection.project(root, []), desired))
})
const texts = (prompt: Prompt.Prompt) =>
  prompt.content.flatMap((message) =>
    message.role === 'system'
      ? [message.content]
      : message.content.flatMap((part) => (part.type === 'text' ? [part.text] : [])),
  )
const hookAccess = Layer.effect(HookExecution, ToolExecution).pipe(Layer.provide(execution))
class CodecService extends Context.Service<CodecService, string>()('test/context/CodecService') {}

describe('positional context planning', () => {
  it.effect('reorders with section remove/readd drafts and tool changes on the last draft', () =>
    Effect.gen(function* () {
      const entries = yield* baseline({
        sections: [section('A'), section('B')],
        tools: [tool('a'), tool('b')],
      })
      const view = yield* Projection.project(root, entries)
      const desired = {
        sections: [section('B'), section('A', 'changed')],
        tools: [tool('b'), tool('a')],
      }
      const planned = yield* Plan.plan(view, desired)
      assert.strictEqual(planned.drafts.length, 2)
      assert.deepEqual(planned.drafts[0]!.data.sections, [
        { _tag: 'remove', key: 'A' },
        { _tag: 'remove', key: 'B' },
      ])
      assert.deepEqual(planned.drafts[0]!.data.tools, [])
      assert.deepEqual(
        planned.drafts[1]!.data.tools.map((change) => change._tag),
        ['remove', 'remove', 'set', 'set'],
      )
      const replayed = yield* Projection.project(root, apply(entries, planned))
      assert.deepEqual(replayed.orderedSections, desired.sections)
      assert.deepEqual(replayed.toolDeclarations, desired.tools)
      assert.deepEqual((yield* Plan.plan(replayed, desired)).drafts, [])
      assert.isTrue(Object.isFrozen(planned.drafts[1]!.data.tools))
      assert.isFalse(Object.isFrozen(desired))
      desired.sections[1] = section('changed-after-plan')
      assert.deepEqual(planned.desired.sections, [section('B'), section('A', 'changed')])
    }),
  )

  it.effect(
    'uses minimal value changes and compares JSON keys structurally including provider options',
    () =>
      Effect.gen(function* () {
        const a = {
          ...tool('a'),
          parameters: { type: 'object', properties: { x: { type: 'string' } } },
          options: { provider: { strict: true } },
        }
        const entries = yield* baseline({
          sections: [section('A'), section('B')],
          tools: [a, tool('b')],
        })
        const same = { ...a, parameters: { properties: { x: { type: 'string' } }, type: 'object' } }
        const view = yield* Projection.project(root, entries)
        assert.deepEqual(
          (yield* Plan.plan(view, { sections: view.orderedSections, tools: [same, tool('b')] }))
            .drafts,
          [],
        )
        const desired = {
          sections: [section('A', 'changed'), section('C')],
          tools: [{ ...same, options: { provider: { strict: false } } }, tool('b')],
        }
        const planned = yield* Plan.plan(view, desired)
        assert.strictEqual(planned.drafts.length, 1)
        assert.deepEqual(planned.drafts[0]!.data.sections, [
          { _tag: 'set', section: section('A', 'changed') },
          { _tag: 'remove', key: 'B' },
          { _tag: 'set', section: section('C') },
        ])
        const replayed = yield* Projection.project(root, apply(entries, planned))
        assert.deepEqual(replayed.toolDeclarations, desired.tools)
        assert.deepEqual(replayed.orderedSections, desired.sections)
      }),
  )

  it.effect(
    'replay converges for all small order changes, removals, replacements and empty inventories',
    () =>
      Effect.gen(function* () {
        const orders = [
          [],
          ['A'],
          ['B', 'A'],
          ['A', 'B', 'C'],
          ['C', 'B', 'A'],
          ['B', 'C', 'A'],
          ['D', 'A'],
        ]
        for (const from of orders)
          for (const to of orders) {
            const entries = yield* baseline({
              sections: from.map((key) => section(key)),
              tools: from.map((name) => tool(name)),
            })
            const desired = {
              sections: to.map((key) => section(key, `new:${key}`)),
              tools: to.map((name) => tool(name, `new:${name}`)),
            }
            const planned = yield* Plan.plan(yield* Projection.project(root, entries), desired)
            const replayed = yield* Projection.project(root, apply(entries, planned))
            assert.deepEqual(replayed.orderedSections, desired.sections)
            assert.deepEqual(replayed.toolDeclarations, desired.tools)
            assert.deepEqual((yield* Plan.plan(replayed, desired)).drafts, [])
          }
      }),
  )

  it.effect(
    'a head change restates a baseline, omits retained old declarations and preserves edits',
    () =>
      Effect.gen(function* () {
        const desired = { sections: [section('A')], tools: [tool('a')] }
        const previous = yield* baseline(desired)
        const entries = [
          ...previous,
          entry(2, { kind: 'input', model: [{ role: 'user', content: 'before' }] }),
          entry(3, {
            head: id(1),
            edits: [
              { _tag: 'replace', target: id(2), messages: [{ role: 'user', content: 'edited' }] },
            ],
          }),
        ]
        const bounded = yield* Projection.project(root, entries, { at: id(2) })
        assert.deepEqual((yield* Plan.plan(bounded, desired)).drafts, [])
        const view = yield* Projection.project(root, entries)
        const planned = yield* Plan.plan(view, desired)
        assert.strictEqual(planned.drafts.length, 1)
        assert.strictEqual(planned.drafts[0]!.data.baseline, true)
        assert.deepEqual(planned.drafts[0]!.edits, [{ _tag: 'omit', target: id(1) }])
        const replayed = yield* Projection.project(root, apply(entries, planned))
        assert.deepEqual(
          replayed.positionalFacts.map((fact) => fact.entryId),
          [id(4)],
        )
        assert.deepEqual(
          texts(
            (yield* Plan.translate(replayed, { sections: 'positional', tools: 'positional' }))
              .prompt,
          ),
          ['A', 'edited'],
        )
        assert.deepEqual((yield* Plan.plan(replayed, desired)).drafts, [])
        assert.deepEqual(entries[0]!.data, previous[0]!.data)
      }),
  )

  it.effect(
    'positions facts, offers an explicit leading/latest fallback and repairs native media/tool results',
    () =>
      Effect.gen(function* () {
        const media: Schema.Json = {
          role: 'user',
          content: [
            {
              type: 'file',
              mediaType: 'image/png',
              data: 'data:image/png;base64,YQ==',
              options: { provider: { detail: 'high' } },
            },
          ],
          options: { provider: { cache: true } },
        }
        const initial = yield* baseline({ sections: [section('A')], tools: [tool('a')] })
        const entries = [
          entry(1, { model: [media] }),
          ...initial.map((fact) => ({ ...fact, id: id(2) })),
          entry(3, {
            model: [
              {
                role: 'assistant',
                content: [
                  {
                    type: 'tool-call',
                    id: 'same-id',
                    name: 'a',
                    params: {},
                    providerExecuted: false,
                  },
                  {
                    type: 'tool-call',
                    id: 'missing',
                    name: 'b',
                    params: {},
                    providerExecuted: false,
                  },
                ],
                options: { provider: { response: 'native' } },
              },
            ],
          }),
          entry(4, {
            model: [
              {
                role: 'tool',
                content: [
                  {
                    type: 'tool-result',
                    id: 'same-id',
                    name: 'wrong',
                    result: 'orphan',
                    isFailure: false,
                    providerExecuted: false,
                  },
                  {
                    type: 'tool-result',
                    id: 'same-id',
                    name: 'a',
                    result: 'correct',
                    isFailure: false,
                    providerExecuted: false,
                    options: { provider: { receipt: true } },
                  },
                ],
                options: { provider: { grouped: true } },
              },
            ],
          }),
        ]
        const changed = yield* Plan.plan(yield* Projection.project(root, entries), {
          sections: [section('A', 'later')],
          tools: [tool('b')],
        })
        const complete = apply(entries, changed)
        const view = yield* Projection.project(root, complete)
        const positional = yield* Plan.translate(view, {
          sections: 'positional',
          tools: 'positional',
        })
        assert.deepEqual(
          positional.prompt.content.map((message) => message.role),
          ['system', 'user', 'assistant', 'tool', 'tool', 'system'],
        )
        assert.deepEqual(texts(positional.prompt), ['A', 'later'])
        assert.deepEqual(
          positional.frames.map((frame) => frame.tools.map((tool) => tool.name)),
          [['a'], ['b']],
        )
        const results = positional.prompt.content
          .flatMap((message) => (message.role === 'tool' ? message.content : []))
          .filter((part) => part.type === 'tool-result')
        assert.deepEqual(
          results.map((part) => [part.id, part.name, part.isFailure]),
          [
            ['same-id', 'a', false],
            ['missing', 'b', true],
          ],
        )
        assert.strictEqual(results[0]!.result, 'correct')
        assert.deepEqual(results[0]!.options, { provider: { receipt: true } })
        assert.deepEqual(positional.prompt.content[3]!.options, { provider: { grouped: true } })
        assert.deepEqual(positional.prompt.content[1], view.prompt.content[0])
        const fallback = yield* Plan.translate(view, { sections: 'leading', tools: 'latest' })
        assert.deepEqual(fallback.positioning, { sections: 'leading', tools: 'latest' })
        assert.deepEqual(texts(fallback.prompt), ['later'])
        assert.deepEqual(fallback.frames, positional.frames)
        assert.deepEqual(fallback.toolPositions, [])
        assert.deepEqual(positional.toolPositions, [
          { entryId: id(2), messageIndex: 0 },
          { entryId: id(5), messageIndex: 5 },
        ])
        assert.deepEqual(fallback.tools, [tool('b')])
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Plan.TranslationSchema))(
          positional,
        )
        const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(Plan.TranslationSchema))(
          encoded,
        )
        assert.deepEqual(decoded.prompt.content, positional.prompt.content)
        assert.isTrue(Object.isFrozen(positional.frames[0]!.tools[0]!.parameters))
        assert.deepEqual(
          texts(
            (yield* Plan.translate(yield* Projection.project(root, complete, { at: id(4) }), {
              sections: 'positional',
              tools: 'latest',
            })).prompt,
          ),
          ['A'],
        )
      }),
  )

  it.effect('tool-only changes have precise native marker offsets after result pairing', () =>
    Effect.gen(function* () {
      const entries = yield* baseline({ sections: [], tools: [tool('a')] })
      const withCall = [
        ...entries,
        entry(2, {
          model: [
            {
              role: 'assistant',
              content: [
                {
                  type: 'tool-call',
                  id: 'pending',
                  name: 'a',
                  params: {},
                  providerExecuted: false,
                },
              ],
            },
          ],
        }),
      ]
      const changed = yield* Plan.plan(yield* Projection.project(root, withCall), {
        sections: [],
        tools: [tool('b')],
      })
      const translated = yield* Plan.translate(
        yield* Projection.project(root, apply(withCall, changed)),
        { sections: 'positional', tools: 'positional' },
      )
      assert.deepEqual(
        translated.prompt.content.map((message) => message.role),
        ['system', 'assistant', 'tool', 'system'],
      )
      assert.deepEqual(translated.toolPositions, [
        { entryId: id(1), messageIndex: 0 },
        { entryId: id(3), messageIndex: 3 },
      ])
      for (const position of translated.toolPositions) {
        assert.strictEqual(translated.prompt.content[position.messageIndex]!.role, 'system')
        assert.isTrue(translated.frames.some((frame) => frame.entryId === position.entryId))
      }
      assert.deepEqual(
        translated.frames.map((frame) => frame.tools.map((tool) => tool.name)),
        [['a'], ['b']],
      )
    }),
  )

  it.effect(
    'exports the transformed native tool schema without invoking its serviceful codec or handler',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          let calls = 0
          let codecCalls = 0
          const original = Tool.make('transformed', {
            parameters: Schema.Struct({ value: Schema.String }),
            success: Schema.String,
          })
          const toolkit = Toolkit.make(original)
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
              toolkit.toLayer({
                transformed: () =>
                  Effect.sync(() => {
                    calls++
                    return 'unexpected'
                  }),
              }),
            ),
          )
          const serviceful = Schema.Finite.pipe(
            Schema.decodeTo(Schema.Finite, {
              decode: SchemaGetter.transformEffect((value: number) =>
                CodecService.pipe(
                  Effect.map(() => {
                    codecCalls++
                    return value
                  }),
                ),
              ),
              encode: SchemaGetter.passthrough(),
            }),
          )
          const outer = Tool.make('transformed', {
            description: 'wrapped numeric input',
            parameters: Schema.Struct({ value: serviceful }),
            success: Schema.String,
          })
          yield* registry.install(
            Registry.toolWrapper(
              { id: 'wrapper', version: 1 },
              ToolWrapper.bind(outer, (item) => Effect.succeed(item)).pipe(
                ToolWrapper.provide(Layer.succeed(CodecService, 'captured')),
              ),
            ),
          )
          const lease = yield* registry.acquire
          const rendered = yield* Plan.render(yield* Projection.project(root, []), lease)
          assert.deepEqual(rendered.tools[0]!.parameters, NativeTool.getJsonSchema(outer))
          assert.notDeepEqual(rendered.tools[0]!.parameters, NativeTool.getJsonSchema(original))
          assert.strictEqual(rendered.tools[0]!.description, 'wrapped numeric input')
          assert.strictEqual(calls, 0)
          assert.strictEqual(codecCalls, 0)
        }),
      ).pipe(Effect.provide(hookAccess)),
  )

  it.effect(
    'renders transformed leased sections in their captured contexts, pins reloads and invokes no tools',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          let invocations = 0
          const offered = Tool.make('offered', {
            description: 'native declaration',
            parameters: Schema.Struct({ value: Schema.String }),
            success: Schema.String,
          })
          const nestedOnly = Tool.make('nestedOnly', { callers: ['tools'], success: Schema.String })
          const toolkit = Toolkit.make(offered, nestedOnly)
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'tools', version: 1 }, toolkit),
              toolkit.toLayer({
                offered: () =>
                  Effect.sync(() => {
                    invocations++
                    return 'unexpected'
                  }),
                nestedOnly: () => Effect.succeed('unexpected'),
              }),
            ),
          )
          const extension = Extension.make({
            name: 'context',
            sections: [
              PromptSection.make({ key: 'context', render: () => Label }),
              PromptSection.make({
                key: 'omitted',
                render: () =>
                  Label.pipe(Effect.map((label) => (label === 'source' ? undefined : label))),
              }),
            ],
          }).pipe(Extension.provide(Layer.succeed(Label, 'source')))
          yield* registry.install(Registry.extension({ id: 'source', version: 1 }, extension))
          const wrap = (version: number, label: string) =>
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
          yield* registry.install(wrap(1, 'old'))
          const leaseScope = yield* Scope.make()
          const lease = yield* Scope.provide(registry.acquire, leaseScope)
          yield* registry.install(wrap(2, 'new'))
          const current = yield* registry.acquire
          const view = yield* Projection.project(root, [])
          const options = { toolOptions: { offered: { provider: { strict: true } } } }
          const rendered = yield* Plan.render(view, lease, options).pipe(
            Effect.provideService(Label, 'ambient'),
          )
          assert.deepEqual(rendered.sections, [section('context', 'source/old')])
          assert.deepEqual(
            rendered.tools.map((tool) => tool.name),
            ['offered'],
          )
          assert.deepEqual(rendered.tools[0]!.parameters, NativeTool.getJsonSchema(offered))
          assert.deepEqual(rendered.tools[0]!.options, options.toolOptions.offered)
          assert.deepEqual((yield* Plan.render(view, current)).sections, [
            section('context', 'source/new'),
          ])
          assert.strictEqual(invocations, 0)
          options.toolOptions.offered.provider.strict = false
          assert.deepEqual(rendered.tools[0]!.options, { provider: { strict: true } })
          yield* registry.uninstall('wrapper')
          assert.deepEqual((yield* Plan.render(view, lease)).sections, rendered.sections)
          yield* Scope.close(leaseScope, Exit.void)
          const failure = yield* Plan.render(view, lease).pipe(Effect.flip)
          assert.isTrue(Schema.is(RegistryError)(failure))
          if (Schema.is(RegistryError)(failure)) assert.strictEqual(failure.reason, 'revoked')
        }),
      ).pipe(Effect.provide(hookAccess)),
  )

  it.effect(
    'honors canonical disabled shadows and model inventory order without falling back',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const a = Tool.make('a', { success: Schema.String })
          const b = Tool.make('b', { success: Schema.String })
          const toolkit = Toolkit.make(a, b)
          const install = (name: string, enabled: boolean) =>
            registry.install(
              Registry.provide(
                Registry.toolkit({ id: name, version: 1, enabled }, toolkit),
                toolkit.toLayer({ a: () => Effect.succeed('a'), b: () => Effect.succeed('b') }),
              ),
            )
          yield* install('visible', true)
          const view = yield* Projection.project(root, [])
          const first = yield* registry.acquire
          assert.deepEqual(
            (yield* Plan.render(view, first, {
              agent: { tools: ['a', 'b'], modelTools: { mode: 'explicit', names: ['b', 'a'] } },
            })).tools.map((tool) => tool.name),
            ['b', 'a'],
          )
          yield* install('disabled-shadow', false)
          const next = yield* registry.acquire
          assert.deepEqual((yield* Plan.render(view, next)).tools, [])
          assert.deepEqual(
            (yield* Plan.render(view, first)).tools.map((tool) => tool.name),
            ['a', 'b'],
          )
        }),
      ).pipe(Effect.provide(hookAccess)),
  )

  it.effect(
    'retains callback failures and requirements while rejecting malformed and duplicate declarations',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          const extension = Extension.make({
            name: 'context',
            sections: [
              PromptSection.make({ key: 'context', render: () => Effect.succeed('text') }),
            ],
          })
          yield* registry.install(Registry.extension({ id: 'source', version: 1 }, extension))
          const lease = yield* registry.acquire
          const view = yield* Projection.project(root, [])
          const failure = new Plan.ContextPlanError({ message: 'callback failed' })
          const renderer: Plan.Renderer<Plan.ContextPlanError, Label> = {
            snapshot: lease.snapshot,
            renderSection: () => Label.pipe(Effect.flatMap(() => Effect.fail(failure))),
          }
          assert.strictEqual(
            yield* Plan.render(view, renderer).pipe(
              Effect.provideService(Label, 'required'),
              Effect.flip,
            ),
            failure,
          )
          for (const desired of [
            { sections: [section('A'), section('A')], tools: [] },
            { sections: [], tools: [tool('a'), tool('a')] },
          ])
            assert.isTrue(
              Schema.is(Plan.ContextPlanError)(yield* Plan.plan(view, desired).pipe(Effect.flip)),
            )
          assert.isTrue(
            Schema.is(Plan.ContextPlanError)(
              yield* Plan.translate(
                { ...view, activeEntries: [entry(1)] },
                { sections: 'positional', tools: 'latest' },
              ).pipe(Effect.flip),
            ),
          )
          assert.isTrue(
            Schema.is(Plan.ContextPlanError)(
              yield* Plan.render(view, lease, { sectionKeys: ['context', 'context'] }).pipe(
                Effect.flip,
              ),
            ),
          )
        }),
      ).pipe(Effect.provide(hookAccess)),
  )
})

describe('versioned context interpretation', () => {
  it.effect(
    'defaults to preserveManaged through the recorded cutoff, keeps custom heads and has an explicit honorAll mode',
    () =>
      Effect.gen(function* () {
        const entries = [
          entry(1, { kind: 'input', head: id(1), model: [{ role: 'user', content: 'first' }] }),
          entry(2, {
            kind: 'assistant',
            head: id(2),
            model: [{ role: 'assistant', content: 'second' }],
          }),
        ]
        const protocol = yield* Plan.activateProtocol({ conversationId: root, tail: id(2) })
        assert.deepEqual(protocol, {
          version: 1,
          origin: { conversationId: root, through: id(2) },
          legacyMode: 'preserveManaged',
        })
        assert.deepEqual(texts((yield* Plan.projectWithProtocol(root, entries, protocol)).prompt), [
          'first',
          'second',
        ])
        assert.deepEqual(
          texts(
            (yield* Plan.projectWithProtocol(root, entries, protocol, { legacyMode: 'honorAll' }))
              .prompt,
          ),
          ['second'],
        )
        assert.deepEqual(
          texts(
            (yield* Plan.projectWithProtocol(
              root,
              entries,
              yield* Plan.activateProtocol({ conversationId: root, tail: id(2) }, 'honorAll'),
            )).prompt,
          ),
          ['second'],
        )
        const custom = [
          ...entries,
          entry(3, { head: id(1), model: [{ role: 'system', content: 'custom' }] }),
        ]
        assert.deepEqual(texts((yield* Plan.projectWithProtocol(root, custom, protocol)).prompt), [
          'custom',
          'first',
          'second',
        ])
        const canonical = [
          ...entries,
          entry(3, { kind: 'input', head: id(3), model: [{ role: 'user', content: 'canonical' }] }),
        ]
        assert.deepEqual(
          texts((yield* Plan.projectWithProtocol(root, canonical, protocol)).prompt),
          ['canonical'],
        )
        const empty = yield* Plan.activateProtocol({ conversationId: root })
        assert.deepEqual(texts((yield* Plan.projectWithProtocol(root, entries, empty)).prompt), [
          'second',
        ])
        assert.deepEqual(yield* Plan.inheritProtocol(protocol), protocol)
        assert.isTrue(Object.isFrozen(protocol.origin))
        assert.strictEqual(
          (yield* Schema.decodeUnknownEffect(Plan.ProtocolSchema)({ ...protocol, version: 2 }).pipe(
            Effect.result,
          ))._tag,
          'Failure',
        )
        assert.deepEqual(
          entries.map((entry) => entry.head),
          [id(1), id(2)],
        )
      }),
  )

  it.effect(
    'persists owned protocol/facts, inherits cutoff across forks and reopens without rewriting history',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const document = Document.define({
            kind: 'test.context.protocol',
            scope: 'conversation',
            history: 'latest',
            fork: 'current',
            version: 1,
            schema: Plan.ProtocolSchema,
            initial: (): Plan.Protocol => ({
              version: 1,
              origin: { conversationId: root },
              legacyMode: 'preserveManaged',
            }),
          })
          const target = { scope: { _tag: 'conversation', conversationId: root } } as const
          const storage = yield* Storage.Storage
          let writes = 0
          const monitored: Storage.Storage['Service'] = {
            ...storage,
            commit: (batch) => {
              writes++
              return storage.commit(batch)
            },
          }
          const saved = yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* Session.make().pipe(
                Effect.provideService(Storage.Storage, monitored),
              )
              const last = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.ensureRoot(tx)
                  yield* Transaction.appendEntry(tx, root, {
                    kind: 'input',
                    head: 'self',
                    model: [{ role: 'user', content: 'legacy A' }],
                  })
                  return yield* Transaction.appendEntry(tx, root, {
                    kind: 'input',
                    head: 'self',
                    model: [{ role: 'user', content: 'legacy B' }],
                  })
                }),
              )
              const protocol = yield* Plan.activateProtocol({ conversationId: root, tail: last.id })
              const desired = { sections: [section('A')], tools: [tool('a')] }
              const context = yield* Plan.readWithProtocol(session, root, protocol)
              const planned = yield* Plan.plan(context, desired)
              const cutoff = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.ensureDocument(tx, document, target)
                  yield* Transaction.setDocument(tx, document, target, protocol)
                  for (const draft of planned.drafts)
                    yield* Transaction.appendEntry(tx, root, draft)
                  return yield* Transaction.appendEntry(tx, root, {
                    kind: 'assistant',
                    model: [
                      {
                        role: 'assistant',
                        content: [
                          {
                            type: 'tool-call',
                            id: 'pending',
                            name: 'a',
                            params: {},
                            providerExecuted: false,
                          },
                        ],
                      },
                    ],
                  })
                }),
              )
              const child = yield* Session.commit(session, (tx) =>
                Transaction.forkConversation(tx, root, cutoff.id, {
                  ownership: { _tag: 'ownerless' },
                }),
              )
              yield* Session.commit(session, (tx) =>
                Transaction.appendEntry(tx, root, {
                  kind: 'custom',
                  head: 'self',
                  model: [{ role: 'user', content: 'later reset' }],
                }),
              )
              return {
                protocol,
                child: child.id,
                cutoff: cutoff.id,
                entries: (yield* Projection.read(session, child.id)).activeEntries,
              }
            }),
          )
          const session = yield* Session.make().pipe(
            Effect.provideService(Storage.Storage, monitored),
          )
          const snapshot = yield* Session.snapshot(session, document, {
            scope: { _tag: 'conversation', conversationId: saved.child },
          })
          assert.isTrue(Option.isSome(snapshot))
          if (Option.isNone(snapshot)) return yield* Effect.die('Expected inherited protocol')
          assert.deepEqual(snapshot.value.value, saved.protocol)
          const count = writes
          const view = yield* Plan.readWithProtocol(session, saved.child, snapshot.value.value)
          const bounded = yield* Plan.readWithProtocol(session, root, saved.protocol, {
            at: saved.cutoff,
          })
          assert.deepEqual(view.prompt.content, bounded.prompt.content)
          assert.deepEqual(texts(view.prompt), ['legacy A', 'legacy B'])
          assert.deepEqual(view.orderedSections, [section('A')])
          const translated = yield* Plan.translate(view, {
            sections: 'positional',
            tools: 'latest',
          })
          const missing = translated.prompt.content
            .flatMap((message) => (message.role === 'tool' ? message.content : []))
            .filter((part) => part.type === 'tool-result')
          assert.strictEqual(missing.length, 1)
          assert.strictEqual(missing[0]!.isFailure, true)
          assert.strictEqual(writes, count)
          assert.deepEqual(
            (yield* Projection.read(session, saved.child)).activeEntries,
            saved.entries,
          )
          assert.deepEqual(
            texts((yield* Plan.readWithProtocol(session, root, saved.protocol)).prompt),
            ['later reset'],
          )
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
})
