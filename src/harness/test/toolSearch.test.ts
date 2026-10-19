/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Deferred tools and `tool_search`, ported from pi's `tool-search.test.ts`.
 */
import { describe as group, expect, test } from 'bun:test'
import { Context, Effect, Layer, Option, Schema } from 'effect'
import type { Schema as SchemaType } from 'effect'
import { Faux } from '../../model/index.ts'
import type { Service as StorageService } from '../../storage/Storage.ts'
import type { EntryRecord } from '../../storage/schema/Entry.ts'
import { Tool, Toolkit, ToolResult } from '../../tool/index.ts'
import * as Agent from '../Agent.ts'
import type { Conversation } from '../Conversation.ts'
import type * as Harness from '../Harness.ts'
import * as Extension from '../Extension.ts'
import * as ToolSearch from '../ToolSearch.ts'
import { entriesOf, open, ownerless, runTest } from './generationSupport.ts'

const declare = <const Name extends string>(name: Name, description: string, deferred = false) =>
  Tool.make(name, {
    description,
    parameters: Schema.Struct({
      number: Schema.optionalKey(Schema.Finite.annotate({ description: 'Issue number' })),
    }),
    success: Schema.String,
    deferred,
  })

const Alpha = declare('alpha', 'Alpha tool')
const GetIssue = declare('github_get_issue', 'Get a GitHub issue by number', true)
const ListRepos = declare('github_list_repos', 'List the repositories of a user', true)
const Omega = declare('omega', 'Omega tool')
/** Loads tools by name, known or not. */
const Grow = Tool.make('grow', {
  description: 'grow',
  parameters: Schema.Struct({}),
  success: ToolResult.Output,
})

const github = Toolkit.make(Alpha, GetIssue, ListRepos, Omega)
const ran = (name: string) => () => Effect.succeed(`${name} ran`)
const handlers = github.toLayer({
  alpha: ran('alpha'),
  github_get_issue: ran('github_get_issue'),
  github_list_repos: ran('github_list_repos'),
  omega: ran('omega'),
})
const growing = Toolkit.make(Grow)
const growHandlers = growing.toLayer({
  grow: () =>
    Effect.succeed(
      ToolResult.output([], { control: { addTools: ['github_get_issue', 'alpha', 'ghost'] } }),
    ),
})

const DONE = Faux.message('done')

const call = (name: string, args: Record<string, SchemaType.Json> = {}, id = 'c1') =>
  Faux.message([Faux.toolCall(name, args, id)], { stopReason: 'toolUse' })

/** `alpha`, the deferred `github_get_issue` and `github_list_repos`, `omega`, then `tool_search`. */
const setup = (storage: StorageService, options: { readonly grow?: boolean } = {}) =>
  Effect.gen(function* () {
    const opened = yield* open(storage, { agent: () => ({ tools: [] }) })
    yield* Effect.orDie(
      opened.extensions
        .install(Extension.make('github', { toolkit: github }))
        .pipe(Effect.provideContext(yield* Layer.build(handlers))),
    )
    if (options.grow === true)
      yield* Effect.orDie(
        opened.extensions
          .install(Extension.make('grow', { toolkit: growing }))
          .pipe(Effect.provideContext(yield* Layer.build(growHandlers))),
      )
    yield* Effect.orDie(
      opened.extensions
        .install(ToolSearch.extension)
        .pipe(Effect.provideContext(yield* Layer.build(ToolSearch.layer))),
    )
    return { ...opened, conversation: yield* ownerless(opened.harness) }
  })

const offered = (conversation: Conversation) =>
  Effect.map(Effect.orDie(conversation.agent), (view) => view.tools)

const ask = (conversation: Conversation, content: string) =>
  Effect.orDie(Effect.flatMap(conversation.submit({ type: 'input', content }), (s) => s.wait))

const textsOf = (entries: ReadonlyArray<EntryRecord>, kind: string) =>
  entries
    .filter((entry) => entry.kind === kind)
    .map((entry) => {
      const content: unknown = entry.model?.[0]?.['content']
      return Array.isArray(content)
        ? content.map((block: { readonly text?: string }) => block.text ?? '').join('')
        : ''
    })

/** The tools each `$system` entry declares added, oldest first. */
const toolsAdded = (entries: ReadonlyArray<EntryRecord>) =>
  entries
    .filter((entry) => entry.kind === '$system')
    .map((entry) => {
      const added: unknown = entry.model?.[0]?.['toolsAdded']
      return Array.isArray(added) ? added.map((tool: { readonly name: string }) => tool.name) : []
    })

const modelToolsOf = (harness: Harness.Service, conversation: Conversation) =>
  Effect.map(
    Effect.orDie(harness.session.snapshot(Agent.document, conversation.id)),
    (state) => state?.modelTools,
  )

group('deferred tools and tool_search', () => {
  test('offers a deferred tool only once tool_search loads it, by appending its declaration', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { faux, harness, conversation } = yield* setup(storage)
        yield* faux.setResponses([
          call('tool_search', { query: 'github issue', limit: 1 }),
          call('github_get_issue', { number: 42 }, 'c2'),
          DONE,
        ])
        expect(yield* offered(conversation)).toEqual(['alpha', 'omega', 'tool_search'])
        expect((yield* Effect.orDie(conversation.agent)).deferred).toEqual([
          'github_get_issue',
          'github_list_repos',
        ])
        yield* ask(conversation, 'go')
        const entries = yield* entriesOf(harness, conversation.id)
        expect(textsOf(entries, '$tool-result')).toEqual([
          'Loaded 1 tool. They are available from your next call:\n- github_get_issue: Get a GitHub issue by number',
          'github_get_issue ran',
        ])
        expect(yield* offered(conversation)).toEqual([
          'alpha',
          'omega',
          'tool_search',
          'github_get_issue',
        ])
        expect((yield* Effect.orDie(conversation.agent)).deferred).toEqual(['github_list_repos'])
        // The load appended the declaration, so the cached prefix holds.
        expect(toolsAdded(entries)).toEqual([
          ['alpha', 'omega', 'tool_search'],
          ['github_get_issue'],
        ])
      }),
    ))

  test('appends each loaded tool in load order, not registry order', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { faux, conversation } = yield* setup(storage)
        yield* faux.setResponses([
          call('tool_search', { query: 'repositories', limit: 1 }),
          call('tool_search', { query: 'issue', limit: 1 }, 'c2'),
          DONE,
        ])
        yield* ask(conversation, 'go')
        expect(yield* offered(conversation)).toEqual([
          'alpha',
          'omega',
          'tool_search',
          'github_list_repos',
          'github_get_issue',
        ])
      }),
    ))

  test('records loads by name: unknown and non-deferred names change nothing, remove wins, a load lifts it', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { faux, harness, conversation } = yield* setup(storage, { grow: true })
        yield* faux.setResponses([call('grow'), DONE, call('grow'), DONE])
        yield* ask(conversation, 'go')
        const loaded = { add: ['github_get_issue', 'alpha', 'ghost'] }
        expect(yield* modelToolsOf(harness, conversation)).toEqual(loaded)
        expect(yield* offered(conversation)).toEqual([
          'alpha',
          'omega',
          'grow',
          'tool_search',
          'github_get_issue',
        ])
        yield* Effect.orDie(
          conversation.configure({
            modelTools: { remove: ['github_get_issue'], add: ['github_get_issue'] },
          }),
        )
        expect(yield* offered(conversation)).not.toContain('github_get_issue')
        yield* ask(conversation, 'again')
        expect(yield* modelToolsOf(harness, conversation)).toEqual(loaded)
        expect(yield* offered(conversation)).toContain('github_get_issue')
      }),
    ))

  test('appends to an explicit array', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { faux, harness, conversation } = yield* setup(storage)
        yield* Effect.orDie(conversation.configure({ modelTools: ['alpha', 'tool_search'] }))
        yield* faux.setResponses([call('tool_search', { query: 'repositories', limit: 1 }), DONE])
        yield* ask(conversation, 'go')
        expect(yield* modelToolsOf(harness, conversation)).toEqual([
          'alpha',
          'tool_search',
          'github_list_repos',
        ])
      }),
    ))

  test('keeps loaded tools per branch and in the baseline after a reset', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { faux, harness, conversation } = yield* setup(storage)
        yield* faux.setResponses([call('tool_search', { query: 'github issue', limit: 1 }), DONE])
        yield* ask(conversation, 'go')
        const entries = yield* entriesOf(harness, conversation.id)
        const unowned = { ownership: { kind: 'ownerless' } } as const
        const before = yield* Effect.orDie(conversation.fork(entries[0]!.id, unowned))
        const after = yield* Effect.orDie(conversation.fork(entries.at(-1)!.id, unowned))
        expect(yield* offered(before)).not.toContain('github_get_issue')
        expect(yield* offered(after)).toContain('github_get_issue')
        yield* Effect.orDie(Effect.flatMap(conversation.reset(), (s) => s.wait))
        yield* faux.setResponses([DONE])
        yield* ask(conversation, 'fresh')
        expect(toolsAdded(yield* entriesOf(harness, conversation.id)).at(-1)).toEqual([
          'alpha',
          'omega',
          'tool_search',
          'github_get_issue',
        ])
      }),
    ))

  test('finds nothing for an unrelated query and loads nothing', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { faux, harness, conversation } = yield* setup(storage)
        yield* faux.setResponses([call('tool_search', { query: 'weather forecast' }), DONE])
        yield* ask(conversation, 'go')
        const entries = yield* entriesOf(harness, conversation.id)
        expect(textsOf(entries, '$tool-result')).toEqual(['No matching tools found.'])
        expect(yield* modelToolsOf(harness, conversation)).toBeUndefined()
      }),
    ))

  test('fails outside a conversation call', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(ToolSearch.layer)
          const found = Context.getOption(context, Toolkit.implementation(ToolSearch.NAME))
          const runner = yield* Option.match(found, {
            onNone: () => Effect.die('tool_search has no implementation'),
            onSome: Effect.succeed,
          })
          const outcome = yield* runner.run({ query: 'issue' }, { callId: 'c1', config: {} })
          expect(outcome).toMatchObject({
            type: 'failure',
            value: { content: [{ text: "tool_search runs only for a conversation's calls" }] },
          })
        }),
      ),
    ))
})
