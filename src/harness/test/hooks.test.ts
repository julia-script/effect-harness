/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The hooks of the built-in tasks, answered by extensions, through whole runs. Ported in spirit
 * from pi's `harness-hooks.test.ts`.
 */
import { describe as group, expect, test } from 'bun:test'
import { Effect, Ref } from 'effect'
import { Faux } from '../../model/index.ts'
import type { AssistantMessage } from '../../model/Message.ts'
import type { EntryRecord } from '../../storage/schema/Entry.ts'
import type { EntryId } from '../../storage/schema/Ids.ts'
import { ToolTask } from '../../tool/index.ts'
import * as Compaction from '../Compaction.ts'
import type { Conversation } from '../Conversation.ts'
import * as Extension from '../Extension.ts'
import * as Generation from '../Generation.ts'
import { describe, entriesOf, open, ownerless, runTest } from '../../test/generation.ts'

const ask = (conversation: Conversation, content: string) =>
  Effect.orDie(Effect.flatMap(conversation.submit({ type: 'input', content }), (s) => s.wait))

const read = (path: string, id: string) =>
  Faux.message([Faux.toolCall('read', { path }, id)], { stopReason: 'toolUse' })

const text = (message: AssistantMessage) =>
  message.content.flatMap((block) => (block.type === 'text' ? [block.text] : [])).join('')

const results = (entries: ReadonlyArray<EntryRecord>) =>
  entries.filter((entry) => entry.kind === '$tool-result')

group('generation hooks', () => {
  test('beforeRequest replaces the messages of one request', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          agent: () => ({
            tools: [],
            hooks: [
              Extension.hook(Generation.Hooks, {
                beforeRequest: ({ messages }) =>
                  Effect.succeed({
                    messages: [...messages, { role: 'user', content: 'extra', timestamp: 0 }],
                  }),
              }),
            ],
          }),
        })
        yield* faux.setResponses([Faux.message('ok')])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'hi')
        const [request] = yield* faux.requests
        expect(request?.messages.map((message) => message.content)).toEqual(['hi', 'extra'])
        // The transcript is left as it was.
        expect(
          (yield* entriesOf(harness, conversation.id)).map((entry) => describe(entry)),
        ).toEqual(['$user: hi', '$assistant: ok'])
      }),
    ))

  test('afterResponse sees every response, and onYield continues the run once', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const seen = yield* Ref.make<ReadonlyArray<string>>([])
        const { harness, faux } = yield* open(storage, {
          agent: () => ({
            tools: [],
            hooks: [
              Extension.hook(Generation.Hooks, {
                afterResponse: (message) => Ref.update(seen, (all) => [...all, text(message)]),
                onYield: (answer) =>
                  Effect.succeed(text(answer) === 'draft' ? { continue: 'check it' } : undefined),
              }),
            ],
          }),
        })
        yield* faux.setResponses([Faux.message('draft'), Faux.message('final')])
        const conversation = yield* ownerless(harness)
        const settled = yield* ask(conversation, 'write')
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry))).toEqual([
          '$user: write',
          '$assistant: draft',
          '$user: check it',
          '$assistant: final',
        ])
        expect(settled).toMatchObject({ status: 'done', answer: entries[3]?.id })
        expect(yield* Ref.get(seen)).toEqual(['draft', 'final'])
      }),
    ))

  test('a failing hook is reported and the run goes on', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          agent: () => ({
            tools: [],
            hooks: [
              Extension.hook(Generation.Hooks, {
                beforeRequest: () => Effect.fail('no'),
                onYield: () => Effect.die(new Error('broken')),
              }),
            ],
          }),
        })
        yield* faux.setResponses([Faux.message('ok')])
        const conversation = yield* ownerless(harness)
        expect((yield* ask(conversation, 'hi')).status).toBe('done')
      }),
    ))

  test('afterTools sees the round results in call order', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const rounds = yield* Ref.make<ReadonlyArray<ReadonlyArray<EntryId>>>([])
        const { harness, faux } = yield* open(storage, {
          agent: () => ({
            hooks: [
              Extension.hook(Generation.Hooks, {
                afterTools: (_assistant, entries) => Ref.update(rounds, (all) => [...all, entries]),
              }),
            ],
          }),
        })
        yield* faux.setResponses([read('a', 'c1'), Faux.message('done')])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'go')
        const entries = yield* entriesOf(harness, conversation.id)
        const ids = entries.filter((entry) => entry.kind === '$tool-result').map(({ id }) => id)
        expect(yield* Ref.get(rounds)).toEqual([ids])
      }),
    ))
})

group('tool hooks', () => {
  test('beforeTool replaces arguments, blocks, and a failing one blocks', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          agent: () => ({
            hooks: [
              Extension.hook(ToolTask.Hooks, {
                beforeTool: (call) => {
                  const path = call.arguments['path']
                  if (path === 'secret') return Effect.succeed({ block: 'Needs approval' })
                  if (path === 'fail') return Effect.fail('policy down')
                  return Effect.succeed({ arguments: { path: `safe/${JSON.stringify(path)}` } })
                },
              }),
            ],
          }),
        })
        yield* faux.setResponses([
          Faux.message(
            [
              Faux.toolCall('read', { path: 'a' }, 'c1'),
              Faux.toolCall('read', { path: 'secret' }, 'c2'),
              Faux.toolCall('read', { path: 'fail' }, 'c3'),
            ],
            { stopReason: 'toolUse' },
          ),
          Faux.message('done'),
        ])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'go')
        const entries = results(yield* entriesOf(harness, conversation.id))
        expect(describe(entries[0]!)).toBe('$tool-result: contents of safe/"a"')
        expect(entries.map((entry) => entry.data)).toMatchObject([
          { diagnostics: [] },
          { diagnostics: [{ code: 'blocked', message: 'Tool call blocked: Needs approval' }] },
          { diagnostics: [{ code: 'blocked', message: 'Tool call blocked: policy down' }] },
        ])
      }),
    ))

  test('afterTool replaces the result', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          agent: () => ({
            hooks: [
              Extension.hook(ToolTask.Hooks, {
                afterTool: (_call, result) =>
                  Effect.succeed({ ...result, output: [{ type: 'text', text: 'redacted' }] }),
              }),
            ],
          }),
        })
        yield* faux.setResponses([read('a', 'c1'), Faux.message('done')])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'go')
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry))).toContain('$tool-result: redacted')
      }),
    ))

  test('a tool wrapper decorates the runner of the tool it targets', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          agent: () => ({
            wraps: [
              Extension.wrapTool('read', (runner) => ({
                ...runner,
                run: (args, context) =>
                  Effect.map(runner.run(args, context), (outcome) =>
                    outcome.type === 'success'
                      ? { ...outcome, value: `${String(outcome.value)} (timed)` }
                      : outcome,
                  ),
              })),
            ],
          }),
        })
        yield* faux.setResponses([read('a', 'c1'), Faux.message('done')])
        const conversation = yield* ownerless(harness)
        yield* ask(conversation, 'go')
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry))).toContain(
          '$tool-result: contents of a (timed)',
        )
      }),
    ))
})

/** Two answers, then a manual compaction a `beforeCompact` hook decides. */
const compact = (decide: Compaction.Hooks['beforeCompact']) =>
  runTest((storage) =>
    Effect.gen(function* () {
      const { harness, faux } = yield* open(storage, {
        agent: () => ({
          tools: [],
          hooks: [Extension.hook(Compaction.Hooks, { beforeCompact: decide })],
        }),
        settings: { compaction: { keepRecentTokens: 1 } },
      })
      yield* faux.setResponses([Faux.message('first'), Faux.message('second')])
      const conversation = yield* ownerless(harness)
      yield* ask(conversation, 'one')
      yield* ask(conversation, 'two')
      const taskId = yield* Effect.orDie(conversation.compact())
      const { state } = yield* Effect.orDie(harness.scheduler.waitForTask(taskId))
      const entries = yield* entriesOf(harness, conversation.id)
      return {
        outcome: state.outcome,
        kinds: entries.map((entry) => entry.kind),
        requests: (yield* faux.requests).length,
        entries,
      }
    }),
  )

group('compaction hooks', () => {
  test('beforeCompact supplies the summary without asking the model', async () => {
    const seen: Array<Compaction.Pending> = []
    const { kinds, requests, entries } = await compact((pending) => {
      seen.push(pending)
      return Effect.succeed({ summary: 'from the hook' })
    })
    expect(requests).toBe(2)
    expect(kinds.at(-1)).toBe('$compaction')
    expect(describe(entries.at(-1)!)).toContain('from the hook')
    expect(seen[0]).toMatchObject({ reason: 'manual', firstKept: entries[3]?.id })
  })

  test('beforeCompact declines', async () => {
    const { outcome, kinds, requests } = await compact(() => Effect.succeed({ decline: true }))
    expect(outcome).toEqual({ status: 'completed', result: {} })
    expect(requests).toBe(2)
    expect(kinds).not.toContain('$compaction')
  })
})
