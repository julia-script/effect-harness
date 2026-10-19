/* oxlint-disable max-lines-per-function, effecttsgo/any-unknown-in-error-context -- describe blocks; sections may fail with anything */
/**
 * The extension registry, agent resolution, and `$agent` through whole runs. Ported in spirit
 * from pi's `harness-extensions.test.ts`.
 */
import { describe as group, expect, test } from 'bun:test'
import { Effect, Ref } from 'effect'
import { Faux } from '../../model/index.ts'
import { Registry, Task } from '../../task/index.ts'
import { Read } from '../../tool/test/support.ts'
import * as Extension from '../Extension.ts'
import * as Extensions from '../Extensions.ts'
import { describe, entriesOf, messageOf, open, ownerless, runTest } from './generationSupport.ts'

const section = (key: string, text: string) => Extension.section(key, () => Effect.succeed(text))

const custom = Task.make({
  name: 'app.custom',
  version: 1,
  initial: () => ({ phase: 'start' }),
  phases: { start: () => Effect.void },
  abort: () => Effect.void,
})

/** A registry over a fresh task `Registry`, and that `Registry`. */
const registries = Effect.gen(function* () {
  const registry = yield* Registry.make()
  const extensions = yield* Extensions.make.pipe(Effect.provideService(Registry.Registry, registry))
  return { registry, extensions }
})

const names = (snapshot: Extensions.Snapshot) => snapshot.map(({ extension }) => extension.name)

group('Extensions', () => {
  test('installs in order, replaces in place, and keeps the task Registry in step', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, extensions } = yield* registries
          yield* extensions.install(Extension.make('a', { tasks: [custom] }))
          yield* extensions.install(Extension.make('b'))
          yield* extensions.install(Extension.make('a', { sections: [section('s', 'two')] }))
          const snapshot = yield* extensions.snapshot
          expect(names(snapshot)).toEqual(['a', 'b'])
          expect(snapshot[0]?.extension.sections.map(({ key }) => key)).toEqual(['s'])
          expect((yield* registry.snapshot).has('app.custom')).toBe(false)
          yield* extensions.install(Extension.make('c', { tasks: [custom] }))
          expect((yield* registry.snapshot).get('app.custom')).toBe(custom)
          yield* extensions.uninstall('c')
          expect((yield* registry.snapshot).has('app.custom')).toBe(false)
          expect(names(yield* extensions.snapshot)).toEqual(['a', 'b'])
        }),
      ),
    ))

  test('refuses an invalid extension and publishes nothing', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { extensions } = yield* registries
          yield* extensions.install(Extension.make('a', { tasks: [custom] }))
          const refused = (extension: Extension.Extension<never>) =>
            Effect.map(Effect.flip(extensions.install(extension)), (error) => error.message)
          expect(yield* refused(Extension.make('b', { tasks: [custom] }))).toBe(
            'Extension b: task app.custom is already installed',
          )
          expect(yield* refused(Extension.make('c', { sections: [section('Bad', '')] }))).toBe(
            'Extension c: section key "Bad" is invalid',
          )
          expect(
            yield* refused(Extension.make('d', { sections: [section('instructions', '')] })),
          ).toBe('Extension d: section key instructions is reserved')
          expect(
            yield* refused(
              Extension.make('e', { sections: [section('s', 'one'), section('s', 'two')] }),
            ),
          ).toBe('Extension e: two sections have key s')
          expect(names(yield* extensions.snapshot)).toEqual(['a'])
        }),
      ),
    ))
})

group('$agent', () => {
  test('configures a conversation: its model, instructions and tools reach the request', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, {
          agent: () => ({ model: null, tools: [Read] }),
        })
        yield* faux.setResponses([Faux.message('hello')])
        const conversation = yield* ownerless(harness)
        yield* Effect.orDie(
          conversation.configure({ model: faux.ref(), instructions: 'Be brief.', tools: [] }),
        )
        expect(yield* Effect.orDie(conversation.agent)).toEqual({
          model: faux.ref(),
          reasoning: 'off',
          extensions: ['test'],
          tools: [],
          sections: ['instructions'],
          instructions: 'Be brief.',
        })
        yield* Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
        )
        const entries = yield* entriesOf(harness, conversation.id)
        expect(messageOf(entries[1]!)).toMatchObject({
          sections: { instructions: '<instructions>\nBe brief.\n</instructions>' },
        })
        expect(messageOf(entries[1]!)['toolsAdded']).toBeUndefined()
        yield* Effect.orDie(conversation.configure({ instructions: null }))
        expect((yield* Effect.orDie(conversation.agent)).sections).toEqual([])
      }),
    ))

  test('reloads an extension: the next request sees the replacement', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const text = yield* Ref.make('one')
        const { harness, faux, extensions } = yield* open(storage, {
          agent: () => ({ tools: [] }),
        })
        yield* extensions.install(
          Extension.make('rules', { sections: [Extension.section('rules', () => Ref.get(text))] }),
        )
        yield* faux.setResponses([Faux.message('a'), Faux.message('b')])
        const conversation = yield* ownerless(harness)
        const ask = (content: string) =>
          Effect.orDie(
            Effect.flatMap(conversation.submit({ type: 'input', content }), (s) => s.wait),
          )
        yield* ask('first')
        yield* extensions.install(Extension.make('rules', { sections: [section('rules', 'two')] }))
        yield* ask('second')
        const entries = yield* entriesOf(harness, conversation.id)
        expect(entries.map((entry) => describe(entry))).toEqual([
          '$user: first',
          '$system',
          '$assistant: a',
          '$user: second',
          '$system',
          '$assistant: b',
        ])
        expect(messageOf(entries[4]!)).toMatchObject({
          sections: { rules: '<rules>\ntwo\n</rules>' },
        })
      }),
    ))

  test('renders the sections a later extension replaces and wraps', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux, extensions } = yield* open(storage, { agent: () => ({ tools: [] }) })
        yield* extensions.install(
          Extension.make('first', { sections: [section('a', 'first a'), section('b', 'first b')] }),
        )
        yield* extensions.install(
          Extension.make('second', {
            sections: [section('a', 'second a')],
            wraps: [
              Extension.wrapSection('b', (each) => ({
                ...each,
                tag: false,
                render: (input) => Effect.map(each.render(input), (text) => `${text}!`),
              })),
            ],
          }),
        )
        yield* faux.setResponses([Faux.message('a')])
        const conversation = yield* ownerless(harness)
        yield* Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
        )
        const entries = yield* entriesOf(harness, conversation.id)
        expect(messageOf(entries[1]!)['sections']).toEqual({
          a: '<a>\nsecond a\n</a>',
          b: 'first b!',
        })
      }),
    ))

  test('a fork starts from the agent at its fork entry', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* open(storage, { agent: () => ({ tools: [] }) })
        yield* faux.setResponses([Faux.message('a')])
        const conversation = yield* ownerless(harness)
        yield* Effect.orDie(conversation.configure({ instructions: 'old' }))
        yield* Effect.orDie(
          Effect.flatMap(conversation.submit({ type: 'input', content: 'hi' }), (s) => s.wait),
        )
        const [first] = yield* entriesOf(harness, conversation.id)
        yield* Effect.orDie(conversation.configure({ instructions: 'new' }))
        const fork = yield* Effect.orDie(
          conversation.fork(first!.id, { ownership: { kind: 'ownerless' } }),
        )
        expect((yield* Effect.orDie(fork.agent)).instructions).toBe('old')
        expect((yield* Effect.orDie(conversation.agent)).instructions).toBe('new')
      }),
    ))
})
