import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Queue from 'effect/Queue'
import * as Stream from 'effect/Stream'
import * as Projection from '../src/ContextProjection.js'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as RowProjection from '../src/internal/Projection.js'
import * as Kernel from '../src/internal/Kernel.js'
import * as SessionRuntime from '../src/internal/SessionRuntime.js'

const root = Record.ROOT_CONVERSATION_ID
const id = Schema.decodeSync(Record.EntryId)
const user = (text: string): Schema.Json => ({ role: 'user', content: text })
const entry = (n: number, rest: Partial<Record.Entry> = {}): Record.Entry => ({
  id: id(n),
  conversationId: root,
  kind: 'custom',
  ...rest,
})
const texts = (view: Projection.ContextView) =>
  view.prompt.content.flatMap((message) =>
    message.role === 'system'
      ? [message.content]
      : message.content.flatMap((part) => (part.type === 'text' ? [part.text] : [])),
  )
const fact = (rest: Partial<Projection.Fact> = {}): Projection.Fact => ({
  _tag: '@effect-harness/ContextFact',
  version: 1,
  sections: [],
  tools: [],
  ...rest,
})

describe('bounded canonical context projection', () => {
  it.effect(
    'successful, failed and historical-fork reads reserve no IDs, mutate no storage and publish no notifications',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const access = yield* RowProjection.makeAccess
          let saves = 0
          let commits = 0
          let reservations = 0
          let failScan = false
          const backend = yield* Kernel.make({
            ...access,
            save: (rows, metadata) => {
              saves++
              return access.save(rows, metadata)
            },
          })
          const storage: Storage.Storage['Service'] = {
            ...backend,
            commit: (writes) => {
              commits++
              return backend.commit(writes)
            },
            mintId: () => {
              reservations++
              return backend.mintId()
            },
            scanEntries: (query) =>
              failScan
                ? Stream.fail(
                    new Storage.StorageError({
                      reason: 'io',
                      operation: 'scanEntries',
                      message: 'Injected scan failure',
                    }),
                  )
                : backend.scanEntries(query),
          }
          const session = yield* Session.make().pipe(
            Effect.provideService(Storage.Storage, storage),
          )
          const cutoff = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureRoot(tx)
              return yield* Transaction.appendEntry(tx, root, {
                kind: 'input',
                model: [user('inherited')],
              })
            }),
          )
          const child = yield* Session.commit(session, (tx) =>
            Transaction.forkConversation(tx, root, cutoff.id, { ownership: { _tag: 'ownerless' } }),
          )
          const corrupt = yield* Session.commit(session, (tx) =>
            Transaction.appendEntry(tx, root, {
              kind: 'corrupt',
              data: { _tag: '@effect-harness/ContextFact', version: 99 },
            }),
          )
          const subscriber = yield* SessionRuntime.line(
            session,
            'test.contextSubscription',
            SessionRuntime.subscribe,
          )
          const before = { saves, commits, reservations }
          assert.deepEqual(texts(yield* Projection.read(session, root, { at: cutoff.id })), [
            'inherited',
          ])
          assert.deepEqual(texts(yield* Projection.read(session, child.id)), ['inherited'])
          assert.deepEqual(texts(yield* Projection.read(session, child.id, { at: cutoff.id })), [
            'inherited',
          ])
          for (const read of [
            Projection.read(session, root),
            Projection.read(session, child.id, { at: corrupt.id }),
            Projection.read(session, root, { at: id(999) }),
          ])
            assert.strictEqual((yield* Effect.result(read))._tag, 'Failure')
          const absentId = yield* Schema.decodeEffect(Record.ConversationId)(999)
          assert.strictEqual(
            (yield* Effect.result(Projection.read(session, absentId)))._tag,
            'Failure',
          )
          failScan = true
          const failure = yield* Effect.result(Projection.read(session, root, { at: cutoff.id }))
          assert.strictEqual(failure._tag, 'Failure')
          if (failure._tag === 'Failure')
            assert.isTrue(Schema.is(Storage.StorageError)(failure.failure))
          failScan = false
          assert.deepEqual({ saves, commits, reservations }, before)
          assert.strictEqual(yield* Queue.size(subscriber.queue), 0)
          assert.isTrue(saves > 0)
          assert.isTrue(commits > 0)
          assert.isTrue(reservations > 0)
          // Positive control proves the durable mutation and notification probes are live.
          yield* Session.commit(session, (tx) =>
            Transaction.appendEntry(tx, child.id, { kind: 'control' }),
          )
          assert.strictEqual(commits, before.commits + 1)
          assert.strictEqual(reservations, before.reservations + 1)
          assert.isTrue(saves > before.saves)
          assert.strictEqual(yield* Queue.size(subscriber.queue), 1)
        }),
      ),
  )
  it.effect(
    'latest in-range edits win, including superseded-head edits; earlier edits do not leak',
    () =>
      Effect.gen(function* () {
        const entries = [
          entry(1, { model: [user('old')] }),
          entry(2, { model: [user('keep')] }),
          entry(3, { edits: [{ _tag: 'omit', target: id(2) }] }),
          entry(4, { head: id(4), model: [user('reset')] }),
          entry(5, {
            head: id(2),
            model: [{ role: 'system', content: 'baseline' }],
            edits: [{ _tag: 'replace', target: id(2), messages: [user('first')] }],
          }),
          entry(6, { edits: [{ _tag: 'replace', target: id(2), messages: [user('last')] }] }),
          entry(7, { edits: [{ _tag: 'omit', target: id(2) }] }),
        ]
        const at6 = yield* Projection.project(root, entries, { at: id(6) })
        assert.deepEqual(
          at6.activeEntries.map((item) => item.id),
          [id(5), id(2), id(3), id(6)],
        )
        assert.deepEqual(texts(at6), ['baseline', 'last'])
        assert.deepEqual(texts(yield* Projection.project(root, entries)), ['baseline'])
        const narrow = yield* Projection.project(root, [
          entry(1, { edits: [{ _tag: 'omit', target: id(2) }] }),
          entry(2, { model: [user('keep')] }),
          entry(3, { head: id(2) }),
        ])
        assert.deepEqual(texts(narrow), ['keep'])
      }),
  )

  it.effect('preserves known managed legacy self-heads, custom heads and explicit honor-all', () =>
    Effect.gen(function* () {
      const entries = [
        entry(1, { kind: 'input', head: id(1), model: [user('A')] }),
        entry(2, { kind: 'assistant', head: id(2), model: [{ role: 'assistant', content: 'B' }] }),
        entry(3, { kind: 'input', head: id(3), model: [user('C')] }),
      ]
      const legacy = { _tag: 'preserveManaged', through: id(3) } as const
      assert.deepEqual(
        texts(yield* Projection.project(root, entries, { legacyInterpretation: legacy })),
        ['A', 'B', 'C'],
      )
      assert.deepEqual(
        texts(
          yield* Projection.project(root, entries, { legacyInterpretation: { _tag: 'honorAll' } }),
        ),
        ['C'],
      )
      assert.deepEqual(texts(yield* Projection.project(root, entries)), ['C'])
      assert.deepEqual(
        texts(
          yield* Projection.project(
            root,
            [...entries, entry(4, { head: id(4), model: [user('custom')] })],
            { legacyInterpretation: legacy },
          ),
        ),
        ['custom'],
      )
      assert.deepEqual(
        texts(
          yield* Projection.project(
            root,
            [...entries, entry(4, { kind: 'input', head: id(4), model: [user('canonical')] })],
            { legacyInterpretation: legacy },
          ),
        ),
        ['canonical'],
      )
      assert.deepEqual(
        texts(
          yield* Projection.project(root, [...entries, entry(4, { kind: 'input', head: id(2) })], {
            legacyInterpretation: { _tag: 'preserveManaged', through: id(4) },
          }),
        ),
        ['B', 'C'],
      )
    }),
  )

  it.effect(
    'repairs missing results and orders grouped/out-of-order results without admitting orphans',
    () =>
      Effect.gen(function* () {
        const calls = (names: ReadonlyArray<string>) => ({
          role: 'assistant',
          content: names.map((name) => ({
            type: 'tool-call',
            id: name,
            name,
            params: {},
            providerExecuted: false,
          })),
        })
        const result = (name: string): Schema.Json => ({
          type: 'tool-result',
          id: name,
          name,
          result: name,
          isFailure: false,
          providerExecuted: false,
          options: { provider: { retained: true } },
        })
        const view = yield* Projection.project(root, [
          entry(1, { model: [{ role: 'tool', content: [result('orphan')] }] }),
          entry(2, { model: [calls(['a', 'b', 'c'])] }),
          entry(3, { model: [user('after')] }),
          entry(4, {
            model: [
              {
                role: 'tool',
                content: [result('c'), result('a'), result('orphan')],
                options: { provider: { message: true } },
              },
            ],
          }),
          entry(5, { model: [calls(['d'])] }),
          entry(6, { model: [{ role: 'tool', content: [result('b')] }] }),
        ])
        assert.deepEqual(
          view.prompt.content.map((message) => message.role),
          ['assistant', 'tool', 'tool', 'tool', 'user', 'assistant', 'tool'],
        )
        const results = view.prompt.content
          .flatMap((message) => (message.role === 'tool' ? message.content : []))
          .filter((part) => part.type === 'tool-result')
        assert.deepEqual(
          results.map((part) => part.id),
          ['a', 'b', 'c', 'd'],
        )
        assert.deepEqual(
          results.filter((part) => part.isFailure).map((part) => part.result),
          [
            {
              reason: 'missing_result',
              message: 'Tool result unavailable: history ends before this call completed.',
            },
            {
              reason: 'missing_result',
              message: 'Tool result unavailable: history ends before this call completed.',
            },
          ],
        )
        assert.deepEqual(results[0]!.options, { provider: { retained: true } })
        assert.deepEqual(view.prompt.content[1]!.options, { provider: { message: true } })
      }),
  )

  for (const grouped of [true, false]) {
    it.effect(
      `pairs the first exact ID/name result with wrong-name orphans (${grouped ? 'grouped' : 'separate'} messages)`,
      () =>
        Effect.gen(function* () {
          const result = (name: string, value: string): Schema.Json => ({
            type: 'tool-result',
            id: 'c',
            name,
            result: value,
            isFailure: false,
            providerExecuted: false,
            options: { provider: { receipt: value } },
          })
          const parts = [
            result('orphan', 'wrong'),
            result('other', 'second'),
            result('wanted', 'correct'),
            result('wanted', 'duplicate'),
          ]
          const messages: Array<Schema.Json> = grouped
            ? [{ role: 'tool', content: parts, options: { provider: { receipt: 'grouped' } } }]
            : parts.map((part, index) => ({
                role: 'tool',
                content: [part],
                options: { provider: { receipt: index } },
              }))
          const view = yield* Projection.project(root, [
            entry(1, {
              model: [
                {
                  role: 'assistant',
                  content: ['wanted', 'other'].map((name) => ({
                    type: 'tool-call',
                    id: 'c',
                    name,
                    params: {},
                    providerExecuted: false,
                  })),
                },
              ],
            }),
            entry(2, { model: messages }),
          ])
          assert.deepEqual(
            view.prompt.content.map((message) => message.role),
            ['assistant', 'tool', 'tool'],
          )
          const results = view.prompt.content
            .flatMap((message) => (message.role === 'tool' ? message.content : []))
            .filter((part) => part.type === 'tool-result')
          assert.deepEqual(
            results.map((part) => [part.id, part.name, part.result, part.isFailure]),
            [
              ['c', 'wanted', 'correct', false],
              ['c', 'other', 'second', false],
            ],
          )
          assert.deepEqual(results[0]!.options, { provider: { receipt: 'correct' } })
          assert.deepEqual(view.prompt.content[1]!.options, {
            provider: { receipt: grouped ? 'grouped' : 2 },
          })
        }),
    )
  }

  it.effect('reused ID/name pairs cannot borrow receipts across assistant windows', () =>
    Effect.gen(function* () {
      const assistant: Schema.Json = {
        role: 'assistant',
        content: [
          { type: 'tool-call', id: 'c', name: 'wanted', params: {}, providerExecuted: false },
        ],
      }
      const tool = (name: string, value: string): Schema.Json => ({
        role: 'tool',
        content: [
          {
            type: 'tool-result',
            id: 'c',
            name,
            result: value,
            isFailure: false,
            providerExecuted: false,
          },
        ],
      })
      const view = yield* Projection.project(root, [
        entry(1, { model: [assistant, tool('wanted', 'first'), tool('wanted', 'duplicate')] }),
        entry(2, { model: [assistant, tool('orphan', 'wrong')] }),
        entry(3, { model: [assistant, tool('wanted', 'last')] }),
      ])
      const results = view.prompt.content
        .flatMap((message) => (message.role === 'tool' ? message.content : []))
        .filter((part) => part.type === 'tool-result')
      assert.deepEqual(
        results.map((part) => [part.id, part.name, part.isFailure]),
        [
          ['c', 'wanted', false],
          ['c', 'wanted', true],
          ['c', 'wanted', false],
        ],
      )
      assert.strictEqual(results[0]!.result, 'first')
      assert.deepEqual(results[1]!.result, {
        reason: 'missing_result',
        message: 'Tool result unavailable: history ends before this call completed.',
      })
      assert.strictEqual(results[2]!.result, 'last')
    }),
  )

  it.effect('filters excluded assistant statuses in original and replacement facts', () =>
    Effect.gen(function* () {
      const entries = ['aborted', 'error', 'deferred'].map((stopReason, index) =>
        entry(index + 1, {
          model: [{ role: 'assistant', content: 'excluded', stopReason }, user(stopReason)],
        }),
      )
      const view = yield* Projection.project(root, [
        ...entries,
        entry(4, {
          model: [{ role: 'assistant', content: 'native excluded' }],
          data: fact({ assistantStopReason: 'deferred' }),
        }),
        entry(5, { model: [{ role: 'assistant', content: 'ok' }] }),
        entry(6, {
          edits: [
            {
              _tag: 'replace',
              target: id(5),
              messages: [
                { role: 'assistant', content: 'excluded replacement', stopReason: 'error' },
              ],
            },
          ],
        }),
      ])
      assert.deepEqual(texts(view), ['aborted', 'error', 'deferred'])
    }),
  )

  it.effect('retains media/options and returns detached deeply frozen serializable values', () =>
    Effect.gen(function* () {
      const media = {
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
      const input = [
        entry(1, { model: [media] }),
        entry(2, { model: [{ role: 'system', content: 'baseline' }] }),
      ]
      const view = yield* Projection.project(root, input)
      assert.strictEqual(view.prompt.content[0]!.role, 'system')
      const native = view.prompt.content[1]!
      assert.strictEqual(native.role, 'user')
      if (native.role === 'user') {
        const file = native.content[0]!
        assert.strictEqual(file.type, 'file')
        if (file.type === 'file') assert.strictEqual(file.data, media.content[0]!.data)
        assert.deepEqual(file.options, media.content[0]!.options)
      }
      assert.isTrue(Object.isFrozen(view))
      assert.isTrue(Object.isFrozen(view.activeEntries[0]!.model))
      assert.isTrue(Object.isFrozen(native.options.provider))
      assert.isFalse(Object.isFrozen(media))
      media.options.provider.cache = false
      assert.deepEqual(native.options, { provider: { cache: true } })
      const json = Schema.fromJsonString(Projection.ContextViewSchema)
      const encoded = yield* Schema.encodeEffect(json)(view)
      const decoded = yield* Schema.decodeEffect(json)(encoded)
      assert.deepEqual(decoded.prompt.content, view.prompt.content)
    }),
  )

  it.effect(
    'replays ordered declarative changes and new baselines without putting fields on Prompt',
    () =>
      Effect.gen(function* () {
        const tool = (name: string): Projection.ToolDeclaration => ({
          name,
          description: name,
          parameters: { type: 'object' },
        })
        const entries = [
          entry(1, {
            data: fact({
              baseline: true,
              sections: [
                { _tag: 'set', section: { key: 'X', text: 'old' } },
                { _tag: 'set', section: { key: 'Y', text: 'Y' } },
              ],
              tools: [
                { _tag: 'set', declaration: tool('a') },
                { _tag: 'set', declaration: tool('b') },
              ],
            }),
          }),
          entry(2, {
            data: fact({
              sections: [
                { _tag: 'set', section: { key: 'X', text: 'new' } },
                { _tag: 'remove', key: 'X' },
                { _tag: 'set', section: { key: 'X', text: 'last' } },
              ],
              tools: [
                { _tag: 'remove', name: 'a' },
                { _tag: 'set', declaration: tool('a') },
              ],
            }),
          }),
          entry(3, {
            head: id(3),
            data: fact({
              baseline: true,
              sections: [{ _tag: 'set', section: { key: 'Z', text: 'reset' } }],
            }),
          }),
        ]
        const before = yield* Projection.project(root, entries, { at: id(2) })
        assert.deepEqual(before.orderedSections, [
          { key: 'Y', text: 'Y' },
          { key: 'X', text: 'last' },
        ])
        assert.deepEqual(
          before.toolDeclarations.map((item) => item.name),
          ['b', 'a'],
        )
        assert.deepEqual(
          before.positionalFacts.map((item) => item.entryId),
          [id(1), id(2)],
        )
        const after = yield* Projection.project(root, entries)
        assert.deepEqual(after.orderedSections, [{ key: 'Z', text: 'reset' }])
        assert.deepEqual(after.toolDeclarations, [])
        assert.deepEqual(after.prompt.content, [])
      }),
  )

  it.effect(
    'rejects invisible cutoffs, missing head targets, unordered entries and invalid tagged facts',
    () =>
      Effect.gen(function* () {
        for (const effect of [
          Projection.project(root, [entry(1)], { at: id(2) }),
          Projection.project(root, [entry(2, { head: id(1) })]),
          Projection.project(root, [entry(1, { head: id(2) }), entry(2)]),
          Projection.project(root, [entry(2), entry(1)]),
          Projection.project(root, [
            entry(1, { data: { _tag: '@effect-harness/ContextFact', version: 9 } }),
          ]),
        ])
          assert.strictEqual((yield* Effect.result(effect))._tag, 'Failure')
        const empty = yield* Projection.project(root, [])
        assert.isUndefined(empty.bounds.tail)
        assert.deepEqual(empty.prompt.content, [])
      }),
  )

  it.effect(
    'edits remove declarative contributions and native provider results/approvals survive',
    () =>
      Effect.gen(function* () {
        const view = yield* Projection.project(root, [
          entry(1, {
            data: fact({ sections: [{ _tag: 'set', section: { key: 'old', text: 'old' } }] }),
          }),
          entry(2, { edits: [{ _tag: 'omit', target: id(1) }] }),
          entry(3, {
            model: [
              {
                role: 'assistant',
                content: [
                  {
                    type: 'tool-call',
                    id: 'provider',
                    name: 'search',
                    params: {},
                    providerExecuted: true,
                  },
                  {
                    type: 'tool-result',
                    id: 'provider',
                    name: 'search',
                    result: 'found',
                    isFailure: false,
                    providerExecuted: true,
                  },
                  { type: 'tool-approval-request', approvalId: 'approval', toolCallId: 'next' },
                ],
              },
            ],
          }),
          entry(4, {
            model: [
              {
                role: 'tool',
                content: [
                  { type: 'tool-approval-response', approvalId: 'approval', approved: true },
                ],
              },
            ],
          }),
        ])
        assert.deepEqual(view.orderedSections, [])
        assert.deepEqual(view.positionalFacts, [])
        assert.deepEqual(
          view.prompt.content.map((message) => message.role),
          ['assistant', 'tool'],
        )
        const assistant = view.prompt.content[0]!
        if (assistant.role === 'assistant')
          assert.strictEqual(
            assistant.content.filter((part) => part.type === 'tool-result').length,
            1,
          )
        const approval = view.prompt.content[1]!
        if (approval.role === 'tool')
          assert.strictEqual(approval.content[0]!.type, 'tool-approval-response')
        const replaced = yield* Projection.project(root, [
          entry(1, {
            data: fact({ assistantStopReason: 'aborted' }),
            model: [{ role: 'assistant', content: 'excluded' }],
          }),
          entry(2, {
            edits: [
              {
                _tag: 'replace',
                target: id(1),
                messages: [{ role: 'assistant', content: 'replacement' }],
              },
            ],
          }),
        ])
        assert.deepEqual(texts(replaced), ['replacement'])
      }),
  )

  it.effect(
    'bounded reads survive parent writes, forks and reopen, and perform zero storage writes',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
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
              const [first, cutoff] = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.ensureRoot(tx)
                  const first = yield* Transaction.appendEntry(tx, root, {
                    kind: 'input',
                    model: [user('A')],
                  })
                  const cutoff = yield* Transaction.appendEntry(tx, root, {
                    kind: 'assistant',
                    model: [
                      {
                        role: 'assistant',
                        content: [
                          {
                            type: 'tool-call',
                            id: 'pending',
                            name: 'lookup',
                            params: {},
                            providerExecuted: false,
                          },
                        ],
                      },
                    ],
                  })
                  return [first, cutoff] as const
                }),
              )
              const options = { at: cutoff.id }
              const beforeCount = writes
              const before = yield* Projection.read(session, root, options)
              assert.strictEqual(writes, beforeCount)
              const child = yield* Session.commit(session, (tx) =>
                Transaction.forkConversation(tx, root, cutoff.id, {
                  ownership: { _tag: 'ownerless' },
                }),
              )
              yield* Session.commit(session, (tx) =>
                Transaction.appendEntry(tx, root, {
                  kind: 'edit',
                  head: 'self',
                  edits: [{ _tag: 'omit', target: first.id }],
                  model: [user('later')],
                }),
              )
              const count = writes
              const after = yield* Projection.read(session, root, options)
              const inherited = yield* Projection.read(session, child.id)
              assert.deepEqual(after, before)
              assert.deepEqual(inherited.prompt.content, before.prompt.content)
              assert.deepEqual(inherited.activeEntries, before.activeEntries)
              assert.deepEqual(texts(yield* Projection.read(session, root)), ['later'])
              assert.strictEqual(writes, count)
              return { cutoff: cutoff.id, child: child.id, before }
            }),
          )
          const reopened = yield* Session.make().pipe(
            Effect.provideService(Storage.Storage, monitored),
          )
          const count = writes
          assert.deepEqual(
            yield* Projection.read(reopened, root, { at: saved.cutoff }),
            saved.before,
          )
          assert.deepEqual(
            (yield* Projection.read(reopened, saved.child)).prompt.content,
            saved.before.prompt.content,
          )
          const missingId = yield* Schema.decodeEffect(Record.ConversationId)(999)
          const absent = yield* Projection.read(reopened, missingId).pipe(Effect.result)
          assert.strictEqual(absent._tag, 'Failure')
          assert.strictEqual(writes, count)
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
})

it.effect(
  'entry messages preserve native multi-message content and reject malformed canonical results',
  () =>
    Effect.gen(function* () {
      const expected = [
        Prompt.userMessage({
          content: [
            Prompt.filePart({
              mediaType: 'image/png',
              data: new Uint8Array([0, 1, 254, 255]),
              options: { openai: { imageDetail: 'high' } },
            }),
          ],
        }),
        Prompt.assistantMessage({ content: [Prompt.textPart({ text: 'second message' })] }),
      ]
      const model = yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))(
        expected,
      )
      assert.deepStrictEqual(yield* Projection.messages(entry(2, { model })), [
        Prompt.userMessage({
          content: [
            Prompt.filePart({
              mediaType: 'image/png',
              data: 'AAH+/w==',
              options: { openai: { imageDetail: 'high' } },
            }),
          ],
        }),
        expected[1]!,
      ])
      assert.deepStrictEqual(yield* Projection.messages(entry(3)), [])
      assert.isTrue(
        Schema.isSchemaError(
          yield* Projection.messages(
            entry(4, {
              kind: 'tool.result',
              data: null,
              model,
            }),
          ).pipe(Effect.flip),
        ),
      )
    }),
)
