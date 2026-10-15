import * as ToolResult from 'effect-harness/ToolResult'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Compaction from '../src/Compaction.js'
import * as Projection from '../src/ContextProjection.js'
import * as Record from '../src/Record.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Transaction from '../src/Transaction.js'
import * as Stream from 'effect/Stream'
import * as Plan from '../src/internal/CompactionPlan.js'
import * as Protocol from '../src/internal/ContextProtocol.js'

const root = Record.ROOT_CONVERSATION_ID
const foreignConversation = Schema.decodeSync(Record.ConversationId)(2)
const id = Schema.decodeSync(Record.EntryId)
const user = (text: string): Schema.Json => ({ role: 'user', content: text })
const entry = (
  n: number,
  model: ReadonlyArray<Schema.Json>,
  rest: Partial<Record.Entry> = {},
): Record.Entry => ({ id: id(n), conversationId: root, kind: 'custom', model, ...rest })
const history = () => [
  entry(1, [user('old')]),
  entry(2, [user('middle')]),
  entry(3, [user('keep')]),
]
const protocol: Protocol.Protocol = {
  version: 1,
  origin: { conversationId: root },
  legacyMode: 'honorAll',
}
const policy: Compaction.Policy = {
  enabled: false,
  reserveTokens: 20,
  keepRecentTokens: 0,
  backgroundTokens: 0,
}
const canonical = Effect.fnUntraced(function* (entries: ReadonlyArray<Record.Entry>, p = protocol) {
  return { entries, protocol: p, view: yield* Protocol.project(root, entries, p) }
})
const pin = Effect.fnUntraced(function* (entries: ReadonlyArray<Record.Entry>) {
  const input = yield* canonical(entries)
  const selection = yield* Compaction.select(input.view, policy, { reason: 'manual' })
  if (selection._tag !== 'Selected') return yield* Effect.die('fixture has no selection')
  return { input, selection, fingerprint: yield* Plan.capture(selection, input) }
})
const rejected = <A, E>(result: Result.Result<A, E>) => {
  assert.isTrue(Result.isFailure(result))
}
const call: Schema.Json = {
  type: 'tool-call',
  id: 'lookup1',
  name: 'lookup',
  params: { path: 'a' },
  providerExecuted: false,
}
const toolResult = {
  type: 'tool-result',
  id: 'lookup1',
  name: 'lookup',
  result: 'found',
  isFailure: false,
  providerExecuted: false,
} satisfies Schema.Json
const assistant = (...content: ReadonlyArray<Schema.Json>): Schema.Json => ({
  role: 'assistant',
  content,
})
const tool = (...content: ReadonlyArray<Schema.Json>): Schema.Json => ({ role: 'tool', content })

describe('pure compaction range planning', () => {
  for (const target of ['selected', 'retained'] as const)
    for (const tag of ['omit', 'replace'] as const) {
      it.effect(
        `real append-only ${tag} on ${target} fails revalidation inside a later placement transaction`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const session = yield* Session.make()
              const originals = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.ensureRoot(tx)
                  const first = yield* Transaction.appendEntry(tx, root, {
                    kind: 'input',
                    model: [user('old')],
                  })
                  const middle = yield* Transaction.appendEntry(tx, root, {
                    kind: 'input',
                    model: [user('middle')],
                  })
                  const tail = yield* Transaction.appendEntry(tx, root, {
                    kind: 'input',
                    model: [user('keep')],
                  })
                  return [first, middle, tail]
                }),
              )
              const { fingerprint } = yield* pin(originals)
              const readCurrent = (tx: Transaction.Transaction) =>
                Transaction.scanEntries(tx, { conversationId: root, order: 'ascending' }).pipe(
                  Stream.runCollect,
                  Effect.flatMap(canonical),
                )
              // Admission's check passes, but it does not grant a future placement guarantee.
              yield* Session.commit(session, (tx) =>
                readCurrent(tx).pipe(
                  Effect.flatMap((current) => Plan.validate(fingerprint, current)),
                ),
              )
              const affected = target === 'selected' ? originals[0]!.id : originals[2]!.id
              yield* Session.commit(session, (tx) =>
                Transaction.appendEntry(tx, root, {
                  kind: 'context.edit',
                  edits: [
                    tag === 'omit'
                      ? { _tag: 'omit', target: affected }
                      : {
                          _tag: 'replace',
                          target: affected,
                          messages: [user('edited after admission')],
                        },
                  ],
                }),
              )
              rejected(
                yield* Effect.result(
                  Session.commit(session, (tx) =>
                    readCurrent(tx).pipe(
                      Effect.flatMap((current) => Plan.validate(fingerprint, current)),
                    ),
                  ),
                ),
              )
              const after = yield* Session.commit(session, (tx) =>
                Transaction.scanEntries(tx, { conversationId: root, order: 'ascending' }).pipe(
                  Stream.runCollect,
                ),
              )
              assert.deepEqual(after.slice(0, 3), originals)
              assert.strictEqual(after.length, 4)
            }).pipe(Effect.provide(Storage.layerMemory)),
          ),
      )
    }

  it.effect(
    'preserves positional facts, native names/IDs/error flags/options and exact persisted media in JSON roundtrip',
    () =>
      Effect.gen(function* () {
        const native = Prompt.fromMessages([
          Prompt.userMessage({
            content: [
              Prompt.filePart({
                mediaType: 'image/png',
                data: new Uint8Array([1, 2]),
                options: { fixture: { quality: 'high' } },
              }),
              Prompt.filePart({
                mediaType: 'image/png',
                data: new URL('https://example.invalid/a'),
              }),
            ],
          }),
        ])
        const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.Prompt))(native)
        const encodedPrompt = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ content: Schema.Array(Schema.Json) }),
        )(encoded)
        const entries = [
          entry(1, encodedPrompt.content),
          entry(2, [assistant(call, { ...toolResult, isFailure: true, providerExecuted: true })]),
          entry(3, [user('keep')]),
        ]
        const { fingerprint } = yield* pin(entries)
        const json = yield* Schema.encodeEffect(Schema.fromJsonString(Plan.FingerprintSchema))(
          fingerprint,
        )
        const restored = yield* Schema.decodeEffect(Schema.fromJsonString(Plan.FingerprintSchema))(
          json,
        )
        yield* Plan.validate(restored, yield* canonical(entries))
        assert.deepEqual(restored.selected[0]!.entry.model, encodedPrompt.content)
        assert.deepEqual(restored.selected[1]!.entry.model, entries[1]!.model)
        const replacement = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.Message))(
          Prompt.userMessage({
            content: [Prompt.filePart({ mediaType: 'image/png', data: 'changed' })],
          }),
        )
        rejected(
          yield* Effect.result(
            Plan.validate(
              restored,
              yield* canonical([
                ...entries,
                entry(4, [], {
                  edits: [{ _tag: 'replace', target: id(1), messages: [replacement] }],
                }),
              ]),
            ),
          ),
        )
      }),
  )

  it.effect(
    'retains original audit records and effective append-only edit provenance at capture',
    () =>
      Effect.gen(function* () {
        const originals = history()
        const edit = entry(4, [], {
          edits: [{ _tag: 'replace', target: id(1), messages: [user('effective replacement')] }],
        })
        const { fingerprint } = yield* pin([...originals, edit])
        assert.deepEqual(fingerprint.selected[0]!.entry.model, [user('old')])
        assert.strictEqual(fingerprint.selected[0]!.contribution[0]!.role, 'user')
        assert.deepEqual(
          fingerprint.edits.map((record) => record.id),
          [id(4)],
        )
        yield* Plan.validate(
          fingerprint,
          yield* canonical([...originals, edit, entry(5, [user('ordinary suffix')])]),
        )
      }),
  )

  it.effect('allows later reused call IDs and unrelated names without borrowing old results', () =>
    Effect.gen(function* () {
      const entries = [
        entry(1, [user('old')]),
        entry(2, [assistant(call)]),
        entry(3, [user('keep')]),
      ]
      const { fingerprint } = yield* pin(entries)
      yield* Plan.validate(
        fingerprint,
        yield* canonical([...entries, entry(4, [assistant(call)]), entry(5, [tool(toolResult)])]),
      )
      yield* Plan.validate(
        fingerprint,
        yield* canonical([...entries, entry(4, [tool({ ...toolResult, name: 'other' })])]),
      )
    }),
  )

  it.effect('rejects late approval responses crossing the pinned tail', () =>
    Effect.gen(function* () {
      const request = {
        type: 'tool-approval-request',
        approvalId: 'approval1',
        toolCallId: 'lookup1',
      }
      const response = { type: 'tool-approval-response', approvalId: 'approval1', approved: true }
      const entries = [
        entry(1, [user('old')]),
        entry(2, [user('middle')]),
        entry(3, [assistant(call, request)]),
      ]
      const { fingerprint } = yield* pin(entries)
      rejected(
        yield* Effect.result(
          Plan.validate(fingerprint, yield* canonical([...entries, entry(4, [tool(response)])])),
        ),
      )
    }),
  )

  it.effect(
    'keeps unchanged ordinary appended context valid without changing the pinned tail',
    () =>
      Effect.gen(function* () {
        const entries = history()
        const { fingerprint } = yield* pin(entries)
        const result = yield* Plan.validate(
          fingerprint,
          yield* canonical([...entries, entry(4, [user('new input')])]),
        )
        assert.notStrictEqual(result, fingerprint)
        assert.notStrictEqual(result.selected, fingerprint.selected)
        assert.strictEqual(result.tail, id(3))
        assert.deepEqual(
          result.selected.map((item) => item.entry.id),
          [id(1), id(2)],
        )
        assert.deepEqual(
          result.retained.map((item) => item.entry.id),
          [id(3)],
        )
        assert.isTrue(Object.isFrozen(result.selected[0]!.contribution))
      }),
  )

  for (const target of [1, 3])
    for (const tag of ['omit', 'replace'] as const) {
      it.effect(
        `rejects append-only ${tag} on ${target === 1 ? 'selected' : 'retained'} history at selection and queued placement`,
        () =>
          Effect.gen(function* () {
            const entries = history()
            const { fingerprint } = yield* pin(entries)
            // Hypothetical admission can pass before the edit; placement must revalidate afterward.
            yield* Plan.validate(fingerprint, yield* canonical(entries))
            const edit: Record.ContextEdit =
              tag === 'omit'
                ? { _tag: 'omit', target: id(target) }
                : { _tag: 'replace', target: id(target), messages: [user('changed')] }
            const changed = [...entries, entry(4, [], { edits: [edit] })]
            rejected(yield* Effect.result(Plan.validate(fingerprint, yield* canonical(changed))))
            // A bounded view misses the late edit and must itself be rejected.
            const bounded = yield* Projection.project(root, changed, { at: id(3) })
            rejected(
              yield* Effect.result(
                Plan.validate(fingerprint, { entries: changed, protocol, view: bounded }),
              ),
            )
          }),
      )
    }

  it.effect('tracks a later replacement even when it restores the same effective text', () =>
    Effect.gen(function* () {
      const entries = history()
      const { fingerprint } = yield* pin(entries)
      rejected(
        yield* Effect.result(
          Plan.validate(
            fingerprint,
            yield* canonical([
              ...entries,
              entry(4, [], {
                edits: [{ _tag: 'replace', target: id(1), messages: [user('old')] }],
              }),
            ]),
          ),
        ),
      )
    }),
  )

  it.effect('ignores edits on ordinary appended entries outside the pinned range', () =>
    Effect.gen(function* () {
      const entries = history()
      const { fingerprint } = yield* pin(entries)
      yield* Plan.validate(
        fingerprint,
        yield* canonical([
          ...entries,
          entry(4, [user('later')]),
          entry(5, [], { edits: [{ _tag: 'omit', target: id(4) }] }),
        ]),
      )
    }),
  )

  it.effect('rejects a new head marker with the same numeric target', () =>
    Effect.gen(function* () {
      const entries = [...history(), entry(4, [user('summary A')], { head: id(1) })]
      const { fingerprint } = yield* pin(entries)
      const current = yield* canonical([...entries, entry(5, [user('summary A')], { head: id(1) })])
      assert.strictEqual(current.view.bounds.head!.head, fingerprint.head!.head)
      assert.notStrictEqual(current.view.bounds.head!.id, fingerprint.head!.id)
      rejected(yield* Effect.result(Plan.validate(fingerprint, current)))
    }),
  )

  it.effect(
    'rejects changed head content, missing originals and changed immutable original data',
    () =>
      Effect.gen(function* () {
        const entries = [...history(), entry(4, [user('summary')], { head: id(1) })]
        const { fingerprint } = yield* pin(entries)
        for (const changed of [
          entries.map((e) => (e.id === id(4) ? { ...e, model: [user('different')] } : e)),
          entries.filter((e) => e.id !== id(2)),
          entries.map((e) => (e.id === id(1) ? { ...e, data: { changed: true } } : e)),
        ]) {
          rejected(yield* Effect.result(Plan.validate(fingerprint, yield* canonical(changed))))
        }
      }),
  )

  it.effect('pins protocol origin and rejects changed interpretation', () =>
    Effect.gen(function* () {
      const entries = history()
      const { fingerprint } = yield* pin(entries)
      const different: Protocol.Protocol = {
        ...protocol,
        origin: { conversationId: root, through: id(1) },
      }
      rejected(
        yield* Effect.result(Plan.validate(fingerprint, yield* canonical(entries, different))),
      )
      const preserved: Protocol.Protocol = { ...different, legacyMode: 'preserveManaged' }
      rejected(
        yield* Effect.result(Plan.validate(fingerprint, yield* canonical(entries, preserved))),
      )
    }),
  )

  it.effect('pins positional facts and later omissions removing them', () =>
    Effect.gen(function* () {
      const fact: Projection.Fact = {
        _tag: '@effect-harness/ContextFact',
        version: 1,
        sections: [{ _tag: 'set', section: { key: 'goal', text: 'first' } }],
        tools: [],
      }
      const entries = [entry(1, [user('old')], { data: fact }), ...history().slice(1)]
      const { fingerprint } = yield* pin(entries)
      assert.deepEqual(fingerprint.selected[0]!.fact, fact)
      rejected(
        yield* Effect.result(
          Plan.validate(
            fingerprint,
            yield* canonical([
              ...entries,
              entry(4, [], { edits: [{ _tag: 'omit', target: id(1) }] }),
            ]),
          ),
        ),
      )
    }),
  )

  it.effect(
    'rejects a late result crossing the cut and a late result crossing the pinned tail',
    () =>
      Effect.gen(function* () {
        for (const n of [2, 3]) {
          const entries = history().map((e) => (e.id === id(n) ? entry(n, [assistant(call)]) : e))
          const { fingerprint } = yield* pin(entries)
          rejected(
            yield* Effect.result(
              Plan.validate(
                fingerprint,
                yield* canonical([...entries, entry(4, [tool(toolResult)])]),
              ),
            ),
          )
        }
      }),
  )

  it.effect('accepts a whole tool group and rejects a caller-supplied crossing selection', () =>
    Effect.gen(function* () {
      const entries = [
        entry(1, [user('old')]),
        entry(2, [assistant(call)]),
        entry(3, [tool(toolResult)]),
        entry(4, [user('keep')]),
      ]
      const { input, selection, fingerprint } = yield* pin(entries)
      yield* Plan.validate(fingerprint, input)
      const forged = {
        ...selection,
        firstKept: id(3),
        source: Projection.projectMessages(input.view.contributions.slice(0, 2).flat()),
        retained: Projection.projectMessages(input.view.contributions.slice(2).flat()),
      }
      rejected(yield* Effect.result(Plan.capture(forged, input)))
    }),
  )

  it.effect(
    'captures detached options, original records and persisted file media; media edits invalidate',
    () =>
      Effect.gen(function* () {
        const options = { fixture: { detail: 'high' } }
        const file: Schema.Json = { type: 'file', mediaType: 'image/png', data: 'AQI=', options }
        const entries = [entry(1, [{ role: 'user', content: [file] }]), ...history().slice(1)]
        const { fingerprint } = yield* pin(entries)
        options.fixture.detail = 'low'
        assert.isFalse(Object.isFrozen(options))
        assert.deepEqual(fingerprint.selected[0]!.contribution[0]!.options, {})
        const stored = fingerprint.selected[0]!.contribution[0]!
        assert.strictEqual(stored.role, 'user')
        if (stored.role === 'user')
          assert.deepEqual(stored.content[0]!.options, { fixture: { detail: 'high' } })
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Plan.FingerprintSchema))(
          fingerprint,
        )
        const restored = yield* Schema.decodeEffect(Schema.fromJsonString(Plan.FingerprintSchema))(
          encoded,
        )
        assert.deepEqual(restored, fingerprint)
        rejected(yield* Effect.result(Plan.validate(fingerprint, yield* canonical(entries))))
      }),
  )

  it.effect('rejects misaligned contributions and foreign selection identities', () =>
    Effect.gen(function* () {
      const { input, selection } = yield* pin(history())
      rejected(
        yield* Effect.result(
          Plan.capture(selection, {
            ...input,
            view: { ...input.view, contributions: input.view.contributions.slice(1) },
          }),
        ),
      )
      rejected(
        yield* Effect.result(
          Plan.capture({ ...selection, conversationId: foreignConversation }, input),
        ),
      )
    }),
  )
})

describe('native text-only compaction transcript', () => {
  it.effect('matches pinned labels, named JSON args, result truncation and prompt focus', () =>
    Effect.gen(function* () {
      const source = Prompt.fromMessages([
        Prompt.systemMessage({ content: 'omit this' }),
        Prompt.userMessage({ content: [Prompt.textPart({ text: 'question' })] }),
        Prompt.assistantMessage({
          content: [
            Prompt.reasoningPart({ text: 'think' }),
            Prompt.textPart({ text: 'answer' }),
            Prompt.toolCallPart({
              id: '1',
              name: 'lookup',
              params: { path: 'a', n: 2 },
              providerExecuted: false,
            }),
          ],
        }),
        Prompt.toolMessage({
          content: [
            Prompt.toolResultPart({
              id: '1',
              name: 'lookup',
              result: 'x'.repeat(2002),
              isFailure: false,
              providerExecuted: false,
            }),
          ],
        }),
      ])
      const expected = `[User]: question\n\n[Assistant]: answer\n\n[Assistant tool calls]: lookup(path="a", n=2)\n\n[Tool result]: ${'x'.repeat(2000)}\n\n[... 2 more characters truncated]`
      assert.strictEqual((yield* Plan.serialize(source)).text, expected)
      const prepared = yield* Plan.preparePrompt(source, 'Focus on paths')
      assert.strictEqual(prepared.prompt.content.length, 2)
      const userMessage = prepared.prompt.content[1]!
      assert.strictEqual(userMessage.role, 'user')
      if (userMessage.role === 'user' && userMessage.content[0]?.type === 'text') {
        assert.isTrue(
          userMessage.content[0].text.startsWith(`<conversation>\n${expected}\n</conversation>`),
        )
        assert.isTrue(userMessage.content[0].text.endsWith('Additional focus: Focus on paths'))
      }
    }),
  )

  for (const params of [null, 1, true, 'a', [1, 'a'], { nested: [null] }]) {
    it.effect(`supports native JSON params/result ${JSON.stringify(params)}`, () =>
      Effect.gen(function* () {
        const source = Prompt.fromMessages([
          Prompt.assistantMessage({
            content: [
              Prompt.toolCallPart({ id: '1', name: 'f', params, providerExecuted: true }),
              Prompt.toolResultPart({
                id: '1',
                name: 'f',
                result: params,
                isFailure: false,
                providerExecuted: true,
              }),
            ],
          }),
        ])
        const transcript = yield* Plan.serialize(source)
        assert.isTrue(transcript.text.includes('[Assistant tool calls]: f('))
        assert.isTrue(
          transcript.text.includes(
            `[Tool result]: ${typeof params === 'string' ? params : JSON.stringify(params)}`,
          ),
        )
      }),
    )
  }

  it.effect(
    'omits and discloses native string/bytes/URL files without retaining mutable media',
    () =>
      Effect.gen(function* () {
        const bytes = new Uint8Array([1, 2])
        const url = new URL('https://example.invalid/media')
        const source = Prompt.fromMessages([
          Prompt.userMessage({
            content: [
              Prompt.textPart({ text: 'visible' }),
              ...[bytes, url, 'AQI='].map((data) =>
                Prompt.filePart({ mediaType: 'image/png', fileName: 'image.png', data }),
              ),
            ],
          }),
        ])
        const transcript = yield* Plan.serialize(source)
        bytes[0] = 9
        url.pathname = '/changed'
        assert.strictEqual(transcript.text, '[User]: visible')
        assert.deepEqual(
          transcript.omittedMedia.map((item) => item.part),
          [1, 2, 3],
        )
        assert.isTrue(Object.isFrozen(transcript.omittedMedia[0]))
        // Native Effect codecs normalize file branches; harness persistence uses PromptWire.
        const json = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.Prompt))(source)
        assert.isTrue(Schema.is(Schema.Json)(json))
        const { input, selection } = yield* pin(history())
        rejected(yield* Effect.result(Plan.capture({ ...selection, source }, input)))
      }),
  )

  it.effect('rejects nonserializable native unknown parameters and results visibly', () =>
    Effect.gen(function* () {
      const cycle: { self?: unknown } = {}
      cycle.self = cycle
      const accessor = Object.defineProperty({}, 'value', { enumerable: true, get: () => 1 })
      for (const value of [
        undefined,
        () => 1,
        1n,
        Symbol('x'),
        Number.NaN,
        cycle,
        accessor,
        [undefined],
        { hidden: undefined },
      ]) {
        for (const kind of ['params', 'result']) {
          const content =
            kind === 'params'
              ? [
                  Prompt.toolCallPart({
                    id: '1',
                    name: 'f',
                    params: value,
                    providerExecuted: false,
                  }),
                ]
              : [
                  Prompt.toolResultPart({
                    id: '1',
                    name: 'f',
                    result: value,
                    isFailure: false,
                    providerExecuted: true,
                  }),
                ]
          const outcome = yield* Effect.result(
            Plan.serialize(Prompt.fromMessages([Prompt.assistantMessage({ content })])),
          )
          rejected(outcome)
          if (Result.isFailure(outcome)) assert.strictEqual(outcome.failure._tag, 'CompactionError')
        }
      }
    }),
  )
})

for (const data of [
  new Uint8Array(4096),
  new URL('https://example.com/image.png'),
  'A'.repeat(8192),
]) {
  it.effect(
    `extracts trailing tool image captions (${typeof data === 'string' ? 'base64' : data.constructor.name})`,
    () =>
      Effect.gen(function* () {
        const envelope = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
          _tag: '@effect-harness/ToolContent',
          content: [
            Prompt.filePart({ mediaType: 'image/png', fileName: 'screen.png', data }),
            Prompt.textPart({ text: 'Click at x=200, y=100; scale x=2 y=2.' }),
          ],
        })
        const transcript = yield* Plan.serialize(
          Prompt.fromMessages([
            Prompt.toolMessage({
              content: [
                Prompt.toolResultPart({
                  id: 'capture-1',
                  name: 'screenshot',
                  result: envelope,
                  isFailure: true,
                  providerExecuted: false,
                }),
              ],
            }),
          ]),
        )
        assert.include(transcript.text, '[Tool result screenshot (id=capture-1, error)]')
        assert.include(transcript.text, 'Click at x=200, y=100; scale x=2 y=2.')
        assert.notInclude(transcript.text, 'AAAA')
        assert.deepEqual(transcript.omittedMedia, [
          {
            message: 0,
            part: 0,
            contentPart: 0,
            callId: 'capture-1',
            name: 'screenshot',
            isFailure: true,
            mediaType: 'image/png',
            fileName: 'screen.png',
          },
        ])
      }),
  )
}

it.effect('keeps malformed or extended envelope-shaped application JSON opaque', () =>
  Effect.gen(function* () {
    for (const result of [
      { _tag: '@effect-harness/ToolContent', content: [{ type: 'file', data: 'private' }] },
      {
        _tag: '@effect-harness/ToolContent',
        content: [{ type: 'text', text: 'caption' }],
        application: true,
      },
      {
        _tag: '@effect-harness/ToolContent',
        content: [
          {
            type: 'file',
            mediaType: 'image/png',
            data: { _tag: '@effect-harness/Bytes', data: 'invalid!' },
          },
        ],
      },
    ]) {
      const transcript = yield* Plan.serialize(
        Prompt.fromMessages([
          Prompt.toolMessage({
            content: [
              Prompt.toolResultPart({
                id: 'a',
                name: 'json',
                result,
                isFailure: false,
                providerExecuted: false,
              }),
            ],
          }),
        ]),
      )
      assert.include(transcript.text, JSON.stringify(result))
      assert.deepEqual(transcript.omittedMedia, [])
    }
  }),
)

it.effect('applies the result text budget after excluding media payloads', () =>
  Effect.gen(function* () {
    const result = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
      _tag: '@effect-harness/ToolContent',
      content: [
        Prompt.filePart({ mediaType: 'image/png', data: 'A'.repeat(8192) }),
        Prompt.textPart({ text: 'caption' }),
        Prompt.textPart({ text: 'x'.repeat(2000) }),
      ],
    })
    const transcript = yield* Plan.serialize(
      Prompt.fromMessages([
        Prompt.toolMessage({
          content: [
            Prompt.toolResultPart({
              id: 'b',
              name: 'screen',
              result,
              isFailure: false,
              providerExecuted: false,
            }),
          ],
        }),
      ]),
    )
    assert.strictEqual(
      transcript.text,
      `[Tool result screen (id=b)]: caption\n${'x'.repeat(1992)}\n\n[... 8 more characters truncated]`,
    )
    assert.lengthOf(transcript.omittedMedia, 1)
  }),
)
