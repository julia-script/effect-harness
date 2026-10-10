import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as AiError from 'effect/ai/AiError'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as Tokenizer from 'effect/ai/Tokenizer'
import * as Compaction from 'effect-harness/Compaction'
import * as Projection from 'effect-harness/ContextProjection'
import * as Identity from 'effect-harness/Identity'
import type * as Model from 'effect-harness/Model'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as Usage from 'effect-harness/Usage'

const root = Record.ROOT_CONVERSATION_ID
const id = Schema.decodeSync(Record.EntryId)
const user = (text: string): Schema.Json => ({ role: 'user', content: text })
const text = (tokens: number) => 'x'.repeat(Math.floor(tokens * 3.5))
const entry = (
  n: number,
  model: ReadonlyArray<Schema.Json>,
  rest: Partial<Record.Entry> = {},
): Record.Entry => ({
  id: id(n),
  conversationId: root,
  kind: 'custom',
  model,
  ...rest,
})
const policy: Compaction.Policy = {
  enabled: true,
  reserveTokens: 20,
  keepRecentTokens: 10,
  backgroundTokens: 10,
}
const manual: Compaction.Input = { reason: 'manual' }
const config: Compaction.SummaryConfig = {
  requestId: Identity.RequestId.make('compaction:1/config:1'),
  attempt: Usage.AttemptId.make(0),
  model: { ref: { provider: 'fixture', modelId: 'summary' }, options: { reasoning: 'off' } },
  stream: { headers: { tenant: 'one' }, metadata: { label: 'captured' } },
  maxOutputTokens: 16,
}
const selected = (value: Compaction.Result): Compaction.Selected => {
  if (value._tag !== 'Selected') assert.fail(`Expected selection, received ${value.reason}`)
  return value
}
const nativeFailure = AiError.make({
  module: 'Fixture',
  method: 'tokenize',
  reason: new AiError.UnknownError({ description: 'fixture failure' }),
})
const call = (callId: string, name = 'lookup'): Schema.Json => ({
  type: 'tool-call',
  id: callId,
  name,
  params: {},
  providerExecuted: false,
})
const result = (callId: string, name = 'lookup'): Schema.Json => ({
  type: 'tool-result',
  id: callId,
  name,
  result: 'found',
  isFailure: false,
  providerExecuted: false,
})
const assistant = (...content: ReadonlyArray<Schema.Json>): Schema.Json => ({
  role: 'assistant',
  content,
})
const tool = (...content: ReadonlyArray<Schema.Json>): Schema.Json => ({ role: 'tool', content })
const messages = (prompt: Prompt.Prompt) =>
  prompt.content.flatMap((message) =>
    message.role === 'system'
      ? [message.content]
      : message.content.flatMap((part) => (part.type === 'text' ? [part.text] : [])),
  )

describe('safe compaction selection foundation', () => {
  for (const [tokens, expected] of [
    [69, 'Noop'],
    [70, 'Noop'],
    [71, 'background'],
    [80, 'background'],
    [81, 'blocking'],
  ] as const) {
    it.effect(`strict threshold at ${tokens} tokens for window100/reserve20/background10`, () =>
      Effect.gen(function* () {
        const view = yield* Projection.project(root, [
          entry(1, [user(text(20))]),
          entry(2, [user(text(tokens - 20))]),
        ])
        const value = yield* Compaction.select(view, policy, {
          reason: 'threshold',
          contextWindow: 100,
        })
        assert.strictEqual(value.measurement.method, 'estimate')
        assert.strictEqual(value.measurement.tokens, tokens)
        if (expected === 'Noop') {
          assert.strictEqual(value._tag, 'Noop')
          if (value._tag === 'Noop') assert.strictEqual(value.reason, 'belowThreshold')
        } else {
          const cut = selected(value)
          assert.strictEqual(cut.mode, expected)
          assert.strictEqual(cut.reason, 'threshold')
          assert.strictEqual(cut.firstKept, id(2))
          assert.strictEqual(cut.tail, id(2))
        }
      }),
    )
  }

  it.effect(
    'planned extra messages affect thresholds without entering the pinned summary range',
    () =>
      Effect.gen(function* () {
        const view = yield* Projection.project(root, [
          entry(1, [user(text(20))]),
          entry(2, [user(text(49))]),
        ])
        const value = selected(
          yield* Compaction.select(view, policy, {
            reason: 'threshold',
            contextWindow: 100,
            extra: [Prompt.userMessage({ content: [Prompt.textPart({ text: text(2) })] })],
          }),
        )
        assert.strictEqual(value.measurement.tokens, 71)
        assert.deepEqual(messages(value.source), [text(20)])
        assert.deepEqual(messages(value.retained), [text(49)])
      }),
  )

  it.effect(
    'manual ignores automatic enablement and window; overflow explicitly selects blocking work',
    () =>
      Effect.gen(function* () {
        const view = yield* Projection.project(root, [
          entry(1, [user('old')]),
          entry(2, [user(text(20))]),
        ])
        const disabled = { ...policy, enabled: false }
        assert.strictEqual(
          selected(yield* Compaction.select(view, disabled, manual)).mode,
          'blocking',
        )
        assert.strictEqual(
          selected(yield* Compaction.select(view, disabled, { reason: 'overflow' })).reason,
          'overflow',
        )
        for (const [settings, input, expected] of [
          [disabled, { reason: 'threshold', contextWindow: 100 }, 'disabled'],
          [policy, { reason: 'threshold' }, 'unknownWindow'],
          [
            { ...policy, backgroundTokens: 0 },
            { reason: 'threshold', contextWindow: 100 },
            'belowThreshold',
          ],
        ] as const) {
          const value = yield* Compaction.select(view, settings, input)
          assert.strictEqual(value._tag, 'Noop')
          if (value._tag === 'Noop') assert.strictEqual(value.reason, expected)
        }
      }),
  )

  it.effect('no safe prefix is an explicit no-op for manual, threshold, and overflow', () =>
    Effect.gen(function* () {
      for (const entries of [
        [],
        [entry(1, [user(text(100))])],
        [entry(1, [user('old')]), entry(2, [user('recent')])],
      ]) {
        const view = yield* Projection.project(root, entries)
        for (const reason of ['manual', 'threshold', 'overflow'] as const) {
          const value = yield* Compaction.select(view, policy, { reason, contextWindow: 10 })
          assert.strictEqual(value._tag, 'Noop')
          if (value._tag === 'Noop') assert.strictEqual(value.reason, 'noSafeCut')
        }
      }
    }),
  )

  it.effect(
    'keeps a requested suffix by moving to an earlier safe boundary, never a later one',
    () =>
      Effect.gen(function* () {
        const view = yield* Projection.project(root, [
          entry(1, [user('old prefix')]),
          entry(2, [assistant(call('a'), call('b'))]),
          entry(3, [user('interleaved user')]),
          entry(4, [tool(result('b'))]),
          entry(5, [tool(result('a'))]),
          entry(6, [user('tail')]),
        ])
        const cut = selected(
          yield* Compaction.select(view, { ...policy, keepRecentTokens: 5 }, manual),
        )
        assert.strictEqual(cut.firstKept, id(2))
        assert.deepEqual(
          cut.source.content.map((message) => message.role),
          ['user'],
        )
        assert.deepEqual(
          cut.retained.content.map((message) => message.role),
          ['assistant', 'tool', 'tool', 'user', 'user'],
        )
        assert.deepEqual(
          cut.retained.content.flatMap((message) =>
            message.role === 'tool'
              ? message.content.flatMap((part) => (part.type === 'tool-result' ? [part.id] : []))
              : [],
          ),
          ['a', 'b'],
        )
      }),
  )

  it.effect(
    'preserves exact name matches, inline provider results and assistant windows with reused IDs',
    () =>
      Effect.gen(function* () {
        const view = yield* Projection.project(root, [
          entry(1, [user('old prefix')]),
          entry(2, [assistant(call('reused', 'first'))]),
          entry(3, [user('user')]),
          entry(4, [tool(result('reused', 'wrong'), result('reused', 'first'))]),
          entry(5, [assistant(call('reused', 'second'), result('reused', 'second'))]),
          entry(6, [user(text(20))]),
        ])
        const cut = selected(yield* Compaction.select(view, policy, manual))
        assert.strictEqual(cut.firstKept, id(6))
        const parts = cut.source.content.flatMap((message) =>
          message.role === 'system'
            ? []
            : message.content.filter((part) => part.type === 'tool-result'),
        )
        assert.strictEqual(parts.filter((part) => part.type === 'tool-result').length, 2)
        assert.isFalse(parts.some((part) => part.type === 'tool-result' && part.name === 'wrong'))
      }),
  )

  it.effect('never splits an approval request and its later response at an intervening user', () =>
    Effect.gen(function* () {
      const view = yield* Projection.project(root, [
        entry(1, [user('old prefix')]),
        entry(2, [
          assistant({ type: 'tool-approval-request', approvalId: 'approval', toolCallId: 'a' }),
        ]),
        entry(3, [user(text(20))]),
        entry(4, [
          tool({ type: 'tool-approval-response', approvalId: 'approval', approved: true }),
        ]),
        entry(5, [user('tail')]),
      ])
      assert.strictEqual(
        selected(yield* Compaction.select(view, { ...policy, keepRecentTokens: 15 }, manual))
          .firstKept,
        id(2),
      )
    }),
  )

  it.effect(
    'media has explicit nonzero estimation cost and provider data survives unmodified',
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
        }
        const view = yield* Projection.project(root, [entry(1, [user('old')]), entry(2, [media])])
        const cut = selected(yield* Compaction.select(view, policy, manual))
        assert.strictEqual(cut.measurement.tokens, 1 + Math.ceil(4800 / 3.5))
        assert.deepEqual(cut.retained.content, view.prompt.content.slice(1))
        assert.isTrue(Object.isFrozen(cut.retained.content))
      }),
  )

  it.effect(
    'heads, bounded edits, zero contributions and newest-entry retention follow canonical projection',
    () =>
      Effect.gen(function* () {
        const entries = [
          entry(1, [user('obsolete')]),
          entry(2, [user('omit me')]),
          entry(3, [user('replaced')]),
          entry(4, [], {
            edits: [
              { _tag: 'omit', target: id(2) },
              { _tag: 'replace', target: id(3), messages: [user('replacement prefix')] },
            ],
          }),
          entry(5, [user(text(20))]),
          entry(6, [user('prior summary')], { head: id(2) }),
        ]
        const view = yield* Projection.project(root, entries)
        assert.deepEqual(
          view.activeEntries.map((value) => value.id),
          [id(6), id(2), id(3), id(4), id(5)],
        )
        const cut = selected(yield* Compaction.select(view, policy, manual))
        assert.strictEqual(cut.firstKept, id(5))
        assert.strictEqual(cut.tail, id(6))
        assert.deepEqual(messages(cut.source), ['prior summary', 'replacement prefix'])
        assert.deepEqual(messages(cut.retained), [text(20)])
        const tiny = yield* Compaction.select(
          yield* Projection.project(root, entries),
          { ...policy, keepRecentTokens: 0 },
          manual,
        )
        assert.strictEqual(selected(tiny).firstKept, id(5))
        const old = selected(
          yield* Compaction.select(
            yield* Projection.project(root, entries, { at: id(5) }),
            policy,
            manual,
          ),
        )
        assert.strictEqual(old.tail, id(5))
        assert.deepEqual(messages(old.source), ['obsolete', 'replacement prefix'])
      }),
  )

  it.effect('discarded orphan results cannot satisfy recent retention or trigger thresholds', () =>
    Effect.gen(function* () {
      const view = yield* Projection.project(root, [
        entry(1, [user('old prefix')]),
        entry(2, [user('middle')]),
        entry(3, [
          tool({
            type: 'tool-result',
            id: 'orphan',
            name: 'lookup',
            result: text(1000),
            providerExecuted: false,
            isFailure: false,
          }),
        ]),
        entry(4, [user('tail')]),
      ])
      const manualResult = yield* Compaction.select(view, policy, manual)
      assert.strictEqual(manualResult._tag, 'Noop')
      if (manualResult._tag === 'Noop') assert.strictEqual(manualResult.reason, 'noSafeCut')
      assert.strictEqual(manualResult.measurement.tokens, 7)
      const threshold = yield* Compaction.select(view, policy, {
        reason: 'threshold',
        contextWindow: 100,
      })
      assert.strictEqual(threshold._tag, 'Noop')
      if (threshold._tag === 'Noop') assert.strictEqual(threshold.reason, 'belowThreshold')
      const orphanPrefix = yield* Projection.project(root, [
        entry(1, [tool(result('orphan'))]),
        entry(2, [user(text(20))]),
      ])
      const emptySource = yield* Compaction.select(orphanPrefix, policy, manual)
      assert.strictEqual(emptySource._tag, 'Noop')
      if (emptySource._tag === 'Noop') assert.strictEqual(emptySource.reason, 'noSafeCut')
    }),
  )

  it.effect(
    'native tokenizer counts whole suffixes without assuming additive entry token counts',
    () =>
      Effect.gen(function* () {
        const view = yield* Projection.project(root, [
          entry(1, [user('old')]),
          entry(2, [user('middle')]),
          entry(3, [user('tail')]),
        ])
        const calls: Array<ReadonlyArray<string>> = []
        const tokenizer = Tokenizer.make({
          tokenize: (prompt) => {
            calls.push(messages(prompt))
            const tokens = prompt.content.length === 1 ? 4 : 21
            return Effect.succeed(Array.from({ length: tokens }, (_, index) => index))
          },
        })
        const cut = selected(
          yield* Compaction.select(view, policy, manual).pipe(
            Effect.provideService(Tokenizer.Tokenizer, tokenizer),
          ),
        )
        assert.strictEqual(cut.measurement.method, 'tokenizer')
        assert.strictEqual(cut.measurement.tokens, 21)
        assert.strictEqual(cut.firstKept, id(2))
        assert.deepEqual(calls, [['old', 'middle', 'tail'], ['tail'], ['middle', 'tail']])
      }),
  )

  it.effect(
    'native tokenization failure keeps the original cause and never silently estimates',
    () =>
      Effect.gen(function* () {
        const view = yield* Projection.project(root, [
          entry(1, [user('old')]),
          entry(2, [user(text(20))]),
        ])
        const failed = yield* Compaction.select(view, policy, manual).pipe(
          Effect.provideService(
            Tokenizer.Tokenizer,
            Tokenizer.make({ tokenize: () => Effect.fail(nativeFailure) }),
          ),
          Effect.result,
        )
        assert.isTrue(Result.isFailure(failed))
        if (Result.isFailure(failed)) {
          assert.instanceOf(failed.failure, Compaction.CompactionError)
          if (failed.failure instanceof Compaction.CompactionError) {
            assert.strictEqual(failed.failure.reason, 'failed')
            assert.strictEqual(failed.failure.cause, nativeFailure)
          }
        }
        const overflow = Compaction.modelFailure(nativeFailure, true)
        assert.strictEqual(overflow.reason, 'contextOverflow')
        assert.strictEqual(overflow.cause, nativeFailure)
        assert.strictEqual(overflow.message, nativeFailure.message)
      }),
  )

  it.effect(
    'captures detached frozen policy, config, bounds, instructions and attempt identities',
    () =>
      Effect.gen(function* () {
        const mutablePolicy = { ...policy }
        const mutableConfig = {
          ...config,
          model: { ...config.model, options: { reasoning: 'off' } },
          stream: { headers: { tenant: 'one' } },
        }
        const input = { reason: 'manual' as const, instructions: 'preserve paths' }
        const view = yield* Projection.project(root, [
          entry(1, [user('prefix')]),
          entry(2, [user(text(20))]),
        ])
        const cut = selected(yield* Compaction.select(view, mutablePolicy, input))
        const pinned = yield* Compaction.pinRequest(cut, mutableConfig)
        mutableConfig.model.options.reasoning = 'high'
        mutableConfig.stream.headers.tenant = 'two'
        mutablePolicy.keepRecentTokens = 0
        input.instructions = 'changed'
        assert.strictEqual(pinned.selection.instructions, 'preserve paths')
        assert.strictEqual(pinned.selection.policy.keepRecentTokens, 10)
        assert.deepEqual(pinned.config.model.options, { reasoning: 'off' })
        assert.deepEqual(pinned.config.stream.headers, { tenant: 'one' })
        assert.strictEqual(pinned.config.requestId, config.requestId)
        assert.strictEqual(pinned.config.attempt, config.attempt)
        assert.isTrue(Object.isFrozen(pinned.config.stream.headers))
        assert.isTrue(Object.isFrozen(pinned.config.model.options))
        assert.isFalse(Object.isFrozen(mutableConfig))
        const encoded = yield* Schema.encodeEffect(
          Schema.fromJsonString(Compaction.SummaryRequestSchema),
        )(pinned)
        const recovered = yield* Schema.decodeEffect(
          Schema.fromJsonString(Compaction.SummaryRequestSchema),
        )(encoded)
        assert.deepEqual(recovered, pinned)
        assert.deepEqual(
          yield* Compaction.pinRequest(recovered.selection, recovered.config),
          pinned,
        )
      }),
  )

  it.effect('rejects malformed policy, misaligned canonical view, and reversed pinned bounds', () =>
    Effect.gen(function* () {
      assert.strictEqual(Compaction.PolicySchema, Settings.CompactionSchema)
      const view = yield* Projection.project(root, [
        entry(1, [user('prefix')]),
        entry(2, [user(text(20))]),
      ])
      const cut = selected(yield* Compaction.select(view, policy, manual))
      for (const operation of [
        Compaction.select(view, { ...policy, reserveTokens: -1 }, manual).pipe(Effect.asVoid),
        Compaction.select({ ...view, contributions: [] }, policy, manual).pipe(Effect.asVoid),
        Compaction.pinRequest({ ...cut, firstKept: id(99) }, config).pipe(Effect.asVoid),
        Compaction.pinRequest(cut, { ...config, maxOutputTokens: 0 }).pipe(Effect.asVoid),
      ])
        assert.isTrue(Result.isFailure(yield* Effect.result(operation)))
    }),
  )

  it.effect(
    'validates native stop text and rejects truncation, tools, reasoning-only and contradictory finishes',
    () =>
      Effect.gen(function* () {
        const content: Model.DeferredResponse['content'] = [
          Response.makePart('reasoning', { text: 'private' }),
          Response.makePart('text', { text: '  summary' }),
          Response.makePart('text', { text: 'next  ' }),
        ]
        assert.strictEqual(
          yield* Compaction.validateSummary({ finishReason: 'stop', content }),
          'summary\nnext',
        )
        for (const finishReason of [
          'length',
          'content-filter',
          'tool-calls',
          'error',
          'pause',
          'other',
          'unknown',
        ] as const)
          assert.isTrue(
            Result.isFailure(
              yield* Effect.result(Compaction.validateSummary({ finishReason, content })),
            ),
          )
        const invalid: ReadonlyArray<Model.DeferredResponse['content']> = [
          [],
          [Response.makePart('text', { text: '   ' })],
          [Response.makePart('reasoning', { text: 'not a summary' })],
          [
            ...content,
            Response.makePart('tool-call', {
              id: 'a',
              name: 'lookup',
              params: {},
              providerExecuted: false,
              metadata: {},
            }),
          ],
          [
            ...content,
            Response.makePart('tool-result', {
              id: 'a',
              name: 'lookup',
              result: 'data',
              encodedResult: 'data',
              preliminary: false,
              isFailure: false,
              providerExecuted: false,
              metadata: {},
            }),
          ],
          [
            ...content,
            Response.makePart('tool-approval-request', {
              approvalId: 'approval',
              toolCallId: 'a',
              metadata: {},
            }),
          ],
          [
            ...content,
            Response.makePart('finish', {
              reason: 'length',
              usage: {
                inputTokens: {
                  uncached: undefined,
                  total: undefined,
                  cacheRead: undefined,
                  cacheWrite: undefined,
                },
                outputTokens: { total: undefined, text: undefined, reasoning: undefined },
              },
            }),
          ],
        ]
        for (const parts of invalid)
          assert.isTrue(
            Result.isFailure(
              yield* Effect.result(
                Compaction.validateSummary({ finishReason: 'stop', content: parts }),
              ),
            ),
          )
      }),
  )

  it.effect(
    'bounded selection and pinned requests survive parent edits, fork inheritance and storage reopen without writes',
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
              const [first, tail] = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.ensureRoot(tx)
                  const first = yield* Transaction.appendEntry(tx, root, {
                    kind: 'input',
                    model: [user('old prefix')],
                  })
                  const tail = yield* Transaction.appendEntry(tx, root, {
                    kind: 'input',
                    model: [user(text(20))],
                  })
                  return [first, tail] as const
                }),
              )
              const before = writes
              const view = yield* Projection.read(session, root, { at: tail.id })
              const cut = selected(yield* Compaction.select(view, policy, manual))
              const request = yield* Compaction.pinRequest(cut, config)
              const encoded = yield* Schema.encodeEffect(
                Schema.fromJsonString(Compaction.SummaryRequestSchema),
              )(request)
              assert.strictEqual(writes, before)
              const child = yield* Session.commit(session, (tx) =>
                Transaction.forkConversation(tx, root, tail.id, {
                  ownership: { _tag: 'ownerless' },
                }),
              )
              yield* Session.commit(session, (tx) =>
                Transaction.appendEntry(tx, root, {
                  kind: 'reset',
                  head: 'self',
                  model: [user('new context')],
                  edits: [{ _tag: 'omit', target: first.id }],
                }),
              )
              const currentWrites = writes
              assert.deepEqual(yield* Projection.read(session, root, { at: tail.id }), view)
              const inherited = selected(
                yield* Compaction.select(yield* Projection.read(session, child.id), policy, manual),
              )
              assert.strictEqual(inherited.firstKept, cut.firstKept)
              assert.strictEqual(inherited.tail, cut.tail)
              assert.deepEqual(inherited.source, cut.source)
              assert.strictEqual(writes, currentWrites)
              return { view, request, child: child.id, encoded }
            }),
          )
          const reopened = yield* Session.make().pipe(
            Effect.provideService(Storage.Storage, monitored),
          )
          const currentWrites = writes
          const recovered = yield* Schema.decodeEffect(
            Schema.fromJsonString(Compaction.SummaryRequestSchema),
          )(saved.encoded)
          assert.deepEqual(recovered, saved.request)
          const bounded = yield* Projection.read(reopened, root, {
            at: recovered.selection.tail,
            legacyInterpretation: recovered.selection.legacyInterpretation,
          })
          assert.deepEqual(bounded, saved.view)
          assert.deepEqual(
            selected(yield* Compaction.select(bounded, recovered.selection.policy, manual)),
            recovered.selection,
          )
          assert.deepEqual(
            (yield* Projection.read(reopened, saved.child)).prompt,
            saved.view.prompt,
          )
          assert.strictEqual(writes, currentWrites)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
