import * as Schema from 'effect/Schema'
import { vi } from 'vitest'
vi.mock('effect/ai/LanguageModel', { spy: true })
import * as LanguageModel from 'effect/ai/LanguageModel'
import { assert, describe, it } from '@effect/vitest'
import * as Model from 'effect-harness/Model'
import * as Usage from 'effect-harness/Usage'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Sink from 'effect/Sink'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Cli from 'effect-harness/provider-claude-code/Cli'
import * as Catalog from 'effect-harness/provider-claude-code/Catalog'
import * as IntentServer from 'effect-harness/provider-claude-code/IntentServer'
import * as RequestOptions from 'effect-harness/provider-claude-code/RequestOptions'

const sessionId = '019a08e0-7c00-7000-8000-000000000001'
const entry: Catalog.Entry = {
  modelId: 'claude-sonnet-4-6',
  contextWindow: 200000,
  maxOutputTokens: 32000,
  efforts: ['low', 'medium', 'high'],
  supportsThinkingOff: true,
}
const fixture = (options?: {
  readonly historyMode?: 'transcript'
  readonly trusted?: boolean
  readonly models?: ReadonlyArray<Catalog.Entry>
}) => {
  const commands: Array<ChildProcess.StandardCommand> = []
  const inputs: Array<string> = []
  let released = 0
  const spawner = ChildProcessSpawner.make(
    Effect.fnUntraced(function* (command) {
      if (!ChildProcess.isStandardCommand(command)) return yield* Effect.die('unexpected pipeline')
      commands.push(command)
      const isStatus = command.args[0] === 'auth'
      if (Stream.isStream(command.options.stdin))
        inputs.push(yield* command.options.stdin.pipe(Stream.decodeText, Stream.mkString))
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          released++
        }),
      )
      const events = [
        { type: 'system', subtype: 'init', tools: [] },
        {
          type: 'assistant',
          message: {
            id: 'msg-1',
            model: entry.modelId,
            content: [{ type: 'text', text: 'Hello' }],
            usage: { input_tokens: 5, output_tokens: 2 },
            stop_reason: 'end_turn',
          },
        },
        {
          type: 'result',
          subtype: 'success',
          is_error: false,
          num_turns: 1,
          usage: { input_tokens: 5, output_tokens: 2 },
          total_cost_usd: 0.03,
        },
      ]
      const text = isStatus
        ? '{"loggedIn":true,"authMethod":"claude.ai","apiProvider":"firstParty"}'
        : events.map((event) => JSON.stringify(event)).join('\n') + '\n'
      const stdout = Stream.succeed(new TextEncoder().encode(text))
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(123),
        stdin: Sink.drain,
        stdout,
        stderr: Stream.empty,
        all: stdout,
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      })
    }),
  )
  const transport = Cli.layer({
    executable: '/fake/claude',
    ...(options?.trusted === false ? {} : { policyTrust: 'trusted-installed-cli' }),
  }).pipe(Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)))
  const layer = Catalog.layer({
    models: options?.models ?? [entry],
    historyMode: options?.historyMode,
  }).pipe(Layer.provideMerge(Layer.merge(transport, IntentServer.layerDisabled)))
  return {
    commands,
    inputs,
    layer,
    get released() {
      return released
    },
  }
}
const resolve = () =>
  Model.Catalog.use((catalog) =>
    catalog.resolve({ provider: 'claude-code', modelId: entry.modelId }),
  )
const argument = (command: ChildProcess.StandardCommand, name: string) =>
  command.args[command.args.indexOf(name) + 1]

describe('Catalog', () => {
  it.effect('empty malformed subjects fail semantically before any native model work', () =>
    Effect.gen(function* () {
      // Deliberately model an untyped JavaScript caller that supplies a malformed subject.
      const invalid: unknown = {}
      const result = Catalog.descriptor(invalid as Catalog.Entry)
      assert.isTrue(Effect.isEffect(result))
      const error = yield* result.pipe(Effect.provide(fixture().layer), Effect.flip)
      assert.strictEqual(error._tag, 'ModelError')
      assert.strictEqual(error.reason._tag, 'ModelUnsupported')
    }),
  )

  describe('installed CLI native catalogue', () => {
    it.effect(
      'translates request controls into actual public flags/environment/settings and structured stdin',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const descriptor = yield* resolve()
            const context = yield* descriptor.configure({
              thinking: 'high',
              options: {},
              sessionId,
              maxTokens: 4000,
              cache: 'long',
            })
            const response = yield* descriptor.model
              .generateText({ prompt: 'Hi' })
              .pipe(Effect.provideContext(context))
            assert.strictEqual(response.text, 'Hello')
            const command = f.commands[1]
            if (command === undefined) return yield* Effect.die('No inference process')
            assert.strictEqual(argument(command, '--model'), entry.modelId)
            assert.strictEqual(argument(command, '--session-id'), sessionId)
            assert.strictEqual(argument(command, '--effort'), 'high')
            assert.strictEqual(command.options.env?.CLAUDE_CODE_MAX_OUTPUT_TOKENS, '4000')
            assert.strictEqual(command.options.env?.CLAUDE_CODE_PROMPT_CACHE_TTL, '1h')
            assert.strictEqual(command.options.env?.DISABLE_PROMPT_CACHING, undefined)
            assert.strictEqual(command.options.env?.MAX_THINKING_TOKENS, undefined)
            assert.deepStrictEqual(JSON.parse(argument(command, '--settings') ?? '{}'), {
              disableAllHooks: true,
              autoMemoryEnabled: false,
              alwaysThinkingEnabled: true,
              autoCompactEnabled: false,
            })
            const input = SchemaJson.parse(f.inputs[0] ?? '{}')
            assert.strictEqual(input.session_id, sessionId)
            assert.strictEqual(f.released, 2)
            const finish = response.content.find((part) => part.type === 'finish')
            const usage = descriptor.usage?.(response.usage, finish?.metadata ?? {})
            assert.strictEqual(usage?.cost.total, 0.03)
            assert.strictEqual(usage?.cost.known, false)
            assert.strictEqual(usage?.cost.totalKnown, true)
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect(
      'off and disabled/short cache use public controls, not private bearer or affinity headers',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const descriptor = yield* resolve()
            const off = yield* descriptor.configure({
              thinking: 'off',
              options: {},
              sessionId,
              cache: 'none',
            })
            yield* descriptor.model.generateText({ prompt: 'Hi' }).pipe(Effect.provideContext(off))
            const command = f.commands[1]
            if (command === undefined) return yield* Effect.die('No inference command')
            assert.strictEqual(command.options.env?.MAX_THINKING_TOKENS, '0')
            assert.strictEqual(command.options.env?.DISABLE_PROMPT_CACHING, '1')
            assert.strictEqual(command.options.env?.CLAUDE_CODE_PROMPT_CACHE_TTL, undefined)
            assert.strictEqual(command.options.env?.CLAUDE_CODE_OAUTH_TOKEN, undefined)
            const short = yield* descriptor.configure({
              thinking: 'default',
              options: { effort: 'low' },
              sessionId,
              cache: 'short',
            })
            yield* descriptor.model
              .generateText({ prompt: 'Hi' })
              .pipe(Effect.provideContext(short))
            const second = f.commands[3]
            if (second === undefined) return yield* Effect.die('No second command')
            assert.strictEqual(second.options.env?.CLAUDE_CODE_PROMPT_CACHE_TTL, '5m')
            assert.strictEqual(argument(second, '--effort'), 'low')
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect(
      'unsupported options, limits, model redirects, and undeclared thinking fail explicitly',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const descriptor = yield* resolve()
            const negatives: ReadonlyArray<Model.RequestOptions> = [
              { thinking: 'high', options: { headers: { session_id: sessionId } } },
              { thinking: 'off', options: { effort: 'low' } },
              { thinking: 'high', options: { effort: 'low' } },
              { thinking: 'xhigh', options: {} },
              { thinking: 'default', options: { max_tokens: 1 } },
              { thinking: 'default', options: {}, maxTokens: 32001 },
              { thinking: 'default', options: {}, maxTokens: 1.5 },
              { thinking: 'default', options: {}, sessionId: 'not-a-uuid' },
            ]
            for (const request of negatives)
              assert.strictEqual(
                (yield* descriptor.configure(request).pipe(Effect.flip)).reason._tag,
                'ModelUnsupported',
              )
            const modern = yield* Catalog.descriptor({ ...entry, supportsThinkingOff: false })
            assert.strictEqual(
              (yield* modern
                .configure({ thinking: 'off', options: {}, sessionId })
                .pipe(Effect.flip)).reason._tag,
              'ModelUnsupported',
            )
            const wrong = Context.make(RequestOptions.Current, { model: 'different' })
            assert.strictEqual(
              (yield* descriptor.model
                .generateText({ prompt: 'Hi' })
                .pipe(Effect.provideContext(wrong), Effect.flip)).reason._tag,
              'InvalidRequestError',
            )
            assert.strictEqual(f.commands.length, 0)
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect('policy and transcript opt-ins remain explicit capabilities', () =>
      Effect.gen(function* () {
        const f = fixture({ trusted: false })
        const prompt = Prompt.fromMessages([
          Prompt.userMessage({ content: [Prompt.makePart('text', { text: 'First' })] }),
          Prompt.assistantMessage({ content: [Prompt.makePart('text', { text: 'Prior' })] }),
          Prompt.userMessage({ content: [Prompt.makePart('text', { text: 'Continue' })] }),
        ])
        return yield* Effect.gen(function* () {
          const descriptor = yield* resolve()
          const context = yield* descriptor.configure({
            thinking: 'default',
            options: {},
            sessionId,
          })
          assert.strictEqual(
            (yield* descriptor.model
              .generateText({ prompt: 'Hi' })
              .pipe(Effect.provideContext(context), Effect.flip)).reason._tag,
            'InvalidRequestError',
          )
          assert.strictEqual(f.commands.length, 0)
          assert.strictEqual(
            (yield* descriptor.model
              .generateText({ prompt })
              .pipe(Effect.provideContext(context), Effect.flip)).reason._tag,
            'InvalidRequestError',
          )
        }).pipe(Effect.provide(f.layer))
      }),
    )
    it.effect('explicit transcript opt-in keeps canonical history and the same UUID7 pin', () =>
      Effect.gen(function* () {
        const f = fixture({ historyMode: 'transcript' })
        return yield* Effect.gen(function* () {
          const descriptor = yield* resolve()
          const context = yield* descriptor.configure({
            thinking: 'default',
            options: {},
            sessionId,
          })
          const prompt = Prompt.fromMessages([
            Prompt.userMessage({ content: [Prompt.makePart('text', { text: 'First' })] }),
            Prompt.assistantMessage({ content: [Prompt.makePart('text', { text: 'Prior' })] }),
            Prompt.userMessage({ content: [Prompt.makePart('text', { text: 'Continue' })] }),
          ])
          yield* descriptor.model.generateText({ prompt }).pipe(Effect.provideContext(context))
          assert.include(f.inputs[0] ?? '', 'effect-harness-transcript/1')
          assert.include(f.inputs[0] ?? '', 'Prior')
          assert.include(f.inputs[0] ?? '', sessionId)
        }).pipe(Effect.provide(f.layer))
      }),
    )
    it.effect(
      'intercepted-tool/missing USD remains unknown and unknown state propagates through aggregation',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const descriptor = yield* resolve()
            const usage = new Response.Usage({
              inputTokens: { uncached: 2, total: 2 },
              outputTokens: { total: 1 },
            })
            const partial = descriptor.usage?.(usage, {
              claudeCode: { interruptedAfterToolIntent: true, costUnavailable: true },
            })
            assert.strictEqual(partial?.cost.known, false)
            assert.strictEqual(partial?.cost.totalKnown, false)
            const total = descriptor.usage?.(usage, { claudeCode: { totalCostUsd: 0.4 } })
            assert.strictEqual(total?.cost.totalKnown, true)
            const combined = Usage.add(total ?? Usage.zero(), partial ?? Usage.zero())
            assert.strictEqual(combined.cost.total, 0.4)
            assert.strictEqual(combined.cost.totalKnown, false)
          }).pipe(Effect.provide(f.layer))
        }),
    )
  })
  const SchemaJson = {
    parse: (text: string) => Schema.decodeUnknownSync(Schema.JsonObject)(JSON.parse(text)),
  }

  describe('catalogue schema admission', () => {
    it.effect(
      'rejects malformed declared prices and limits with original SchemaError cause before client work',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const constructions = vi.mocked(LanguageModel.make).mock.calls.length
            const invalid: ReadonlyArray<Catalog.Entry> = [
              { ...entry, modelId: '' },
              { ...entry, contextWindow: Number.MAX_SAFE_INTEGER + 1 },
              { ...entry, maxOutputTokens: 0 },
              { ...entry, maxOutputTokens: entry.contextWindow + 1 },
            ]
            for (const value of invalid) {
              const error = yield* Catalog.descriptor(value).pipe(Effect.flip)
              assert.strictEqual(error.reason._tag, 'ModelUnsupported')
              assert.isTrue(Schema.isSchemaError(error.cause))
            }
            assert.strictEqual(vi.mocked(LanguageModel.make).mock.calls.length, constructions)
            assert.strictEqual(f.commands.length, 0)
          }).pipe(Effect.provide(f.layer))
        }),
    )
  })
})
