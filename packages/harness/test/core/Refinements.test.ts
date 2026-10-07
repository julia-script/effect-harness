import { assert, describe, it } from '@effect/vitest'
import * as DateTime from 'effect/DateTime'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as Agent from '../../src/Agent.ts'
import * as Context from '../../src/Context.ts'
import * as Env from '../../src/Env.ts'
import * as Executor from '../../src/Executor.ts'
import * as Invocation from '../../src/Invocation.ts'
import * as Model from '../../src/Model.ts'
import * as Output from '../../src/Output.ts'
import * as Progress from '../../src/Progress.ts'
import * as Tool from '../../src/Tool.ts'
import * as ToolResult from '../../src/ToolResult.ts'
import * as Usage from '../../src/Usage.ts'
import * as Decode from '../../src/env/Decode.ts'
import * as Scanner from '../../src/env/LineScan.ts'
import * as Bash from '../../src/tools/Bash.ts'
import * as Edit from '../../src/tools/Edit.ts'
import * as Read from '../../src/tools/Read.ts'
import * as Write from '../../src/tools/Write.ts'

const hasMarker = (value: object): boolean =>
  Object.getOwnPropertyNames(value).some((name) => name.startsWith('~@effect-harness/harness/'))

describe('owned nominal capabilities and decoded data refinements', () => {
  it.effect(
    'factory handles accept frozen configs, retain live getters and never mark caller data',
    () =>
      Effect.gen(function* () {
        const line = Object.freeze({ readLine: Effect.succeed(Option.none<Env.TextLine>()) })
        const lineReader = Env.makeTextLineReader(line)
        assert.isFalse(Object.is(lineReader, line))
        assert.isTrue(Env.isTextLineReader(lineReader))
        assert.isFalse(Env.isTextLineReader(line))
        assert.isTrue(Option.isNone(yield* lineReader.readLine))
        const info = Effect.succeed<Env.FileInfo>({
          path: 'owned',
          name: 'owned',
          kind: 'file',
          size: 0,
          mtimeMs: DateTime.fromEpochSeconds(0),
        })
        const binaryInput = Object.freeze({
          info,
          read: () => Effect.succeed(new Uint8Array()),
          scanLines: () =>
            Effect.fail(new Env.FileError({ reason: new Env.FileInvalid({ message: 'fixture' }) })),
        })
        const binary = Env.makeBinaryReader(binaryInput)
        assert.isFalse(Object.is(binary, binaryInput))
        assert.isTrue(Env.isBinaryReader(binary))
        assert.isFalse(Env.isBinaryReader(binaryInput))
        assert.strictEqual(binary.info, info)
        const directoryInput = Object.freeze({
          next: () => Effect.succeed({ entries: [], done: true }),
        })
        const directory = Env.makeDirReader(directoryInput)
        assert.isTrue(Env.isDirReader(directory))
        assert.isFalse(Env.isDirReader(directoryInput))
        assert.deepStrictEqual(yield* directory.next(1), { entries: [], done: true })
        let mode: 'native' | 'polling' = 'native'
        let modeReads = 0
        const watchInput = Object.freeze({
          get mode() {
            modeReads++
            return mode
          },
          changes: Stream.empty,
        })
        const watcher = Env.makeWatcher(watchInput)
        assert.isFalse(Object.is(watcher, watchInput))
        assert.strictEqual(modeReads, 0)
        assert.strictEqual(watcher.mode, 'native')
        mode = 'polling'
        assert.strictEqual(watcher.mode, 'polling')
        assert.isTrue(Env.isWatcher(watcher))
        assert.isFalse(Env.isWatcher(watchInput))
        assert.deepStrictEqual(Object.keys(watcher), Object.keys(watchInput))
        assert.isFalse(Env.isWatcher({ ...watcher }))
        assert.isTrue(Env.isWatcher(Env.makeWatcher({ ...watcher })))
        for (const plain of [line, binaryInput, directoryInput, watchInput])
          assert.isFalse(hasMarker(plain))
        const progressInput = Object.freeze({
          mark: Effect.void,
          markAndWait: Effect.void,
          stop: Effect.succeed([]),
        })
        const progress = Progress.makeProgress(progressInput)
        assert.isFalse(Object.is(progress, progressInput))
        assert.isTrue(Progress.isProgress(progress))
        assert.isFalse(Progress.isProgress(progressInput))
        const output = Output.make()
        const outputInput = Object.freeze({ ...output })
        assert.isFalse(Output.isBuffer(outputInput))
        const copied = Output.makeBuffer(outputInput)
        assert.isFalse(Object.is(copied, outputInput))
        assert.isTrue(Output.isBuffer(copied))
        assert.deepStrictEqual(Object.keys(copied), Object.keys(outputInput))
        yield* Output.push(copied, 'fresh')
        assert.strictEqual(Output.snapshot(copied).text, 'fresh')
        assert.strictEqual(outputInput.totalBytes, 0)
        assert.deepStrictEqual(outputInput.chunks, [])
        assert.isTrue(Decode.isDecoder(Decode.make()))
        assert.isFalse(Decode.isDecoder({ decoder: new TextDecoder(), started: false }))
        assert.isTrue(Scanner.isState(Result.getOrThrow(Scanner.make(0))))
        for (const input of [null, undefined, 0, {}, 'handle']) {
          assert.isFalse(Env.isBinaryReader(input))
          assert.isFalse(Env.isTextLineReader(input))
          assert.isFalse(Env.isDirReader(input))
          assert.isFalse(Env.isWatcher(input))
          assert.isFalse(Output.isBuffer(input))
          assert.isFalse(Progress.isProgress(input))
          assert.isFalse(Decode.isDecoder(input))
          assert.isFalse(Scanner.isState(input))
        }
      }),
  )
  it.effect(
    'getter-backed output chunks remain owned while configuration accessors stay live',
    () =>
      Effect.gen(function* () {
        const callerChunks: Output.Buffer['chunks'] = []
        let limits: Output.OutputLimits = { ...Output.defaults }
        const input = Object.freeze({
          ...Output.make(),
          get chunks() {
            return callerChunks
          },
          get limits() {
            return limits
          },
        })
        const buffer = Output.makeBuffer(input)
        assert.isFalse(Object.is(buffer.chunks, callerChunks))
        assert.strictEqual(Object.getOwnPropertyDescriptor(buffer, 'chunks')?.writable, true)
        limits = { ...Output.defaults, maxBytes: 5 }
        assert.strictEqual(buffer.limits, limits)
        yield* Output.push(buffer, 'freshtext')
        assert.strictEqual(Output.snapshot(buffer).text, 'fresh')
        assert.deepStrictEqual(callerChunks, [])
        assert.strictEqual(input.totalBytes, 0)
      }),
  )
  it.effect(
    'data aliases validate decoded native values and do not gain owned runtime markers',
    () =>
      Effect.gen(function* () {
        const result = { content: [Prompt.textPart({ text: 'native' })] }
        assert.isTrue(Invocation.isToolResult(result))
        assert.isFalse(Invocation.isToolResult({ content: [{ type: 'text', text: 'wire' }] }))
        assert.isTrue(Output.isOutputLimits(Output.defaults))
        assert.isFalse(Output.isOutputLimits({ maxBytes: '10', maxLines: 10, retain: 'head' }))
        const decision = yield* Schema.decodeEffect(Model.DeferredDecision)({
          handle: 'job',
          pollAfterMs: 1.5,
        })
        assert.isTrue(Model.isDeferredDecision(decision))
        assert.isFalse(Model.isDeferredDecision({ handle: 'job', pollAfterMs: 1.5 }))
        assert.isTrue(Agent.isSettings(Agent.defaultSettings))
        assert.isFalse(
          Agent.isSettings(yield* Schema.encodeEffect(Agent.Settings)(Agent.defaultSettings)),
        )
        const valid: ReadonlyArray<readonly [(input: unknown) => boolean, object]> = [
          [Agent.isModelRef, { provider: 'owned', modelId: 'model' }],
          [Agent.isSelectionEdit, { remove: [] }],
          [Agent.isSelection, ['one']],
          [Agent.isToolSelection, ['read']],
          [Agent.isState, {}],
          [Context.isEdit, { target: 1, action: 'omit' }],
          [Executor.isSummary, { summary: 'owned', usage: Usage.zero() }],
          [Invocation.isDiagnostic, { kind: 'owned' }],
          [Invocation.isControl, { terminate: true }],
          [Model.isRequestOptions, { thinking: 'off', options: {} }],
          [Tool.isIntent, { id: 'c', name: 'read', args: {}, replay: 'safe' }],
          [Tool.isExecution, { outcome: 'completed', result }],
          [Usage.isCost, Usage.zero().cost],
          [Usage.isUsage, Usage.zero()],
          [Usage.isState, Usage.empty()],
          [Bash.isInput, { command: 'true' }],
          [Edit.isInput, { path: 'owned', edits: [] }],
          [Read.isInput, { path: 'owned' }],
          [Write.isInput, { path: 'owned', content: 'data' }],
        ]
        for (const [guard, value] of valid) {
          assert.isTrue(guard(value), JSON.stringify(value))
          assert.isFalse(guard(null))
          assert.isFalse(hasMarker(value))
        }
        const envelope = yield* ToolResult.decode(yield* ToolResult.encode(result))
        assert.isTrue(ToolResult.isEnvelope(envelope))
        assert.isFalse(ToolResult.isEnvelope({ content: [] }))
        assert.isFalse(hasMarker(envelope))
      }),
  )
})
