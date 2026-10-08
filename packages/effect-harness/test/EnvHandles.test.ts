import { assertNone } from '@effect/vitest/utils'
import { assert, describe, it } from '@effect/vitest'

import * as DateTime from 'effect/DateTime'

import * as Effect from 'effect/Effect'

import * as Option from 'effect/Option'

import * as Result from 'effect/Result'

import * as Schema from 'effect/Schema'

import * as Stream from 'effect/Stream'

import * as Prompt from 'effect/ai/Prompt'

import * as Agent from 'effect-harness/Agent'

import * as Transcript from 'effect-harness/Transcript'

import * as Env from 'effect-harness/Env'

import * as Executor from 'effect-harness/Executor'

import * as Invocation from 'effect-harness/Invocation'

import * as Model from 'effect-harness/Model'

import * as Output from 'effect-harness/Output'

import * as Progress from 'effect-harness/Progress'

import * as ToolRegistration from 'effect-harness/ToolRegistration'

import * as ToolResult from 'effect-harness/ToolResult'

import * as Usage from 'effect-harness/Usage'

import * as Decode from 'effect-harness/env/Decode'

import * as LineScan from 'effect-harness/env/LineScan'

import * as Bash from 'effect-harness/tools/Bash'

import * as Edit from 'effect-harness/tools/Edit'

import * as Read from 'effect-harness/tools/Read'

import * as Write from 'effect-harness/tools/Write'

describe('EnvHandles', () => {
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
          assertNone(yield* lineReader.readLine)
          const info = Effect.succeed<NativeFiles.FileInfo>({
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
              Effect.fail(
                new FileError.FileError({
                  reason: new FileError.FileInvalidError({ message: 'fixture' }),
                }),
              ),
          })
          const binary = NativeFiles.makeBinaryReader(binaryInput)
          assert.isFalse(Object.is(binary, binaryInput))
          assert.isTrue(NativeFiles.isBinaryReader(binary))
          assert.isFalse(NativeFiles.isBinaryReader(binaryInput))
          assert.strictEqual(binary.info, info)
          const directoryInput = Object.freeze({
            next: () => Effect.succeed({ entries: [], done: true }),
          })
          const directory = NativeFiles.makeDirReader(directoryInput)
          assert.isTrue(NativeFiles.isDirReader(directory))
          assert.isFalse(NativeFiles.isDirReader(directoryInput))
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
          assert.strictEqual(Output.snapshotUnsafe(copied).text, 'fresh')
          assert.strictEqual(outputInput.totalBytes, 0)
          assert.deepStrictEqual(outputInput.chunks, [])
          assert.isTrue(Decode.isDecoder(Decode.make()))
          assert.isFalse(Decode.isDecoder({ decoder: new TextDecoder(), started: false }))
          assert.isTrue(LineScan.isState(Result.getOrThrow(LineScan.make(0))))
          for (const input of [null, undefined, 0, {}, 'handle']) {
            assert.isFalse(NativeFiles.isBinaryReader(input))
            assert.isFalse(Env.isTextLineReader(input))
            assert.isFalse(NativeFiles.isDirReader(input))
            assert.isFalse(Env.isWatcher(input))
            assert.isFalse(Output.isBuffer(input))
            assert.isFalse(Progress.isProgress(input))
            assert.isFalse(Decode.isDecoder(input))
            assert.isFalse(LineScan.isState(input))
          }
        }),
    )
    it.effect(
      'getter-backed output chunks remain owned while configuration accessors stay live',
      () =>
        Effect.gen(function* () {
          const callerChunks: Output.Buffer['chunks'] = []
          let limits: Output.Limits = { ...Output.defaults }
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
          assert.strictEqual(Output.snapshotUnsafe(buffer).text, 'fresh')
          assert.deepStrictEqual(callerChunks, [])
          assert.strictEqual(input.totalBytes, 0)
        }),
    )
    it.effect(
      'data aliases validate decoded native values and do not gain owned runtime markers',
      () =>
        Effect.gen(function* () {
          const result = { content: [Prompt.textPart({ text: 'native' })] }
          assert.isTrue(Invocation.isResult(result))
          assert.isFalse(Invocation.isResult({ content: [{ type: 'text', text: 'wire' }] }))
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
            [Transcript.isEdit, { target: 1, _tag: 'omit' }],
            [Executor.isSummary, { summary: 'owned', usage: Usage.make() }],
            [Invocation.isDiagnostic, { kind: 'owned' }],
            [Invocation.isControl, { terminate: true }],
            [Model.isRequestOptions, { thinking: 'off', options: {} }],
            [ToolRegistration.isIntent, { id: 'c', name: 'read', args: {}, replay: 'safe' }],
            [ToolRegistration.isExecution, { outcome: 'completed', result }],
            [Usage.isCost, Usage.make().cost],
            [Usage.isUsage, Usage.make()],
            [Usage.isState, Usage.makeState()],
            [Bash.isParameters, { command: 'true' }],
            [Edit.isParameters, { path: 'owned', edits: [] }],
            [Read.isParameters, { path: 'owned' }],
            [Write.isParameters, { path: 'owned', content: 'data' }],
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
})

import * as NativeFiles from 'effect-harness/NativeFiles'
import * as FileError from 'effect-harness/FileError'
