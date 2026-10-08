import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Schema from 'effect/Schema'
import type * as Result from 'effect/Result'
import type * as Option from 'effect/Option'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Tool from 'effect/ai/Tool'
import type * as Prompt from 'effect/ai/Prompt'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as Hook from 'effect-harness/Hook'
import type * as Invocation from 'effect-harness/Invocation'
import * as Transcript from 'effect-harness/Transcript'
import * as Compaction from 'effect-harness/Compaction'
import * as Output from 'effect-harness/Output'
import * as Decode from 'effect-harness/env/Decode'
import * as LineScan from 'effect-harness/env/LineScan'
import * as EditDiff from 'effect-harness/tools/EditDiff'
import * as FileError from 'effect-harness/FileError'
import * as ExecutionError from 'effect-harness/ExecutionError'
import type { ToolError } from 'effect-harness/ToolError'
import type { OutputError } from 'effect-harness/OutputError'
import * as Harness from 'effect-harness'
import * as Agent from 'effect-harness/Agent'

class Host extends Context.Service<Host, string>()('typetest/Cleanup/Host') {}
declare const registration: ToolRegistration.Registration
declare const prompt: Prompt.Prompt
const native = Tool.make('echo', { parameters: Schema.String, success: Schema.String })
const toolkit = Toolkit.make(native)
const handlers: Hook.Handlers<Host | Invocation.Invocation> = {
  beforeRequest: () => Effect.map(Host, () => prompt),
}

test('both dual forms infer native handler and captured host requirements from emitted declarations', () => {
  expect(ToolRegistration.bind(toolkit)).type.toBe<
    Effect.Effect<
      Array<ToolRegistration.Registration>,
      never,
      Tool.HandlersFor<{ echo: typeof native }>
    >
  >()
  expect(ToolRegistration.bind({ echo: { replay: 'safe' } })(toolkit)).type.toBe<
    Effect.Effect<
      Array<ToolRegistration.Registration>,
      never,
      Tool.HandlersFor<{ echo: typeof native }>
    >
  >()
  expect(ToolRegistration.makeIntent(registration, { id: 'call', decoded: 'value' })).type.toBe<
    Effect.Effect<ToolRegistration.Intent, ToolError>
  >()
  expect(ToolRegistration.makeIntent({ id: 'call', decoded: 'value' })(registration)).type.toBe<
    Effect.Effect<ToolRegistration.Intent, ToolError>
  >()
  expect(Hook.bind(handlers)).type.toBe<Effect.Effect<Hook.Handlers, never, Host>>()
  expect(Hook.bind([])(handlers)).type.toBe<Effect.Effect<Hook.Handlers, never, Host>>()
  expect(ToolRegistration.makeIntent).type.not.toBeCallableWith(registration, 'call', 'value')
  expect(ToolRegistration.bind).type.not.toBeCallableWith(toolkit, {
    echo: { replay: 'sometimes' },
  })
})
test('safe conversion and Unsafe twins retain distinct failure channels and readonly inputs', () => {
  expect(Decode.decode(Decode.make(), new Uint8Array())).type.toBe<
    Effect.Effect<string, FileError.FileError>
  >()
  expect(Decode.decodeUnsafe(Decode.make())).type.toBe<string>()
  expect(LineScan.make(0, { endLine: 1 })).type.toBe<
    Result.Result<LineScan.State, FileError.FileError>
  >()
  expect(LineScan.makeUnsafe(0)).type.toBe<LineScan.State>()
  expect(Output.end(Output.make())).type.toBe<Effect.Effect<void, OutputError>>()
  expect(Output.snapshot(Output.make())).type.toBe<
    Effect.Effect<Output.BoundedOutput, OutputError>
  >()
  expect(Output.snapshotUnsafe(Output.make())).type.toBe<Output.BoundedOutput>()
  const edits: ReadonlyArray<EditDiff.Edit> = Object.freeze([{ oldText: 'a', newText: 'b' }])
  expect(EditDiff.applyEditsToNormalizedContent('a', edits, 'file')).type.toBe<
    Result.Result<EditDiff.applyEditsToNormalizedContent.Result, EditDiff.EditError>
  >()
  expect(EditDiff.applyEditsToNormalizedContent(edits, 'file')('a')).type.toBe<
    Result.Result<EditDiff.applyEditsToNormalizedContent.Result, EditDiff.EditError>
  >()
  expect(
    EditDiff.applyEditsToNormalizedContentUnsafe(edits, 'file')('a'),
  ).type.toBe<EditDiff.applyEditsToNormalizedContent.Result>()
  expect(
    EditDiff.applyEditsToNormalizedContentUnsafe('a', edits, 'file'),
  ).type.toBe<EditDiff.applyEditsToNormalizedContent.Result>()
  expect(LineScan.make).type.not.toBeCallableWith(0, 1)
  expect(Output.delta).type.not.toBeCallableWith('a', false)
})
test('optional dual tails and canonical error namespaces preserve the public domain types', () => {
  expect(Compaction.selectCut(Transcript.make(), 1, () => 1)).type.toBe<Option.Option<number>>()
  expect(Compaction.selectCut(1, () => 1)(Transcript.make())).type.toBe<Option.Option<number>>()
  expect(Compaction.summarizedMessages(Transcript.make(), 1)).type.toBe<Array<Prompt.Message>>()
  expect(Compaction.summarizedMessages(1)(Transcript.make())).type.toBe<Array<Prompt.Message>>()
  expect(Output.delta('a', 'b', 32)).type.toBe<Output.Delta>()
  expect(Output.delta('b', 32)('a')).type.toBe<Output.Delta>()
  expect(Harness.FileError.FileError).type.toBe<typeof FileError.FileError>()
  expect(Harness.ExecutionError.ExecutionError).type.toBe<typeof ExecutionError.ExecutionError>()
  expect(FileError.FileError).type.toBe<typeof FileError.FileError>()
  expect(ExecutionError.ExecutionError).type.toBe<typeof ExecutionError.ExecutionError>()
})

test('selection exposes newly allocated mutable output in both dual forms', () => {
  const state: Agent.Selection | undefined = undefined
  expect(Agent.select(state, ['echo'])).type.toBe<Array<string>>()
  expect(Agent.select(['echo'])(state)).type.toBe<Array<string>>()
})
