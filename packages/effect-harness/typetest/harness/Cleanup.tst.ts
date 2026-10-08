import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Schema from 'effect/Schema'
import type * as Result from 'effect/Result'
import type * as Option from 'effect/Option'
import * as Toolkit from 'effect/ai/Toolkit'
import * as AiTool from 'effect/ai/Tool'
// effect-review-allow P9-namespace-alias-equals-module: effect/ai/Tool and harness/Tool share Tool; AiTool names the native declaration constructor.
import type * as Prompt from 'effect/ai/Prompt'
import * as Tool from 'effect-harness/Tool'
import * as Hook from 'effect-harness/Hook'
import * as Invocation from 'effect-harness/Invocation'
import * as View from 'effect-harness/Context'
// effect-review-allow P9-namespace-alias-equals-module: effect/Context and harness/Context share Context; View names the committed transcript projection.
import * as Compaction from 'effect-harness/Compaction'
import * as Output from 'effect-harness/Output'
import * as Decode from 'effect-harness/env/Decode'
import * as LineScan from 'effect-harness/env/LineScan'
import * as EditDiff from 'effect-harness/tools/EditDiff'
import * as FileError from 'effect-harness/FileError'
import * as ExecutionError from 'effect-harness/ExecutionError'
import * as Env from 'effect-harness/Env'
import type { ToolError } from 'effect-harness/ToolError'
import type { OutputError } from 'effect-harness/OutputError'
import * as Harness from 'effect-harness'
import * as Agent from 'effect-harness/Agent'

class Host extends Context.Service<Host, string>()('typetest/Cleanup/Host') {}
declare const registration: Tool.Registration
declare const prompt: Prompt.Prompt
const native = AiTool.make('echo', { parameters: Schema.String, success: Schema.String })
const toolkit = Toolkit.make(native)
const handlers: Hook.Handlers<Host | Invocation.Invocation> = {
  beforeRequest: () => Effect.map(Host, () => prompt),
}

test('both dual forms infer native handler and captured host requirements from emitted declarations', () => {
  expect(Tool.bind(toolkit)).type.toBe<
    Effect.Effect<Array<Tool.Registration>, never, AiTool.HandlersFor<{ echo: typeof native }>>
  >()
  expect(Tool.bind({ echo: { replay: 'safe' } })(toolkit)).type.toBe<
    Effect.Effect<Array<Tool.Registration>, never, AiTool.HandlersFor<{ echo: typeof native }>>
  >()
  expect(Tool.makeIntent(registration, { id: 'call', decoded: 'value' })).type.toBe<
    Effect.Effect<Tool.Intent, ToolError>
  >()
  expect(Tool.makeIntent({ id: 'call', decoded: 'value' })(registration)).type.toBe<
    Effect.Effect<Tool.Intent, ToolError>
  >()
  expect(Hook.bind(handlers)).type.toBe<Effect.Effect<Hook.Handlers, never, Host>>()
  expect(Hook.bind([])(handlers)).type.toBe<Effect.Effect<Hook.Handlers, never, Host>>()
  expect(Tool.makeIntent).type.not.toBeCallableWith(registration, 'call', 'value')
  expect(Tool.bind).type.not.toBeCallableWith(toolkit, { echo: { replay: 'sometimes' } })
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
    Result.Result<EditDiff.AppliedEditsResult, EditDiff.EditError>
  >()
  expect(EditDiff.applyEditsToNormalizedContent(edits, 'file')('a')).type.toBe<
    Result.Result<EditDiff.AppliedEditsResult, EditDiff.EditError>
  >()
  expect(
    EditDiff.applyEditsToNormalizedContentUnsafe(edits, 'file')('a'),
  ).type.toBe<EditDiff.AppliedEditsResult>()
  expect(LineScan.make).type.not.toBeCallableWith(0, 1)
  expect(Output.delta).type.not.toBeCallableWith('a', false)
})
test('optional dual tails and canonical error namespaces preserve the public domain types', () => {
  expect(Compaction.selectCut(View.empty(), 1, () => 1)).type.toBe<Option.Option<number>>()
  expect(Compaction.selectCut(1, () => 1)(View.empty())).type.toBe<Option.Option<number>>()
  expect(Compaction.summarizedMessages(1)(View.empty())).type.toBe<Array<Prompt.Message>>()
  expect(Output.delta('a', 'b', 32)).type.toBe<Output.Delta>()
  expect(Output.delta('b', 32)('a')).type.toBe<Output.Delta>()
  expect(Harness.FileError.FileError).type.toBe<typeof Env.FileError>()
  expect(Harness.ExecutionError.ExecutionError).type.toBe<typeof Env.ExecutionError>()
  expect(FileError.FileError).type.toBe<typeof Env.FileError>()
  expect(ExecutionError.ExecutionError).type.toBe<typeof Env.ExecutionError>()
})

test('selection exposes newly allocated mutable output in both dual forms', () => {
  const state: Agent.Selection | undefined = undefined
  expect(Agent.select(state, ['echo'])).type.toBe<Array<string>>()
  expect(Agent.select(['echo'])(state)).type.toBe<Array<string>>()
})
