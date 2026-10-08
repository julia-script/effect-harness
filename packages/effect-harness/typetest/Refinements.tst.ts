import { expect, test } from 'tstyche'
import * as Agent from 'effect-harness/Agent'
import * as Transcript from 'effect-harness/Transcript'
import * as Executor from 'effect-harness/Executor'
import * as Invocation from 'effect-harness/Invocation'
import * as Model from 'effect-harness/Model'
import * as Output from 'effect-harness/Output'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Usage from 'effect-harness/Usage'
import * as Bash from 'effect-harness/tools/Bash'
import * as Edit from 'effect-harness/tools/Edit'
import * as Read from 'effect-harness/tools/Read'
import * as Write from 'effect-harness/tools/Write'
import * as ResponseAccumulator from 'effect-harness/ResponseAccumulator'
import type * as Option from 'effect/Option'
import type * as Prompt from 'effect/ai/Prompt'
declare const input: unknown

test('named unknown refinements use the actual public decoded codec, including aliases', () => {
  if (Agent.isModelRef(input)) expect(input).type.toBe<Agent.ModelRef>()
  if (Agent.isSelectionEdit(input)) expect(input).type.toBe<Agent.SelectionEdit>()
  if (Agent.isSelection(input)) expect(input).type.toBe<Agent.Selection>()
  if (Agent.isToolSelection(input)) expect(input).type.toBe<Agent.ToolSelection>()
  if (Agent.isState(input)) expect(input).type.toBe<Agent.State>()
  if (Transcript.isEdit(input)) expect(input).type.toBe<Transcript.Edit>()
  if (Executor.isRequest(input)) expect(input).type.toBe<Executor.Request>()
  if (Executor.isSummaryRequest(input)) expect(input).type.toBe<Executor.SummaryRequest>()
  if (Executor.isSummary(input)) expect(input).type.toBe<Executor.Summary>()
  if (Executor.isDisposition(input)) expect(input).type.toBe<Executor.Disposition>()
  if (Invocation.isDiagnostic(input)) expect(input).type.toBe<Invocation.Diagnostic>()
  if (Invocation.isControl(input)) expect(input).type.toBe<Invocation.Control>()
  if (Invocation.isResult(input)) expect(input).type.toBe<Invocation.Result>()
  if (Model.isRequestOptions(input)) expect(input).type.toBe<Model.RequestOptions>()
  if (Model.isDeferredDecision(input)) expect(input).type.toBe<Model.DeferredDecision>()
  if (Output.isOutputLimits(input)) expect(input).type.toBe<Output.Limits>()
  if (ToolRegistration.isIntent(input)) expect(input).type.toBe<ToolRegistration.Intent>()
  if (ToolRegistration.isExecution(input)) expect(input).type.toBe<ToolRegistration.Execution>()
  if (ToolResult.isEnvelope(input)) expect(input).type.toBe<ToolResult.Envelope>()
  if (Usage.isUsage(input)) expect(input).type.toBe<Usage.Usage>()
  if (Usage.isState(input)) expect(input).type.toBe<Usage.State>()
  if (Bash.isParameters(input)) expect(input).type.toBe<Bash.Parameters>()
  if (Edit.isParameters(input)) expect(input).type.toBe<Edit.Parameters>()
  if (Read.isParameters(input)) expect(input).type.toBe<Read.Parameters>()
  if (Write.isParameters(input)) expect(input).type.toBe<Write.Parameters>()
  expect(ResponseAccumulator.partial(ResponseAccumulator.make())).type.toBe<
    Option.Option<Prompt.AssistantMessage>
  >()
})
