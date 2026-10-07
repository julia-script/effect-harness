import { expect, test } from 'tstyche'
import * as Agent from '@effect-harness/harness/Agent'
import * as Context from '@effect-harness/harness/Context'
import * as Executor from '@effect-harness/harness/Executor'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Model from '@effect-harness/harness/Model'
import * as Output from '@effect-harness/harness/Output'
import * as Tool from '@effect-harness/harness/Tool'
import * as ToolResult from '@effect-harness/harness/ToolResult'
import * as Usage from '@effect-harness/harness/Usage'
import * as Bash from '@effect-harness/harness/tools/Bash'
import * as Edit from '@effect-harness/harness/tools/Edit'
import * as Read from '@effect-harness/harness/tools/Read'
import * as Write from '@effect-harness/harness/tools/Write'
import * as Response from '@effect-harness/harness/Response'
import type * as Option from 'effect/Option'
import type * as Prompt from 'effect/ai/Prompt'
declare const input: unknown

test('named unknown refinements use the actual public decoded codec, including aliases', () => {
  if (Agent.isModelRef(input)) expect(input).type.toBe<Agent.ModelRef>()
  if (Agent.isSelectionEdit(input)) expect(input).type.toBe<typeof Agent.SelectionEdit.Type>()
  if (Agent.isSelection(input)) expect(input).type.toBe<Agent.Selection>()
  if (Agent.isToolSelection(input)) expect(input).type.toBe<Agent.ToolSelection>()
  if (Agent.isState(input)) expect(input).type.toBe<Agent.State>()
  if (Context.isEdit(input)) expect(input).type.toBe<Context.Edit>()
  if (Executor.isRequest(input)) expect(input).type.toBe<Executor.Request>()
  if (Executor.isSummaryRequest(input)) expect(input).type.toBe<Executor.SummaryRequest>()
  if (Executor.isSummary(input)) expect(input).type.toBe<Executor.Summary>()
  if (Executor.isDisposition(input)) expect(input).type.toBe<Executor.Disposition>()
  if (Invocation.isDiagnostic(input)) expect(input).type.toBe<Invocation.Diagnostic>()
  if (Invocation.isControl(input)) expect(input).type.toBe<Invocation.Control>()
  if (Invocation.isToolResult(input)) expect(input).type.toBe<Invocation.ToolResult>()
  if (Model.isRequestOptions(input)) expect(input).type.toBe<Model.RequestOptions>()
  if (Model.isDeferredDecision(input)) expect(input).type.toBe<Model.DeferredDecision>()
  if (Output.isOutputLimits(input)) expect(input).type.toBe<Output.OutputLimits>()
  if (Tool.isIntent(input)) expect(input).type.toBe<Tool.Intent>()
  if (Tool.isExecution(input)) expect(input).type.toBe<Tool.Execution>()
  if (ToolResult.isEnvelope(input)) expect(input).type.toBe<ToolResult.Envelope>()
  if (Usage.isUsage(input)) expect(input).type.toBe<Usage.Usage>()
  if (Usage.isState(input)) expect(input).type.toBe<Usage.State>()
  if (Bash.isInput(input)) expect(input).type.toBe<Bash.Input>()
  if (Edit.isInput(input)) expect(input).type.toBe<Edit.Input>()
  if (Read.isInput(input)) expect(input).type.toBe<Read.Input>()
  if (Write.isInput(input)) expect(input).type.toBe<Write.Input>()
  expect(Response.partial(Response.empty())).type.toBe<Option.Option<Prompt.AssistantMessage>>()
})
