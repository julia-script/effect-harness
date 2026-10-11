import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Prompt from 'effect/ai/Prompt'
import * as Plan from 'effect-harness/ContextPlan'
import type * as Projection from 'effect-harness/ContextProjection'
import type { HookExecution } from 'effect-harness/HookExecution'
import type * as Record from 'effect-harness/Record'
import type * as RegistryScope from 'effect-harness/RegistryScope'
import type * as Session from 'effect-harness/Session'
import type * as Transaction from 'effect-harness/Transaction'
import type { Failure } from 'effect-harness/ExecutionError'
import type { RegistryError } from 'effect-harness/RegistryError'

class RenderService extends Context.Service<RenderService, string>()(
  'test/context/RenderService',
) {}
class RenderFailure extends Schema.TaggedError<RenderFailure>()('RenderFailure', {
  message: Schema.String,
}) {}
declare const view: Projection.ContextView
declare const desired: Plan.Desired
declare const lease: RegistryScope.Lease
declare const renderer: Plan.Renderer<RenderFailure, RenderService>
declare const protocol: Plan.Protocol
declare const conversationId: Record.ConversationId
declare const session: Session.Session
declare const entries: ReadonlyArray<Record.Entry>

test('leased rendering preserves compiled invocation requirements and native failure identities', () => {
  expect(lease).type.toBeAssignableTo<Plan.Renderer<RegistryError | Failure, HookExecution>>()
  expect(Plan.render(view, lease)).type.toBe<
    Effect.Effect<
      Plan.Desired,
      RegistryError | Failure | Schema.SchemaError | Plan.ContextPlanError,
      HookExecution
    >
  >()
  expect(Plan.render(view, renderer)).type.toBe<
    Effect.Effect<
      Plan.Desired,
      RenderFailure | Schema.SchemaError | Plan.ContextPlanError,
      RenderService
    >
  >()
  expect<Plan.Renderer<RenderFailure, RenderService>['renderSection']>().type.toBe<
    (
      key: string,
      prompt: Prompt.Prompt,
      selection?: RegistryScope.Selection,
    ) => Effect.Effect<string | undefined, RenderFailure, RenderService>
  >()
})

test('plans/translations remain pure serializable declaration values and require explicit positioning', () => {
  expect(Plan.plan(view, desired)).type.toBe<
    Effect.Effect<Plan.Plan, Schema.SchemaError | Plan.ContextPlanError>
  >()
  expect(Plan.translate(view, { sections: 'positional', tools: 'latest' })).type.toBe<
    Effect.Effect<Plan.Translation, Schema.SchemaError | Plan.ContextPlanError>
  >()
  expect<Plan.Plan>().type.toBeAssignableTo<Schema.Json>()
  expect<Plan.Draft>().type.toBeAssignableTo<Transaction.EntryDraft>()
  expect<Plan.Protocol>().type.toBeAssignableTo<Schema.Json>()
  expect<Plan.Translation['prompt']>().type.toBe<Prompt.Prompt>()
  expect<Plan.Translation['tools']>().type.toBe<ReadonlyArray<Projection.ToolDeclaration>>()
  expect<Plan.Frame['entryId']>().type.toBe<Record.EntryId>()
  expect<{ tools: 'latest' }>().type.not.toBeAssignableTo<Plan.Positioning>()
  expect<{ sections: 'flatten'; tools: 'latest' }>().type.not.toBeAssignableTo<Plan.Positioning>()
  expect<{
    sections: []
    tools: [{ name: 'bad'; description: ''; parameters: () => {} }]
  }>().type.not.toBeAssignableTo<Plan.Desired>()
})

test('activation captures a durable fork-inherited boundary without adding runtime requirements', () => {
  expect(Plan.activateProtocol({ conversationId })).type.toBe<
    Effect.Effect<Plan.Protocol, Schema.SchemaError>
  >()
  expect(Plan.inheritProtocol(protocol)).type.toBe<
    Effect.Effect<Plan.Protocol, Schema.SchemaError>
  >()
  expect(Plan.protocolOptions(protocol)).type.toBe<
    Effect.Effect<Projection.ReadOptions, Schema.SchemaError>
  >()
  expect(Plan.projectWithProtocol(conversationId, entries, protocol)).type.toBe<
    Effect.Effect<Projection.ContextView, Projection.Failure>
  >()
  expect(Plan.readWithProtocol(session, conversationId, protocol)).type.toBe<
    Effect.Effect<Projection.ContextView, Projection.Failure>
  >()
  expect(Plan.protocolOptions(protocol, { legacyMode: 'honorAll' })).type.toBe<
    Effect.Effect<Projection.ReadOptions, Schema.SchemaError>
  >()
  expect<{
    version: 2
    origin: { conversationId: Record.ConversationId }
    legacyMode: 'preserveManaged'
  }>().type.not.toBeAssignableTo<Plan.Protocol>()
})
