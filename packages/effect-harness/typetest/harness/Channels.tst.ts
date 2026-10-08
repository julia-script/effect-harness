import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import type * as Prompt from 'effect/ai/Prompt'
import type * as ResponseParts from 'effect/ai/Response'
import type * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as Layer from 'effect/Layer'
import * as Agent from 'effect-harness/Agent'
import * as Compaction from 'effect-harness/Compaction'
import * as View from 'effect-harness/Context'
import * as Hook from 'effect-harness/Hook'
import * as Invocation from 'effect-harness/Invocation'
import * as Model from 'effect-harness/Model'
import * as Output from 'effect-harness/Output'
import * as Progress from 'effect-harness/Progress'
import * as Registry from 'effect-harness/Registry'
import * as ToolResult from 'effect-harness/ToolResult'
import type { RegistryError } from 'effect-harness/RegistryError'
import type { OutputError } from 'effect-harness/OutputError'

class Example extends Context.Service<Example, string>()('typetest/Example') {}
declare const prompt: Prompt.Prompt
declare const parts: ReadonlyArray<ResponseParts.AnyPart>
declare const registry: Registry.Resolved
declare const writer: Effect.Effect<number, 'write-failed', Example>
declare const capability: Model.DeferredCapability

test('Hook recovery and query exports preserve full generic success/context channels', () => {
  const recovered = Hook.recover(Effect.map(Example, (value) => value.length))
  expect(recovered).type.toBe<
    Effect.Effect<number | undefined, never, Example | Invocation.Invocation>
  >()
  expect(Hook.recover(Effect.fail('foreign'))).type.toBe<
    Effect.Effect<undefined, never, Invocation.Invocation>
  >()
  expect(Hook.beforeRequest([], prompt)).type.toBe<
    Effect.Effect<Prompt.Prompt, never, Invocation.Invocation>
  >()
  expect(Hook.afterTool([], { id: 'call', name: 'tool', args: {} }, {})).type.toBe<
    Effect.Effect<Invocation.ToolResult, never, Invocation.Invocation>
  >()
  expect(
    Hook.beforeCompact([], { reason: 'manual', firstKept: entry, view: View.empty() }),
  ).type.toBe<Effect.Effect<Option.Option<Hook.CompactDecision>, never, Invocation.Invocation>>()
  expect(Hook.onYield([], parts)).type.toBe<
    Effect.Effect<Option.Option<Prompt.UserMessage>, never, Invocation.Invocation>
  >()
  expect(Hook.afterResponse([], parts)).type.toBe<
    Effect.Effect<void, never, Invocation.Invocation>
  >()
  expect(Hook.afterTools([], [])).type.toBe<Effect.Effect<void, never, Invocation.Invocation>>()
  expect(Hook.conversationCreated([], conversation)).type.toBe<
    Effect.Effect<void, never, Invocation.Invocation>
  >()
  // Both native dual call forms must remove precisely the supplied service.
  expect(Effect.provideService(recovered, Example, 'abc')).type.toBe<
    Effect.Effect<number | undefined, never, Invocation.Invocation>
  >()
  expect(recovered.pipe(Effect.provideService(Example, 'abc'))).type.toBe<
    Effect.Effect<number | undefined, never, Invocation.Invocation>
  >()
  expect(Hook.beforeRequest).type.not.toBeCallableWith([], 'raw prompt')
})
import type { ConversationId, EntryId } from 'effect-harness/Identity'
declare const entry: EntryId
declare const conversation: ConversationId

test('partial selections and buffer/progress channels are exact', () => {
  expect(Compaction.selectCut(View.empty(), 1)).type.toBe<Option.Option<number>>()
  expect(Compaction.threshold(0, 1, Agent.defaultCompaction)).type.toBe<
    Option.Option<'blocking' | 'background'>
  >()
  expect(capability.inspect(parts)).type.toBe<Option.Option<Model.DeferredDecision>>()
  expect(Agent.settings()).type.toBe<Effect.Effect<Agent.Settings, Schema.SchemaError>>()
  expect(Invocation.layerSilent).type.toBe<Layer.Layer<Invocation.Invocation>>()
  expect(Output.push(Output.make(), 'text')).type.toBe<Effect.Effect<boolean, OutputError>>()
  expect(Output.makeWindow()).type.toBe<Effect.Effect<Output.Window>>()
  expect(Progress.make(writer, { minIntervalMs: '10 millis' })).type.toBe<
    Effect.Effect<Progress.Progress<'write-failed'>, Schema.SchemaError, Example | Scope.Scope>
  >()
  expect(Registry.make()).type.toBe<Effect.Effect<Registry.Registry['Service'], RegistryError>>()
  expect(Registry.resolve({ revision: 0, extensions: [] }, {}, Agent.defaultSettings)).type.toBe<
    Effect.Effect<Registry.Resolved, never, Invocation.Invocation>
  >()
  expect(Registry.render(registry, View.empty(), new Map())).type.toBe<
    Effect.Effect<Map<string, string>, never, Invocation.Invocation>
  >()
  expect(ToolResult.encode({})).type.toBe<Effect.Effect<Schema.Json, Schema.SchemaError>>()
  expect(ToolResult.decode({})).type.toBe<Effect.Effect<ToolResult.Envelope, Schema.SchemaError>>()
  expect(ToolResult.encode).type.not.toBeCallableWith({ content: [{ type: 'text', text: 1 }] })
})
