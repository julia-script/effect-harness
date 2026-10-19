import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as Execution from 'effect-harness/Execution'
import * as ProgressPolicy from 'effect-harness/ProgressPolicy'
import * as Record from 'effect-harness/Record'
import * as ToolProgress from 'effect-harness/ToolProgress'
import type * as ToolResult from 'effect-harness/ToolResult'

class Backend extends Context.Service<Backend, number>()('types/ToolProgress/Backend') {}
class Credentials extends Context.Service<Credentials, number>()(
  'types/ToolProgress/Credentials',
) {}
class Domain extends Context.Service<Domain, number>()('types/ToolProgress/Domain') {}
class PersistError extends Schema.TaggedError<PersistError>()('ProgressPersistError', {}) {}
class ReceiptError extends Schema.TaggedError<ReceiptError>()('ProgressReceiptError', {}) {}
class BuildError extends Schema.TaggedError<BuildError>()('ProgressBuildError', {}) {}
declare const gate: ToolProgress.Gate<PersistError, Backend>
declare const access: Execution.Access
declare const writer: ToolProgress.Writer<PersistError, Backend>
declare const layer: Layer.Layer<Backend, BuildError, Credentials>
const options = { conversationId: Record.ROOT_CONVERSATION_ID, callId: 'types-call', attempt: 0 }

test('the frozen policy is a schema-backed pair of named numbers', () => {
  expect(ProgressPolicy.defaultPolicy).type.toBe<ProgressPolicy.Policy>()
  expect(
    Schema.decodeEffect(ProgressPolicy.Policy)({ minInterval: 0, bytesPerSecond: 102400 }),
  ).type.toBe<Effect.Effect<ProgressPolicy.Policy, Schema.SchemaError>>()
})
test('construction preserves the gate requirements/error and invocation scope', () => {
  expect(ToolProgress.make(gate, options)).type.toBe<
    Effect.Effect<
      ToolProgress.Writer<PersistError, Backend>,
      ToolProgress.Failure<PersistError>,
      Backend | Scope.Scope
    >
  >()
  expect(Effect.runPromise(ToolProgress.make(gate, options))).type.toRaiseError()
  expect(Effect.runPromise(Effect.scoped(ToolProgress.make(gate, options)))).type.toRaiseError()
  expect(Effect.scoped(ToolProgress.make(gate, options)).pipe(Effect.provide(layer))).type.toBe<
    Effect.Effect<
      ToolProgress.Writer<PersistError, Backend>,
      ToolProgress.Failure<PersistError> | BuildError,
      Credentials
    >
  >()
  expect(ToolProgress.make(access, options)).type.toBe<
    Effect.Effect<
      ToolProgress.Writer<import('effect-harness/ExecutionError').Failure, never>,
      ToolProgress.Failure<import('effect-harness/ExecutionError').Failure>,
      Scope.Scope
    >
  >()
  expect(
    ToolProgress.make({ commit: (change) => Effect.scoped(change({})) }, options),
  ).type.toRaiseError()
})
test('reports retain persistence failures and settlement keeps receipt requirements and failures', () => {
  expect(writer.output(new Uint8Array([1]))).type.toBe<
    Effect.Effect<void, ToolProgress.Failure<PersistError>>
  >()
  expect(writer.details({ count: 1 })).type.toBe<
    Effect.Effect<void, ToolProgress.Failure<PersistError>>
  >()
  expect(
    writer.settle('completed', () => Domain.pipe(Effect.andThen(Effect.fail(new ReceiptError())))),
  ).type.toBe<
    Effect.Effect<never, ToolProgress.Failure<PersistError> | ReceiptError, Backend | Domain>
  >()
  expect(writer.settle('running', () => Effect.void)).type.toRaiseError()
  expect(writer.details({ fn: () => 1 })).type.toRaiseError()
})
test('retention preserves decoded structured output and extra owner fields', () => {
  const result = {
    content: [] satisfies ToolResult.Content,
    details: { ui: true },
    structuredOutput: { success: 1 },
    owner: 'exact' as const,
  }
  const retained = ToolProgress.retainResult(result)
  expect(retained).type.toBeAssignableTo<
    Effect.Effect<
      {
        readonly content: ToolResult.Content
        readonly details: { ui: boolean }
        readonly structuredOutput: { success: number }
        readonly owner: 'exact'
        readonly diagnostics?: ReadonlyArray<ToolResult.Diagnostic>
      },
      import('effect-harness/OutputRetention').OutputRetentionError
    >
  >()
  expect(ToolProgress.document.definition.schema).type.toBe<typeof ToolProgress.Snapshot>()
  expect(ToolProgress.make({}, options)).type.toRaiseError()
})
