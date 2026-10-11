import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Scope from 'effect/Scope'
import * as Subagent from 'effect-harness/Subagent'
import * as CompactionTask from 'effect-harness/CompactionTask'
import * as Conversation from 'effect-harness/Conversation'
import * as Document from 'effect-harness/Document'
import * as Schema from 'effect/Schema'
import type * as Option from 'effect/Option'
import type * as Stream from 'effect/Stream'
import type * as TaskExecution from 'effect-harness/TaskExecution'
import type * as TaskScheduler from 'effect-harness/TaskScheduler'
import type { ToolExecution } from 'effect-harness/ToolExecution'
import type * as RegistryScope from 'effect-harness/RegistryScope'
import type { HarnessBackend } from 'effect-harness/HarnessBackend'
import type { HarnessError } from 'effect-harness/HarnessError'

class Use extends Context.Service<Use, string>()('types/subagent/Use') {}
declare const native: TaskExecution.Access
declare const tool: ToolExecution['Service']
declare const handle: Subagent.Handle
declare const compaction: CompactionTask.Handle
declare const lease: RegistryScope.Lease
declare const factory: NonNullable<TaskScheduler.Options<Use>['subagent']>

test('native factory retains host requirements and the explicit operation Scope', () => {
  expect(factory.open(native, lease, 'run')).type.toBe<
    Effect.Effect<Subagent.Access, Subagent.Failure, Use | Scope.Scope>
  >()
  expect<TaskExecution.Access['subagent']>().type.toBe<Subagent.Access | undefined>()
  expect<ToolExecution['Service']['subagent']>().type.toBe<Subagent.Access | undefined>()
  expect(Subagent.spawn(native, { draft: { type: 'input', content: 'child' } })).type.toBe<
    Effect.Effect<Subagent.Handle, Subagent.Failure>
  >()
  expect(Subagent.wait(tool, handle)).type.toBe<Effect.Effect<Subagent.Result, Subagent.Failure>>()
  expect(Subagent.open(native, handle)).type.toBe<
    Effect.Effect<Conversation.Conversation, Subagent.Failure>
  >()
})

test('conversation callbacks retain their concrete error and service channels', () => {
  const result = Subagent.withConversation(native, handle, () =>
    Effect.flatMap(Use, () => Effect.fail('callback' as const)),
  )
  expect(result).type.toBe<Effect.Effect<never, Subagent.Failure | 'callback', Use>>()
  expect(result).type.not.toBeAssignableTo<Effect.Effect<never, Subagent.Failure>>()
  expect(Subagent.withConversation(native, handle, () => Effect.as(Use, 42 as const))).type.toBe<
    Effect.Effect<42, Subagent.Failure, Use>
  >()
})

test('compaction DTO helpers keep the ambient transport requirement even for live identities', () => {
  expect(CompactionTask.wait(compaction)).type.toBe<
    Effect.Effect<CompactionTask.Outcome, HarnessError, HarnessBackend>
  >()
  expect(CompactionTask.abort(compaction)).type.toBe<
    Effect.Effect<void, HarnessError, HarnessBackend>
  >()
})

test('document observations retain consumer decoding services and errors', () => {
  const codec = Schema.Struct({
    value: Schema.Finite.pipe(
      Schema.middlewareDecoding((decode) =>
        Effect.gen(function* () {
          yield* Use
          yield* Scope.Scope
          return yield* decode
        }),
      ),
    ),
  })
  const document = Document.define({
    kind: 'types.subagent.observation',
    version: 1,
    scope: 'conversation',
    history: 'latest',
    fork: 'current',
    schema: codec,
    initial: () => ({ value: 1 }),
  })
  const read = Conversation.snapshot(document, { scope: { _tag: 'session' } })
  expect(read).type.toBe<
    (
      self: Conversation.Conversation,
    ) => Effect.Effect<
      Option.Option<Document.Snapshot<typeof codec>>,
      HarnessError | Schema.SchemaError,
      Use | Scope.Scope
    >
  >()
  const watch = Conversation.watch(document, { scope: { _tag: 'session' } })
  expect(watch).type.toBe<
    (
      self: Conversation.Conversation,
    ) => Stream.Stream<
      Document.Snapshot<typeof codec>,
      HarnessError | Schema.SchemaError,
      Use | Scope.Scope
    >
  >()
})
