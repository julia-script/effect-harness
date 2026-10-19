import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Projection from 'effect-harness/ContextProjection'
import * as Record from 'effect-harness/Record'
import type * as Session from 'effect-harness/Session'

declare const session: Session.Session
declare const entries: ReadonlyArray<Record.Entry>

test('bounded projection is service-free and exposes native typed failures', () => {
  expect(Projection.read(session, Record.ROOT_CONVERSATION_ID)).type.toBe<
    Effect.Effect<Projection.ContextView, Projection.Failure>
  >()
  expect(Projection.project(Record.ROOT_CONVERSATION_ID, entries)).type.toBe<
    Effect.Effect<Projection.ContextView, Projection.Failure>
  >()
  expect<Projection.ContextView['activeEntries']>().type.toBe<ReadonlyArray<Record.Entry>>()
  expect<Projection.ContextView['orderedSections']>().type.toBe<ReadonlyArray<Projection.Section>>()
})

test('legacy policy requires explicit cutoff and declarative facts cannot contain handlers', () => {
  expect({
    _tag: 'preserveManaged',
    through: Schema.decodeSync(Record.EntryId)(1),
  } as const).type.toBeAssignableTo<Projection.LegacyInterpretation>()
  expect({ _tag: 'preserveManaged' }).type.not.toBeAssignableTo<Projection.LegacyInterpretation>()
  expect({
    name: 'read',
    description: 'Read a file',
    parameters: { type: 'object' },
  }).type.toBeAssignableTo<Projection.ToolDeclaration>()
  expect({
    name: 'read',
    description: 'Read a file',
    parameters: () => ({}),
  }).type.not.toBeAssignableTo<Projection.ToolDeclaration>()
  expect<Projection.Fact>().type.toBeAssignableTo<Schema.Json>()
})
