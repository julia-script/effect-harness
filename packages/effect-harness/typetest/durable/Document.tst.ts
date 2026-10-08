import { expect, test } from 'tstyche'
import * as Schema from 'effect/Schema'
import type * as Effect from 'effect/Effect'
import * as Document from 'effect-harness/durable/Document'
import * as Record from 'effect-harness/durable/Record'
import * as View from 'effect-harness/durable/View'
import * as Runner from 'effect-harness/durable/testing/Runner'
import * as Storage from 'effect-harness/durable/testing/Storage'

test('invariant token erasure accepts actual typed handles but never recovers their T', () => {
  const token = Document.defineUnsafe({
    kind: 'counter',
    version: 1,
    scope: 'session',
    schema: Schema.Struct({ count: Schema.Finite }),
    initial: () => ({ count: 0 }),
    checkpointWhen: (value) => value.count > 0,
  })
  expect(token).type.toBe<Document.Document<{ readonly count: number }>>()
  expect(token).type.toBeAssignableTo<Document.Document.Any>()
  expect(token.definition).type.toBeAssignableTo<Document.Document.AnyDefinition>()
  expect<Document.Document.Any>().type.not.toBeAssignableTo<Document.Document<object>>()
  expect<Document.Document.Any>().type.not.toBeAssignableTo<Document.Document<never>>()
  expect<Document.Document.AnyDefinition>().type.not.toBeAssignableTo<
    Document.Document.Definition<object>
  >()
  expect<Document.Document.AnyDefinition>().type.not.toBeAssignableTo<
    Document.Document.Definition<never>
  >()
  const u: unknown = token
  if (Document.isDocument(u)) {
    expect(u).type.toBe<Document.Document.Any>()
    expect(u.definition.schema).type.toBe<Schema.Constraint>()
    expect(u.definition.initial()).type.toBe<object>()
    if (u.definition.checkpointWhen !== undefined)
      expect<Parameters<typeof u.definition.checkpointWhen>[0]>().type.toBe<never>()
  }
  const d: unknown = token.definition
  if (Document.isDefinition(d)) expect(d).type.toBe<Document.Document.AnyDefinition>()
})
test('safe variance erasure does not allow executing unknown case requirements', () => {
  expect<Document.Document.Snapshot<{ readonly count: number }>>().type.toBeAssignableTo<
    Document.Document.Snapshot<object>
  >()
  expect<Runner.Runner<'Failure', { readonly Service: unique symbol }>>().type.toBeAssignableTo<
    Runner.Runner<never, never>
  >()
  expect<Runner.Runner<never, never>>().type.not.toBeAssignableTo<
    Runner.Runner<'Failure', { readonly Service: unique symbol }>
  >()
  expect<Storage.Case<{ readonly Service: unique symbol }>>().type.toBeAssignableTo<
    Storage.Case<unknown>
  >()
  expect<Storage.Case<unknown>>().type.not.toBeAssignableTo<Storage.Case<never>>()
  const u: unknown = undefined
  if (Document.isSnapshot(u)) expect(u).type.toBe<Document.Document.Snapshot<object>>()
  if (Runner.isRunner(u)) expect(u).type.toBe<Runner.Runner<never, never>>()
  if (Storage.isCase(u))
    // effect-nit-allow P2-no-unknown-any-in-channels: this type-only witness proves erased case requirements stay unknown and cannot be run without recovering their service; no effect is executed.
    // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- assert the non-executable existential requirements witness
    expect(u.run).type.toBe<Storage.Case<unknown>['run']>()
  if (View.isState(u)) expect(u).type.toBe<View.View.State>()
})
test('decoded entry schema parameter supports covariance while retaining precise decoding', () => {
  const narrow = Schema.Struct({ text: Schema.Literal('x') })
  expect<Record.DecodedEntryToken<'entry', typeof narrow>>().type.toBeAssignableTo<
    Record.DecodedEntryToken<'entry', Schema.Constraint>
  >()
  expect<
    Record.DecodedEntryToken<
      'entry',
      Schema.ConstraintDecoder<{ readonly text: 'x' }, 'Dependency'>
    >
  >().type.toBeAssignableTo<
    Record.DecodedEntryToken<'entry', Schema.ConstraintDecoder<{ readonly text: string }, unknown>>
  >()
  expect<Record.DecodedEntryToken<'entry', Schema.Constraint>>().type.not.toBeAssignableTo<
    Record.DecodedEntryToken<'entry', typeof narrow>
  >()
  const token = Record.defineEntryUnsafe('entry', narrow)
  expect(token.schema).type.toBe<typeof narrow>()
  expect(token.decode({ text: 'x' })).type.toBe<
    Effect.Effect<{ readonly text: 'x' }, Schema.SchemaError>
  >()
})
