import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type { Failure } from 'effect-harness/ExecutionError'
import * as Record from 'effect-harness/Record'
import type { ToolExecution } from 'effect-harness/ToolExecution'
import * as Usage from 'effect-harness/Usage'

class Credential extends Context.Service<Credential, string>()('types/round/Credential') {}
class DomainError extends Schema.TaggedError<DomainError>()('RoundDomainError', {}) {}
declare const access: ToolExecution['Service']

test('native child access preserves domain errors and service requirements through its transaction gate', () => {
  expect(access.taskId).type.toBe<Record.TaskId>()
  expect(access.callId).type.toBe<string>()
  expect(
    access.commit(() => Credential.pipe(Effect.andThen(Effect.fail(new DomainError())))),
  ).type.toBe<Effect.Effect<never, Failure | DomainError, Credential>>()
  expect(Effect.runPromise(access.commit(() => Credential))).type.toRaiseError()
  expect(access.output('accepted output')).type.toBe<Effect.Effect<void, Failure>>()
  expect(access.output(new Uint8Array())).type.toRaiseError()
})

test('tool-call accounting preserves the native task brand and original provider ID', () => {
  const identity = {
    _tag: 'toolCall',
    taskId: Record.TaskId.make(2),
    callId: 'provider-id',
    attempt: Usage.AttemptId.make(0),
  } satisfies Usage.ReceiptIdentity
  expect(identity).type.toBeAssignableTo<Usage.ReceiptIdentity>()
  expect<
    Extract<Usage.ReceiptIdentity, { readonly _tag: 'toolCall' }>['taskId']
  >().type.toBeAssignableTo<Record.TaskId>()
  expect<
    Extract<Usage.ReceiptIdentity, { readonly _tag: 'toolCall' }>['callId']
  >().type.toBe<string>()
  expect<
    Extract<Usage.ReceiptIdentity, { readonly _tag: 'toolCall' }>['attempt']
  >().type.toBe<Usage.AttemptId>()
})

import type * as Transaction from 'effect-harness/Transaction'
import type * as SessionError from 'effect-harness/SessionError'
declare const tx: Transaction.Transaction
declare const receipt: Usage.Receipt
test('public receipt protection preserves the existing error and service channels', () => {
  expect(Usage.record(tx, receipt)).type.toBe<Effect.Effect<Usage.Recorded, SessionError.Failure>>()
  expect(
    Usage.settle(tx, receipt, () =>
      Credential.pipe(Effect.andThen(Effect.fail(new DomainError()))),
    ),
  ).type.toBe<Effect.Effect<Usage.Recorded, SessionError.Failure | DomainError, Credential>>()
})
