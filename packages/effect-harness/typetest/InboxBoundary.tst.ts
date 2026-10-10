import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import type * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as RunControl from 'effect-harness/RunControl'
import type { Failure } from 'effect-harness/SessionError'
import type * as Transaction from 'effect-harness/Transaction'
import * as Inbox from '../src/internal/Inbox.js'

declare const tx: Transaction.Transaction
const root = Record.ROOT_CONVERSATION_ID

test('queue payloads are serializable entry drafts and queue modes are independently bounded', () => {
  expect({
    mode: 'write',
    payload: { kind: 'reset', head: 'self', data: { reason: 'reset' } },
  } as const).type.toBeAssignableTo<Inbox.Draft>()
  expect({
    mode: 'write',
    payload: () => Effect.void,
  } as const).type.not.toBeAssignableTo<Inbox.Draft>()
  expect({
    mode: 'reject',
    payload: { kind: 'input' },
  } as const).type.not.toBeAssignableTo<Inbox.Draft>()
  expect({ followUp: 'all', steering: 'one' } as const).type.toBeAssignableTo<Inbox.QueueModes>()
  expect({ steering: 'interrupt' } as const).type.not.toBeAssignableTo<Inbox.QueueModes>()
  expect<Inbox.Boundary>().type.toBe<'postTools' | 'final'>()
})

test('helpers require a transaction, expose failures, and add no hidden runtime services', () => {
  expect(Inbox.initialize(tx, root)).type.toBe<Effect.Effect<void, Failure>>()
  expect(Inbox.admit(tx, root, { mode: 'followUp', payload: { kind: 'input' } })).type.toBe<
    Effect.Effect<Record.Submission, Failure>
  >()
  expect(Inbox.withdraw(tx, root, Record.SubmissionId.make(2))).type.toBe<
    Effect.Effect<boolean, Failure>
  >()
  expect(Inbox.select(tx, root, { boundary: 'final' })).type.toBe<
    Effect.Effect<Inbox.Selection, Failure>
  >()
  expect(Inbox.document).type.toBeAssignableTo<Document.Document<typeof Inbox.StateSchema>>()
  expect(RunControl.document).type.toBeAssignableTo<
    Document.Document<typeof RunControl.StateSchema>
  >()
  expect<Inbox.Selection['active']>().type.toBe<RunControl.Membership | undefined>()
  expect({
    taskId: Record.TaskId.make(3),
    submissionIds: [3],
  }).type.not.toBeAssignableTo<RunControl.Membership>()
})
