import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Transaction from 'effect-harness/Transaction'

const schema = Schema.Struct({ count: Schema.Natural })
const document = Document.define({
  kind: 'typed.task',
  version: 1,
  scope: 'task',
  schema,
  initial: () => ({ count: 0 }),
})
const target = { scope: { _tag: 'task', taskId: Record.TaskId.make(2) } } satisfies Document.Target
declare const tx: Transaction.Transaction
declare const session: Session.Session
declare const task: Record.Task

test('terminal settlement and task document access retain typed public failures', () => {
  expect(
    Transaction.putTask(tx, {
      ...task,
      state: { status: 'terminal', outcome: { status: 'completed', result: null } },
    }),
  ).type.toBe<Effect.Effect<void, Session.Failure>>()
  expect(tx.pipe(Transaction.putTask(task))).type.toBe<Effect.Effect<void, Session.Failure>>()
  expect(Transaction.ensureDocument(tx, document, target)).type.toBe<
    Effect.Effect<Document.Snapshot<typeof schema>, Session.Failure>
  >()
  expect(Transaction.setDocument(tx, document, target, { count: 1 })).type.toBe<
    Effect.Effect<void, Session.Failure>
  >()
  expect(
    Transaction.updateDocument(tx, document, target, (value) => {
      expect(value).type.toBe<{ readonly count: number }>()
      return { count: value.count + 1 }
    }),
  ).type.toBe<Effect.Effect<void, Session.Failure>>()
  expect(Session.snapshot(session, document, target)).type.toBe<
    Effect.Effect<Option.Option<Document.Snapshot<typeof schema>>, Session.Failure>
  >()
})
