/** Simulated checkout: failFast joins live payments; the parent compensates completed charges. */
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Record from 'effect-harness/Record'
import * as Task from 'effect-harness/Task'
import { TaskRuntime } from 'effect-harness/TaskRuntime'
import * as Runtime from './Runtime.ts'

const BankEvent = Schema.Struct({
  key: Schema.String,
  payment: Schema.String,
  operation: Schema.Literals(['charge', 'refund']),
})
const PaymentInput = Schema.Struct({
  payment: Schema.String,
  decline: Schema.Boolean,
  hold: Schema.Boolean,
  completedCharge: Schema.optionalKey(Record.TaskId),
})
const PaymentCheckpoint = Schema.Struct({ phase: Schema.Literals(['authorize', 'decline']) })
const CheckoutInput = Schema.Struct({ order: Schema.String, decline: Schema.Boolean })
const CheckoutCheckpoint = Schema.Union([
  Schema.Struct({ phase: Schema.tag('start') }),
  Schema.Struct({ phase: Schema.tag('join'), children: Schema.Array(Record.TaskId) }),
])
export const Result = Schema.Struct({
  success: Task.Outcome,
  decline: Task.Outcome,
  events: Schema.Array(BankEvent),
  payments: Schema.Array(Schema.Struct({ payment: Schema.String, outcome: Task.Outcome })),
  compensation: Schema.String,
})

const bank = Effect.gen(function* () {
  // This process-local ledger simulates an external bank with stable idempotency keys.
  const ledger = yield* Ref.make<ReadonlyArray<typeof BankEvent.Type>>([])
  const charge = (payment: string) =>
    Ref.update(ledger, (events) =>
      events.some((event) => event.key === `charge:${payment}`)
        ? events
        : [...events, { key: `charge:${payment}`, payment, operation: 'charge' as const }],
    )
  const refund = (payment: string) =>
    Ref.update(ledger, (events) =>
      !events.some((event) => event.key === `charge:${payment}`) ||
      events.some((event) => event.key === `refund:${payment}`)
        ? events
        : [...events, { key: `refund:${payment}`, payment, operation: 'refund' as const }],
    )
  return { charge, refund, events: Ref.get(ledger) }
})
const payments = (order: string, decline: boolean) =>
  decline
    ? [`${order}/card`, `${order}/credit`, `${order}/declined`]
    : [`${order}/card`, `${order}/credit`]

export const run = Effect.scoped(
  Effect.gen(function* () {
    const simulated = yield* bank
    const chargedSibling = yield* Deferred.make<void>()
    const chargedCard = yield* Deferred.make<void>()
    const payment = yield* Task.bind(
      Task.define({
        name: 'tour/payment',
        version: 1,
        input: PaymentInput,
        checkpoint: PaymentCheckpoint,
        result: Schema.String,
        initial: () => ({ phase: 'authorize' as const }),
        run: Effect.fn('tour.payment')(function* (task) {
          if (task.checkpoint.phase === 'decline') return Task.fail('Simulated card declined')
          if (task.input.decline) {
            // Both other charges are visible before the decline starts the failFast cascade.
            yield* Deferred.await(chargedSibling)
            yield* Deferred.await(chargedCard)
            if (task.input.completedCharge === undefined)
              return Task.fail('Missing charged sibling')
            return Task.wait({ phase: 'decline' as const }, [task.input.completedCharge])
          }
          yield* simulated.charge(task.input.payment)
          if (task.input.payment === 'decline/card') yield* Deferred.succeed(chargedCard, undefined)
          if (task.input.hold) {
            yield* Deferred.succeed(chargedSibling, undefined)
            return yield* Effect.never
          }
          return Task.complete(task.input.payment)
        }),
        abort: Effect.fn('tour.payment.abort')(function* (task) {
          yield* simulated.refund(task.input.payment)
          return Task.aborted('Payment compensated')
        }),
      }),
    )
    const checkout = yield* Task.bind(
      Task.define({
        name: 'tour/checkout',
        version: 1,
        input: CheckoutInput,
        checkpoint: CheckoutCheckpoint,
        result: Schema.String,
        initial: () => ({ phase: 'start' as const }),
        run: Effect.fn('tour.checkout')(function* (task) {
          const runtime = yield* TaskRuntime
          if (task.checkpoint.phase === 'start') {
            yield* runtime.commit(
              Effect.fn('tour.checkout.spawn')(function* (tx) {
                const children: Array<Record.TaskId> = []
                for (const [index, name] of payments(
                  task.input.order,
                  task.input.decline,
                ).entries()) {
                  const prepared = yield* payment.prepare({
                    payment: name,
                    decline: task.input.decline && index === 2,
                    hold: task.input.decline && index === 1,
                    ...(index === 2 && children[0] !== undefined
                      ? { completedCharge: children[0] }
                      : {}),
                  })
                  children.push(
                    yield* tx.createTask({
                      conversationId: task.conversationId,
                      owner: task.id,
                      kind: payment.name,
                      version: payment.version,
                      input: prepared.input,
                      background: false,
                      abortRequested: false,
                      state: { status: 'pending', checkpoint: prepared.checkpoint },
                    }),
                  )
                }
                return Task.wait({ phase: 'join', children }, children, 'failFast')
              }),
            )
            return
          }
          const outcomes = yield* Effect.forEach(
            yield* runtime.outcomes(task.checkpoint.children),
            (outcome) => Schema.decodeUnknownEffect(Task.Outcome)(outcome),
          )
          if (outcomes.every((outcome) => outcome.status === 'completed'))
            return Task.complete(`Confirmed ${task.input.order}`)
          // A completed payment is terminal; scheduler cancellation cannot rerun its abort handler.
          // Parent compensation covers those charges as well as the already-refunded live sibling.
          yield* Effect.forEach(payments(task.input.order, task.input.decline), simulated.refund)
          return Task.fail('Checkout declined; simulated charges refunded')
        }),
        abort: Effect.fn('tour.checkout.abort')(function* (task) {
          yield* Effect.forEach(payments(task.input.order, task.input.decline), simulated.refund)
          return Task.aborted('Checkout compensated')
        }),
      }),
    )
    const { harness } = yield* Runtime.open({ tasks: [payment, checkout] })
    const conversation = yield* harness.root
    const success = yield* harness.spawn(
      checkout,
      { order: 'success', decline: false },
      {
        conversationId: conversation.id,
      },
    )
    const succeeded = yield* harness.awaitTask(success)
    const successOutcome = yield* Schema.decodeUnknownEffect(Task.Outcome)(succeeded.state.outcome)
    yield* Runtime.check(successOutcome.status === 'completed', 'Checkout must succeed')
    const decline = yield* harness.spawn(
      checkout,
      { order: 'decline', decline: true },
      {
        conversationId: conversation.id,
      },
    )
    const declined = yield* harness.awaitTask(decline)
    const declineOutcome = yield* Schema.decodeUnknownEffect(Task.Outcome)(declined.state.outcome)
    yield* Runtime.check(declineOutcome.status === 'failed', 'Checkout must decline')
    const events = yield* simulated.events
    yield* Runtime.check(
      events.filter((event) => event.operation === 'refund').length === 2,
      'Both simulated decline charges must be refunded exactly once',
    )
    const paymentOutcomes = yield* Effect.forEach(
      (yield* harness.snapshot(conversation.id)).tasks.filter((task) => task.kind === payment.name),
      Effect.fn(function* (task) {
        const input = yield* Schema.decodeUnknownEffect(PaymentInput)(task.input)
        return {
          payment: input.payment,
          outcome: yield* Schema.decodeUnknownEffect(Task.Outcome)(task.state.outcome),
        }
      }),
    )
    return yield* Schema.decodeEffect(Result)({
      success: successOutcome,
      decline: declineOutcome,
      events,
      payments: paymentOutcomes,
      compensation: 'Live siblings abort bottom-up; checkout explicitly refunds completed charges.',
    })
  }),
)
