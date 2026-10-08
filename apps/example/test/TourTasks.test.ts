import { BunServices } from '@effect/platform-bun'
import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Checkout from '../dist/tour/Checkout.js'
import * as Reminder from '../dist/tour/Reminder.js'

it.live('checkout joins parallel payments and compensates declined charges exactly once', () =>
  Checkout.run.pipe(
    Effect.tap((result) =>
      Effect.sync(() => {
        assert.strictEqual(result.success?.status, 'completed')
        assert.strictEqual(result.decline?.status, 'failed')
        assert.strictEqual(
          result.payments.find((payment) => payment.payment === 'decline/card')?.outcome.status,
          'completed',
        )
        assert.strictEqual(
          result.payments.find((payment) => payment.payment === 'decline/credit')?.outcome.status,
          'aborted',
        )
        const declineCharges = result.events.filter(
          (event) => event.payment.startsWith('decline/') && event.operation === 'charge',
        )
        const refunds = result.events.filter((event) => event.operation === 'refund')
        assert.strictEqual(declineCharges.length, 2)
        assert.strictEqual(refunds.length, 2)
        assert.strictEqual(
          new Set(result.events.map((event) => event.key)).size,
          result.events.length,
        )
        assert.deepEqual(
          refunds.map((event) => event.payment).sort(),
          declineCharges.map((event) => event.payment).sort(),
        )
      }),
    ),
    Effect.provide(BunServices.layer),
  ),
)

it.live('reminder retains its deadline across reopen and respects background cancellation', () =>
  Reminder.run.pipe(
    Effect.tap((result) =>
      Effect.sync(() => {
        assert.isNumber(result.deadline)
        assert.deepEqual(result.messages, ['Saved deadline reached'])
        assert.strictEqual(result.recovered?.status, 'completed')
        assert.strictEqual(result.background?.status, 'aborted')
      }),
    ),
    Effect.provide(BunServices.layer),
  ),
)
