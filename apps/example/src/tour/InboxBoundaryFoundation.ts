import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Record from 'effect-harness/Record'
import * as RunControl from 'effect-harness/RunControl'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

// Run after integration build: node apps/example/dist/tour/InboxBoundaryFoundation.js
// RunControl is a schema-backed foundation. The legacy scheduler remains authoritative.
// Membership alone neither schedules a task nor settles its input receipts.
const program = Effect.scoped(
  Effect.gen(function* () {
    const session = yield* Session.make()
    const root = Record.ROOT_CONVERSATION_ID
    yield* Session.commit(session, (tx) =>
      Effect.gen(function* () {
        yield* Transaction.ensureRoot(tx)
        yield* Transaction.ensureDocument(tx, RunControl.document, RunControl.target(root))
      }),
    )
    const snapshot = yield* Session.snapshot(session, RunControl.document, RunControl.target(root))
    if (Option.isNone(snapshot)) return yield* Effect.die('Run membership was not initialized')
    yield* Effect.log(
      'Run membership starts empty and forks initialize fresh state',
      snapshot.value.value,
    )
  }),
).pipe(Effect.provide(Storage.layerMemory))
NodeRuntime.runMain(program)
