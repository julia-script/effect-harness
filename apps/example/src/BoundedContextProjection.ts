/** Run after integration builds the public modules: node apps/example/dist/BoundedContextProjection.js */
import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as ContextProjection from 'effect-harness/ContextProjection'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const program = Effect.scoped(
  Effect.gen(function* () {
    const session = yield* Session.make()
    const cutoff = yield* Session.commit(session, (tx) =>
      Effect.gen(function* () {
        yield* Transaction.ensureRoot(tx)
        yield* Transaction.appendEntry(tx, Record.ROOT_CONVERSATION_ID, {
          kind: 'input',
          model: [{ role: 'user', content: 'Look up the answer.' }],
        })
        return yield* Transaction.appendEntry(tx, Record.ROOT_CONVERSATION_ID, {
          kind: 'assistant',
          model: [
            {
              role: 'assistant',
              content: [
                {
                  type: 'tool-call',
                  id: 'lookup-1',
                  name: 'lookup',
                  params: {},
                  providerExecuted: false,
                },
              ],
            },
          ],
        })
      }),
    )
    const view = yield* ContextProjection.read(session, Record.ROOT_CONVERSATION_ID, {
      at: cutoff.id,
    })
    yield* Effect.log(view.prompt.content)
  }),
).pipe(Effect.provide(Storage.layerMemory))

NodeRuntime.runMain(program)
