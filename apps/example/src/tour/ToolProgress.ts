import { NodeServices } from '@effect/platform-node'
import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as ExecutionError from 'effect-harness/ExecutionError'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as ToolProgress from 'effect-harness/ToolProgress'
import * as ToolSpill from 'effect-harness/ToolSpill'
import * as Transaction from 'effect-harness/Transaction'

// After the integration build: node apps/example/dist/tour/ToolProgress.js
// The runtime/tool-round owner must still connect this writer to legacy RunLoop reporting.
const root = Record.ROOT_CONVERSATION_ID
NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const folder = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
      const journal = `${folder}/progress.jsonl`
      const spillLayer = ToolSpill.layer({
        provider: 'progress-example',
        directory: `${folder}/spills`,
        lifetime: 'persistent',
      })
      // The host/provider scope is longer than either invocation or reopened Session.
      const provider = yield* Layer.build(spillLayer)
      const spill = Context.get(provider, ToolSpill.ToolSpill)
      const storage = () => Layer.fresh(Storage.layerJsonl({ filePath: journal }))
      yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          yield* Effect.scoped(
            Effect.gen(function* () {
              let active = true
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  active = false
                }),
              )
              // A real invocation must supply its task/scope gate (for example ToolExecution.commit).
              const gate: ToolProgress.Gate<ExecutionError.Failure> = {
                commit: (change) =>
                  Effect.gen(function* () {
                    if (!active)
                      return yield* new ExecutionError.ExecutionError({
                        reason: 'revoked',
                        operation: 'example.progress.commit',
                        message: 'Invocation ended',
                      })
                    return yield* Session.commit(session, change)
                  }),
              }
              const writer = yield* ToolProgress.make(gate, {
                conversationId: root,
                callId: 'demo',
                attempt: 0,
                retention: { retain: 'tail', maxBytes: 24, maxLines: 2 },
                policy: { minInterval: 1000, bytesPerSecond: 102400 },
              })
              const raw = new TextEncoder().encode(`${'verbose output\n'.repeat(100)}last 😀\n`)
              const full = yield* spill.open
              yield* full.append('stdout', raw)
              const reference = yield* full.commit
              yield* writer.output(raw)
              const details = yield* writer
                .details({ spill: reference, phase: 'finished' })
                .pipe(Effect.forkChild({ startImmediately: true }))
              yield* writer.settle('completed', (tx, latest) =>
                Effect.gen(function* () {
                  // Idempotent inside this callback; staging alone cannot acknowledge the details caller.
                  yield* writer.flush(tx)
                  return yield* Transaction.appendEntry(tx, root, {
                    kind: 'example.tool.receipt',
                    data: {
                      callId: latest.callId,
                      status: latest.status,
                      output: latest.output,
                      details: latest.details ?? null,
                    },
                  })
                }),
              )
              yield* Fiber.join(details)
            }),
          )
        }),
      ).pipe(Effect.provide(storage()))

      // Cold replay: bounded state and receipt persisted, and the persistent reference survived the call scope.
      yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const found = yield* Session.snapshot(session, ToolProgress.document, {
            scope: { _tag: 'conversation', conversationId: root },
            key: 'demo',
          })
          if (Option.isNone(found)) return yield* Effect.die('Missing committed progress')
          const detail = yield* Schema.decodeUnknownEffect(
            Schema.Struct({ spill: ToolSpill.Reference }),
          )(found.value.value.details)
          yield* spill.revalidate(detail.spill)
          yield* Effect.log({
            retained: found.value.value.output.text,
            skippedBytes: found.value.value.output.skippedBytes,
            status: found.value.value.status,
          })
          // Persistent providers never silently delete a successful reference at invocation shutdown.
          yield* spill.remove(detail.spill)
        }),
      ).pipe(Effect.provide(storage()))
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
