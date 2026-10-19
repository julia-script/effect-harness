import * as Config from 'effect/Config'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Subagent from 'effect-harness/Subagent'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Usage from 'effect-harness/Usage'

// Build first, then run node or bun apps/example/dist/tour/DurableSubagent.js.
// The memory default is a complete example. For disk recovery, set backend/file,
// start with HOLD=true, wait for READY, SIGKILL, and reopen with HOLD=false.
// Memory recovery keeps the original Storage service in the same process instead.
const nativeBun = 'Bun' in globalThis
const services = nativeBun
  ? (await import('@effect/platform-bun')).BunServices.layer
  : (await import('@effect/platform-node')).NodeServices.layer
const contract = {
  kind: 'example.subagent.program',
  version: 1,
  input: Schema.Literals(['foreground', 'background']),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.tag('admit') }),
    Schema.Struct({ phase: Schema.tag('consume'), handle: Subagent.HandleSchema }),
  ]),
  success: Subagent.ReceiptSchema,
  failure: Schema.String,
} as const

const run = Effect.scoped(
  Effect.gen(function* () {
    const backend = yield* Config.Literals(
      ['memory', 'sqlite', 'jsonl'],
      'HARNESS_SUBAGENT_BACKEND',
    ).pipe(Config.withDefault('memory'))
    const mode = yield* Config.Literals(['foreground', 'background'], 'HARNESS_SUBAGENT_MODE').pipe(
      Config.withDefault('foreground'),
    )
    const file = yield* Config.String('HARNESS_SUBAGENT_FILE').pipe(Config.withDefault(''))
    const hold = yield* Config.Boolean('HARNESS_SUBAGENT_HOLD').pipe(Config.withDefault(false))
    const cold = yield* Config.Boolean('HARNESS_SUBAGENT_COLD').pipe(Config.withDefault(false))
    const pauseProvider = yield* Config.Boolean('HARNESS_SUBAGENT_PAUSE_PROVIDER').pipe(
      Config.withDefault(false),
    )
    const stateFile = yield* Config.String('HARNESS_SUBAGENT_STATE').pipe(Config.withDefault(''))
    let requests = 0
    const program = Task.make({
      ...contract,
      initial: () => ({ phase: 'admit' as const }),
      phases: {
        admit: ({ input }) =>
          Effect.gen(function* () {
            const access = yield* TaskExecution
            yield* access.commit((tx) =>
              Effect.gen(function* () {
                if (access.subagent === undefined)
                  return yield* Effect.die('No native subagent integration')
                const handle = yield* access.subagent.admit(tx, {
                  draft: { type: 'input', content: 'Return a supervised child answer' },
                  options: { key: 'child', mode: input },
                })
                yield* access.progress(tx, contract, {
                  status: 'waiting',
                  on: [handle.taskId],
                  policy: 'allSettled',
                  checkpoint: { phase: 'consume', handle },
                })
              }),
            )
          }),
        consume: ({ checkpoint }) =>
          Effect.gen(function* () {
            const access = yield* TaskExecution
            const result = yield* Subagent.wait(access, checkpoint.handle)
            if (result.receipt === undefined)
              return yield* Effect.die('The child has no saved receipt')
            const receipt = result.receipt
            if (hold) {
              if (stateFile !== '') {
                const fs = yield* FileSystem.FileSystem
                const encoded = yield* Schema.encodeEffect(
                  Schema.fromJsonString(Subagent.ReceiptSchema),
                )(receipt)
                yield* fs.writeFileString(stateFile, encoded)
              }
              yield* Console.log('READY')
              return yield* Effect.never
            }
            yield* access.commit((tx) =>
              access.progress(tx, contract, {
                status: 'completing',
                outcome: { status: 'completed', result: receipt },
              }),
            )
          }),
      },
    })
    const model = yield* LanguageModel.make({
      generateText: () => Effect.die('This example exercises native streaming'),
      streamText: () =>
        Stream.unwrap(
          Effect.gen(function* () {
            if (pauseProvider) {
              yield* Console.log('READY')
              return Stream.never
            }
            if (cold) return Stream.die('Saved paid work must not invoke the provider again')
            requests++
            return Stream.fromIterable([
              { type: 'text-start' as const, id: 'answer' },
              { type: 'text-delta' as const, id: 'answer', delta: 'Supervised child answer' },
              { type: 'text-end' as const, id: 'answer' },
              {
                type: 'finish' as const,
                reason: 'stop' as const,
                usage: {
                  inputTokens: {
                    total: 7,
                    uncached: 7,
                    cacheRead: undefined,
                    cacheWrite: undefined,
                  },
                  outputTokens: { total: 4, text: 4, reasoning: undefined },
                },
                response: undefined,
              },
            ])
          }),
        ),
    })
    const sqlite = nativeBun
      ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
      : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
    const storage = (() => {
      if (backend === 'memory') return Storage.layerMemory
      if (backend === 'jsonl') return Storage.layerJsonl({ filePath: file })
      return Storage.layerSql().pipe(Layer.provide(sqlite({ filename: file })))
    })()
    const context = yield* Layer.build(storage)
    const registry = yield* Registry.make
    yield* registry.install(
      Registry.withTask(
        Registry.toolkit({ id: 'example-subagent', version: 1 }, Toolkit.make()),
        program,
      ),
    )
    const runtime = yield* HarnessRuntime.make({ registry }).pipe(
      Effect.provideContext(context),
      Effect.provideService(LanguageModel.LanguageModel, model),
    )
    const records = yield* Session.scanTasks(runtime.session, { kind: contract.kind }).pipe(
      Stream.runCollect,
    )
    const existing = records[0]
    if (cold && existing === undefined)
      return yield* Effect.die('Cold recovery has no saved parent')
    const ref =
      existing === undefined
        ? yield* runtime.scheduler.start(program, mode, {
            conversationId: yield* runtime.backend.root,
          })
        : { id: existing.id, definition: program }
    const result = yield* runtime.scheduler.wait(ref)
    if (result.status !== 'completed') return yield* Effect.die(result)
    const receipt = result.result
    const spend = yield* Session.commit(runtime.session, (tx) =>
      Usage.receipts(tx, receipt.conversationId),
    )
    const entries = yield* Session.scanEntries(runtime.session, {
      conversationId: receipt.conversationId,
    }).pipe(Stream.runCollect)
    if (
      spend.length !== 1 ||
      entries.filter((entry) => entry.kind === 'assistant').length !== 1 ||
      Option.isSome(yield* runtime.scheduler.failure)
    )
      return yield* Effect.die('Saved terminal accounting changed')
    const report = yield* Schema.encodeEffect(
      Schema.fromJsonString(
        Schema.Struct({
          receipt: Subagent.ReceiptSchema,
          requests: Schema.Natural,
          usageReceipts: Schema.Natural,
          assistantEntries: Schema.Natural,
        }),
      ),
    )({
      receipt,
      requests,
      usageReceipts: spend.length,
      assistantEntries: entries.filter((entry) => entry.kind === 'assistant').length,
    })
    yield* Console.log(report)
  }),
).pipe(Effect.provide(services))

if (nativeBun) (await import('@effect/platform-bun/BunRuntime')).runMain(run)
else (await import('@effect/platform-node/NodeRuntime')).runMain(run)
