import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { strict as assert } from 'node:assert'
import * as Context from 'effect/Context'
import * as Clock from 'effect/Clock'
import * as Fiber from 'effect/Fiber'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import type * as Agent from 'effect-harness/Agent'
import * as DirectoryReader from 'effect-harness/DirectoryReader'
import * as FileWatch from 'effect-harness/FileWatch'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as ImagePreparation from 'effect-harness/ImagePreparation'
import type { ImagePreparationError } from 'effect-harness/ImagePreparationError'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as ToolkitProcess from 'effect-harness/ToolkitProcess'
import * as Transaction from 'effect-harness/Transaction'
import { ExecutionError } from 'effect-harness/ExecutionError'

const bun = 'Bun' in globalThis
export const services = bun
  ? (await import('@effect/platform-bun')).BunServices.layer
  : (await import('@effect/platform-node')).NodeServices.layer
const images: Layer.Layer<ImagePreparation.ImagePreparation, ImagePreparationError> = bun
  ? (await import('effect-harness/images-bun')).layer
  : (await import('effect-harness/images-node')).layer
const watchAdapter = bun
  ? (await import('effect-harness/FileWatchBun')).adapter
  : (await import('effect-harness/FileWatchNode')).adapter
const directoryReader = bun
  ? (await import('effect-harness/DirectoryReaderBun')).layer
  : (await import('effect-harness/DirectoryReaderNode')).layer
const sqlite = bun
  ? (await import('@effect/sql-sqlite-bun/SqliteClient')).layer
  : (await import('@effect/sql-sqlite-node/SqliteClient')).layer

const Manifest = Schema.Struct({
  a: Record.ConversationId,
  b: Record.ConversationId,
  fork: Record.ConversationId,
})
export type Backend = 'memory' | 'sqlite' | 'jsonl'
export type Phase = 'seed' | 'cold' | 'prepare-denial' | 'missing' | 'version'
const selection = (owner: 'A' | 'B'): Agent.State => ({
  tools: ['native-bundle'],
  toolkits: {
    inherit: false,
    registrations: [owner],
    bindings: { 'native-bundle': { binding: owner, version: 1 } },
  },
})
const finish: Response.PartEncoded = {
  type: 'finish',
  reason: 'tool-calls',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}
// The inherited native image fixture, embedded to keep the public consumer self-contained.
const png = Uint8Array.from(
  Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAMAAAACCAYAAACddGYaAAAACXBIWXMAAAPoAAAD6AG1e1JrAAAAFElEQVQImWP4z8DwHwwZ/v9HRv8Bqm8O8jjYJxEAAAAASUVORK5CYII=',
    'base64',
  ),
)

/** Reconstruct host services from stable registration IDs; live services are never serialized. */
export const run = (backend: Backend, location: string, phase: Phase) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      yield* fs.makeDirectory(location, { recursive: true })
      const root = yield* fs.realPath(location)
      const paths = { A: `${root}/A`, B: `${root}/B`, ambient: `${root}/ambient` }
      for (const [label, directory] of Object.entries(paths)) {
        yield* fs.makeDirectory(`${directory}/effects`, { recursive: true })
        yield* fs.writeFileString(`${directory}/marker.txt`, label)
        yield* fs.writeFile(`${directory}/same.png`, png)
      }
      const counts = { A: 0, B: 0, ambient: 0, provider: 0 }
      const prepared = { A: 0, B: 0, ambient: 0 }
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const pendingEntered = yield* Deferred.make<void>()
      const nativeImages = yield* Layer.build(images).pipe(
        Effect.map((c) => Context.get(c, ImagePreparation.ImagePreparation)),
      )
      const ready = new Map<string, Deferred.Deferred<void>>()
      const nativeAdapter = yield* Layer.build(watchAdapter).pipe(
        Effect.map((c) => Context.get(c, FileWatch.Adapter)),
      )
      const observedAdapter = Layer.succeed(FileWatch.Adapter, {
        subscribe: (targets, observer, options) =>
          Effect.gen(function* () {
            const clock = yield* Clock.Clock
            const settled = yield* Deferred.make<void>()
            let worker: number | undefined
            const observedClock: Clock.Clock = {
              currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe(),
              currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
              monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
              currentTimeMillis: clock.currentTimeMillis,
              currentTimeNanos: clock.currentTimeNanos,
              monotonicTimeNanos: clock.monotonicTimeNanos,
              sleep: (duration) =>
                Effect.withFiber((fiber) => {
                  worker = fiber.id
                  return clock.sleep(duration)
                }),
            }
            for (const target of targets) ready.set(target.path, settled)
            return yield* nativeAdapter
              .subscribe(
                targets,
                {
                  fail: observer.fail,
                  emit: (event) => {
                    if (
                      event._tag === 'Rescan' &&
                      event.reason === 'coverage' &&
                      worker !== undefined &&
                      Fiber.getCurrent()?.id === worker
                    )
                      Deferred.doneUnsafe(settled, Effect.void)
                    observer.emit(event)
                  },
                },
                options,
              )
              .pipe(Effect.provideService(Clock.Clock, observedClock))
          }),
      })
      const bound = (label: keyof typeof paths) =>
        Layer.mergeAll(
          ToolkitFileSystem.layer({ root: paths[label], pathPolicy: 'within-root' }),
          ToolkitProcess.layer({ root: paths[label], env: { RUNTIME_BINDING: label } }),
          FileWatch.layer({ root: paths[label], pathPolicy: 'within-root' }).pipe(
            Layer.provide([observedAdapter, services]),
          ),
          directoryReader({ root: paths[label], pathPolicy: 'within-root' }),
          Layer.succeed(ImagePreparation.ImagePreparation, {
            prepare: (bytes, mime, limits) =>
              Effect.sync(() => prepared[label]++).pipe(
                Effect.andThen(nativeImages.prepare(bytes, mime, limits)),
              ),
          }),
        )
      const contexts = {
        A: yield* Layer.build(bound('A')),
        B: yield* Layer.build(bound('B')),
        ambient: yield* Layer.build(bound('ambient')),
      }
      const toolkit = Toolkit.make(
        Tool.makeResult('native-bundle', {
          parameters: Schema.Struct({ token: Schema.String, hold: Schema.Boolean }),
          success: Schema.String,
          replay: 'safe',
          resultValidation: 'strict',
        })
          .addDependency(FileSystem.FileSystem)
          .addDependency(Path.Path)
          .addDependency(ChildProcessSpawner.ChildProcessSpawner)
          .addDependency(ImagePreparation.ImagePreparation)
          .addDependency(DirectoryReader.DirectoryReader)
          .addDependency(FileWatch.FileWatch),
      )
      const registry = yield* Registry.make
      for (const label of ['A', 'B'] as const) {
        if (phase === 'missing' && label === 'A') continue
        const version = phase === 'version' && label === 'A' ? 2 : 1
        yield* registry.install(
          Registry.provide(
            Registry.toolkit({ id: label, version }, toolkit),
            toolkit
              .toLayer({
                'native-bundle': ({ token, hold }) =>
                  Effect.gen(function* () {
                    counts[label]++
                    const selectedFs = yield* FileSystem.FileSystem
                    const selectedPath = yield* Path.Path
                    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
                    const selectedImages = yield* ImagePreparation.ImagePreparation
                    const selectedWatch = yield* FileWatch.FileWatch
                    const selectedReader = yield* DirectoryReader.DirectoryReader
                    assert.equal(yield* selectedFs.readFileString('marker.txt'), label)
                    assert.equal(selectedPath.resolve('effects'), `${paths[label]}/effects`)
                    const reader = yield* selectedReader.open('effects', {
                      requireOpenedIdentity: true,
                      requireBoundedEnumeration: true,
                    })
                    assert.equal(reader.path, `${paths[label]}/effects`)
                    assert.equal(reader.capabilities.identity, 'opened-directory')
                    assert.equal(reader.capabilities.allocation, 'native-buffer')
                    const observed = yield* selectedWatch.open([{ path: 'effects' }])
                    assert.equal(observed.coverage.mode, 'native')
                    // Observe the genuine Clock-driven settling worker, not callback rescans.
                    const settled = ready.get(`${paths[label]}/effects`)
                    assert(settled, 'native subscription installed')
                    yield* Deferred.await(settled).pipe(Effect.timeout('5 seconds'))
                    // Exclude initial/settling coverage rescans: require an actual native change.
                    const pull = yield* Stream.toPull(
                      observed.events.pipe(Stream.filter((event) => event._tag === 'Change')),
                    )
                    if (hold) {
                      yield* Deferred.succeed(entered, undefined)
                      yield* Deferred.await(release)
                    }
                    yield* selectedFs.writeFileString(`effects/${token}.txt`, label)
                    const events = yield* pull.pipe(Effect.timeout('5 seconds'))
                    assert(
                      events.some(
                        (event) =>
                          event._tag === 'Change' &&
                          event.path === `${paths[label]}/effects/${token}.txt`,
                      ),
                      'native watch reports selected file',
                    )
                    assert.equal(yield* selectedFs.readFileString(`effects/${token}.txt`), label)
                    // The retained opened directory handle stays valid while the other conversation runs.
                    const names: string[] = []
                    let done = false
                    while (!done) {
                      const page = yield* reader.next(1)
                      assert(page.entries.length <= 1)
                      names.push(...page.entries.map((entry) => entry.name))
                      done = page.done
                    }
                    assert(names.includes(`${token}.txt`), 'opened directory reads selected root')
                    yield* reader.close
                    assert(Exit.isFailure(yield* reader.next(1).pipe(Effect.exit)))
                    assert.equal(
                      yield* spawner.string(
                        ChildProcess.make(process.execPath, [
                          '-e',
                          "process.stdout.write(JSON.stringify([require('node:fs').readFileSync('marker.txt','utf8'),process.env.RUNTIME_BINDING,process.cwd(),process.argv.slice(1)]))",
                          'literal ; $HOME',
                        ]),
                      ),
                      JSON.stringify([label, label, paths[label], ['literal ; $HOME']]),
                    )
                    const image = yield* selectedImages.prepare(
                      yield* selectedFs.readFile('same.png'),
                      'image/png',
                      { maxWidth: 10, maxHeight: 10, maxBytes: 100000, maxInputPixels: 100 },
                    )
                    assert.deepEqual(image.dimensions, { width: 3, height: 2 })
                    yield* observed.close
                    yield* observed.close
                    assert.deepEqual(yield* Stream.runCollect(observed.events), [])
                    return {
                      content: [Prompt.textPart({ text: label })],
                      structuredOutput: label,
                      control: { terminate: true as const },
                    }
                  }).pipe(
                    Effect.mapError(
                      (cause) =>
                        new ExecutionError({
                          reason: 'invalid',
                          operation: 'binding.consumer',
                          message: 'Native binding consumer failed',
                          cause,
                        }),
                    ),
                  ),
              })
              .pipe(Layer.provideMerge(Layer.succeedContext(contexts[label]))),
          ),
        )
      }
      const model = yield* LanguageModel.make({
        generateText: ({ prompt }) =>
          Effect.sync(() => {
            counts.provider++
            const token = prompt.content
              .flatMap((message) =>
                message.role === 'user'
                  ? message.content.filter((part) => part.type === 'text').map((part) => part.text)
                  : [],
              )
              .at(-1)!
            return [
              {
                type: 'tool-call' as const,
                id: `call-${token}`,
                name: 'native-bundle',
                params: { token, hold: token === 'held' },
                providerExecuted: false,
              },
              finish,
            ]
          }).pipe(
            Effect.tap(() =>
              phase === 'prepare-denial'
                ? Deferred.succeed(pendingEntered, undefined).pipe(Effect.andThen(Effect.never))
                : Effect.void,
            ),
          ),
        streamText: ({ prompt }) =>
          Stream.fromEffect(
            Effect.sync(() => {
              counts.provider++
              const token = prompt.content
                .flatMap((message) =>
                  message.role === 'user'
                    ? message.content
                        .filter((part) => part.type === 'text')
                        .map((part) => part.text)
                    : [],
                )
                .at(-1)!
              return {
                type: 'tool-call' as const,
                id: `call-${token}`,
                name: 'native-bundle',
                params: { token, hold: token === 'held' },
                providerExecuted: false,
              }
            }).pipe(
              Effect.tap(() =>
                phase === 'prepare-denial'
                  ? Deferred.succeed(pendingEntered, undefined).pipe(Effect.andThen(Effect.never))
                  : Effect.void,
              ),
            ),
          ).pipe(Stream.concat(Stream.succeed(finish))),
      })
      const storage = (() => {
        if (backend === 'memory') return Storage.layerMemory
        if (backend === 'jsonl') return Storage.layerJsonl({ filePath: `${root}/store.jsonl` })
        return Storage.layerJournal().pipe(
          Layer.provide(SqlEventJournal.layer()),
          Layer.provide(sqlite({ filename: `${root}/store.sqlite` })),
        )
      })()
      const storageContext = yield* Layer.build(storage)
      const runtime = yield* HarnessRuntime.make({
        registry,
        agent: selection('B'),
        nativeModel: { id: 'binding-provider', version: 1 },
      }).pipe(
        Effect.provideContext(storageContext),
        Effect.provideService(LanguageModel.LanguageModel, model),
        Effect.provideContext(contexts.ambient),
      )
      const submit = (conversationId: Record.ConversationId, token: string) =>
        runtime.backend.submit({ conversationId, draft: { type: 'input', content: token } })
      const invoke = (conversationId: Record.ConversationId, token: string) =>
        Effect.gen(function* () {
          const job = yield* submit(conversationId, token)
          const outcome = yield* runtime.backend.wait(job.id)
          assert.equal(outcome.status, 'done', JSON.stringify(outcome))
        })
      if (phase === 'seed') {
        const a = yield* runtime.backend.create({ agent: selection('A') })
        const b = yield* runtime.backend.create()
        const marker = yield* Session.commit(runtime.session, (tx) =>
          Transaction.appendEntry(tx, a, { kind: 'binding.historical' }),
        )
        const held = yield* submit(a, 'held')
        yield* Deferred.await(entered)
        yield* runtime.backend.configure({ conversationId: a, change: selection('B') })
        yield* invoke(b, 'independent')
        assert.deepEqual(counts, { A: 1, B: 1, ambient: 0, provider: 2 })
        assert.equal(yield* fs.exists(`${paths.A}/effects/held.txt`), false)
        yield* Deferred.succeed(release, undefined)
        assert.equal((yield* runtime.backend.wait(held.id)).status, 'done')
        const fork = yield* runtime.backend.fork({ conversationId: a, options: { at: marker.id } })
        assert.deepEqual((yield* runtime.backend.agent(fork)).toolkits, selection('A').toolkits)
        yield* invoke(fork, 'historical')
        yield* invoke(a, 'configured')
        assert.deepEqual(counts, { A: 2, B: 2, ambient: 0, provider: 4 })
        assert.deepEqual(prepared, { A: 2, B: 2, ambient: 0 })
        yield* fs.writeFileString(
          `${root}/manifest.json`,
          yield* Schema.encodeEffect(Schema.fromJsonString(Manifest))({ a, b, fork }),
        )
      } else {
        const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(
          yield* fs.readFileString(`${root}/manifest.json`),
        )
        assert.deepEqual(
          (yield* runtime.backend.agent(manifest.fork)).toolkits,
          selection('A').toolkits,
        )
        assert.deepEqual(
          (yield* runtime.backend.agent(manifest.a)).toolkits,
          selection('B').toolkits,
        )
        assert.deepEqual(
          counts,
          { A: 0, B: 0, ambient: 0, provider: 0 },
          'reopening completed runs never replays provider',
        )
        if (phase === 'cold') {
          yield* invoke(manifest.fork, 'cold-fork')
          yield* invoke(manifest.a, 'cold-configured')
          yield* invoke(manifest.b, 'cold-independent')
          assert.deepEqual(counts, { A: 1, B: 2, ambient: 0, provider: 3 })
          assert.deepEqual(prepared, { A: 1, B: 2, ambient: 0 })
        } else if (phase === 'prepare-denial') {
          const job = yield* submit(manifest.fork, 'deny-held')
          yield* Deferred.await(pendingEntered).pipe(Effect.timeout('5 seconds'))
          const tasks = yield* Stream.runCollect(
            Session.scanTasks(runtime.session, {
              conversationId: manifest.fork,
              kind: 'harness.run',
            }),
          )
          const task = tasks.findLast((record) => record.state.status !== 'terminal')
          assert(task, 'actual saved native preparation exists')
          yield* fs.writeFileString(
            `${root}/denial.json`,
            JSON.stringify({ taskId: task.id, submissionId: job.id }),
          )
          assert.deepEqual(
            counts,
            { A: 0, B: 0, ambient: 0, provider: 1 },
            'only the original native provider attempt entered',
          )
        } else {
          const denial = yield* Schema.decodeEffect(
            Schema.fromJsonString(
              Schema.Struct({ taskId: Record.TaskId, submissionId: Record.SubmissionId }),
            ),
          )(yield* fs.readFileString(`${root}/denial.json`))
          yield* runtime.scheduler.resume
          const inspected = yield* runtime.scheduler.inspect(denial.taskId)
          assert.equal(
            inspected.scheduling,
            'blocked',
            'saved preparation rejects missing or incompatible owner',
          )
          assert.equal(inspected.resolution._tag, 'blocked')
          if (inspected.resolution._tag === 'blocked')
            assert.match(inspected.resolution.error.message, /registration|binding/i)
          assert.equal(inspected.invoking, false)
          const pending = yield* runtime.backend.read(denial.submissionId)
          assert.notEqual(pending.status, 'done')
          assert.deepEqual(
            counts,
            { A: 0, B: 0, ambient: 0, provider: 0 },
            'binding denial precedes provider and native effects',
          )
          assert.deepEqual(prepared, { A: 0, B: 0, ambient: 0 })
          for (const directory of Object.values(paths))
            assert.equal(yield* fs.exists(`${directory}/effects/denied-${phase}.txt`), false)
        }
      }
      assert.deepEqual(
        yield* fs.readDirectory(`${paths.ambient}/effects`),
        [],
        'ambient poison root remains untouched',
      )
      assert(Option.isNone(yield* runtime.scheduler.failure))
      return { backend, phase, host: bun ? 'bun' : 'node', counts, prepared }
    }),
  )

/** Run only the changed public structural contracts on the matching native driver. */
export const structural = (backend: Backend) =>
  Effect.gen(function* () {
    const names = new Set([
      'storage: safe-integer allocator exhaustion',
      'storage: prototype-sensitive JSON keys',
      'storage: lossless indexed string identities',
      'storage: out-of-ID-order entry streams',
      'storage: multigeneration fork cutoffs',
      'storage: waiting and completing task records',
      'storage: passive write submission records',
      'storage: long document replacement tails',
    ])
    const directories: string[] = []
    const cases = Testing.storageConformance({
      make: Effect.gen(function* () {
        if (backend === 'memory')
          return {
            open: Layer.build(Storage.layerMemory).pipe(
              Effect.map((c) => Context.get(c, Storage.Storage)),
            ),
          }
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'structural-reopen-' })
        directories.push(directory)
        const layer =
          backend === 'sqlite'
            ? Storage.layerJournal().pipe(
                Layer.provide(SqlEventJournal.layer()),
                Layer.provide(sqlite({ filename: `${directory}/storage` })),
              )
            : Storage.layerJsonl({ filePath: `${directory}/storage` })
        return {
          open: Layer.build(Layer.fresh(layer)).pipe(
            Effect.map((c) => Context.get(c, Storage.Storage)),
          ),
        }
      }),
      capabilities: { history: true, reopen: backend !== 'memory' },
    }).filter((c) => names.has(c.name))
    assert.equal(cases.length, names.size)
    for (const c of cases) yield* c.run
    const fs = yield* FileSystem.FileSystem
    for (const directory of directories)
      assert.equal(yield* fs.exists(directory), false, 'factory cleanup after actual reopen')
    return {
      backend,
      phase: 'structural',
      host: bun ? 'bun' : 'node',
      cases: cases.map((c) => c.name),
    }
  })
