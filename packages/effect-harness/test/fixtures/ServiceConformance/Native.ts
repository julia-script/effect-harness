import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as NodeSpawner from '@effect/platform-node/NodeChildProcessSpawner'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import type * as PlatformError from 'effect/PlatformError'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as Sink from 'effect/Sink'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Directory from 'effect-harness/DirectoryReader'
import * as DirectoryNode from 'effect-harness/DirectoryReaderNode'
import * as Reader from 'effect-harness/FileReader'
import * as Watch from 'effect-harness/FileWatch'
import * as WatchNode from 'effect-harness/FileWatchNode'
import * as Registry from 'effect-harness/Registry'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFiles from 'effect-harness/ToolkitFileSystem'
import * as ToolkitProcess from 'effect-harness/ToolkitProcess'
import * as Spill from 'effect-harness/ToolSpill'
import * as ToolExecution from 'effect-harness/ToolExecution'
import * as Record from 'effect-harness/Record'
import * as Bash from 'effect-harness/coding/Bash'
import * as ShellProcess from 'effect-harness/coding/ShellProcess'
import type { FileSystemFixture } from 'effect-harness/testing/FileSystem'
import type { ProcessFixture } from 'effect-harness/testing/Process'
import type { ToolkitIsolationFixture } from 'effect-harness/testing/ToolkitIsolation'
import * as Images from 'effect-harness/ImagePreparation'
import * as ImagesNode from 'effect-harness/images-node'
import * as ImagesWasm from 'effect-harness/images-wasm'
import * as ImageErrors from 'effect-harness/ImagePreparationError'
import type { ImagesFixture, OrientationSample } from 'effect-harness/testing/Images'
import sharp from 'sharp'
import * as Photon from '@cf-wasm/photon/node'

export const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
export const temp = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  return yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
})

export const fileFixture = (
  directory: (
    root: string,
  ) => Layer.Layer<Directory.DirectoryReader, Directory.DirectoryReaderError> = (root) =>
    DirectoryNode.layer({ root }),
  watch: (root: string) => Layer.Layer<Watch.FileWatch, Watch.FileWatchError> = (root) =>
    WatchNode.layer({ root }),
  backend = files,
) =>
  Effect.gen(function* () {
    const root = yield* temp.pipe(Effect.provide(backend))
    const services = yield* Layer.build(
      Layer.mergeAll(
        ToolkitFiles.layer({ root }).pipe(Layer.provide(backend)),
        Reader.layer.pipe(Layer.provide(ToolkitFiles.layer({ root }).pipe(Layer.provide(backend)))),
        directory(root),
        watch(root),
      ),
    )
    return {
      cleanup: Effect.void,
      root,
      fs: Context.get(services, FileSystem.FileSystem),
      path: Context.get(services, Path.Path),
      reader: Context.get(services, Reader.FileReader),
      directory: Context.get(services, Directory.DirectoryReader),
      watch: Context.get(services, Watch.FileWatch),
    } satisfies FileSystemFixture
  })

export const execution = Layer.succeed(ToolExecution.ToolExecution, {
  conversationId: Record.ConversationId.make(1),
  taskId: Record.TaskId.make(2),
  callId: 'conformance',
  snapshot: () => Effect.succeedNone,
  watch: () => Stream.empty,
  commit: () => Effect.die('No session commit in service conformance'),
  output: () => Effect.void,
  details: () => Effect.void,
  diagnostic: () => Effect.void,
})

/** Real native processes; the separately selected strict adapter is a scoped deterministic tree. */
export const processFixture = (
  strict = false,
  backend = files,
  spawnLayer = NodeSpawner.layer,
  lifetime: 'persistent' | 'toolkit' = 'persistent',
) =>
  Effect.gen(function* () {
    const root = yield* temp.pipe(Effect.provide(backend))
    const native = yield* Layer.build(spawnLayer.pipe(Layer.provide(backend)))
    const original = Context.get(native, ChildProcessSpawner.ChildProcessSpawner)
    let spawns = 0
    const owned = new Set<number>()
    const counted = Layer.succeed(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make((command) =>
        Effect.suspend(() => {
          spawns++
          return original.spawn(command)
        }),
      ),
    )
    const bound = Layer.merge(ToolkitFiles.layer({ root }), ToolkitProcess.layer({ root })).pipe(
      Layer.provide(Layer.merge(backend, counted)),
    )
    const strictLayer = Layer.succeed(ShellProcess.ShellProcess, {
      cleanup: 'owned-tree',
      spawn: () =>
        Effect.gen(function* () {
          spawns++
          owned.add(spawns)
          const identity = spawns
          let running = true
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              running = false
              owned.delete(identity)
            }),
          )
          return ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(99),
            stdout: Stream.make(new TextEncoder().encode('strict-ok')),
            stderr: Stream.empty,
            all: Stream.empty,
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
            isRunning: Effect.sync(() => running),
            kill: () =>
              Effect.sync(() => {
                running = false
              }),
            stdin: Sink.drain,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
            unref: Effect.succeed(Effect.void),
          })
        }),
    })
    const spillScope = yield* Scope.fork(yield* Scope.Scope)
    const spillLayer = Spill.layer({ provider: root, directory: `${root}/spills`, lifetime }).pipe(
      Layer.provide(bound),
    )
    const spill = Context.get(
      yield* Layer.build(spillLayer).pipe(Effect.provideService(Scope.Scope, spillScope)),
      Spill.ToolSpill,
    )
    const services = yield* Layer.build(
      Layer.mergeAll(
        strict ? strictLayer : ShellProcess.layerNative,
        Layer.succeed(Spill.ToolSpill, spill),
        execution,
      ).pipe(Layer.provideMerge(bound)),
    )
    return {
      cleanup: Effect.void,
      root,
      fs: Context.get(services, FileSystem.FileSystem),
      spawner: Context.get(services, ChildProcessSpawner.ChildProcessSpawner),
      path: Context.get(services, Path.Path),
      ownedTreeEmpty: Effect.sync(() => owned.size === 0),
      literal: (args, cwd) =>
        ChildProcess.make(
          process.execPath,
          [
            '-e',
            'process.stdout.write(JSON.stringify(process.argv.slice(1))); process.stderr.write(process.cwd())',
            ...args,
          ],
          { cwd, shell: false },
        ),
      waiting: ChildProcess.make(
        process.execPath,
        ['-e', "process.stdout.write('ready'); setInterval(()=>{},1000)"],
        { forceKillAfter: '100 millis' },
      ),
      shellArgv: [process.execPath, '-e', "process.stdout.write('strict-ok')"],
      cleanupCapability: strict ? 'owned-tree' : 'best-effort-process-group',
      spawnCount: Effect.sync(() => spawns),
      runShell: (input, options) => Bash.run(input, options).pipe(Effect.provideContext(services)),
      spill,
      closeSpillProvider: Scope.close(spillScope, Exit.void),
      reopenSpill: Layer.build(spillLayer).pipe(
        Effect.map((context) => Context.get(context, Spill.ToolSpill)),
      ),
    } satisfies ProcessFixture<PlatformError.PlatformError>
  })

class Label extends Context.Service<Label, string>()('test/service-conformance/Label') {}
const declaration = Tool.make('same', {
  success: Schema.String,
  failure: Schema.String,
}).addDependency(Label)
const toolkit = Toolkit.make(declaration)
const handlers = toolkit.toLayer({ same: () => Label })

export const isolationFixture = Effect.gen(function* () {
  const registry = yield* Registry.make
  const released = new Set<string>()
  const registration = (id: string, value: string, version: number) =>
    Registry.provide(
      Registry.toolkit({ id, version }, toolkit),
      handlers.pipe(
        Layer.provideMerge(
          Layer.effect(
            Label,
            Effect.acquireRelease(Effect.succeed(value), () =>
              Effect.sync(() => {
                released.add(value)
              }),
            ),
          ),
        ),
      ),
    )
  return {
    cleanup: Effect.void,
    registry,
    a: registration('a', 'A', 1),
    b: registration('b', 'B', 1),
    aReload: registration('a', 'A2', 2),
    call: { id: 'conformance', name: 'same', arguments: {} },
    expectedA: 'A',
    expectedB: 'B',
    expectedReload: 'A2',
    read: (result) => result.details,
    released: (key) => Effect.sync(() => released.has({ a: 'A', b: 'B', reload: 'A2' }[key])),
  } satisfies ToolkitIsolationFixture
})

export const imageFixture = (
  imageLayer: Layer.Layer<
    Images.ImagePreparation,
    ImageErrors.ImagePreparationError
  > = ImagesNode.layer,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const load = (name: string) =>
      fs.readFile(new URL(`../images-native/${name}`, import.meta.url).pathname)
    const service = Context.get(yield* Layer.build(imageLayer), Images.ImagePreparation)
    const colors = [
      [255, 0, 0, 255],
      [0, 255, 0, 255],
      [0, 0, 255, 255],
      [255, 255, 0, 255],
      [255, 0, 255, 255],
      [0, 255, 255, 255],
    ]
    const grids = [
      [0, 1, 2, 3, 4, 5],
      [2, 1, 0, 5, 4, 3],
      [5, 4, 3, 2, 1, 0],
      [3, 4, 5, 0, 1, 2],
      [0, 3, 1, 4, 2, 5],
      [3, 0, 4, 1, 5, 2],
      [5, 2, 4, 1, 3, 0],
      [2, 5, 1, 4, 0, 3],
    ]
    const orientations: OrientationSample[] = []
    for (const orientation of [1, 2, 3, 4, 5, 6, 7, 8] as const)
      orientations.push({
        bytes: yield* load(`orientation-${orientation}.png`),
        mimeType: 'image/png',
        dimensions: { width: 3, height: 2 },
        orientation,
        upright: orientation < 5 ? { width: 3, height: 2 } : { width: 2, height: 3 },
        rgba: grids[orientation - 1]!.flatMap((i) => colors[i]!),
      })
    const bytes = yield* load('opaque.png')
    return {
      cleanup: Effect.void,
      service,
      sample: { bytes, mimeType: 'image/png', dimensions: { width: 3, height: 2 } },
      malformed: bytes.subarray(0, bytes.length - 20),
      orientations,
      inspect: (bytes: Uint8Array) =>
        Effect.tryPromise({
          try: async () => {
            const decoded = await sharp(bytes)
              .ensureAlpha()
              .raw()
              .toBuffer({ resolveWithObject: true })
            const metadata = await sharp(bytes).metadata()
            return {
              dimensions: { width: decoded.info.width, height: decoded.info.height },
              rgba: [...decoded.data],
              orientation: metadata.orientation,
            }
          },
          catch: (cause) =>
            new ImageErrors.ImageDecodeFailed({
              mimeType: 'image/png',
              message: 'Independent inspector failed',
              cause,
            }),
        }),
    } satisfies ImagesFixture<ImageErrors.ImageDecodeFailed>
  }).pipe(Effect.provide(files))

/** Holds a real sharp decode result behind a completion gate while preserving the actual native handle. */
export const imageLeaseFixture = Effect.gen(function* () {
  const fixture = yield* imageFixture()
  let armed = false
  let released = false
  let announce: () => void = () => {}
  let finish: () => void = () => {}
  const started = new Promise<void>((resolve) => {
    announce = resolve
  })
  const pending = new Promise<void>((resolve) => {
    finish = resolve
  })
  const codec: typeof sharp = new Proxy(sharp, {
    apply(target, _receiver, args: Parameters<typeof sharp>) {
      const image = target(...args)
      const destroy = image.destroy.bind(image)
      image.destroy = (error) => {
        released = true
        return destroy(error)
      }
      if (armed) {
        image.toBuffer = new Proxy(image.toBuffer.bind(image), {
          apply(buffer, receiver, args: [{ resolveWithObject: true }]) {
            const actual = buffer.call(receiver, ...args)
            announce()
            return actual.then(async (result) => {
              await pending
              return result
            })
          },
        })
      }
      return image
    },
  })
  return {
    ...fixture,
    service: Images.make(ImagesNode.makeAdapter(codec, ImagesWasm.makeAdapter(Photon))),
    lease: {
      arm: Effect.sync(() => {
        armed = true
      }),
      started: Effect.promise(() => started),
      settle: Effect.sync(() => finish()),
      released: Effect.sync(() => released),
    },
  }
})
