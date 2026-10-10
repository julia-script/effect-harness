import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Result from 'effect/Result'
import * as Stream from 'effect/Stream'
import * as Context from 'effect/Context'
import * as Queue from 'effect/Queue'
import type * as Cause from 'effect/Cause'
import * as Watch from 'effect-harness/FileWatch'
import * as ImagesWasm from 'effect-harness/images-wasm'
import * as ImageErrors from 'effect-harness/ImagePreparationError'
import * as FileChecks from 'effect-harness/testing/FileSystem'
import * as ProcessChecks from 'effect-harness/testing/Process'
import * as ImageChecks from 'effect-harness/testing/Images'
import * as IsolationChecks from 'effect-harness/testing/ToolkitIsolation'
import {
  fileFixture,
  processFixture,
  isolationFixture,
  imageFixture,
  imageLeaseFixture,
  execution,
} from './fixtures/ServiceConformance/Native.js'

const files = () =>
  FileChecks.fileSystemConformance({
    make: fileFixture(),
    capabilities: { noFollow: 'unsupported', boundedDirectory: true, watch: true },
  })
const processes = (strict = false) =>
  ProcessChecks.processConformance({
    make: processFixture(strict),
    capabilities: { strictOwnership: true, spill: true, spillLifetime: 'persistent' },
  })
const one = <E, R>(cases: ReadonlyArray<FileChecks.ConformanceCase<E, R>>, name: string) => {
  const found = cases.find((c) => c.name === name)
  assert.isDefined(found, `Named conformance case must exist: ${name}`)
  return found!.run
}
const red = <E, R>(run: Effect.Effect<void, E, R>, caseName: string) =>
  Effect.gen(function* () {
    const result = yield* Effect.result(run)
    assert.isTrue(Result.isFailure(result), 'Broken fixture must fail, not disappear')
    if (Result.isFailure(result)) {
      assert.instanceOf(result.failure, FileChecks.ServiceConformanceError)
      if (result.failure instanceof FileChecks.ServiceConformanceError)
        assert.strictEqual(result.failure.caseName, caseName)
    }
  })

describe('public bound service conformance', () => {
  for (const c of files()) it.live(c.name, () => c.run)
  for (const c of processes()) it.live(c.name, () => c.run)
  for (const c of processes(true).filter((c) => c.name.includes('strict cleanup')))
    it.live(`custom owned tree: ${c.name}`, () => c.run)
  it.live('toolkit spill expires when its provider scope closes', () =>
    one(
      ProcessChecks.processConformance({
        make: processFixture(false, undefined, undefined, 'toolkit'),
        capabilities: { spillLifetime: 'toolkit' },
      }),
      'process: spill provider lifetime and reopen',
    ),
  )
  for (const [name, make] of [
    ['Node', imageFixture()],
    ['WASM', imageFixture(ImagesWasm.layer)],
  ] as const)
    for (const c of ImageChecks.imagesConformance({ make, capabilities: { orientations: true } }))
      it.live(`${name}: ${c.name}`, () => c.run)
  for (const c of ImageChecks.imagesConformance({
    make: imageLeaseFixture,
    capabilities: { lease: true },
  }).filter((c) => c.name.includes('lease')))
    it.live(`real sharp: ${c.name}`, () => c.run)
  for (const c of IsolationChecks.toolkitIsolationConformance({
    make: isolationFixture,
    capabilities: { leases: true },
  }))
    it.effect(c.name, () => c.run.pipe(Effect.provide(execution)))

  it.live('overflow is tested through the real public watch queue', () => {
    const make = Effect.gen(function* () {
      const f = yield* fileFixture()
      let observer: Watch.Observer | undefined
      const adapter = Layer.succeed(Watch.Adapter, {
        subscribe: (_targets, supplied) => {
          observer = supplied
          return Effect.succeed({
            mode: 'native',
            recursive: 'native',
            ancestors: 'native',
            symlinks: 'unsupported',
            reliability: 'best-effort',
          } as const)
        },
      })
      const context = yield* Layer.build(
        Watch.layer({ root: f.root }).pipe(
          Layer.provide([adapter, Layer.succeed(Path.Path, f.path)]),
        ),
      )
      return {
        ...f,
        watch: Context.get(context, Watch.FileWatch),
        burst: (paths: ReadonlyArray<string>) =>
          Effect.sync(() =>
            paths.forEach((path) => observer?.emit({ _tag: 'Change', path, kind: 'change' })),
          ),
      }
    })
    return one(
      FileChecks.fileSystemConformance({ make, capabilities: { watchOverflow: true } }),
      'filesystem: watch overflow requires rescan',
    )
  })

  it.effect('selected absent capabilities are named failures', () =>
    Effect.gen(function* () {
      const make = fileFixture().pipe(
        Effect.map((f) => ({
          cleanup: Effect.void,
          root: f.root,
          fs: f.fs,
          path: f.path,
          reader: f.reader,
        })),
      )
      for (const [capabilities, name] of [
        [{ directory: true }, 'filesystem: directory pages and capability'],
        [{ watch: true }, 'filesystem: missing and recreated watch targets'],
        [{ watchOverflow: true }, 'filesystem: watch overflow requires rescan'],
      ] as const)
        yield* red(one(FileChecks.fileSystemConformance({ make, capabilities }), name), name)
    }),
  )

  it.live('broken byte reader fails its identity/ranges case', () => {
    const name = 'filesystem: opened identity and positional ranges'
    const make = fileFixture().pipe(
      Effect.map((f) => ({
        ...f,
        reader: {
          open: (path: string) =>
            f.reader.open(path).pipe(
              Effect.map((reader) => ({
                ...reader,
                readAt: () => Effect.succeed(new Uint8Array()),
              })),
            ),
        },
      })),
    )
    return red(one(FileChecks.fileSystemConformance({ make }), name), name)
  })
  it.live('path-reopening reader fails renamed identity', () => {
    const name = 'filesystem: opened identity and positional ranges'
    const make = fileFixture().pipe(
      Effect.map((f) => ({
        ...f,
        reader: {
          open: (path: string) =>
            f.reader.open(path).pipe(
              Effect.map((reader) => ({
                ...reader,
                readAt: (offset: bigint, length: number) =>
                  offset < 0n || !Number.isSafeInteger(length) || length > 1024 * 1024
                    ? reader.readAt(offset, length)
                    : f.fs.readFile(path).pipe(
                        Effect.map((bytes) =>
                          bytes.subarray(Number(offset), Number(offset) + length),
                        ),
                        Effect.mapError(
                          (cause) =>
                            new ReaderError({
                              reason: 'filesystem',
                              operation: 'read',
                              path,
                              message: cause.message,
                              cause,
                            }),
                        ),
                      ),
              })),
            ),
        },
      })),
    )
    return red(one(FileChecks.fileSystemConformance({ make }), name), name)
  })
  it.live('empty directory adapter fails complete membership', () => {
    const name = 'filesystem: directory pages and capability'
    const make = fileFixture().pipe(
      Effect.map((f) => ({
        ...f,
        directory: {
          open: (path: string) =>
            f.directory.open(path).pipe(
              Effect.map((d) => ({
                ...d,
                next: () => Effect.succeed({ entries: [], done: true }),
              })),
            ),
        },
      })),
    )
    return red(
      one(FileChecks.fileSystemConformance({ make, capabilities: { directory: true } }), name),
      name,
    )
  })
  it.live('startup-only watch fails instead of counting initial coverage as a mutation', () => {
    const name = 'filesystem: missing and recreated watch targets'
    const make = fileFixture().pipe(
      Effect.map((f) => ({
        ...f,
        watch: {
          open: () =>
            Effect.succeed({
              coverage: {
                mode: 'native',
                recursive: 'native',
                ancestors: 'native',
                symlinks: 'unsupported',
                reliability: 'best-effort',
              } as const,
              events: Stream.concat(
                Stream.make({
                  _tag: 'Rescan',
                  paths: [`${f.root}/parent/tree`],
                  reason: 'coverage',
                } as const),
                Stream.never,
              ),
              close: Effect.void,
            }),
        },
      })),
    )
    return red(
      one(FileChecks.fileSystemConformance({ make, capabilities: { watch: true } }), name),
      name,
    )
  })
  it.live('queued duplicate startup notices cannot substitute for later mutation notices', () => {
    const name = 'filesystem: missing and recreated watch targets'
    const make = Effect.gen(function* () {
      const f = yield* fileFixture()
      const queue = yield* Queue.make<Watch.Event, Cause.Done>()
      for (let i = 0; i < 8; i++)
        Queue.offerUnsafe(queue, {
          _tag: 'Rescan',
          paths: [`${f.root}/parent/tree`],
          reason: 'coverage',
        })
      return {
        ...f,
        watch: {
          open: () =>
            Effect.succeed({
              coverage: {
                mode: 'native',
                recursive: 'native',
                ancestors: 'native',
                symlinks: 'unsupported',
                reliability: 'best-effort',
              } as const,
              events: Stream.fromQueue(queue),
              close: Effect.sync(() => Queue.endUnsafe(queue)),
            }),
        },
      }
    })
    return red(
      one(FileChecks.fileSystemConformance({ make, capabilities: { watch: true } }), name),
      name,
    )
  })
  it.live('lying noFollow adapter fails explicit unsupported selection', () => {
    const name = 'filesystem: explicit noFollow capability'
    const make = fileFixture().pipe(
      Effect.map((f) => ({ ...f, reader: { open: (path: string) => f.reader.open(path) } })),
    )
    return red(
      one(
        FileChecks.fileSystemConformance({ make, capabilities: { noFollow: 'unsupported' } }),
        name,
      ),
      name,
    )
  })
  it.live('borrowed image output fails detached-byte assertion', () => {
    const name = 'images: validated passthrough owns bytes'
    const make = imageFixture().pipe(
      Effect.map((f) => ({
        ...f,
        service: {
          prepare: (bytes: Uint8Array, mime: string, limits: ImagePolicy.Limits) =>
            f.service
              .prepare(bytes, mime, limits)
              .pipe(Effect.map((result) => ({ ...result, bytes }))),
        },
      })),
    )
    return red(one(ImageChecks.imagesConformance({ make }), name), name)
  })
  it.live('wrong oriented pixels fail the independently specified grid', () => {
    const name = 'images: all EXIF orientations match independent pixels'
    const make = imageFixture().pipe(
      Effect.map((f) => ({
        ...f,
        inspect: (bytes: Uint8Array) =>
          f
            .inspect(bytes)
            .pipe(Effect.map((result) => ({ ...result, rgba: [...result.rgba].reverse() }))),
      })),
    )
    return red(
      one(ImageChecks.imagesConformance({ make, capabilities: { orientations: true } }), name),
      name,
    )
  })
  it.live('header-only image validation fails the malformed body case', () => {
    const name = 'images: malformed body and input allocation guard'
    const make = imageFixture().pipe(
      Effect.map((f) => ({
        ...f,
        service: {
          prepare: (bytes: Uint8Array, mime: string, limits: ImagePolicy.Limits) =>
            f.service.prepare(bytes === f.malformed ? f.sample.bytes : bytes, mime, limits),
        },
      })),
    )
    return red(one(ImageChecks.imagesConformance({ make }), name), name)
  })
  it.live(
    'early resource-release observation fails the lease case and still settles its worker',
    () => {
      const name = 'images: interrupted codec retains completion lease'
      const make = imageLeaseFixture.pipe(
        Effect.map((f) => ({ ...f, lease: { ...f.lease, released: Effect.succeed(true) } })),
      )
      return red(
        one(ImageChecks.imagesConformance({ make, capabilities: { lease: true } }), name),
        name,
      )
    },
  )
  it.live('rewritten direct argv fails the literal process case', () => {
    const name = 'process: literal argv cwd and independent output'
    const make = processFixture().pipe(
      Effect.map((f) => ({
        ...f,
        literal: (_args: ReadonlyArray<string>, cwd?: string) => f.literal(['lost'], cwd),
      })),
    )
    return red(one(ProcessChecks.processConformance({ make }), name), name)
  })
  it.live('image platform unavailability is explicitly tested', () => {
    const make = imageFixture().pipe(
      Effect.map((f) => ({
        ...f,
        service: {
          prepare: () =>
            Effect.fail(
              new ImageErrors.UnsupportedImage({
                mimeType: 'image/png',
                operation: 'decode',
                message: 'Unavailable host',
              }),
            ),
        },
      })),
    )
    return one(
      ImageChecks.imagesConformance({ make, capabilities: { unavailable: true } }),
      'images: unavailable platform is explicit',
    )
  })
  it.live('unavailable image selection cannot suppress requested orientation checks', () => {
    const name = 'images: unavailable adapter cannot select supported capabilities'
    return red(
      one(
        ImageChecks.imagesConformance({
          make: imageFixture(),
          capabilities: { unavailable: true, orientations: true },
        }),
        name,
      ),
      name,
    )
  })
  it.live('selecting a missing lease hook fails visibly', () => {
    const name = 'images: interrupted codec retains completion lease'
    return red(
      one(
        ImageChecks.imagesConformance({ make: imageFixture(), capabilities: { lease: true } }),
        name,
      ),
      name,
    )
  })
  it.effect('merged same-tag registrations fail isolated binding expectations', () => {
    const name = 'toolkit isolation: same-tag bindings survive reload'
    const make = isolationFixture.pipe(
      Effect.map((f) => ({ ...f, a: { ...f.a, acquire: f.b.acquire } })),
    )
    return red(one(IsolationChecks.toolkitIsolationConformance({ make }), name), name).pipe(
      Effect.provide(execution),
    )
  })
  it.effect('typed factory and cleanup errors survive the runner-neutral boundary', () =>
    Effect.gen(function* () {
      const failedFactory = FileChecks.fileSystemConformance<never, never, 'factory failed'>({
        make: Effect.fail('factory failed' as const),
      })
      assert.strictEqual(
        yield* one(failedFactory, 'filesystem: opened identity and positional ranges').pipe(
          Effect.flip,
        ),
        'factory failed',
      )
      const make = fileFixture().pipe(
        Effect.map((f) => ({ ...f, cleanup: Effect.fail('cleanup failed' as const) })),
      )
      const result = yield* one(
        FileChecks.fileSystemConformance({ make }),
        'filesystem: opened identity and positional ranges',
      ).pipe(Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure, 'cleanup failed')
    }),
  )
  it.live('fixture cleanup runs exactly once after success failure and interruption', () =>
    Effect.gen(function* () {
      let cleanups = 0
      const base = fileFixture().pipe(
        Effect.map((f) => ({
          ...f,
          cleanup: Effect.sync(() => {
            cleanups++
          }),
        })),
      )
      yield* one(
        FileChecks.fileSystemConformance({ make: base }),
        'filesystem: opened identity and positional ranges',
      )
      assert.strictEqual(cleanups, 1)
      const bad = base.pipe(
        Effect.map((f) => ({
          ...f,
          reader: {
            open: (path: string) =>
              f.reader.open(path).pipe(
                Effect.map((reader) => ({
                  ...reader,
                  readAt: () => Effect.succeed(new Uint8Array()),
                })),
              ),
          },
        })),
      )
      yield* red(
        one(
          FileChecks.fileSystemConformance({ make: bad }),
          'filesystem: opened identity and positional ranges',
        ),
        'filesystem: opened identity and positional ranges',
      )
      assert.strictEqual(cleanups, 2)
      const started = yield* Deferred.make<void>()
      const suspended = base.pipe(
        Effect.map((f) => ({
          ...f,
          reader: {
            open: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
          },
        })),
      )
      const fiber = yield* one(
        FileChecks.fileSystemConformance({ make: suspended }),
        'filesystem: opened identity and positional ranges',
      ).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      yield* Fiber.interrupt(fiber)
      assert.strictEqual(cleanups, 3)
    }),
  )
})

import * as Path from 'effect/Path'
import { FileReaderError as ReaderError } from 'effect-harness/FileReader'
import type * as ImagePolicy from 'effect-harness/ImagePreparation'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
