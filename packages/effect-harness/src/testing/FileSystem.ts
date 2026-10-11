/** Runner-neutral checks over supplied toolkit-bound filesystem capabilities. */
import type * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Cause from 'effect/Cause'
import * as Fiber from 'effect/Fiber'
import * as Exit from 'effect/Exit'
import type * as FileSystem from 'effect/FileSystem'
import type * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Queue from 'effect/Queue'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import type * as DirectoryReader from '../DirectoryReader.js'
import * as FileReader from '../FileReader.js'
import type * as FileWatch from '../FileWatch.js'

import {
  type assertions,
  scopedCase,
  type ConformanceCase,
  type ScopedFixture,
  ServiceConformanceError,
} from '../internal/ServiceConformance.js'

export { ServiceConformanceError } from '../internal/ServiceConformance.js'
export type { ConformanceCase, ScopedFixture } from '../internal/ServiceConformance.js'

export type FileSystemError =
  | PlatformError.PlatformError
  | FileReader.FileReaderError
  | DirectoryReader.DirectoryReaderError
  | FileWatch.FileWatchError

/** Fresh empty writable bound root per evaluation, with coherent Path and backend identities. */
export interface FileSystemFixture<E = never, R = never> extends ScopedFixture<E, R> {
  readonly fs: Context.Service.Shape<typeof FileSystem.FileSystem>
  readonly path: Context.Service.Shape<typeof Path.Path>
  readonly root: string
  readonly reader: Context.Service.Shape<typeof FileReader.FileReader>
  readonly directory?: Context.Service.Shape<typeof DirectoryReader.DirectoryReader>
  readonly watch?: Context.Service.Shape<typeof FileWatch.FileWatch>
  /** Emit a synchronous burst through the real watch Adapter observer, while the consumer is idle. */
  readonly burst?: (paths: ReadonlyArray<string>) => Effect.Effect<void, E, R>
}

/** Omitted optional capabilities produce no case; selected but absent support is a named failure. */
export interface FileSystemCapabilities {
  readonly noFollow?: 'unsupported' | 'atomic'
  readonly directory?: boolean
  readonly boundedDirectory?: boolean
  readonly watch?: boolean
  readonly watchOverflow?: boolean
  readonly unavailableWatch?: boolean
  readonly unavailableDirectory?: boolean
}

export interface FileSystemOptions<E = never, R = never, FE = E> {
  readonly make: Effect.Effect<FileSystemFixture<E, R>, FE, R | Scope.Scope>
  readonly capabilities?: FileSystemCapabilities
}

/**
 * Checks opened identity, serialized byte ranges and scoped lifetime; optional paging/watch checks
 * exercise installed narrow services. Watches are best-effort invalidations followed by content reads,
 * not a lossless event journal. Each step has a three-second deadline, with startup consumed first.
 * Native FileReader explicitly refuses atomic noFollow. Directory buffers are not a byte-memory bound.
 */
export const fileSystemConformance = <E = never, R = never, FE = E>(
  options: FileSystemOptions<E, R, FE>,
): ReadonlyArray<
  ConformanceCase<E | FE | FileSystemError | ServiceConformanceError, Exclude<R, Scope.Scope>>
> => {
  type Failure = E | FileSystemError | ServiceConformanceError
  const cases: Array<
    ConformanceCase<E | FE | FileSystemError | ServiceConformanceError, Exclude<R, Scope.Scope>>
  > = []
  const add = (
    name: string,
    run: (
      f: FileSystemFixture<E, R>,
      a: ReturnType<typeof assertions>,
    ) => Effect.Effect<void, Failure, R | Scope.Scope>,
  ) =>
    cases.push(
      scopedCase<FileSystemFixture<E, R>, E, R, FE, Failure, R | Scope.Scope>(
        name,
        options.make,
        run,
      ),
    )
  const caps = { ...options.capabilities }
  const decode = (bytes: Uint8Array) => new TextDecoder().decode(bytes)
  if (caps.unavailableDirectory && (caps.directory || caps.boundedDirectory))
    add('filesystem: incompatible directory capability selections', (_f, { same }) =>
      same('paging checks require an available directory adapter', false, true),
    )
  if (caps.unavailableWatch && (caps.watch || caps.watchOverflow))
    add('filesystem: incompatible watch capability selections', (_f, { same }) =>
      same('watch checks require an available watch adapter', false, true),
    )

  add('filesystem: opened identity and positional ranges', (f, { same, failure }) =>
    Effect.gen(function* () {
      yield* f.fs.writeFileString('data', 'hello world')
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const reader = yield* f.reader.open('data').pipe(Effect.provideService(Scope.Scope, scope))
      yield* same('opened size', (yield* reader.stat).size, 11n)
      const parts = yield* Effect.all([reader.readAt(0n, 5), reader.readAt(6n, 100)], {
        concurrency: 'unbounded',
      })
      yield* same('independent concurrent ranges', parts.map(decode), ['hello', 'world'])
      for (const [offset, length] of [
        [11n, 4],
        [50n, 1],
        [3n, 0],
      ] as const)
        yield* same('EOF and empty ranges', (yield* reader.readAt(offset, length)).length, 0)
      for (const [offset, length] of [
        [-1n, 1],
        [0n, 1.5],
        [0n, FileReader.maxReadBytes + 1],
      ] as const)
        yield* failure(
          'invalid range refused',
          reader.readAt(offset, length),
          (e) => e.reason === 'invalid',
        )
      yield* f.fs.rename('data', 'old')
      yield* f.fs.writeFileString('data', 'replacement')
      yield* same(
        'handle retains renamed file',
        decode(yield* reader.readAt(0n, 20)),
        'hello world',
      )
      yield* Scope.close(scope, Exit.void)
      yield* failure('read after close', reader.readAt(0n, 1), (e) => e.reason === 'closed')
      yield* failure('stat after close', reader.stat, (e) => e.reason === 'closed')
      yield* failure('missing file', f.reader.open('missing'), (e) => e.reason === 'filesystem')
      yield* f.fs.makeDirectory('folder')
      yield* failure('directory refused', f.reader.open('folder'), (e) => e.reason === 'notFile')
    }),
  )

  if (caps.noFollow !== undefined)
    add('filesystem: explicit noFollow capability', (f, { same, failure }) =>
      Effect.gen(function* () {
        yield* f.fs.writeFileString('target', 'target')
        if (caps.noFollow === 'unsupported') {
          yield* failure(
            'atomic noFollow unavailable',
            f.reader.open('target', { noFollow: true }),
            (e) => e.reason === 'unsupported',
          )
        } else {
          const reader = yield* f.reader.open('target', { noFollow: true })
          yield* same('regular file supported', decode(yield* reader.readAt(0n, 6)), 'target')
          yield* f.fs.symlink('target', 'link')
          yield* failure(
            'final symlink refused',
            f.reader.open('link', { noFollow: true }),
            (e) => e.reason === 'filesystem' || e.reason === 'notFile',
          )
        }
      }),
    )

  if (caps.directory || caps.boundedDirectory || caps.unavailableDirectory)
    add('filesystem: directory pages and capability', (f, { same, failure }) =>
      Effect.gen(function* () {
        if (f.directory === undefined) return yield* same('directory fixture required', false, true)
        if (caps.unavailableDirectory) {
          yield* failure(
            'unavailable platform is explicit',
            f.directory.open('.'),
            (e) => e.reason === 'unsupported',
          )
          return
        }
        const names = Array.from({ length: 19 }, (_, i) => `file-${i}`)
        yield* f.fs.makeDirectory('pages')
        yield* Effect.forEach(names, (n) => f.fs.writeFileString(`pages/${n}`, n), {
          discard: true,
        })
        const dir = yield* f.directory.open('pages', {
          requireOpenedIdentity: caps.boundedDirectory ?? false,
          requireBoundedEnumeration: caps.boundedDirectory ?? false,
        })
        if (caps.boundedDirectory) {
          yield* same('opened identity', dir.capabilities.identity, 'opened-directory')
          yield* same('native buffering', dir.capabilities.allocation, 'native-buffer')
        }
        const found: string[] = []
        let done = false
        for (let i = 0; i < 30; i++) {
          const page = yield* dir.next(3)
          yield* same('bounded page', page.entries.length <= 3, true)
          found.push(...page.entries.map((e) => e.name))
          if (i === 0 && caps.boundedDirectory) {
            yield* f.fs.rename('pages', 'old-pages')
            yield* f.fs.makeDirectory('pages')
            yield* f.fs.writeFileString('pages/replacement', 'new directory identity')
          }
          if (page.done) {
            done = true
            break
          }
        }
        yield* same('finite enumeration', done, true)
        yield* same('complete unique membership', found.sort(), names.sort())
        yield* same('sticky EOF', yield* dir.next(3), { entries: [], done: true })
        yield* failure('invalid page size', dir.next(0), (e) => e.reason === 'invalid')
        yield* dir.close
        yield* dir.close
        yield* failure('closed directory', dir.next(1), (e) => e.reason === 'closed')
        yield* failure(
          'missing directory',
          f.directory.open('missing'),
          (e) => e.reason === 'notFound',
        )
        yield* f.fs.writeFileString('regular-file', 'x')
        yield* failure(
          'regular file refused',
          f.directory.open('regular-file'),
          (e) => e.reason === 'notDirectory',
        )
      }),
    )

  if (caps.watch || caps.unavailableWatch)
    add('filesystem: missing and recreated watch targets', (f, { same, failure }) =>
      Effect.gen(function* () {
        if (f.watch === undefined) return yield* same('watch fixture required', false, true)
        if (caps.unavailableWatch) {
          yield* failure(
            'unavailable platform is explicit',
            f.watch.open([{ path: 'missing' }]),
            (e) => e.reason === 'unsupported',
          )
          return
        }
        const target = f.path.join(f.root, 'parent', 'tree')
        const watch = yield* f.watch.open([
          { path: 'parent/tree', recursive: true, exclude: { hidden: true, names: ['ignored'] } },
        ])
        const covers = (event: FileWatch.Event, value: string) => {
          const paths = event._tag === 'Change' ? [event.path] : event.paths
          return paths.some((p) => p === value || value.startsWith(p + f.path.sep))
        }
        // One continuous source consumer timestamps observed arrivals with an ordinal.
        // A bounded collector cannot reuse already observed startup/previous-step events.
        let ordinal = 0
        let after = 0
        const observed = yield* Queue.make<
          { readonly ordinal: number; readonly event: FileWatch.Event },
          FileWatch.FileWatchError | Cause.Done
        >({ capacity: 256, strategy: 'sliding' })
        const collecting = yield* watch.events.pipe(
          Stream.runForEach((event) =>
            Effect.sync(() => {
              ordinal++
              Queue.offerUnsafe(observed, { ordinal, event })
            }),
          ),
          Effect.onExit((exit) =>
            Effect.sync(() => {
              if (Exit.isFailure(exit)) Queue.failCauseUnsafe(observed, exit.cause)
              else Queue.endUnsafe(observed)
            }),
          ),
          Effect.forkScoped({ startImmediately: true }),
        )
        const boundary = Effect.yieldNow.pipe(
          Effect.andThen(
            Effect.sync(() => {
              after = ordinal
            }),
          ),
        )
        const fresh = () =>
          Stream.fromQueue(observed).pipe(
            Stream.filter((record) => record.ordinal > after),
            Stream.map((record) => record.event),
          )
        const wait = (value: string) =>
          fresh().pipe(
            Stream.filter((e) => covers(e, value)),
            Stream.take(1),
            Stream.runCollect,
            Effect.timeout('3 seconds'),
            Effect.flatMap((events) => same('relevant fresh invalidation', events.length, 1)),
          )
        // Initial coverage cannot stand in for a mutation invalidation.
        yield* wait(target)
        yield* same('truthful reliability', watch.coverage.reliability, 'best-effort')
        yield* boundary
        yield* f.fs.makeDirectory('parent/tree', { recursive: true })
        yield* f.fs.writeFileString('parent/tree/item', 'created:1')
        yield* wait(f.path.join(target, 'item'))
        yield* same('created contents', yield* f.fs.readFileString('parent/tree/item'), 'created:1')
        yield* boundary
        yield* f.fs.rename('parent', 'old-parent')
        yield* wait(target)
        yield* same('renamed target absent', yield* f.fs.exists('parent/tree'), false)
        yield* boundary
        yield* f.fs.makeDirectory('parent/tree', { recursive: true })
        yield* f.fs.writeFileString('parent/tree/item', 'recreated:2')
        yield* wait(f.path.join(target, 'item'))
        yield* same(
          'recreated contents',
          yield* f.fs.readFileString('parent/tree/item'),
          'recreated:2',
        )
        yield* boundary
        yield* f.fs.makeDirectory('parent/tree/ignored')
        yield* f.fs.writeFileString('parent/tree/ignored/private', 'excluded')
        yield* f.fs.writeFileString('parent/tree/.draft', 'published:3')
        yield* f.fs.rename('parent/tree/.draft', 'parent/tree/published')
        const events = yield* fresh().pipe(
          Stream.takeUntil((e) => covers(e, f.path.join(target, 'published'))),
          Stream.runCollect,
          Effect.timeout('3 seconds'),
        )
        yield* same(
          'publication invalidated',
          events.some((e) => covers(e, f.path.join(target, 'published'))),
          true,
        )
        for (const event of events) {
          const paths = event._tag === 'Change' ? [event.path] : event.paths
          yield* same(
            'excluded paths never emitted',
            paths.some((p) => {
              const parts = f.path.relative(target, p).split(f.path.sep)
              return parts.some((part) => part === 'ignored' || part.startsWith('.'))
            }),
            false,
          )
        }
        yield* same(
          'published contents',
          yield* f.fs.readFileString('parent/tree/published'),
          'published:3',
        )
        yield* watch.close
        yield* watch.close
        yield* Fiber.join(collecting).pipe(Effect.timeout('3 seconds'))
        yield* same(
          'close drains and ends stream',
          yield* Stream.runCollect(watch.events).pipe(Effect.timeout('3 seconds')),
          [],
        )
      }).pipe(
        Effect.catchTag('TimeoutError', (e) =>
          Effect.fail(
            new ServiceConformanceError({
              caseName: 'filesystem: missing and recreated watch targets',
              assertion: 'watch deadline',
              message: 'filesystem: missing and recreated watch targets: watch deadline',
              actual: e,
              expected: 'fresh relevant invalidation within 3 seconds',
            }),
          ),
        ),
      ),
    )

  if (caps.watchOverflow)
    add('filesystem: watch overflow requires rescan', (f, { same }) =>
      Effect.gen(function* () {
        if (f.watch === undefined || f.burst === undefined)
          return yield* same('watch and synchronous burst fixture required', false, true)
        const watch = yield* f.watch.open([{ path: '.' }], { queueCapacity: 1 })
        yield* f.burst(['one', 'two', 'three'].map((p) => f.path.join(f.root, p)))
        const events = yield* watch.events.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.timeoutOrElse({
            duration: '3 seconds',
            orElse: () => same('overflow invalidation deadline', false, true).pipe(Effect.as([])),
          }),
        )
        yield* same('overflow replaces queued events', events, [
          { _tag: 'Rescan', paths: [f.root], reason: 'overflow' },
        ])
        yield* watch.close
      }),
    )
  return cases
}
