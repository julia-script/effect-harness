/** Literal process, scope ownership and retained spill checks without a host runtime import. */
import type * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import type * as FileSystem from 'effect/FileSystem'
import type * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import type * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import type * as ChildProcess from 'effect/process/ChildProcess'
import type * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import type { Failure as ExecutionFailure } from '../ExecutionError.js'
import type * as ToolResult from '../ToolResult.js'
import type * as ToolSpill from '../ToolSpill.js'
import type * as Bash from '../coding/Bash.js'
import type * as ShellProcess from '../coding/ShellProcess.js'
import {
  assertions,
  scopedCase,
  type ConformanceCase,
  type ScopedFixture,
  ServiceConformanceError,
} from '../internal/ServiceConformance.js'

export type ProcessError =
  | PlatformError.PlatformError
  | Bash.ShellError
  | ToolSpill.ToolSpillError
  | ExecutionFailure

/**
 * The fixture chooses executables in its own namespace. literal must emit JSON.stringify(args)
 * to stdout and its actual cwd to stderr, without shell parsing. waiting emits "ready" then
 * remains alive until cancelled. runShell invokes the installed coding shell through captured services.
 */
export interface ProcessFixture<E = never, R = never> extends ScopedFixture<E, R> {
  readonly spawner: Context.Service.Shape<typeof ChildProcessSpawner.ChildProcessSpawner>
  readonly fs: Context.Service.Shape<typeof FileSystem.FileSystem>
  readonly root: string
  readonly path: Context.Service.Shape<typeof Path.Path>
  readonly literal: (args: ReadonlyArray<string>, cwd?: string) => ChildProcess.Command
  readonly waiting: ChildProcess.Command
  readonly runShell: (
    input: Bash.Parameters,
    options?: Bash.Options,
  ) => Effect.Effect<ToolResult.Output<Bash.Outcome>, E | Bash.ShellError | ExecutionFailure, R>
  /** Direct argv that exits successfully and writes "strict-ok". Never script text. */
  readonly shellArgv: ReadonlyArray<string>
  readonly cleanupCapability: ShellProcess.Cleanup
  /** Actual spawn counter, used to prove strict refusal happened before spawning. */
  readonly spawnCount: Effect.Effect<number, E, R>
  /** Required by a selected owned-tree adapter: inspect all its owned descendants after settlement. */
  readonly ownedTreeEmpty?: Effect.Effect<boolean, E, R>
  readonly spill?: Context.Service.Shape<typeof ToolSpill.ToolSpill>
  readonly closeSpillProvider?: Effect.Effect<void, E, R>
  readonly reopenSpill?: Effect.Effect<
    Context.Service.Shape<typeof ToolSpill.ToolSpill>,
    E | ToolSpill.ToolSpillError,
    R | Scope.Scope
  >
}

export interface ProcessOptions<E = never, R = never, FE = E> {
  readonly make: Effect.Effect<ProcessFixture<E, R>, FE, R | Scope.Scope>
  readonly capabilities?: {
    readonly strictOwnership?: boolean
    readonly spill?: boolean
    readonly spillLifetime?: 'persistent' | 'toolkit'
  }
}

/**
 * Every invocation scope is closed before checking termination. A timeout is distinct from caller
 * interruption. Native best-effort group cleanup is never promoted to exclusive tree ownership;
 * strict-capable custom adapters must run the same selected strict shell path successfully.
 * This does not prove physical OS process-tree fencing or escaped descendant cleanup.
 */
export const processConformance = <E = never, R = never, FE = E>(
  options: ProcessOptions<E, R, FE>,
): ReadonlyArray<
  ConformanceCase<E | FE | ProcessError | ServiceConformanceError, Exclude<R, Scope.Scope>>
> => {
  type Failure = E | ProcessError | ServiceConformanceError
  const cases: Array<
    ConformanceCase<E | FE | ProcessError | ServiceConformanceError, Exclude<R, Scope.Scope>>
  > = []
  const add = (
    name: string,
    run: (
      f: ProcessFixture<E, R>,
      a: ReturnType<typeof assertions>,
    ) => Effect.Effect<void, Failure, R | Scope.Scope>,
  ) =>
    cases.push(
      scopedCase<ProcessFixture<E, R>, E, R, FE, Failure, R | Scope.Scope>(name, options.make, run),
    )
  const caps = { ...options.capabilities }
  const ready = (handle: ChildProcessSpawner.ChildProcessHandle) =>
    handle.stdout.pipe(
      Stream.decodeText,
      Stream.take(1),
      Stream.mkString,
      Effect.timeoutOrElse({
        duration: '3 seconds',
        orElse: () => assertions('process: readiness').same('ready within deadline', false, true),
      }),
    )

  add('process: literal argv cwd and independent output', (f, { same }) =>
    Effect.gen(function* () {
      const args = ['space here', '$(touch SHOULD_NOT_EXIST); "quote" & * | > x', '', 'é🦊']
      yield* f.fs.makeDirectory('sub')
      const command = f.literal(args, 'sub')
      const handle = yield* f.spawner.spawn(command)
      const [stdout, stderr, code] = yield* Effect.all(
        [
          Stream.mkString(Stream.decodeText(handle.stdout)),
          Stream.mkString(Stream.decodeText(handle.stderr)),
          handle.exitCode,
        ],
        { concurrency: 'unbounded' },
      )
      yield* same('literal arguments', stdout, JSON.stringify(args))
      yield* same('caller cwd resolved under root', stderr, f.path.join(f.root, 'sub'))
      yield* same('successful exit', code, 0)
      yield* same('no shell side effect', yield* f.fs.exists('sub/SHOULD_NOT_EXIST'), false)
      const defaultHandle = yield* f.spawner.spawn(f.literal([]))
      const outputs = yield* Effect.all(
        [
          Stream.mkString(Stream.decodeText(defaultHandle.stdout)),
          Stream.mkString(Stream.decodeText(defaultHandle.stderr)),
          defaultHandle.exitCode,
        ],
        { concurrency: 'unbounded' },
      )
      yield* same('default cwd', outputs, ['[]', f.root, 0])
    }),
  )

  add('process: timeout releases scoped process', (f, { same }) =>
    Effect.gen(function* () {
      let handle: ChildProcessSpawner.ChildProcessHandle | undefined
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          handle = yield* f.spawner.spawn(f.waiting)
          yield* same('readiness', yield* ready(handle), 'ready')
          return yield* handle.exitCode.pipe(Effect.timeout('30 millis'), Effect.result)
        }),
      )
      yield* same('timeout differs from successful exit', result._tag, 'Failure')
      if (result._tag === 'Failure')
        yield* same('timeout reason', result.failure._tag, 'TimeoutError')
      if (handle === undefined) return yield* same('spawned handle', false, true)
      yield* same('scope killed leader', yield* handle.isRunning, false)
    }),
  )

  add('process: cancellation owns only its invocation', (f, { same }) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<ChildProcessSpawner.ChildProcessHandle>()
      const run = Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* f.spawner.spawn(f.waiting)
          yield* same('readiness', yield* ready(handle), 'ready')
          yield* Deferred.succeed(started, handle)
          return yield* Effect.never
        }),
      )
      const fiber = yield* run.pipe(Effect.forkChild)
      const first = yield* Deferred.await(started).pipe(
        Effect.timeoutOrElse({
          duration: '3 seconds',
          orElse: () =>
            Effect.fail(
              new ServiceConformanceError({
                caseName: 'process: cancellation owns only its invocation',
                assertion: 'spawn readiness',
                message: 'process: cancellation owns only its invocation: spawn readiness',
                actual: 'not ready',
                expected: 'ready within 3 seconds',
              }),
            ),
        }),
      )
      const other = yield* f.spawner.spawn(f.waiting)
      yield* same('other readiness', yield* ready(other), 'ready')
      yield* Fiber.interrupt(fiber)
      yield* same('cancelled leader stopped', yield* first.isRunning, false)
      yield* same('other scope still running', yield* other.isRunning, true)
    }),
  )

  if (caps.strictOwnership)
    add('process: explicit strict cleanup selection', (f, { same, failure }) =>
      Effect.gen(function* () {
        const before = yield* f.spawnCount
        if (f.cleanupCapability === 'best-effort-process-group') {
          yield* failure(
            'strict refusal',
            f.runShell({ command: f.shellArgv }, { requireOwnedTree: true }),
            (e) =>
              typeof e === 'object' &&
              e !== null &&
              'reason' in e &&
              e.reason === 'unsupportedCleanup',
          )
          yield* same('refused before spawning', yield* f.spawnCount, before)
          const normal = yield* f.runShell({ command: f.shellArgv })
          yield* same(
            'best effort remains usable',
            normal.structuredOutput?.cleanup,
            f.cleanupCapability,
          )
        } else {
          if (f.ownedTreeEmpty === undefined)
            return yield* same('owned-tree inspection required', false, true)
          const strict = yield* f.runShell({ command: f.shellArgv }, { requireOwnedTree: true })
          yield* same('owned-tree selected', strict.structuredOutput?.cleanup, 'owned-tree')
          yield* same('strict output', strict.structuredOutput?.output, 'strict-ok')
          yield* same('actually spawned once', yield* f.spawnCount, before + 1)
          yield* same('all owned descendants released', yield* f.ownedTreeEmpty, true)
        }
      }),
    )

  if (caps.spill)
    add('process: spill survives call scope and expires on removal', (f, { same, failure }) =>
      Effect.gen(function* () {
        if (f.spill === undefined)
          return yield* same('selected spill fixture required', false, true)
        const spill = f.spill
        const reference = yield* Effect.scoped(
          Effect.gen(function* () {
            const writer = yield* spill.open
            yield* writer.append('stdout', new Uint8Array([0xf0, 0x9f]))
            yield* writer.append('stderr', new Uint8Array([0, 255]))
            yield* writer.append('stdout', new Uint8Array([0xa6, 0x8a]))
            return yield* writer.commit
          }),
        )
        yield* same('reference remains valid', yield* spill.revalidate(reference), reference)
        yield* same(
          'raw independent stdout',
          [...(yield* f.fs.readFile(reference.stdoutPath))],
          [240, 159, 166, 138],
        )
        yield* same(
          'raw independent stderr',
          [...(yield* f.fs.readFile(reference.stderrPath))],
          [0, 255],
        )
        yield* same(
          'byte counts',
          [reference.bytes, reference.stdoutBytes, reference.stderrBytes],
          [6, 4, 2],
        )
        yield* failure(
          'foreign provider refused',
          spill.revalidate({ ...reference, provider: 'foreign' }),
          (e) => e.reason === 'unavailable',
        )
        yield* spill.remove(reference)
        yield* failure(
          'removed spill unavailable',
          spill.revalidate(reference),
          (e) => e.reason === 'unavailable',
        )
        yield* same('removed backing files', yield* f.fs.exists(reference.path), false)
      }),
    )
  if (caps.spillLifetime !== undefined)
    add('process: spill provider lifetime and reopen', (f, { same, failure }) =>
      Effect.gen(function* () {
        if (
          f.spill === undefined ||
          f.closeSpillProvider === undefined ||
          f.reopenSpill === undefined
        )
          return yield* same('selected spill lifecycle hooks required', false, true)
        const spill = f.spill
        const reference = yield* Effect.scoped(
          Effect.gen(function* () {
            const writer = yield* spill.open
            yield* writer.append('stdout', new Uint8Array([1, 2, 3]))
            return yield* writer.commit
          }),
        )
        yield* same('declared lifetime', reference.lifetime, caps.spillLifetime)
        yield* f.closeSpillProvider
        yield* failure(
          'closed provider cannot open writer',
          spill.open,
          (e) => e.reason === 'closed',
        )
        const reopened = yield* f.reopenSpill
        if (caps.spillLifetime === 'persistent') {
          yield* same(
            'persistent reference survives reopen',
            yield* reopened.revalidate(reference),
            reference,
          )
          yield* same(
            'persistent exact bytes',
            [...(yield* f.fs.readFile(reference.stdoutPath))],
            [1, 2, 3],
          )
          yield* reopened.remove(reference)
        } else {
          yield* same(
            'toolkit shutdown removes backing files',
            yield* f.fs.exists(reference.path),
            false,
          )
          yield* failure(
            'expired reference stays unavailable',
            reopened.revalidate(reference),
            (e) => e.reason === 'unavailable',
          )
        }
      }),
    )
  return cases
}
