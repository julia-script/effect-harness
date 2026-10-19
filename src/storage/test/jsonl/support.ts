import { BunServices } from '@effect/platform-bun'
import { Effect, Exit, FileSystem, Path, PlatformError, Scope } from 'effect'
import { pendingTask } from '../support.ts'
import type { RunCase } from '../support.ts'
import { Storage } from '../../index.ts'
import type { Document, Task, Write } from '../../schema/index.ts'
import { Ids } from '../../schema/index.ts'

export type Services = FileSystem.FileSystem | Path.Path | Scope.Scope

/** Run a test body with the Bun platform and a fresh temporary directory. */
export const runInDirectory = <A, E>(
  body: (directory: string) => Effect.Effect<A, E, Services>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.flatMap(
        Effect.flatMap(Effect.service(FileSystem.FileSystem), (fs) => fs.makeTempDirectoryScoped()),
        body,
      ),
    ).pipe(
      // Each test is an entry point that provides its own platform.
      // oxlint-disable-next-line effecttsgo/strict-effect-provide
      Effect.provide(BunServices.layer),
    ),
  )

export const runJsonl: RunCase = (body) =>
  runInDirectory((directory) => Effect.flatMap(Storage.makeJsonl(directory), body))

/** A store whose every commit closes and reopens the directory, so reads exercise recovery. */
const makeReopening = Effect.fnUntraced(function* (directory: string) {
  const services = yield* Effect.context<FileSystem.FileSystem | Path.Path>()
  let scope = yield* Scope.make()
  let current = yield* Storage.makeJsonl(directory).pipe(Scope.provide(scope))
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
  const reopen = Effect.gen(function* () {
    yield* Scope.close(scope, Exit.void)
    scope = yield* Scope.make()
    current = yield* Storage.makeJsonl(directory).pipe(Scope.provide(scope), Effect.orDie)
  }).pipe(Effect.provideContext(services))
  const service: Storage.Service = {
    commit: (writes) =>
      Effect.ensuring(
        Effect.suspend(() => current.commit(writes)),
        reopen,
      ),
    mintId: (id) => Effect.suspend(() => current.mintId(id)),
    conversation: (id) => Effect.suspend(() => current.conversation(id)),
    scanConversations: (...args) => Effect.suspend(() => current.scanConversations(...args)),
    entry: (id) => Effect.suspend(() => current.entry(id)),
    visibleEntry: (...args) => Effect.suspend(() => current.visibleEntry(...args)),
    findLatestHeadMarker: (...args) => Effect.suspend(() => current.findLatestHeadMarker(...args)),
    scanEntries: (...args) => Effect.suspend(() => current.scanEntries(...args)),
    task: (id) => Effect.suspend(() => current.task(id)),
    scanTasks: (...args) => Effect.suspend(() => current.scanTasks(...args)),
    submission: (id) => Effect.suspend(() => current.submission(id)),
    scanSubmissions: (...args) => Effect.suspend(() => current.scanSubmissions(...args)),
    submissionByRequest: (...args) => Effect.suspend(() => current.submissionByRequest(...args)),
    findDocument: (...args) => Effect.suspend(() => current.findDocument(...args)),
    document: (...args) => Effect.suspend(() => current.document(...args)),
    scanDocuments: (...args) => Effect.suspend(() => current.scanDocuments(...args)),
  }
  return service
})

export const runReopening: RunCase = (body) =>
  runInDirectory((directory) => Effect.flatMap(makeReopening(directory), body))

/** The complete lines of a file, or none when it is missing. */
export const fileLines = Effect.fnUntraced(function* (path: string) {
  const fs = yield* FileSystem.FileSystem
  if (!(yield* fs.exists(path))) return []
  const text = yield* fs.readFileString(path)
  return text === '' ? [] : text.trimEnd().split('\n')
})

export const fileSize = (path: string) =>
  Effect.flatMap(Effect.service(FileSystem.FileSystem), (fs) =>
    Effect.map(fs.stat(path), (info) => Number(info.size)),
  )

export const appendBytes = (path: string, bytes: Uint8Array) =>
  Effect.flatMap(Effect.service(FileSystem.FileSystem), (fs) =>
    fs.writeFile(path, bytes, { flag: 'a' }),
  )

/** An append failure to inject: which append call fails, and how much it writes first. */
export interface Fault {
  readonly call: number
  readonly mode: 'before' | 'after' | 'short'
}

/** Arms at most one append fault; appends are counted from when it is armed. */
export interface Faults {
  fault?: Fault
  appends: number
}

const injected = (path: string) =>
  PlatformError.systemError({
    _tag: 'Unknown',
    module: 'FileSystem',
    method: 'writeFileString',
    pathOrDescriptor: path,
    description: 'injected append failure',
  })

/** The platform file system with append failures injected per `faults`. */
export const faultyFileSystem = (faults: Faults) =>
  Effect.map(Effect.service(FileSystem.FileSystem), (fs): FileSystem.FileSystem => ({
    ...fs,
    writeFileString: (path, data, options) =>
      Effect.suspend(() => {
        if (options?.flag !== 'a' || faults.fault === undefined) {
          return fs.writeFileString(path, data, options)
        }
        faults.appends++
        const { call, mode } = faults.fault
        if (faults.appends !== call) return fs.writeFileString(path, data, options)
        if (mode === 'before') return Effect.fail(injected(path))
        const written = mode === 'short' ? data.slice(0, Math.max(1, data.length / 2)) : data
        return Effect.andThen(
          fs.writeFileString(path, written, options),
          Effect.fail(injected(path)),
        )
      }),
  }))

/** Open the JSONL store in `directory`, optionally through a fault-injecting file system. */
export const open = (directory: string, faults?: Faults) =>
  faults === undefined
    ? Storage.makeJsonl(directory)
    : Effect.flatMap(faultyFileSystem(faults), (fs) =>
        Storage.makeJsonl(directory).pipe(Effect.provideService(FileSystem.FileSystem, fs)),
      )

/** The message `makeJsonl` fails with when it cannot open `directory`. */
export const openFailure = (directory: string) =>
  Effect.map(Effect.flip(Storage.makeJsonl(directory)), (error) => error.message)

export const terminalTask = (id: Ids.TaskId): Task.TaskRecord => ({
  ...pendingTask(id, Ids.ROOT_CONVERSATION_ID),
  state: { status: 'terminal', outcome: { status: 'completed', result: null } },
})

export const sessionDocument = (
  id: Ids.DocumentId,
  kind = 'test.document',
): Document.DocumentCreate => ({ id, kind, scope: { kind: 'session' } })

/** Write a document creation with a base value. */
export const createDocument = (
  record: Document.DocumentCreate,
  value: Document.DocumentBase['value'] = {},
): Write.StorageWrite => ({
  type: 'document.create',
  record,
  content: { kind: 'base', version: 1, value },
})

export const join = (directory: string, file: string) =>
  Effect.map(Effect.service(Path.Path), (path) => path.join(directory, file))
