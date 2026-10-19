import { Effect } from 'effect'
import { isCurrentOnly } from '../memory/state.ts'
import type { SidecarState } from './encode.ts'
import { filePath, io, readLines, truncate } from './files.ts'
import type { Files, ParsedFile } from './files.ts'
import {
  MAIN_FILE,
  isReclaimFileName,
  isSidecarFileName,
  parseMarker,
  parseSidecar,
  sidecarFileName,
  sidecarId,
  sidecarKey,
} from './format.ts'
import type { MainMarker, SidecarRecord } from './format.ts'
import { StorageError } from '../../StorageError.ts'

/** Everything read from disk, indexed for confirmation against the markers. */
export interface Disk {
  readonly markers: ReadonlyArray<MainMarker>
  readonly sidecars: ReadonlyArray<ParsedFile<SidecarRecord>>
  readonly records: ReadonlyMap<string, SidecarRecord>
}

/** What the complete markers imply about which sidecar lines are still needed. */
export interface Analysis {
  readonly currentOnly: ReadonlySet<number>
  readonly retiredCurrentOnly: ReadonlySet<number>
  /** Whether each task's last write left it live in a sidecar. */
  readonly taskIsLive: ReadonlyMap<number, boolean>
  readonly terminalTasks: ReadonlySet<number>
  /** The latest base record of each current-only document. */
  readonly latestBases: ReadonlyMap<number, SidecarRecord>
}

const corrupt = (message: string) => Effect.fail(new StorageError({ message }))

const isBefore = (seq: number, ordinal: number, other: SidecarRecord): boolean =>
  seq < other.seq || (seq === other.seq && ordinal < other.ordinal)

const readMain = Effect.fnUntraced(function* (files: Files) {
  const main = yield* readLines(files, MAIN_FILE, parseMarker)
  const markers = main.lines.map(({ value }) => value)
  for (const [index, marker] of markers.entries()) {
    if (marker.seq <= (markers[index - 1]?.seq ?? 0)) {
      return yield* corrupt(`Commit sequence does not strictly increase in ${MAIN_FILE}`)
    }
  }
  return markers
})

const checkOrder = (parsed: ParsedFile<SidecarRecord>) =>
  Effect.forEach(
    parsed.lines,
    ({ value }, index) => {
      const previous = parsed.lines[index - 1]?.value
      return previous === undefined || isBefore(previous.seq, previous.ordinal, value)
        ? Effect.void
        : corrupt(`Sidecar records are out of order in ${parsed.file}`)
    },
    { discard: true },
  )

/** Read every sidecar, after dropping replacements a crash interrupted. */
const readSidecars = Effect.fnUntraced(function* (files: Files) {
  const names = yield* files.fs
    .readDirectory(files.directory)
    .pipe(Effect.mapError(io('directory listing')))
  yield* Effect.forEach(
    names.filter((name) => isReclaimFileName(name)),
    (name) => Effect.ignore(files.fs.remove(filePath(files, name), { force: true })),
    { discard: true },
  )
  return yield* Effect.forEach(names.filter((name) => isSidecarFileName(name)).toSorted(), (file) =>
    Effect.tap(readLines(files, file, parseSidecar(file)), checkOrder),
  )
})

export const readDisk = Effect.fnUntraced(function* (files: Files) {
  const markers = yield* readMain(files)
  const sidecars = yield* readSidecars(files)
  const records = new Map<string, SidecarRecord>()
  for (const parsed of sidecars) {
    for (const { value } of parsed.lines) {
      records.set(sidecarKey(parsed.file, value.seq, value.ordinal), value)
    }
  }
  const disk: Disk = { markers, sidecars, records }
  return disk
})

/** The document a sidecar-backed operation writes, if any. */
const documentOf = (operation: MainMarker['writes'][number]): number | undefined => {
  if (operation.type === 'document.create') return operation.record.id
  if (operation.type === 'document.change') return operation.id
  return undefined
}

const findLatestBases = (disk: Disk, currentOnly: ReadonlySet<number>) => {
  const latestBases = new Map<number, SidecarRecord>()
  for (const marker of disk.markers) {
    for (const operation of marker.writes) {
      const id = documentOf(operation)
      if (id === undefined || !currentOnly.has(id) || !('ordinal' in operation)) continue
      const record = disk.records.get(
        sidecarKey(sidecarFileName('doc', id), marker.seq, operation.ordinal),
      )
      const previous = latestBases.get(id)
      if (
        record?.payload.type === 'document' &&
        record.payload.id === id &&
        record.payload.content.kind === 'base' &&
        (previous === undefined || isBefore(previous.seq, previous.ordinal, record))
      ) {
        latestBases.set(id, record)
      }
    }
  }
  return latestBases
}

export const analyze = (disk: Disk): Analysis => {
  const currentOnly = new Set<number>()
  const retired = new Set<number>()
  const taskIsLive = new Map<number, boolean>()
  for (const operation of disk.markers.flatMap((marker) => marker.writes)) {
    if (operation.type === 'document.create' && isCurrentOnly(operation.record)) {
      currentOnly.add(operation.record.id)
    } else if (operation.type === 'document.retire') {
      retired.add(operation.id)
    } else if (operation.type === 'task') {
      taskIsLive.set(operation.value.id, false)
    } else if (operation.type === 'task.sidecar') {
      taskIsLive.set(operation.id, true)
    }
  }
  return {
    currentOnly,
    retiredCurrentOnly: new Set([...retired].filter((id) => currentOnly.has(id))),
    taskIsLive,
    terminalTasks: new Set([...taskIsLive].filter(([, live]) => !live).map(([id]) => id)),
    latestBases: findLatestBases(disk, currentOnly),
  }
}

/** Whether a document line is obsolete: its document was retired, or a later base replaced it. */
export const isReclaimed = (analysis: Analysis, id: number, seq: number, ordinal: number) => {
  const base = analysis.latestBases.get(id)
  return analysis.retiredCurrentOnly.has(id) || (base !== undefined && isBefore(seq, ordinal, base))
}

/** The lines of one sidecar that its owner's final state still needs, or all confirmed lines. */
const retainedLines = (
  analysis: Analysis,
  parsed: ParsedFile<SidecarRecord>,
  confirmedLines: ParsedFile<SidecarRecord>['lines'],
) => {
  const id = sidecarId(parsed.file)
  if (parsed.file.startsWith('task-')) return analysis.terminalTasks.has(id) ? [] : undefined
  if (analysis.retiredCurrentOnly.has(id)) return []
  return analysis.latestBases.has(id)
    ? confirmedLines.filter(({ value }) => !isReclaimed(analysis, id, value.seq, value.ordinal))
    : undefined
}

/**
 * Truncate each sidecar's unconfirmed tail, written by a commit whose marker never landed, and
 * plan the rewrites that drop lines the final state no longer needs.
 */
export const cleanSidecars = Effect.fnUntraced(function* (
  files: Files,
  disk: Disk,
  analysis: Analysis,
  confirmed: ReadonlySet<string>,
) {
  const reclamations = new Map<string, string>()
  for (const parsed of disk.sidecars) {
    const isConfirmed = ({ value }: { readonly value: SidecarRecord }) =>
      confirmed.has(sidecarKey(parsed.file, value.seq, value.ordinal))
    const tail = parsed.lines.findIndex((line) => !isConfirmed(line))
    if (tail !== -1 && parsed.lines.slice(tail).some((line) => isConfirmed(line))) {
      return yield* corrupt(`Confirmed record follows an unconfirmed tail in ${parsed.file}`)
    }
    const tailLine = tail === -1 ? undefined : parsed.lines[tail]
    if (tailLine !== undefined) yield* truncate(files, parsed.file, tailLine.start, 'tail')
    const confirmedLines = tail === -1 ? parsed.lines : parsed.lines.slice(0, tail)
    const retained = retainedLines(analysis, parsed, confirmedLines)
    if (
      retained !== undefined &&
      (retained.length < confirmedLines.length || retained.length === 0)
    ) {
      reclamations.set(parsed.file, retained.map(({ text }) => `${text}\n`).join(''))
    }
  }
  return reclamations
})

/** Carry what recovery learned into the live sidecar state. */
export const adoptAnalysis = (sidecarState: SidecarState, analysis: Analysis): void => {
  for (const id of analysis.currentOnly) sidecarState.currentOnlyDocuments.add(id)
  for (const [id, live] of analysis.taskIsLive) {
    if (live) sidecarState.liveTaskSidecars.add(id)
  }
}
