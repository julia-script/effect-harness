import { Effect } from 'effect'
import type { DocumentBase, DocumentContent } from '../../schema/Document.ts'
import type { ResolvedWrite } from '../memory/documents.ts'
import { prepareCommit } from '../memory/commit.ts'
import type { State } from '../memory/state.ts'
import { isReclaimed } from './recover.ts'
import type { Analysis, Disk } from './recover.ts'
import { sidecarFileName, sidecarKey } from './format.ts'
import type { MainMarker, MainOperation, SidecarRecord } from './format.ts'
import { StorageError } from '../../StorageError.ts'

/** Rebuilds the in-memory state commit by commit, recording which sidecar lines it confirmed. */
interface Replay {
  readonly disk: Disk
  readonly analysis: Analysis
  readonly confirmed: Set<string>
}

/** Placeholder content for a creation whose sidecar line was legitimately reclaimed. */
const EMPTY_BASE: DocumentBase = { kind: 'base', version: 1, value: {} }

const corrupt = (message: string) => Effect.fail(new StorageError({ message }))

/**
 * The sidecar line a marker operation points at. A missing line is corruption unless the final
 * state made it obsolete (`optional`), since reclamation may already have removed it.
 */
const confirm = Effect.fnUntraced(function* (
  replay: Replay,
  marker: MainMarker,
  file: string,
  ordinal: number,
  optional: boolean,
) {
  const key = sidecarKey(file, marker.seq, ordinal)
  if (replay.confirmed.has(key)) return yield* corrupt('Sidecar record is confirmed more than once')
  const record = replay.disk.records.get(key)
  if (record === undefined) {
    return optional
      ? undefined
      : yield* corrupt(`Missing confirmed sidecar record ${file} at sequence ${marker.seq}`)
  }
  replay.confirmed.add(key)
  return record
})

const taskWrites = Effect.fnUntraced(function* (
  replay: Replay,
  marker: MainMarker,
  operation: Extract<MainOperation, { readonly type: 'task.sidecar' }>,
) {
  const optional = replay.analysis.terminalTasks.has(operation.id)
  const file = sidecarFileName('task', operation.id)
  const record = yield* confirm(replay, marker, file, operation.ordinal, optional)
  if (record === undefined) return []
  if (record.payload.type !== 'task' || record.payload.value.id !== operation.id) {
    return yield* corrupt(`Confirmed task sidecar data does not match commit ${marker.seq}`)
  }
  const writes: ReadonlyArray<ResolvedWrite> = optional
    ? []
    : [{ type: 'task', value: record.payload.value }]
  return writes
})

const documentContent = (
  marker: MainMarker,
  id: number,
  record: SidecarRecord,
): Effect.Effect<DocumentContent, StorageError> => {
  if (record.payload.type !== 'document' || record.payload.id !== id) {
    return corrupt(`Confirmed document sidecar data does not match commit ${marker.seq}`)
  }
  return Effect.succeed(record.payload.content)
}

const documentWrites = Effect.fnUntraced(function* (
  replay: Replay,
  marker: MainMarker,
  operation: Extract<MainOperation, { readonly type: 'document.create' | 'document.change' }>,
) {
  const id = operation.type === 'document.create' ? operation.record.id : operation.id
  const reclaimed = isReclaimed(replay.analysis, id, marker.seq, operation.ordinal)
  const file = sidecarFileName('doc', id)
  const record = yield* confirm(replay, marker, file, operation.ordinal, reclaimed)
  const content = record === undefined ? undefined : yield* documentContent(marker, id, record)
  if (operation.type === 'document.change') {
    const writes: ReadonlyArray<ResolvedWrite> =
      reclaimed || content === undefined
        ? []
        : [{ type: 'document.change', id: operation.id, content }]
    return writes
  }
  if (content?.kind === 'delta') {
    return yield* corrupt(`Document creation lacks a confirmed base in commit ${marker.seq}`)
  }
  const writes: ReadonlyArray<ResolvedWrite> = [
    {
      type: 'document.create',
      record: operation.record,
      content: reclaimed || content === undefined ? EMPTY_BASE : content,
    },
  ]
  return writes
})

const operationWrites = (
  replay: Replay,
  marker: MainMarker,
  operation: MainOperation,
): Effect.Effect<ReadonlyArray<ResolvedWrite>, StorageError> => {
  if (operation.type === 'task.sidecar') return taskWrites(replay, marker, operation)
  if (operation.type === 'document.create' || operation.type === 'document.change') {
    return documentWrites(replay, marker, operation)
  }
  return Effect.succeed([operation])
}

const replayMarker = Effect.fnUntraced(function* (
  memory: State,
  replay: Replay,
  marker: MainMarker,
) {
  const writes = yield* Effect.forEach(marker.writes, (operation) =>
    operationWrites(replay, marker, operation),
  )
  const prepared = yield* prepareCommit(memory, writes.flat(), marker.seq).pipe(
    Effect.mapError(
      (cause) =>
        new StorageError({ message: `Invalid committed state at sequence ${marker.seq}`, cause }),
    ),
  )
  prepared.apply()
})

/** Apply every complete commit to `memory`; returns the keys of the sidecar lines they confirm. */
export const replay = Effect.fnUntraced(function* (memory: State, disk: Disk, analysis: Analysis) {
  const state: Replay = { disk, analysis, confirmed: new Set() }
  yield* Effect.forEach(disk.markers, (marker) => replayMarker(memory, state, marker), {
    discard: true,
  })
  const confirmed: ReadonlySet<string> = state.confirmed
  return confirmed
})
