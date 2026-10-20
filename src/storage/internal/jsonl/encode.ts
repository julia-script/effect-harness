import { Effect } from 'effect'
import { FORMAT_VERSION, markerLine, sidecarFileName, sidecarLine } from './format.ts'
import type { MainOperation, SidecarPayload, SidecarRecord } from './format.ts'
import type { Seq } from '../../schema/Ids.ts'
import type { ResolvedWrite } from '../memory/documents.ts'
import { getOrCreate, isCurrentOnly } from '../memory/state.ts'

/** Which sidecars hold state a later commit may make obsolete. */
export interface SidecarState {
  /** Documents whose sidecar keeps only lines from the latest base on. */
  readonly currentOnlyDocuments: Set<number>
  /** Tasks whose latest state lives in a sidecar. */
  readonly liveTaskSidecars: Set<number>
}

/** The serialized lines of one commit: its marker and the text appended to each sidecar. */
export interface EncodedCommit {
  readonly marker: string
  readonly sidecars: ReadonlyMap<string, string>
}

interface Split {
  readonly operations: ReadonlyArray<MainOperation>
  readonly records: ReadonlyMap<string, ReadonlyArray<SidecarRecord>>
}

/** Route each write to the marker or to a sidecar record numbered by a commit-wide ordinal. */
const split = (seq: Seq, writes: ReadonlyArray<ResolvedWrite>): Split => {
  const operations: Array<MainOperation> = []
  const records = new Map<string, Array<SidecarRecord>>()
  let nextOrdinal = 0
  const sidecar = (file: string, payload: SidecarPayload): number => {
    const ordinal = nextOrdinal++
    getOrCreate(records, file, (): Array<SidecarRecord> => []).push({
      format: FORMAT_VERSION,
      type: 'record',
      seq,
      ordinal,
      payload,
    })
    return ordinal
  }
  for (const write of writes) {
    if (write.type === 'task' && write.value.state.status !== 'terminal') {
      const ordinal = sidecar(sidecarFileName('task', write.value.id), write)
      operations.push({ type: 'task.sidecar', id: write.value.id, ordinal })
    } else if (write.type === 'document.create') {
      const { id } = write.record
      const ordinal = sidecar(sidecarFileName('doc', id), {
        type: 'document',
        id,
        content: write.content,
      })
      operations.push({ type: 'document.create', record: write.record, ordinal })
    } else if (write.type === 'document.change') {
      const ordinal = sidecar(sidecarFileName('doc', write.id), {
        type: 'document',
        id: write.id,
        content: write.content,
      })
      operations.push({ type: 'document.change', id: write.id, ordinal })
    } else {
      operations.push(write)
    }
  }
  return { operations, records }
}

/** Serialize a prepared commit completely before any I/O, so a value JSON rejects writes nothing. */
export const encodeCommit = Effect.fnUntraced(function* (
  seq: Seq,
  writes: ReadonlyArray<ResolvedWrite>,
) {
  const { operations, records } = split(seq, writes)
  const sidecars = new Map<string, string>()
  for (const [file, lines] of records) {
    const encoded: ReadonlyArray<string> = yield* Effect.forEach(lines, sidecarLine)
    sidecars.set(file, encoded.join(''))
  }
  const marker: string = yield* markerLine({
    format: FORMAT_VERSION,
    type: 'commit',
    seq,
    writes: operations,
  })
  const encoded: EncodedCommit = { marker, sidecars }
  return encoded
})

interface Obsoleting {
  readonly created: Set<number>
  readonly retired: Set<number>
  readonly rebased: Set<number>
  readonly terminalTasks: Set<number>
}

const obsoleting = (writes: ReadonlyArray<ResolvedWrite>): Obsoleting => {
  const found: Obsoleting = {
    created: new Set(),
    retired: new Set(),
    rebased: new Set(),
    terminalTasks: new Set(),
  }
  for (const write of writes) {
    if (write.type === 'document.create' && isCurrentOnly(write.record)) {
      found.created.add(write.record.id)
    } else if (write.type === 'document.change' && write.content.kind === 'base') {
      found.rebased.add(write.id)
    } else if (write.type === 'document.retire') {
      found.retired.add(write.id)
    } else if (write.type === 'task') {
      // The last write of a task in the batch decides its state.
      if (write.value.state.status === 'terminal') found.terminalTasks.add(write.value.id)
      else found.terminalTasks.delete(write.value.id)
    }
  }
  return found
}

/**
 * The sidecar rewrites a published commit allows: an empty string removes the file. Retired
 * current-only documents and terminal tasks lose their sidecar; a current-only document rebased in
 * this commit keeps only this commit's lines. Rewindable history is never reclaimed.
 */
export const planReclamations = (
  sidecarState: SidecarState,
  writes: ReadonlyArray<ResolvedWrite>,
  encoded: EncodedCommit,
): ReadonlyMap<string, string> => {
  const found = obsoleting(writes)
  const currentOnly = (id: number) =>
    sidecarState.currentOnlyDocuments.has(id) || found.created.has(id)
  const replacements = new Map<string, string>()
  for (const id of found.retired) {
    if (currentOnly(id)) replacements.set(sidecarFileName('doc', id), '')
  }
  for (const id of found.rebased) {
    const file = sidecarFileName('doc', id)
    const content = encoded.sidecars.get(file)
    if (currentOnly(id) && !found.retired.has(id) && content !== undefined) {
      replacements.set(file, content)
    }
  }
  for (const id of found.terminalTasks) {
    const file = sidecarFileName('task', id)
    if (sidecarState.liveTaskSidecars.has(id) || encoded.sidecars.has(file)) {
      replacements.set(file, '')
    }
  }
  return replacements
}

/** Record the sidecar state a published commit leaves behind. */
export const adoptSidecarState = (
  sidecarState: SidecarState,
  writes: ReadonlyArray<ResolvedWrite>,
): void => {
  for (const write of writes) {
    if (write.type === 'document.create' && isCurrentOnly(write.record)) {
      sidecarState.currentOnlyDocuments.add(write.record.id)
    } else if (write.type === 'task') {
      if (write.value.state.status === 'terminal')
        sidecarState.liveTaskSidecars.delete(write.value.id)
      else sidecarState.liveTaskSidecars.add(write.value.id)
    }
  }
}
