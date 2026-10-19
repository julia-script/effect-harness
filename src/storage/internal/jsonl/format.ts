import { Effect, Schema } from 'effect'
import { ConversationRecord } from '../../schema/Conversation.ts'
import { DocumentContent, DocumentCreate } from '../../schema/Document.ts'
import { EntryRecord } from '../../schema/Entry.ts'
import { DocumentId, NonNegativeInt, Seq, TaskId } from '../../schema/Ids.ts'
import { SubmissionRecord } from '../../schema/Submission.ts'
import { TaskRecord } from '../../schema/Task.ts'
import { StorageError } from '../../StorageError.ts'

/**
 * The on-disk JSONL format, with the files and line shapes of pi's `storage/jsonl/storage.ts`.
 *
 * `main.jsonl` holds one commit marker per line. Immutable records and terminal tasks live in the
 * marker itself; live task states and document revisions go to per-record sidecars
 * (`task-<id>.jsonl`, `doc-<id>.jsonl`), and the marker points at them by commit-wide ordinal. A
 * sidecar record exists only once the marker naming it is complete, so the marker publishes the
 * commit.
 */

export const FORMAT_VERSION = 1
export const MAIN_FILE = 'main.jsonl'
export const RECLAIM_SUFFIX = '.reclaim'

const Format = Schema.Literal(FORMAT_VERSION)
const Ordinal = NonNegativeInt

const TerminalTask = TaskRecord.check(
  Schema.makeFilter((task) => task.state.status === 'terminal' || 'a terminal task'),
)
const LiveTask = TaskRecord.check(
  Schema.makeFilter((task) => task.state.status !== 'terminal' || 'a live task'),
)

/** One write as the main file records it, with sidecar payloads replaced by ordinals. */
export const MainOperation = Schema.Union([
  Schema.Struct({ type: Schema.Literal('conversation'), value: ConversationRecord }),
  Schema.Struct({ type: Schema.Literal('entry'), value: EntryRecord }),
  Schema.Struct({ type: Schema.Literal('submission'), value: SubmissionRecord }),
  Schema.Struct({ type: Schema.Literal('document.retire'), id: DocumentId }),
  Schema.Struct({ type: Schema.Literal('task'), value: TerminalTask }),
  Schema.Struct({ type: Schema.Literal('task.sidecar'), id: TaskId, ordinal: Ordinal }),
  Schema.Struct({
    type: Schema.Literal('document.create'),
    record: DocumentCreate,
    ordinal: Ordinal,
  }),
  Schema.Struct({ type: Schema.Literal('document.change'), id: DocumentId, ordinal: Ordinal }),
])
export type MainOperation = typeof MainOperation.Type

/** One `main.jsonl` line: the marker that publishes a commit. */
export const MainMarker = Schema.Struct({
  format: Format,
  type: Schema.Literal('commit'),
  seq: Seq,
  writes: Schema.Array(MainOperation),
})
export type MainMarker = typeof MainMarker.Type

export const SidecarPayload = Schema.Union([
  Schema.Struct({ type: Schema.Literal('task'), value: LiveTask }),
  Schema.Struct({ type: Schema.Literal('document'), id: DocumentId, content: DocumentContent }),
])
export type SidecarPayload = typeof SidecarPayload.Type

/** One sidecar line, keyed by the commit and ordinal of the marker that confirms it. */
export const SidecarRecord = Schema.Struct({
  format: Format,
  type: Schema.Literal('record'),
  seq: Seq,
  ordinal: Ordinal,
  payload: SidecarPayload,
})
export type SidecarRecord = typeof SidecarRecord.Type

export const sidecarFileName = (kind: 'doc' | 'task', id: number): string => `${kind}-${id}.jsonl`

const SIDECAR_NAME = /^(?:doc|task)-(?:0|[1-9]\d*)\.jsonl$/u
const RECLAIM_NAME = /^(?:doc|task)-(?:0|[1-9]\d*)\.jsonl\.reclaim$/u

export const isSidecarFileName = (name: string): boolean => SIDECAR_NAME.test(name)
export const isReclaimFileName = (name: string): boolean => RECLAIM_NAME.test(name)

/** The record ID a sidecar file name carries. */
export const sidecarId = (file: string): number =>
  Number(file.slice(file.indexOf('-') + 1, -'.jsonl'.length))

export const sidecarKey = (file: string, seq: number, ordinal: number): string =>
  JSON.stringify([file, seq, ordinal])

const encodeMarkerLine = Schema.encodeEffect(Schema.fromJsonString(MainMarker))
const encodeSidecarLine = Schema.encodeEffect(Schema.fromJsonString(SidecarRecord))

const encodeFailure = (cause: Schema.SchemaError) =>
  new StorageError({ message: `Commit cannot be serialized: ${cause.message}`, cause })

/** Serialize a marker as one complete line, rejecting values JSON cannot round-trip. */
export const markerLine = (marker: MainMarker): Effect.Effect<string, StorageError> =>
  encodeMarkerLine(marker).pipe(
    Effect.map((line) => `${line}\n`),
    Effect.mapError(encodeFailure),
  )

/** Serialize a sidecar record as one complete line. */
export const sidecarLine = (record: SidecarRecord): Effect.Effect<string, StorageError> =>
  encodeSidecarLine(record).pipe(
    Effect.map((line) => `${line}\n`),
    Effect.mapError(encodeFailure),
  )

const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))
const decodeMarker = Schema.decodeUnknownEffect(MainMarker)
const decodeSidecar = Schema.decodeUnknownEffect(SidecarRecord)

const corrupt = (message: string) => (cause: Schema.SchemaError) =>
  new StorageError({ message, cause })

const parseJson = (text: string, description: string) =>
  decodeJson(text).pipe(Effect.mapError(corrupt(`Malformed complete ${description}`)))

/** Decode one complete `main.jsonl` line. */
export const parseMarker = (text: string, line: number): Effect.Effect<MainMarker, StorageError> =>
  Effect.flatMap(parseJson(text, `${MAIN_FILE} line ${line}`), (value) =>
    decodeMarker(value).pipe(
      Effect.mapError(corrupt(`Invalid commit marker in ${MAIN_FILE} line ${line}`)),
    ),
  )

/** Decode one complete sidecar line. */
export const parseSidecar =
  (file: string) =>
  (text: string, line: number): Effect.Effect<SidecarRecord, StorageError> =>
    Effect.flatMap(parseJson(text, `${file} line ${line}`), (value) =>
      decodeSidecar(value).pipe(
        Effect.mapError(corrupt(`Invalid sidecar record in ${file} line ${line}`)),
      ),
    )
