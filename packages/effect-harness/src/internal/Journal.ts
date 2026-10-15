/** Single-owner storage over an application-supplied Effect event journal. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as EventJournal from 'effect/eventlog/EventJournal'
import type { Access } from './Kernel.js'
import * as Kernel from './Kernel.js'
import * as Memory from './Memory.js'
import * as Metadata from './Metadata.js'
import * as Row from './Row.js'
import * as Errors from '../StorageError.js'

const event = 'effect-harness/commit'
const Envelope = Schema.Struct({
  version: Metadata.Counter,
  revision: Schema.Finite.check(
    Schema.isInt(),
    Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
  ),
  previous: Schema.NullOr(Schema.String),
  metadata: Metadata.Metadata,
  rows: Schema.Array(Schema.Unknown),
})
const Frame = Envelope.mapFields((fields) => ({ ...fields, rows: Schema.Array(Row.Stored) }))
const FrameJson = Schema.fromJsonString(Frame)
const EnvelopeJson = Schema.fromJsonString(Envelope)

// Detect duplicate owners of the same injected service. Applications must also ensure
// exclusive ownership across separate journal instances, processes and browser tabs.
const owners = new WeakMap<EventJournal.EventJournal['Service'], Set<string>>()

export const make = Effect.fnUntraced(function* (options: { readonly storeId?: string }) {
  const storeId = options.storeId ?? 'default'
  yield* Schema.decodeEffect(Schema.NonEmptyString)(storeId).pipe(
    Effect.mapError(Errors.invalid('initialize')),
  )
  const journal = yield* EventJournal.EventJournal
  const memory = yield* Memory.makeAccess
  yield* Effect.acquireRelease(
    Effect.suspend(() => {
      let stores = owners.get(journal)
      if (stores?.has(storeId))
        return Effect.fail(
          Errors.make('invalid', 'initialize', 'Journal store already has an owner'),
        )
      if (stores === undefined) {
        stores = new Set()
        owners.set(journal, stores)
      }
      stores.add(storeId)
      return Effect.void
    }),
    // Kernel closes admission first; settle any admitted write before releasing ownership.
    () =>
      memory.exclusive(Effect.sync(() => owners.get(journal)?.delete(storeId))).pipe(Effect.orDie),
  )
  const entries = yield* journal.entries.pipe(
    Effect.mapError((cause) => Errors.make('io', 'initialize', 'Cannot read journal', cause)),
  )
  const frames = yield* Effect.forEach(
    entries.filter((entry) => entry.event === event && entry.primaryKey === storeId),
    (entry) =>
      Effect.gen(function* () {
        const text = yield* Effect.try({
          try: () => new TextDecoder('utf-8', { fatal: true }).decode(entry.payload),
          catch: Errors.corrupt('recover'),
        })
        const envelope = yield* Schema.decodeEffect(EnvelopeJson)(text).pipe(
          Effect.mapError(Errors.corrupt('recover')),
        )
        if (envelope.version !== 1)
          return yield* Errors.make('unsupportedFormat', 'recover', 'Unsupported journal protocol')
        // Inspect every reader barrier before decoding any domain row.
        yield* Metadata.requireSupported(envelope.metadata)
        return { id: entry.idString, envelope }
      }),
  )
  frames.sort((a, b) => a.envelope.revision - b.envelope.revision)
  let revision = 0
  let previous: string | null = null
  let metadata = Metadata.initialMetadata
  const identities = new Map<number, Row.Kind>()
  for (const { id, envelope } of frames) {
    if (envelope.revision !== revision + 1 || envelope.previous !== previous)
      return yield* Errors.make('corrupt', 'recover', 'Journal revision chain is broken')
    const frame = yield* Schema.decodeUnknownEffect(Frame)(envelope).pipe(
      Effect.mapError(Errors.corrupt('recover')),
    )
    if (
      frame.metadata.format < metadata.format ||
      (frame.metadata.minimumReader ?? 1) < (metadata.minimumReader ?? 1) ||
      frame.metadata.nextId < metadata.nextId ||
      (frame.metadata.nextSeq !== metadata.nextSeq &&
        frame.metadata.nextSeq !== metadata.nextSeq + 1) ||
      (frame.rows.length > 0 && frame.metadata.nextSeq !== metadata.nextSeq + 1)
    )
      return yield* Errors.make(
        'corrupt',
        'recover',
        'Journal allocation counters are inconsistent',
      )
    const seen = new Set<number>()
    for (const row of frame.rows) {
      const rowId = Row.idOf(row)
      const existing = identities.get(rowId)
      if (
        seen.has(rowId) ||
        rowId >= frame.metadata.nextId ||
        (existing !== undefined && existing !== row._tag)
      )
        return yield* Errors.make(
          'corrupt',
          'recover',
          'Journal contains conflicting record identities',
        )
      if (row._tag === 'entry' && row.value.commitSeq !== frame.metadata.nextSeq - 1)
        return yield* Errors.make(
          'corrupt',
          'recover',
          'Journal entry has an invalid commit sequence',
        )
      seen.add(rowId)
      identities.set(rowId, row._tag)
    }
    yield* memory.save(frame.rows, frame.metadata)
    revision = frame.revision
    previous = id
    metadata = frame.metadata
  }
  const access: Access = {
    ...memory,
    save: Effect.fnUntraced(function* (rows, metadata) {
      if (revision >= Number.MAX_SAFE_INTEGER)
        return yield* Errors.make('invalid', 'commit', 'Journal revision counter is exhausted')
      const encoded = yield* Schema.encodeEffect(FrameJson)({
        version: 1,
        revision: revision + 1,
        previous,
        rows,
        metadata,
      }).pipe(Effect.mapError(Errors.invalid('commit')))
      // The callback runs BEFORE the journal commits. Never publish the projection there.
      const id = yield* journal
        .write({
          event,
          primaryKey: storeId,
          payload: new TextEncoder().encode(encoded),
          effect: (entry) => Effect.succeed(entry.idString),
        })
        .pipe(
          Effect.catchCause((cause) =>
            Effect.fail(
              Errors.make(
                'uncertain',
                'commit',
                'Journal append outcome is uncertain; reopen storage',
                cause,
              ),
            ),
          ),
        )
      yield* memory.save(rows, metadata)
      revision++
      previous = id
    }),
  }
  return yield* Kernel.make(access)
})
