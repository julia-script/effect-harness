import * as Effect from 'effect/Effect'
import type * as Schema from 'effect/Schema'
import * as Media from '../../src/StorageMedia.js'

/** Opaque fault fixtures have no typed image slots; retain a valid physical manifest. */
export const empty = (body: Schema.Json): Media.PackedBody =>
  Effect.runSync(Media.pack(body, { category: 'conversation' })).packed
