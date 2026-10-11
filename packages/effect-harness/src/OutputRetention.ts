import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Buffer from './internal/OutputBuffer.js'

/** Full requires complete output within the hard budgets; overflow is a typed failure. */
export const Policy = Schema.Struct({
  retain: Schema.Literals(['head', 'tail', 'full']),
  maxBytes: Schema.Finite,
  maxLines: Schema.Finite,
})
export type Policy = typeof Policy.Type
export const defaultPolicy: Policy = { retain: 'tail', maxBytes: 50 * 1024, maxLines: 2000 }
export const Snapshot = Schema.Struct({
  text: Schema.String,
  totalBytes: Schema.Finite,
  totalLines: Schema.Finite,
  retainedBytes: Schema.Finite,
  retainedLines: Schema.Finite,
  skippedBytes: Schema.Finite,
  skippedLines: Schema.Finite,
  truncated: Schema.Boolean,
})
export type Snapshot = typeof Snapshot.Type
export const Skip = Schema.Struct({
  bytes: Schema.Finite,
  newlines: Schema.Finite,
  endsWithNewline: Schema.Boolean,
})
export type Skip = typeof Skip.Type

export class OutputRetentionError extends Schema.TaggedError<OutputRetentionError>()(
  'OutputRetentionError',
  { reason: Schema.Literals(['invalidPolicy', 'invalidSkip', 'overflow']), message: Schema.String },
) {}

export type OutputBuffer = Buffer.OutputBuffer
/** Budgets are nonnegative safe integers. Memory is at most maxBytes + 4 retained bytes. */
export const make = Effect.fnUntraced(function* (policy: Policy = defaultPolicy) {
  if (
    !Number.isSafeInteger(policy.maxBytes) ||
    policy.maxBytes < 0 ||
    !Number.isSafeInteger(policy.maxLines) ||
    policy.maxLines < 0 ||
    !['head', 'tail', 'full'].includes(policy.retain) ||
    policy.maxBytes > 0xfffffff0
  )
    return yield* new OutputRetentionError({
      reason: 'invalidPolicy',
      message:
        'Retention requires nonnegative safe budgets; maxBytes must fit a bounded byte array.',
    })
  return Buffer.make(policy)
})

/**
 * Incremental UTF-8, with a separate buffer for each independent byte stream.
 * Byte totals describe decoded UTF-8 (invalid bytes become U+FFFD), not raw input.
 * Skips require tail mode and a gap larger than its window, so prior text cannot survive.
 * A string/skip flushes incomplete earlier bytes; U+FEFF is preserved as text.
 */
export const append = Effect.fnUntraced(function* (
  buffer: OutputBuffer,
  chunk: string | Uint8Array,
  skipped?: Skip,
) {
  if (
    skipped !== undefined &&
    (buffer.policy.retain !== 'tail' ||
      !Number.isSafeInteger(skipped.bytes) ||
      skipped.bytes <= 0 ||
      !Number.isSafeInteger(skipped.newlines) ||
      skipped.newlines < 0 ||
      skipped.newlines > skipped.bytes ||
      (skipped.endsWithNewline && skipped.newlines === 0) ||
      (skipped.bytes <= buffer.policy.maxBytes && skipped.newlines <= buffer.policy.maxLines))
  )
    return yield* new OutputRetentionError({
      reason: 'invalidSkip',
      message:
        'Skipped tail output must exceed a byte or line window and carry exact UTF-8 counters.',
    })
  Buffer.append(buffer, chunk, skipped)
})
/** Snapshot does not flush an incomplete code point. */
export const snapshot = Buffer.snapshot
/** Finish flushes an incomplete code point once. Full mode refuses any loss. */
export const finish = Effect.fnUntraced(function* (buffer: OutputBuffer) {
  Buffer.end(buffer)
  const value = Buffer.snapshot(buffer)
  if (buffer.policy.retain === 'full' && value.truncated)
    return yield* new OutputRetentionError({
      reason: 'overflow',
      message: 'Full output exceeded its declared hard retention budget.',
    })
  return value
})
