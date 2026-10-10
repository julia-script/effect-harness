import * as Schema from 'effect/Schema'
import { StorageError } from '../StorageError.js'

/** Causal evidence carried only by an escaping native checkpoint write, never a callback. */
export class NativeWriteFailure extends Schema.TaggedError<NativeWriteFailure>()(
  'NativeWriteFailure',
  { error: StorageError },
) {}
