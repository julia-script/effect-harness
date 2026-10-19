/**
 * Errors a `Storage` backend reports.
 */
import { Schema } from 'effect'

/**
 * A read rejected an invalid request, such as an unknown conversation or a cursor from another
 * scan, with no durable effect. It fails only that read.
 */
export class StorageRequestError extends Schema.TaggedError<
  StorageRequestError,
  { readonly brand: unique symbol }
>('effect-harness/storage/StorageError/StorageRequestError')('StorageRequestError', {
  message: Schema.String,
}) {}

/**
 * Storage failed: a rejected commit, an exhausted ID space, corrupt state, a closed backend, or a
 * backend I/O failure. Unlike `StorageRequestError` it is fatal to the Session, which nothing
 * retries, so a backend retries transient failures itself before failing with it.
 */
export class StorageError extends Schema.TaggedError<
  StorageError,
  { readonly brand: unique symbol }
>('effect-harness/storage/StorageError/StorageError')('StorageError', {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
