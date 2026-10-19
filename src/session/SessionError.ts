/**
 * Errors a `Session` reports.
 */
import { Schema } from 'effect'
import type { StorageRequestError } from '../storage/StorageError.ts'

/**
 * The Session failed, so nothing runs or commits any more. It fails on the first `StorageError`
 * its Storage reports; `cause` is that error. Reopen the Session over the same Storage to recover
 * from what was committed.
 */
export class SessionFailed extends Schema.TaggedError<
  SessionFailed,
  { readonly brand: unique symbol }
>('effect-harness/session/SessionError/SessionFailed')('SessionFailed', {
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

/** The Session's scope closed: it admits no more commits. */
export class SessionClosed extends Schema.TaggedError<
  SessionClosed,
  { readonly brand: unique symbol }
>('effect-harness/session/SessionError/SessionClosed')('SessionClosed', {
  message: Schema.String,
}) {}

/**
 * A transaction read a table after its first table write. Read every required row before
 * writing.
 */
export class ReadAfterWrite extends Schema.TaggedError<
  ReadAfterWrite,
  { readonly brand: unique symbol }
>('effect-harness/session/SessionError/ReadAfterWrite')('ReadAfterWrite', {
  message: Schema.String,
  /** The `Tx` read that was refused. */
  method: Schema.String,
}) {}

/**
 * A transaction operation was rejected: it names a record that does not exist or cannot change
 * that way, it ran after its transaction settled, or the batch failed validation before Storage
 * saw it. Nothing of the transaction is committed, and the Session stays usable.
 */
export class TransactionError extends Schema.TaggedError<
  TransactionError,
  { readonly brand: unique symbol }
>('effect-harness/session/SessionError/TransactionError')('TransactionError', {
  message: Schema.String,
}) {}

/**
 * A document access was refused: its token disagrees with the stored incarnation's scope,
 * history, fork policy, or version, a value is not strict JSON, or a stored value cannot be read.
 * Nothing of the transaction is committed, and the Session stays usable.
 */
export class DocumentError extends Schema.TaggedError<
  DocumentError,
  { readonly brand: unique symbol }
>('effect-harness/session/SessionError/DocumentError')('DocumentError', {
  message: Schema.String,
}) {}

/** How a `Tx` operation fails. */
export type TxError = TransactionError | SessionFailed

/** How a `Tx` table read fails. */
export type TxReadError = TxError | ReadAfterWrite | StorageRequestError

/** How a `Tx` document operation fails. */
export type TxDocumentError = TxError | DocumentError | StorageRequestError

/** How a commit fails besides its callback's own errors. */
export type CommitError = TxError | DocumentError | SessionClosed | StorageRequestError

/** How a committed document read fails. */
export type SnapshotError = DocumentError | SessionFailed | SessionClosed | StorageRequestError
