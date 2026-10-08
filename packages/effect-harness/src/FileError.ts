/**
 * Semantic file operation failures with retained native causes and paths.
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'

const Payload = Schema.Struct({
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  path: SchemaField.optional(Schema.String),
})

/**
 * Schema for file error code.
 *
 * @category schemas
 */
export const FileErrorCode = Schema.Literals([
  'aborted',
  'not_found',
  'permission_denied',
  'not_directory',
  'is_directory',
  'invalid',
  'not_supported',
  'unknown',
])
/**
 * Decoded value validated by the `FileErrorCode` schema.
 *
 * @category models
 */
export type FileErrorCode = typeof FileErrorCode.Type
/**
 * File operation failure reporting cancellation.
 *
 * @category errors
 */
export class FileAbortedError extends Schema.TaggedError<FileAbortedError>(
  '@effect-harness/harness/FileError/FileAbortedError',
)('FileAbortedError', Payload.fields) {
  get code(): 'aborted' {
    return 'aborted'
  }
}
/**
 * File operation failure reporting a missing path.
 *
 * @category errors
 */
export class FileNotFoundError extends Schema.TaggedError<FileNotFoundError>(
  '@effect-harness/harness/FileError/FileNotFoundError',
)('FileNotFoundError', Payload.fields) {
  get code(): 'not_found' {
    return 'not_found'
  }
}
/**
 * File operation failure reporting denied filesystem access.
 *
 * @category errors
 */
export class FilePermissionDeniedError extends Schema.TaggedError<FilePermissionDeniedError>(
  '@effect-harness/harness/FileError/FilePermissionDeniedError',
)('FilePermissionDeniedError', Payload.fields) {
  get code(): 'permission_denied' {
    return 'permission_denied'
  }
}
/**
 * File operation failure reporting a directory operation on a non-directory.
 *
 * @category errors
 */
export class FileNotDirectoryError extends Schema.TaggedError<FileNotDirectoryError>(
  '@effect-harness/harness/FileError/FileNotDirectoryError',
)('FileNotDirectoryError', Payload.fields) {
  get code(): 'not_directory' {
    return 'not_directory'
  }
}
/**
 * File operation failure reporting a file operation on a directory.
 *
 * @category errors
 */
export class FileIsDirectoryError extends Schema.TaggedError<FileIsDirectoryError>(
  '@effect-harness/harness/FileError/FileIsDirectoryError',
)('FileIsDirectoryError', Payload.fields) {
  get code(): 'is_directory' {
    return 'is_directory'
  }
}
/**
 * File operation failure reporting invalid path or operation input.
 *
 * @category errors
 */
export class FileInvalidError extends Schema.TaggedError<FileInvalidError>(
  '@effect-harness/harness/FileError/FileInvalidError',
)('FileInvalidError', Payload.fields) {
  get code(): 'invalid' {
    return 'invalid'
  }
}
/**
 * File operation failure reporting an unavailable adapter capability.
 *
 * @category errors
 */
export class FileNotSupportedError extends Schema.TaggedError<FileNotSupportedError>(
  '@effect-harness/harness/FileError/FileNotSupportedError',
)('FileNotSupportedError', Payload.fields) {
  get code(): 'not_supported' {
    return 'not_supported'
  }
}
/**
 * File operation failure retaining an otherwise unclassified native cause.
 *
 * @category errors
 */
export class FileUnknownError extends Schema.TaggedError<FileUnknownError>(
  '@effect-harness/harness/FileError/FileUnknownError',
)('FileUnknownError', Payload.fields) {
  get code(): 'unknown' {
    return 'unknown'
  }
}
/**
 * Schema for file error reason.
 *
 * @category schemas
 */
export const FileErrorReason = Schema.Union([
  FileAbortedError,
  FileNotFoundError,
  FilePermissionDeniedError,
  FileNotDirectoryError,
  FileIsDirectoryError,
  FileInvalidError,
  FileNotSupportedError,
  FileUnknownError,
])
/**
 * Decoded value validated by the `FileErrorReason` schema.
 *
 * @category models
 */
export type FileErrorReason = typeof FileErrorReason.Type
/**
 * Semantic file error with its retained cause.
 *
 * @category errors
 */
export class FileError extends Schema.TaggedError<FileError>(
  '@effect-harness/harness/FileError/FileError',
)('FileError', { reason: FileErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  get code(): FileErrorCode {
    return this.reason.code
  }
  get path(): string | undefined {
    return this.reason.path
  }
}
/**
 * Constructs the semantic file reason for a known code and its original boundary metadata.
 *
 * @category combinators
 */
export const fileReason = (
  code: FileErrorCode,
  fields: {
    readonly message: string
    // oxlint-disable-next-line typescript/no-redundant-type-constituents -- Public optional boundary inputs retain explicit undefined even when unknown already includes it (C10).
    readonly cause?: unknown | undefined
    readonly path?: string | undefined
  },
): FileErrorReason => {
  switch (code) {
    case 'aborted':
      return new FileAbortedError(fields)
    case 'not_found':
      return new FileNotFoundError(fields)
    case 'permission_denied':
      return new FilePermissionDeniedError(fields)
    case 'not_directory':
      return new FileNotDirectoryError(fields)
    case 'is_directory':
      return new FileIsDirectoryError(fields)
    case 'invalid':
      return new FileInvalidError(fields)
    case 'not_supported':
      return new FileNotSupportedError(fields)
    case 'unknown':
      return new FileUnknownError(fields)
  }
}
