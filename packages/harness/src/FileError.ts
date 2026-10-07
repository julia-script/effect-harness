/**
 * Semantic file operation failures with retained native causes and paths.
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'

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
export class FileAborted extends Schema.TaggedError<FileAborted>(
  '@effect-harness/harness/FileError/FileAborted',
)('FileAborted', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  path: SchemaField.optional(Schema.String),
}) {
  get code(): 'aborted' {
    return 'aborted'
  }
}
/**
 * File operation failure reporting a missing path.
 *
 * @category errors
 */
export class FileNotFound extends Schema.TaggedError<FileNotFound>(
  '@effect-harness/harness/FileError/FileNotFound',
)('FileNotFound', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  path: SchemaField.optional(Schema.String),
}) {
  get code(): 'not_found' {
    return 'not_found'
  }
}
/**
 * File operation failure reporting denied filesystem access.
 *
 * @category errors
 */
export class FilePermissionDenied extends Schema.TaggedError<FilePermissionDenied>(
  '@effect-harness/harness/FileError/FilePermissionDenied',
)('FilePermissionDenied', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  path: SchemaField.optional(Schema.String),
}) {
  get code(): 'permission_denied' {
    return 'permission_denied'
  }
}
/**
 * File operation failure reporting a directory operation on a non-directory.
 *
 * @category errors
 */
export class FileNotDirectory extends Schema.TaggedError<FileNotDirectory>(
  '@effect-harness/harness/FileError/FileNotDirectory',
)('FileNotDirectory', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  path: SchemaField.optional(Schema.String),
}) {
  get code(): 'not_directory' {
    return 'not_directory'
  }
}
/**
 * File operation failure reporting a file operation on a directory.
 *
 * @category errors
 */
export class FileIsDirectory extends Schema.TaggedError<FileIsDirectory>(
  '@effect-harness/harness/FileError/FileIsDirectory',
)('FileIsDirectory', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  path: SchemaField.optional(Schema.String),
}) {
  get code(): 'is_directory' {
    return 'is_directory'
  }
}
/**
 * File operation failure reporting invalid path or operation input.
 *
 * @category errors
 */
export class FileInvalid extends Schema.TaggedError<FileInvalid>(
  '@effect-harness/harness/FileError/FileInvalid',
)('FileInvalid', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  path: SchemaField.optional(Schema.String),
}) {
  get code(): 'invalid' {
    return 'invalid'
  }
}
/**
 * File operation failure reporting an unavailable adapter capability.
 *
 * @category errors
 */
export class FileNotSupported extends Schema.TaggedError<FileNotSupported>(
  '@effect-harness/harness/FileError/FileNotSupported',
)('FileNotSupported', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  path: SchemaField.optional(Schema.String),
}) {
  get code(): 'not_supported' {
    return 'not_supported'
  }
}
/**
 * File operation failure retaining an otherwise unclassified native cause.
 *
 * @category errors
 */
export class FileUnknown extends Schema.TaggedError<FileUnknown>(
  '@effect-harness/harness/FileError/FileUnknown',
)('FileUnknown', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  path: SchemaField.optional(Schema.String),
}) {
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
  FileAborted,
  FileNotFound,
  FilePermissionDenied,
  FileNotDirectory,
  FileIsDirectory,
  FileInvalid,
  FileNotSupported,
  FileUnknown,
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
      return new FileAborted(fields)
    case 'not_found':
      return new FileNotFound(fields)
    case 'permission_denied':
      return new FilePermissionDenied(fields)
    case 'not_directory':
      return new FileNotDirectory(fields)
    case 'is_directory':
      return new FileIsDirectory(fields)
    case 'invalid':
      return new FileInvalid(fields)
    case 'not_supported':
      return new FileNotSupported(fields)
    case 'unknown':
      return new FileUnknown(fields)
  }
}
