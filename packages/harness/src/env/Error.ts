import * as Schema from 'effect/Schema'

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
export type FileErrorCode = typeof FileErrorCode.Type
export class FileAborted extends Schema.TaggedError<FileAborted>(
  '@effect-harness/harness/FileAborted',
)('FileAborted', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  path: Schema.optionalKey(Schema.String),
}) {
  get code(): 'aborted' {
    return 'aborted'
  }
}
export class FileNotFound extends Schema.TaggedError<FileNotFound>(
  '@effect-harness/harness/FileNotFound',
)('FileNotFound', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  path: Schema.optionalKey(Schema.String),
}) {
  get code(): 'not_found' {
    return 'not_found'
  }
}
export class FilePermissionDenied extends Schema.TaggedError<FilePermissionDenied>(
  '@effect-harness/harness/FilePermissionDenied',
)('FilePermissionDenied', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  path: Schema.optionalKey(Schema.String),
}) {
  get code(): 'permission_denied' {
    return 'permission_denied'
  }
}
export class FileNotDirectory extends Schema.TaggedError<FileNotDirectory>(
  '@effect-harness/harness/FileNotDirectory',
)('FileNotDirectory', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  path: Schema.optionalKey(Schema.String),
}) {
  get code(): 'not_directory' {
    return 'not_directory'
  }
}
export class FileIsDirectory extends Schema.TaggedError<FileIsDirectory>(
  '@effect-harness/harness/FileIsDirectory',
)('FileIsDirectory', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  path: Schema.optionalKey(Schema.String),
}) {
  get code(): 'is_directory' {
    return 'is_directory'
  }
}
export class FileInvalid extends Schema.TaggedError<FileInvalid>(
  '@effect-harness/harness/FileInvalid',
)('FileInvalid', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  path: Schema.optionalKey(Schema.String),
}) {
  get code(): 'invalid' {
    return 'invalid'
  }
}
export class FileNotSupported extends Schema.TaggedError<FileNotSupported>(
  '@effect-harness/harness/FileNotSupported',
)('FileNotSupported', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  path: Schema.optionalKey(Schema.String),
}) {
  get code(): 'not_supported' {
    return 'not_supported'
  }
}
export class FileUnknown extends Schema.TaggedError<FileUnknown>(
  '@effect-harness/harness/FileUnknown',
)('FileUnknown', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  path: Schema.optionalKey(Schema.String),
}) {
  get code(): 'unknown' {
    return 'unknown'
  }
}
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
export type FileErrorReason = typeof FileErrorReason.Type
export class FileError extends Schema.TaggedError<FileError>('@effect-harness/harness/FileError')(
  'FileError',
  { reason: FileErrorReason },
) {
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
export const fileReason = (
  code: FileErrorCode,
  fields: { readonly message: string; readonly cause?: unknown; readonly path?: string },
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
export const ExecutionErrorCode = Schema.Literals([
  'aborted',
  'timeout',
  'shell_unavailable',
  'spawn_error',
  'callback_error',
  'unknown',
])
export type ExecutionErrorCode = typeof ExecutionErrorCode.Type
export class ExecutionAborted extends Schema.TaggedError<ExecutionAborted>(
  '@effect-harness/harness/ExecutionAborted',
)('ExecutionAborted', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  spillPath: Schema.optionalKey(Schema.String),
}) {
  get code(): 'aborted' {
    return 'aborted'
  }
}
export class ExecutionTimeout extends Schema.TaggedError<ExecutionTimeout>(
  '@effect-harness/harness/ExecutionTimeout',
)('ExecutionTimeout', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  spillPath: Schema.optionalKey(Schema.String),
}) {
  get code(): 'timeout' {
    return 'timeout'
  }
}
export class ExecutionShellUnavailable extends Schema.TaggedError<ExecutionShellUnavailable>(
  '@effect-harness/harness/ExecutionShellUnavailable',
)('ExecutionShellUnavailable', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  spillPath: Schema.optionalKey(Schema.String),
}) {
  get code(): 'shell_unavailable' {
    return 'shell_unavailable'
  }
}
export class ExecutionSpawnError extends Schema.TaggedError<ExecutionSpawnError>(
  '@effect-harness/harness/ExecutionSpawnError',
)('ExecutionSpawnError', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  spillPath: Schema.optionalKey(Schema.String),
}) {
  get code(): 'spawn_error' {
    return 'spawn_error'
  }
}
export class ExecutionCallbackError extends Schema.TaggedError<ExecutionCallbackError>(
  '@effect-harness/harness/ExecutionCallbackError',
)('ExecutionCallbackError', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  spillPath: Schema.optionalKey(Schema.String),
}) {
  get code(): 'callback_error' {
    return 'callback_error'
  }
}
export class ExecutionUnknown extends Schema.TaggedError<ExecutionUnknown>(
  '@effect-harness/harness/ExecutionUnknown',
)('ExecutionUnknown', {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
  spillPath: Schema.optionalKey(Schema.String),
}) {
  get code(): 'unknown' {
    return 'unknown'
  }
}
export const ExecutionErrorReason = Schema.Union([
  ExecutionAborted,
  ExecutionTimeout,
  ExecutionShellUnavailable,
  ExecutionSpawnError,
  ExecutionCallbackError,
  ExecutionUnknown,
])
export type ExecutionErrorReason = typeof ExecutionErrorReason.Type
export class ExecutionError extends Schema.TaggedError<ExecutionError>(
  '@effect-harness/harness/ExecutionError',
)('ExecutionError', { reason: ExecutionErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  get code(): ExecutionErrorCode {
    return this.reason.code
  }
  get spillPath(): string | undefined {
    return this.reason.spillPath
  }
}
