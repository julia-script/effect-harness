import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
import * as Usage from './Usage.ts'

export class ModelNoModel extends Schema.TaggedError<ModelNoModel>(
  '@effect-harness/harness/ModelNoModel',
)('ModelNoModel', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  usage: SchemaField.optional(Usage.Usage),
}) {
  get isRetryable(): boolean {
    return false
  }
}
export class ModelUnsupported extends Schema.TaggedError<ModelUnsupported>(
  '@effect-harness/harness/ModelUnsupported',
)('ModelUnsupported', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  usage: SchemaField.optional(Usage.Usage),
}) {
  get isRetryable(): boolean {
    return false
  }
}
export class ModelInvalidResponse extends Schema.TaggedError<ModelInvalidResponse>(
  '@effect-harness/harness/ModelInvalidResponse',
)('ModelInvalidResponse', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  usage: SchemaField.optional(Usage.Usage),
}) {
  get isRetryable(): boolean {
    return false
  }
}
export const ModelErrorReason = Schema.Union([ModelNoModel, ModelUnsupported, ModelInvalidResponse])
export type ModelErrorReason = typeof ModelErrorReason.Type
export class ModelError extends Schema.TaggedError<ModelError>(
  '@effect-harness/harness/ModelError',
)('ModelError', { reason: ModelErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  get usage(): Usage.Usage | undefined {
    return this.reason.usage
  }
  get isRetryable(): boolean {
    return this.reason.isRetryable
  }
}
export class ToolUnavailable extends Schema.TaggedError<ToolUnavailable>(
  '@effect-harness/harness/ToolUnavailable',
)('ToolUnavailable', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
export class ToolBlocked extends Schema.TaggedError<ToolBlocked>(
  '@effect-harness/harness/ToolBlocked',
)('ToolBlocked', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
export class ToolInvalidParameters extends Schema.TaggedError<ToolInvalidParameters>(
  '@effect-harness/harness/ToolInvalidParameters',
)('ToolInvalidParameters', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
export class ToolExecution extends Schema.TaggedError<ToolExecution>(
  '@effect-harness/harness/ToolExecution',
)('ToolExecution', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
export class ToolInvalidResult extends Schema.TaggedError<ToolInvalidResult>(
  '@effect-harness/harness/ToolInvalidResult',
)('ToolInvalidResult', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
export class ToolInterrupted extends Schema.TaggedError<ToolInterrupted>(
  '@effect-harness/harness/ToolInterrupted',
)('ToolInterrupted', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
export const ToolErrorReason = Schema.Union([
  ToolUnavailable,
  ToolBlocked,
  ToolInvalidParameters,
  ToolExecution,
  ToolInvalidResult,
  ToolInterrupted,
])
export type ToolErrorReason = typeof ToolErrorReason.Type
export class ToolError extends Schema.TaggedError<ToolError>('@effect-harness/harness/ToolError')(
  'ToolError',
  { reason: ToolErrorReason },
) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  override get name(): string {
    return this.reason.name
  }
}
export class RegistryFailure extends Schema.TaggedError<RegistryFailure>(
  '@effect-harness/harness/RegistryFailure',
)('RegistryFailure', { message: Schema.String, cause: SchemaField.optional(Schema.Defect()) }) {}
export const RegistryErrorReason = Schema.Union([RegistryFailure])
export type RegistryErrorReason = typeof RegistryErrorReason.Type
export class RegistryError extends Schema.TaggedError<RegistryError>(
  '@effect-harness/harness/RegistryError',
)('RegistryError', { reason: RegistryErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
}
export class HookFailure extends Schema.TaggedError<HookFailure>(
  '@effect-harness/harness/HookFailure',
)('HookFailure', { message: Schema.String, cause: SchemaField.optional(Schema.Defect()) }) {}
export const HookErrorReason = Schema.Union([HookFailure])
export type HookErrorReason = typeof HookErrorReason.Type
export class HookError extends Schema.TaggedError<HookError>('@effect-harness/harness/HookError')(
  'HookError',
  { reason: HookErrorReason },
) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
}
export class OutputFailure extends Schema.TaggedError<OutputFailure>(
  '@effect-harness/harness/OutputFailure',
)('OutputFailure', { message: Schema.String, cause: SchemaField.optional(Schema.Defect()) }) {}
export const OutputErrorReason = Schema.Union([OutputFailure])
export type OutputErrorReason = typeof OutputErrorReason.Type
export class OutputError extends Schema.TaggedError<OutputError>(
  '@effect-harness/harness/OutputError',
)('OutputError', { reason: OutputErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
}
