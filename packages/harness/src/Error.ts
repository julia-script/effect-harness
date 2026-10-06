import * as Schema from 'effect/Schema'
import * as Usage from './Usage.ts'

export class RegistryError extends Schema.TaggedError<RegistryError>()(
  '@effect-harness/harness/RegistryError',
  { message: Schema.String },
) {}
export class ModelError extends Schema.TaggedError<ModelError>()(
  '@effect-harness/harness/ModelError',
  {
    reason: Schema.Literals(['no_model', 'unsupported', 'invalid_response']),
    message: Schema.String,
    usage: Schema.optionalKey(Usage.Usage),
  },
) {}
export class ToolError extends Schema.TaggedError<ToolError>()(
  '@effect-harness/harness/ToolError',
  {
    reason: Schema.Literals([
      'unavailable',
      'blocked',
      'invalid_parameters',
      'execution',
      'invalid_result',
      'interrupted',
    ]),
    name: Schema.String,
    message: Schema.String,
  },
) {}
export class OutputError extends Schema.TaggedError<OutputError>()(
  '@effect-harness/harness/OutputError',
  { message: Schema.String },
) {}

export class HookError extends Schema.TaggedError<HookError>()(
  '@effect-harness/harness/HookError',
  { message: Schema.String },
) {}
