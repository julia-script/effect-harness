/** Invocation-scoped declaration metadata; no handlers, resources or registry contexts escape. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import { ToolExecution } from './ToolExecution.js'
import { ExecutionError } from './ExecutionError.js'

export interface Metadata {
  readonly name: string
  readonly description: string
  readonly parameters: Schema.JsonObject
}
export class CatalogError extends Schema.TaggedError<CatalogError>()('ToolCatalogError', {
  name: Schema.String,
  message: Schema.String,
}) {}

/** Read permitted deferred tools not offered in the executing request. Access expires with the handler. */
export const deferred = Effect.flatMap(
  ToolExecution,
  (access) =>
    access.deferredTools ??
    Effect.fail(
      new ExecutionError({
        reason: 'invalid',
        operation: 'tool.catalog',
        message: 'No invocation catalog is available',
      }),
    ),
)
