/**
 * Tools: declarations, the results the model sees, and the built-in tool task that runs a
 * model-issued call durably.
 */

/** Tool declarations: Schemas, config, replay policy, output limits. */
export * as Tool from './Tool.ts'

/** Sets of tools, and their implementations. */
export * as Toolkit from './Toolkit.ts'

/** What a call returns to the model, and its `$tool-result` entry. */
export * as ToolResult from './ToolResult.ts'

/** The built-in `$tool` task. */
export * as ToolTask from './ToolTask.ts'

/** Errors of the tool task. */
export * as ToolError from './ToolError.ts'

/** Nested calls: a running tool calling another. */
export * as Call from './Call.ts'
