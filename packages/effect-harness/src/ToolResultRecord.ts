/** Read canonical tool results from committed transcript entries. */
import type * as Effect from 'effect/Effect'
import type * as Schema from 'effect/Schema'
import type * as Record from './Record.js'
import type * as ToolResult from './ToolResult.js'
import * as ToolResultEntry from './internal/ToolResultEntry.js'

/**
 * Decode a committed `tool.result` entry into its independent result channels.
 *
 * **When to use**
 * Use after reading an entry through Session or Transaction to inspect its content,
 * structured output, UI details, diagnostics, error flag, spend, and control.
 *
 * **Details**
 * Restores storage aliases and native media through the canonical result codec.
 * Requires no services and fails with SchemaError for a different entry kind or
 * malformed canonical data. The stored wire shape is private; ResultSchema describes
 * the decoded channels, not entry.data. No earlier storage shape is accepted.
 *
 * **Gotchas**
 * Content is the retained handler/hook content, before diagnostic rendering. Use
 * ContextProjection.messages for model messages with rendered diagnostic text.
 * Structured output remains JSON; decode it separately with the tool's schema if
 * your application needs transformed values or schema-provided services.
 *
 * @category reading
 */
export const read = (entry: Record.Entry): Effect.Effect<ToolResult.Result, Schema.SchemaError> =>
  ToolResultEntry.read(entry)
