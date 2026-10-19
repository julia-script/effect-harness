/**
 * Errors of the tool task.
 */
import { Schema } from 'effect'
import { EntryId } from '../storage/schema/Ids.ts'

/** No call `callId` in the assistant entry: the task's input is broken, so the task faults. */
export class CallNotFound extends Schema.TaggedError<
  CallNotFound,
  { readonly brand: unique symbol }
>('effect-harness/tool/ToolError/CallNotFound')('CallNotFound', {
  message: Schema.String,
  assistant: EntryId,
  callId: Schema.String,
}) {}

/**
 * A nested call that cannot be made: outside a tool call, after the calling tool settled, or under
 * a key the caller already used for another tool or other arguments.
 */
export class NestedCallRefused extends Schema.TaggedError<
  NestedCallRefused,
  { readonly brand: unique symbol }
>('effect-harness/tool/ToolError/NestedCallRefused')('NestedCallRefused', {
  message: Schema.String,
}) {}
