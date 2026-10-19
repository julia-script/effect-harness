import * as Context from 'effect/Context'
import type * as Record from './Record.js'
import type * as Execution from './Execution.js'

/** Session access for a lifecycle callback; conversation creation need not have a task. */
export class HookExecution extends Context.Service<
  HookExecution,
  Execution.Access & { readonly taskId?: Record.TaskId }
>()('effect-harness/HookExecution') {}
