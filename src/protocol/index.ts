/**
 * The contract between a Harness and its clients: the requests a client sends, and the
 * submissions, views, events and errors it gets back. The loop serves it and the client calls it;
 * nothing here imports what the loop runs.
 */

/** `HarnessRpcs`: every request a client sends the loop. */
export * as HarnessRpcs from './HarnessRpcs.ts'

/** Named `init` operations. */
export * as Init from './Init.ts'

/** Submissions: drafts, handles, and settled records. */
export * as Submission from './Submission.ts'

/** Errors the loop reports. */
export * as HarnessError from './HarnessError.ts'

/** What an agent is made of, as Schemas the client shares. */
export * as AgentState from './AgentState.ts'

/** What a client sees of the loop: built-in document states, views, and the task graph. */
export * as ViewState from './ViewState.ts'

/** Agent events: one conversation's activity, batch by batch. */
export * as AgentEvent from './AgentEvent.ts'
