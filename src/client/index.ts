/**
 * The client side of a Harness: the protocol, the client, and transports. Nothing here imports
 * what the loop runs, so a client can live in another process, or in a browser.
 */

/** `HarnessRpcs`: every request a client sends the loop. */
export * as HarnessRpcs from './HarnessRpcs.ts'

/** The `HarnessClient` service calling code uses. */
export * as HarnessClient from './HarnessClient.ts'

/** Transports between a client and the loop; `layerMemory` in one process. */
export * as HarnessTransport from './HarnessTransport.ts'

/** Named `init` operations. */
export * as Init from './Init.ts'

/** Submission drafts and settled records. */
export * as Submission from '../harness/Submission.ts'

/** Errors the loop reports. */
export * as HarnessError from '../harness/HarnessError.ts'

/** Views and the task graph a client watches. */
export * as ViewState from '../harness/ViewState.ts'

/** Agent events a client watches. */
export * as AgentEvent from '../harness/AgentEvent.ts'
