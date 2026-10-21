/**
 * The client side of a Harness: the client and its transports. Nothing here imports what the loop
 * runs, so a client can live in another process, or in a browser. What it sends and receives is in
 * `effect-harness/protocol`.
 */

/** The `HarnessClient` service calling code uses. */
export * as HarnessClient from './HarnessClient.ts'

/** Transports between a client and the loop; `layerMemory` in one process. */
export * as HarnessTransport from './HarnessTransport.ts'
