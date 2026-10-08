/** This entrypoint runs unchanged in Node, Bun, Deno or a browser module. */
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import { run } from './Portable.ts'

await Effect.runPromise(
  run.pipe(Effect.flatMap((result) => Console.log(`portable: ${JSON.stringify(result)}`))),
)
