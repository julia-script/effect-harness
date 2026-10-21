/**
 * Test support: a Harness loop launched over a memory transport, its client, and the scripted
 * stand-in for the generation task.
 */
import { Effect, Layer, Queue } from 'effect'
import type { Context, Scope } from 'effect'
import { HarnessServer } from '../../harness/index.ts'
import type { Step } from '../../test/harness.ts'
import { standIn } from '../../test/harness.ts'
import * as Settings from '../../harness/Settings.ts'
import type { Service as StorageService } from '../../storage/Storage.ts'
import { Storage } from '../../storage/Storage.ts'
import { Model } from '../../model/index.ts'
import { HarnessClient, HarnessTransport } from '../index.ts'

export interface Opened {
  readonly client: HarnessClient.Service
  readonly answer: (text: string) => Effect.Effect<void>
}

/** Launch a loop over `storage` and a memory transport in the scope, and connect a client. */
export const open = (
  storage: StorageService,
  options: HarnessServer.Options<readonly []> = {},
  transport: Effect.Effect<
    Context.Context<HarnessTransport.Protocols>,
    never,
    Scope.Scope
  > = HarnessTransport.makeMemory,
): Effect.Effect<Opened, never, Scope.Scope> =>
  Effect.gen(function* () {
    const steps = yield* Queue.unbounded<Step>()
    const settings = Effect.succeed(Settings.defaults)
    const loop = HarnessServer.layer({ ...options, tasks: [standIn(steps, settings)] }).pipe(
      Layer.provide([Model.layer([]), Layer.succeed(Storage, storage)]),
    )
    const protocols = yield* transport
    yield* Layer.launch(loop).pipe(
      Effect.provideContext(protocols),
      Effect.orDie,
      Effect.forkScoped,
    )
    const client = yield* Effect.provideContext(HarnessClient.make, protocols)
    return {
      client,
      answer: (text) => Effect.asVoid(Queue.offer(steps, { kind: 'answer', text })),
    }
  })

export { runTest } from '../../test/task.ts'
