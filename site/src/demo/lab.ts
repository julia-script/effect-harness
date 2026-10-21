/**
 * A harness loop in this browser tab, over storage that outlives it. `start` builds the loop and
 * its client in a scope of their own; `crash` closes that scope in the middle of whatever it was
 * doing, and the next `start` reopens the same storage, where the scheduler finds the unfinished
 * work and resumes it. The model is `Faux`, answering every request with one scripted step.
 */
import {
  Cause,
  Context,
  Data,
  Effect,
  Exit,
  Layer,
  Option,
  Scope,
  Stream,
  SynchronizedRef,
} from 'effect'
import type { RpcServer } from 'effect/rpc'
import { HarnessClient, HarnessTransport } from 'effect-harness/client'
import { Faux, Model } from 'effect-harness/model'
import type { AgentEvent } from 'effect-harness/protocol'
import { Storage } from 'effect-harness/storage'

export class LabError extends Data.TaggedError('LabError')<{ readonly message: string }> {}

const labError = (cause: unknown) =>
  new LabError({
    message:
      typeof cause === 'object' && cause !== null && 'message' in cause
        ? String(cause.message)
        : 'The loop failed.',
  })

/** What the lab provides the loop. */
type Provided = Storage.Storage | Model.Model | RpcServer.Protocol

export interface Options<A, E> {
  /** The loop to run: `HarnessServer.layer` with its tools, given the model to ask. */
  readonly loop: (model: Model.Ref) => Layer.Layer<A, E, Provided>
  readonly respond: Faux.Respond
  /** How fast the model streams. */
  readonly tokensPerSecond: number
}

export interface Lab {
  /** Start the loop, unless it is running. */
  readonly start: Effect.Effect<void, LabError>
  /** Stop the loop wherever it is. Storage keeps everything it committed. */
  readonly crash: Effect.Effect<void>
  readonly submit: (content: string) => Effect.Effect<void, LabError>
  /** The root conversation's events, starting with a snapshot, until the loop stops. */
  readonly events: Stream.Stream<ReadonlyArray<AgentEvent.AgentEvent>, LabError>
}

interface Running {
  readonly scope: Scope.Closeable
  readonly root: HarnessClient.Conversation
}

export const make = <A, E>(options: Options<A, E>): Effect.Effect<Lab, never, Scope.Scope> =>
  Effect.gen(function* () {
    const storage = yield* Storage.makeMemory
    const faux = yield* Faux.make({ tokensPerSecond: options.tokensPerSecond })
    // Every request takes the scripted step, so a request repeated after a crash gets it too.
    const provider: Model.Provider = {
      ...faux.provider,
      stream: (request, model) =>
        Stream.unwrap(
          Effect.as(faux.setResponses([options.respond]), faux.provider.stream(request, model)),
        ),
    }
    const AppLive = Layer.merge(options.loop(faux.ref()), HarnessClient.layer).pipe(
      Layer.provide(HarnessTransport.layerMemory),
      Layer.provide([Layer.succeed(Storage.Storage, storage), Model.layer([provider])]),
    )
    const current = yield* SynchronizedRef.make(Option.none<Running>())

    const boot = Effect.gen(function* () {
      const scope = yield* Scope.make()
      const root = yield* Layer.buildWithScope(AppLive, scope).pipe(
        Effect.flatMap((built) => Context.get(built, HarnessClient.HarnessClient).root()),
        Effect.onError(() => Scope.close(scope, Exit.void)),
      )
      return Option.some<Running>({ scope, root })
    }).pipe(Effect.mapError(labError))

    const running = SynchronizedRef.get(current).pipe(
      Effect.flatMap(Effect.fromOption),
      Effect.mapError(() => new LabError({ message: 'The loop is not running.' })),
    )

    const lab: Lab = {
      start: SynchronizedRef.updateEffect(current, (maybeRunning) =>
        Option.isSome(maybeRunning) ? Effect.succeed(maybeRunning) : boot,
      ),
      crash: SynchronizedRef.updateEffect(current, (maybeRunning) =>
        Option.match(maybeRunning, {
          onNone: () => Effect.succeed(maybeRunning),
          onSome: ({ scope }) => Effect.as(Scope.close(scope, Exit.void), Option.none()),
        }),
      ),
      submit: (content) =>
        running.pipe(
          Effect.flatMap(({ root }) =>
            root
              .submit({ type: 'input', content, whenBusy: 'followUp' })
              .pipe(Effect.mapError(labError)),
          ),
          Effect.asVoid,
        ),
      events: Stream.unwrap(
        running.pipe(
          Effect.flatMap(({ root }) => Effect.mapError(root.events, labError)),
          Effect.map(({ snapshot, batches }) =>
            Stream.concat(Stream.make([snapshot]), Stream.mapError(batches, labError)),
          ),
        ),
      ).pipe(
        // A crash interrupts the watch along with the loop: that ends it, it does not fail it.
        Stream.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause) ? Stream.empty : Stream.failCause(cause),
        ),
      ),
    }
    yield* Effect.addFinalizer(() => lab.crash)
    return lab
  })
