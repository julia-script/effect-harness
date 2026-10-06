import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import { Session, type Service as SessionService } from './Session.ts'
import { rejected, type StorageError, NotFound } from './StorageError.ts'

/** Resolves already scoped Session services for native Workflow executor Layers. */
export class SessionDirectory extends Context.Service<
  SessionDirectory,
  {
    readonly resolve: (sessionId: string) => Effect.Effect<SessionService, StorageError>
  }
>()('@effect-harness/durable/SessionDirectory') {}

/** Registers scoped sessions without creating or owning a separate runtime. */
export const layer = (
  sessions: ReadonlyMap<string, SessionService>,
): Layer.Layer<SessionDirectory> => {
  const entries = new Map(sessions)
  return Layer.succeed(
    SessionDirectory,
    SessionDirectory.of({
      resolve: (sessionId) => {
        const session = entries.get(sessionId)
        return session === undefined
          ? Effect.fail(rejected(`Session ${sessionId} is not registered`, NotFound))
          : Effect.succeed(session)
      },
    }),
  )
}

/** Associates the application's ordinary Session Layer with one durable session identity. */
export const layerSingle = (sessionId: string): Layer.Layer<SessionDirectory, never, Session> =>
  Layer.effect(
    SessionDirectory,
    Effect.gen(function* () {
      const session = yield* Session
      return SessionDirectory.of({
        resolve: (requested) =>
          requested === sessionId
            ? Effect.succeed(session)
            : Effect.fail(rejected(`Session ${requested} is not registered`, NotFound)),
      })
    }),
  )
