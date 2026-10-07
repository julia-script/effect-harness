/**
 * Identity-keyed registration and resolution of scoped sessions.
 *
 * @since 0.0.0
 */
import * as Option from 'effect/Option'
import type * as Identity from './Identity.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import { Session, type Service as SessionService } from './Session.ts'
import { rejected, type StorageError, NotFound } from './StorageError.ts'

/**
 * Resolves already scoped Session services for native Workflow executor Layers.
 *
 * @category services
 * @since 0.0.0
 */
export class SessionDirectory extends Context.Service<
  SessionDirectory,
  {
    readonly resolve: (sessionId: Identity.SessionId) => Effect.Effect<SessionService, StorageError>
  }
>()('@effect-harness/durable/SessionDirectory') {}

/**
 * Explicit application binding of already scoped Session references; no default registrations.
 *
 * @category services
 * @since 0.0.0
 */
export class Registrations extends Context.Service<
  Registrations,
  ReadonlyMap<Identity.SessionId, SessionService>
>()('@effect-harness/durable/SessionDirectory/Registrations') {}

/**
 * Snapshot the explicitly supplied registration map when this Layer is built.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer: Layer.Layer<SessionDirectory, never, Registrations> = Layer.effect(
  SessionDirectory,
  Effect.gen(function* () {
    const entries = new Map(yield* Registrations)
    return SessionDirectory.of({
      resolve: (sessionId) => {
        return Effect.fromOption(Option.fromUndefinedOr(entries.get(sessionId)), () =>
          rejected(`Session ${sessionId} is not registered`, NotFound),
        )
      },
    })
  }),
)

/**
 * Associates the application's ordinary Session Layer with one durable session identity.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerSingle = (
  sessionId: Identity.SessionId,
): Layer.Layer<SessionDirectory, never, Session> =>
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
