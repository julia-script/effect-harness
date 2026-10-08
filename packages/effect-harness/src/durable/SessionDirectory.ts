/**
 * Identity-keyed registration and resolution of scoped sessions.
 */
import * as Option from 'effect/Option'
import type * as Identity from './Identity.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import { Session, type Service as SessionService } from './Session.ts'
import { rejected, type StorageError, NotFound } from './StorageError.ts'

/**
 * Service resolving durable session identities to already scoped Sessions.
 *
 * **Gotchas**
 *
 * The directory does not acquire or extend Session lifetimes. An unregistered identity fails
 * with NotFound.
 *
 * @category services
 */
export class SessionDirectory extends Context.Service<
  SessionDirectory,
  {
    /**
     * Returns the already scoped Session registered under this identity; missing registrations
     * fail with NotFound.
     */
    readonly resolve: (sessionId: Identity.SessionId) => Effect.Effect<SessionService, StorageError>
  }
>()('@effect-harness/durable/SessionDirectory') {}

/**
 * Application-owned map of durable identities to scoped Session services.
 *
 * **Details**
 *
 * Supply explicit registrations before building layer. The Layer snapshots the map but
 * retains the supplied Session references.
 *
 * @category services
 */
export class Registrations extends Context.Service<
  Registrations,
  ReadonlyMap<Identity.SessionId, SessionService>
>()('@effect-harness/durable/SessionDirectory/Registrations') {}

/**
 * Builds a directory from a snapshot of explicit Session registrations.
 *
 * **Gotchas**
 *
 * Later map changes do not update the directory. Keep every registered Session’s owning
 * Scope alive.
 *
 * @category layers
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
 * Binds the supplied Session to one durable session identity.
 *
 * **When to use**
 *
 * Use when one application runtime hosts a single Session.
 *
 * **Gotchas**
 *
 * Other requested identities fail with NotFound. This Layer does not create a Session or
 * manage a second storage lifetime.
 *
 * @category layers
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
