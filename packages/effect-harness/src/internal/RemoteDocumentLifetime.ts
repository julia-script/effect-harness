/** Private associations are installed only by a native or actual transport constructor. */
import * as Effect from 'effect/Effect'
import type * as Remote from '../RemoteDocument.js'
import type { HarnessBackendService } from '../HarnessBackend.js'
import { RemoteDocumentError } from '../RemoteDocumentError.js'
import { observation } from './ConversationInvocation.js'

export type Kind = 'Current' | 'Historical' | 'Watch' | 'Frame'
export interface Lifetime {
  readonly observe: <A, E, R>(
    kind: Kind,
    run: () => Effect.Effect<A, E, R>,
    frame?: Remote.WireFrame,
  ) => Effect.Effect<A, E | Remote.Failure, R>
}
const lifetimes = new WeakMap<HarnessBackendService, Lifetime>()
const authorizations = new WeakMap<
  HarnessBackendService,
  (request: Remote.ReadRequest) => Effect.Effect<void, Remote.Failure>
>()
const nativeOwners = new WeakMap<object, HarnessBackendService>()
export const registerAuthorization = (
  backend: HarnessBackendService,
  authorize: (request: Remote.ReadRequest) => Effect.Effect<void, Remote.Failure>,
): void => {
  authorizations.set(backend, authorize)
  if (backend.documents !== undefined)
    for (const method of [
      backend.documents.snapshot,
      backend.documents.snapshotAsOf,
      backend.documents.watch,
    ])
      if (method !== undefined) nativeOwners.set(method, backend)
}
export const validateBackend = (
  backend: HarnessBackendService,
): Effect.Effect<void, RemoteDocumentError> =>
  Effect.suspend(() => {
    const copied =
      backend.documents !== undefined &&
      [backend.documents.snapshot, backend.documents.snapshotAsOf, backend.documents.watch].some(
        (method) =>
          method !== undefined && nativeOwners.has(method) && nativeOwners.get(method) !== backend,
      )
    return copied
      ? Effect.fail(
          new RemoteDocumentError({
            reason: 'unsupported',
            operation: 'remoteDocument.authority',
            message: 'Copied native document methods do not carry an observation lifetime',
          }),
        )
      : Effect.void
  })
export const authorize = (
  backend: HarnessBackendService,
  request: Remote.ReadRequest,
): Effect.Effect<void, Remote.Failure> => authorizations.get(backend)?.(request) ?? Effect.void
export const register = (backend: HarnessBackendService, lifetime: Lifetime): void => {
  lifetimes.set(backend, lifetime)
}
export const observe = <A, E, R>(
  backend: HarnessBackendService,
  kind: Kind,
  run: () => Effect.Effect<A, E, R>,
  frame?: Remote.WireFrame,
): Effect.Effect<A, E | Remote.Failure, R> => {
  const remote = lifetimes.get(backend)
  if (remote !== undefined) return remote.observe(kind, run, frame)
  return validateBackend(backend).pipe(
    Effect.andThen(observation(backend)?.observe(run) ?? Effect.scoped(Effect.suspend(run))),
  )
}
