import type * as Cause from 'effect/Cause'

import * as Deferred from 'effect/Deferred'

import * as Effect from 'effect/Effect'

import * as Exit from 'effect/Exit'

import * as Queue from 'effect/Queue'

import type * as Scope from 'effect/Scope'

import * as Stream from 'effect/Stream'

import type { DirectoryNotifications } from 'effect-harness/NativeFiles'
import type { FileError } from 'effect-harness/FileError'

/** Controlled installation and typed notifications, with the same observable startup contract as native adapters. */
export const notifications = (
  install: (
    queue: Queue.Queue<string | undefined, FileError | Cause.Done>,
  ) => Effect.Effect<unknown, FileError, Scope.Scope>,
): Effect.Effect<DirectoryNotifications> =>
  Effect.gen(function* () {
    const installed = yield* Deferred.make<void, FileError>()
    const changes = Stream.callback<string | undefined, FileError>((queue) =>
      install(queue).pipe(
        Effect.onExit((exit) =>
          Effect.gen(function* () {
            yield* Deferred.done(installed, Exit.asVoid(exit))
            if (exit._tag === 'Failure') yield* Queue.failCause(queue, exit.cause)
          }),
        ),
      ),
    )
    return { changes, started: Deferred.await(installed) }
  })
