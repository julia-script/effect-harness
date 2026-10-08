/** Portable native signal-termination facts; PID, temporary command path and stack remain unconstrained. */
import { assertFailure } from '@effect/vitest/utils'
import * as Result from 'effect/Result'
import type * as PlatformError from 'effect/PlatformError'

export const assertKilled = <A extends number>(
  exit: Result.Result<A, PlatformError.PlatformError>,
): void => {
  // effect-nit-allow P8-option-result-assert-helpers: the native spawner owns host command paths and stack frames; compare its stable typed signal-termination facts as a complete projected Result.
  assertFailure(
    Result.mapError(exit, (failure) => ({
      _tag: failure._tag,
      module: failure.reason.module,
      method: failure.reason.method,
      reason: failure.reason._tag,
      signal: failure.cause instanceof Error ? failure.cause.message : undefined,
    })),
    {
      _tag: 'PlatformError',
      module: 'ChildProcess',
      method: 'exitCode',
      reason: 'Unknown',
      signal: "Process interrupted due to receipt of signal: 'SIGKILL'",
    },
  )
}
