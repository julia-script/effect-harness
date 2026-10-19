/** Shared private assertions and scoped service conformance case construction. */
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'

/** A named contract failure; adapter, acquisition and cleanup failures keep their own channels. */
export class ServiceConformanceError extends Schema.TaggedError<ServiceConformanceError>()(
  'ServiceConformanceError',
  {
    caseName: Schema.String,
    assertion: Schema.String,
    message: Schema.String,
    actual: Schema.Defect(),
    expected: Schema.Defect(),
  },
) {}

/** Run at a runtime boundary or pass directly to an Effect test runner. */
export interface ConformanceCase<E = never, R = never> {
  readonly name: string
  readonly run: Effect.Effect<void, E, R>
}

/** Typed backing-data cleanup runs on every exit before scope release; use Effect.void when Scope owns it. */
export interface ScopedFixture<E = never, R = never> {
  readonly cleanup: Effect.Effect<void, E, R>
}

/** Shared assertion helper for these suites; no runner or native imports. */
export const assertions = (caseName: string) => {
  const same = (assertion: string, actual: unknown, expected: unknown) =>
    Effect.suspend(() =>
      Equal.equals(actual, expected)
        ? Effect.void
        : Effect.fail(
            new ServiceConformanceError({
              caseName,
              assertion,
              message: `${caseName}: ${assertion}`,
              actual,
              expected,
            }),
          ),
    )
  const failure = <A, E, R>(
    assertion: string,
    effect: Effect.Effect<A, E, R>,
    matches: (error: E) => boolean,
  ) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(effect)
      yield* same(assertion, Result.isFailure(result) && matches(result.failure), true)
    })
  return { same, failure }
}

/** Evaluates the factory once per run; no hidden provision or error swallowing. */
export const scopedCase = <F extends ScopedFixture<E, R>, E, R, FE, CE, CR>(
  name: string,
  make: Effect.Effect<F, FE, R | Scope.Scope>,
  run: (fixture: F, assert: ReturnType<typeof assertions>) => Effect.Effect<void, CE, CR>,
): ConformanceCase<E | FE | CE, Exclude<R | CR, Scope.Scope>> => ({
  name,
  run: Effect.scoped(
    Effect.flatMap(make, (fixture) =>
      run(fixture, assertions(name)).pipe(Effect.onExit(() => fixture.cleanup)),
    ),
  ),
})
