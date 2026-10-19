import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as References from 'effect/References'
import * as Tracer from 'effect/Tracer'

/** Effect.fn / withSpan add stack frames that clone Causes on propagation.
 * Fault provenance relies on occurrence identity, so these boundaries only install a span.
 */
export const withSpan =
  (name: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.useSpan(name, {}, (span) => Effect.provideService(effect, Tracer.ParentSpan, span))

/** Keep registration-owned dependencies, but use the executing fiber's tracing context. */
export const context = <R, R2>(captured: Context.Context<R>, current: Context.Context<R2>) => {
  const parent = Context.getOption(current, Tracer.ParentSpan)
  const base = captured.pipe(
    Context.omit(Tracer.ParentSpan),
    Context.add(Tracer.Tracer, Context.get(current, Tracer.Tracer)),
    Context.add(References.TracerEnabled, Context.get(current, References.TracerEnabled)),
    Context.add(
      References.TracerTimingEnabled,
      Context.get(current, References.TracerTimingEnabled),
    ),
    Context.add(
      References.TracerSpanAnnotations,
      Context.get(current, References.TracerSpanAnnotations),
    ),
    Context.add(References.TracerSpanLinks, Context.get(current, References.TracerSpanLinks)),
  )
  // ParentSpan is optional tracing state, not a captured application dependency.
  // Omit narrows a generic R even though every application service is retained.
  return (
    parent._tag === 'Some' ? Context.add(base, Tracer.ParentSpan, parent.value) : base
  ) as Context.Context<R>
}
