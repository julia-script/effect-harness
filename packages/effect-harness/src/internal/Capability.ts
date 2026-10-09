/** Deferred provisioning for runtime-owned hooks, extensions, and prompt sections. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import { pipeArguments } from 'effect/Pipeable'

const providers = new WeakMap<object, ReadonlyArray<Layer.Layer<never>>>()
export const pipe = function (this: object) {
  return pipeArguments(this, arguments)
}
export const provide = <A extends object, ROut, E, RIn>(
  self: A,
  layer: Layer.Layer<ROut, E, RIn>,
): A => {
  const result = { ...self }
  // Provisioning channels are retained by public phantom types; this private collection erases them.
  providers.set(result, [...(providers.get(self) ?? []), layer as unknown as Layer.Layer<never>])
  return result
}
export const context = Effect.fnUntraced(function* (self: object) {
  let captured = yield* Effect.context<never>()
  for (const layer of providers.get(self) ?? []) {
    const supplied = yield* Layer.build(layer).pipe(Effect.provideContext(captured))
    captured = Context.merge(captured, supplied)
  }
  return captured
})
