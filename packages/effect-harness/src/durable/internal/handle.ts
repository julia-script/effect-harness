import * as Inspectable from 'effect/Inspectable'
import * as Pipeable from 'effect/Pipeable'

export type Input<A, Marker extends keyof A> = Omit<
  A,
  Marker | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
>

const Proto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
}
export const prototype = (id: string): object => ({
  ...Proto,
  toJSON() {
    return { _id: id }
  },
})

export const make = <A extends object>(
  proto: object,
  input: A,
): A & Pipeable.Pipeable & Inspectable.Inspectable =>
  // Object.create is the sole native allocation boundary. Descriptor installation
  // retains frozen inputs and live getters without invoking them at construction.
  Object.create(proto, Object.getOwnPropertyDescriptors(input)) as A &
    Pipeable.Pipeable &
    Inspectable.Inspectable

/** Installs an owned marker alongside untouched input descriptors. */
export const marked = <A extends object, K extends PropertyKey, V>(
  input: A,
  key: K,
  value: V,
): A & { readonly [P in K]: V } =>
  // The computed descriptor defines exactly K. Object.create preserves the other
  // own descriptors without sampling getters; the mapped-key assertion records it.
  Object.create(null, {
    ...Object.getOwnPropertyDescriptors(input),
    [key]: { value, enumerable: false },
  }) as A & { readonly [P in K]: V }
