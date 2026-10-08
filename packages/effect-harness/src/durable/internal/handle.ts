import * as Inspectable from 'effect/Inspectable'
import * as Pipeable from 'effect/Pipeable'

export type Input<A, Marker extends keyof A> = Omit<
  A,
  Marker | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
>

type DiagnosticValue = string | number | boolean | null
interface Projection {
  readonly fields: ReadonlyArray<string>
}
const projections = new WeakMap<object, Projection>()
const diagnostics = new WeakMap<object, Readonly<Record<string, DiagnosticValue>>>()
const Proto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
}
/** Selects bounded diagnostic fields; construction captures descriptors, never getters. */
export const prototype = (
  options: string | { readonly id: string; readonly fields: ReadonlyArray<string> },
): object => {
  const id = typeof options === 'string' ? options : options.id
  const fields = typeof options === 'string' ? [] : options.fields
  const proto = {
    ...Proto,
    toJSON(this: object) {
      return { _id: id, ...diagnostics.get(this) }
    },
  }
  projections.set(proto, { fields: [...fields] })
  return proto
}

export const make = <A extends object>(
  proto: object,
  input: A,
  metadata?: object,
): A & Pipeable.Pipeable & Inspectable.Inspectable => {
  const descriptors = Object.getOwnPropertyDescriptors(input)
  // Object.create is the sole native allocation boundary. Descriptor installation
  // retains frozen inputs and live getters without invoking them at construction.
  const value = Object.create(proto, descriptors) as A & Pipeable.Pipeable & Inspectable.Inspectable
  const projection = projections.get(proto)
  if (projection !== undefined) {
    const capturedDescriptors =
      metadata === undefined
        ? descriptors
        : { ...descriptors, ...Object.getOwnPropertyDescriptors(metadata) }
    const capturedFields: Record<string, DiagnosticValue> = {}
    for (const field of projection.fields) {
      const descriptor = capturedDescriptors[field]
      if (descriptor === undefined) continue
      if (!('value' in descriptor)) {
        capturedFields[field] = '[Getter]'
        continue
      }
      const captured: unknown = descriptor.value
      if (
        captured === null ||
        typeof captured === 'string' ||
        typeof captured === 'boolean' ||
        (typeof captured === 'number' && Number.isFinite(captured))
      ) {
        capturedFields[field] =
          typeof captured === 'string' && captured.length > 160
            ? captured.slice(0, 160) + '…'
            : captured
      } else if (typeof captured === 'object') {
        // Opaque fields may be live, recursive or revoked proxies. Never inspect them.
        capturedFields[field] = '[Opaque]'
      } else if (typeof captured === 'function') {
        capturedFields[field] = '[Function]'
      }
    }
    diagnostics.set(value, capturedFields)
  }
  return value
}

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
