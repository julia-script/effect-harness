/**
 * Named `init` operations. A conversation's `init` cannot cross a transport as a callback, so a
 * client names an `Init` declared here, with a Schema payload, and the loop runs the
 * `Initializer` registered under that name in the commit that creates the conversation.
 */
import { Schema } from 'effect'

/** A payload schema: its encoded form is JSON, and it needs no services. */
export type Payload = Schema.Codec<unknown, Schema.Json>

/** The declaration of one named `init`. */
export interface Init<S extends Payload = Payload> {
  readonly name: string
  readonly payload: S
}

/** Declare a named `init` and its payload schema, such as `Schema.Null` for none. */
export const make = <S extends Payload>(name: string, payload: S): Init<S> => ({ name, payload })

/** An `init` with its payload, as a client passes it. */
export interface Bound<S extends Payload = Payload> {
  readonly init: Init<S>
  readonly payload: S['Type']
}

/** Bind `init` to its payload. */
export const call = <S extends Payload>(init: Init<S>, payload: S['Type']): Bound<S> => ({
  init,
  payload,
})

/** A bound `init` on the wire: its name and encoded payload. */
export const Call = Schema.Struct({ name: Schema.String, payload: Schema.Json })
export type Call = typeof Call.Type
