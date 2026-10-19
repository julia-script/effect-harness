/** Versioned interpretation facts. Activation is a value, not a storage write or runtime switch. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Record from '../Record.js'
import * as RecordSchema from '../Record.js'
import * as Projection from '../ContextProjection.js'
import type * as Session from '../Session.js'
import * as Value from './DocumentValue.js'

export const LegacyModeSchema = Schema.Literals(['preserveManaged', 'honorAll'])
export type LegacyMode = typeof LegacyModeSchema.Type

/** Forks retain the original activation boundary; they must not recapture a later parent tail. */
export const ProtocolSchema = Schema.Struct({
  version: Schema.Literal(1),
  origin: Schema.Struct({
    conversationId: RecordSchema.ConversationId,
    through: Schema.optionalKey(RecordSchema.EntryId),
  }),
  legacyMode: LegacyModeSchema,
})
export type Protocol = typeof ProtocolSchema.Type

const own = Effect.fnUntraced(function* (protocol: Protocol) {
  const copied = yield* Value.copy(ProtocolSchema, protocol)
  Object.freeze(copied.origin)
  return Object.freeze(copied)
})

/** Capture under the caller's serialized activation transaction and persist unchanged with fork-copy semantics. */
export const activate = Effect.fnUntraced(function* (
  bounds: Pick<Projection.Bounds, 'conversationId' | 'tail'>,
  legacyMode: LegacyMode = 'preserveManaged',
) {
  return yield* own({
    version: 1,
    origin: {
      conversationId: bounds.conversationId,
      ...(bounds.tail === undefined ? {} : { through: bounds.tail }),
    },
    legacyMode,
  })
})

/** An owned, validated copy for descendants; origin/cutoff never advance on a fork or reopen. */
export const inherit = own

/** Explicit honorAll overrides the recorded default. An empty activation ignores no future records. */
export const options = Effect.fnUntraced(function* (
  protocol: Protocol,
  input: { readonly at?: Record.EntryId; readonly legacyMode?: LegacyMode } = {},
) {
  const captured = yield* own(protocol)
  const mode = yield* Schema.decodeEffect(LegacyModeSchema)(input.legacyMode ?? captured.legacyMode)
  let legacyInterpretation: Projection.LegacyInterpretation = { _tag: 'honorAll' }
  if (mode === 'preserveManaged' && captured.origin.through !== undefined)
    legacyInterpretation = { _tag: 'preserveManaged', through: captured.origin.through }
  return yield* Value.copy(Projection.ReadOptionsSchema, {
    ...(input.at === undefined ? {} : { at: input.at }),
    legacyInterpretation,
  })
})

export const project = Effect.fnUntraced(function* (
  conversationId: Record.ConversationId,
  entries: ReadonlyArray<Record.Entry>,
  protocol: Protocol,
  input: { readonly at?: Record.EntryId; readonly legacyMode?: LegacyMode } = {},
) {
  return yield* Projection.project(conversationId, entries, yield* options(protocol, input))
})

export const read = Effect.fnUntraced(function* (
  session: Session.Session,
  conversationId: Record.ConversationId,
  protocol: Protocol,
  input: { readonly at?: Record.EntryId; readonly legacyMode?: LegacyMode } = {},
) {
  return yield* Projection.read(session, conversationId, yield* options(protocol, input))
})
