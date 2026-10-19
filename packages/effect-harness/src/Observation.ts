/** Atomic observations use mount epochs, not an invented durable storage tip. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Stream from 'effect/Stream'
import * as Record from './Record.js'
import type * as Session from './Session.js'
import type * as Projection from './ContextProjection.js'
import type { HarnessError } from './HarnessError.js'
import type * as TaskGraph from './TaskGraph.js'
import type * as ConversationView from './ConversationView.js'
import type * as AgentEvent from './AgentEvent.js'
import { ObservationError } from './ObservationError.js'

export const protocol = 1
export const defaultCapacity = 64
export const CounterSchema = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
)
export const RevisionSchema = Schema.Struct({ epoch: Schema.NonEmptyString, cursor: CounterSchema })
export type Revision = typeof RevisionSchema.Type
export const TargetSchema = Schema.Union([
  Schema.TaggedStruct('sessionGraph', {}),
  Schema.TaggedStruct('conversation', { conversationId: Record.ConversationId }),
])
export type Target = typeof TargetSchema.Type
export const WatchOptionsSchema = Schema.Struct({
  capacity: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4096 }))),
})
export type WatchOptions = typeof WatchOptionsSchema.Type
export type Failure = ObservationError | HarnessError | Session.Failure | Projection.Failure
export interface Capabilities {
  readonly graphSnapshot?: () => Effect.Effect<TaskGraph.Snapshot, Failure>
  readonly watchGraph?: (options?: WatchOptions) => Stream.Stream<TaskGraph.Frame, Failure>
  readonly conversationSnapshot?: (
    id: Record.ConversationId,
  ) => Effect.Effect<ConversationView.Snapshot, Failure>
  readonly watchView?: (
    id: Record.ConversationId,
    options?: WatchOptions,
  ) => Stream.Stream<ConversationView.Frame, Failure>
  readonly watchEvents?: (
    id: Record.ConversationId,
    options?: WatchOptions,
  ) => Stream.Stream<AgentEvent.Frame, Failure>
}
export const unsupported = (operation: string) =>
  Effect.fail(
    new ObservationError({
      reason: 'unsupported',
      operation,
      message: 'This backend does not support the requested observation',
    }),
  )

/** A new epoch requires a new initial snapshot; rebases repair gaps only within one mount. */
export const check = Effect.fnUntraced(function* (
  current: { readonly protocol: number; readonly target: Target; readonly revision: Revision },
  next: {
    readonly _tag: string
    readonly protocol: number
    readonly target: Target
    readonly revision?: Revision
    readonly epoch?: string
    readonly from?: number
    readonly to?: number
  },
) {
  const invalid = () =>
    new ObservationError({
      reason: 'continuity',
      operation: 'observation.reduce',
      message: 'Foreign, duplicate, or discontinuous observation frame',
    })
  if (
    current.protocol !== protocol ||
    next.protocol !== protocol ||
    !Schema.toEquivalence(TargetSchema)(current.target, next.target)
  )
    return yield* invalid()
  if (next._tag === 'Snapshot') return yield* invalid()
  if (next._tag === 'Rebase') {
    if (
      next.revision?.epoch !== current.revision.epoch ||
      next.revision.cursor <= current.revision.cursor
    )
      return yield* invalid()
    return next.revision
  }
  if (
    next.epoch !== current.revision.epoch ||
    next.from !== current.revision.cursor ||
    next.to !== next.from + 1
  )
    return yield* invalid()
  return { epoch: next.epoch, cursor: next.to }
})
