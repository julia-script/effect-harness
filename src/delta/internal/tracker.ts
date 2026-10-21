import { Effect } from 'effect'
import { cast } from 'effect/Function'
import { AdoptRejected, ChangeSettled, InvalidWrite } from '../DeltaError.ts'
import type { AdoptReason } from '../DeltaError.ts'
import type { Change, Draft, Prepared, Tracker } from '../Tracker.ts'
import { isWritable } from './context.ts'
import type { OverlayContext, StatusCell, TrackerHost } from './context.ts'
import type { Container, Json } from '../../internal/json.ts'
import { abortContext, clearContext, createContext, materialize } from './lifecycle.ts'
import { releaseOverlayReferences } from './lifecycle.ts'
import type { Op } from './ops.ts'

/**
 * The tracker, its changes, and their preparations. Roots are trusted strict JSON typed by the
 * caller, so values cross between `T` and `Json` with `cast` at this boundary only.
 */

const PREPARED = new WeakMap<object, OverlayContext>()
const PRUNE_BUDGET = 256

class PreparedImpl<T extends object> implements Prepared<T> {
  readonly base: T
  readonly value: T
  readonly ops: ReadonlyArray<Op>
  readonly baseRevision: number
  readonly abort: Effect.Effect<void>

  constructor(context: OverlayContext, base: T, value: Json, ops: ReadonlyArray<Op>) {
    this.base = base
    this.value = cast(value)
    this.ops = ops
    this.baseRevision = context.baseRevision
    this.abort = Effect.sync(() => {
      abortContext(context)
    })
    PREPARED.set(this, context)
  }
}

const prepared = <T extends object>(context: OverlayContext, base: T): Prepared<T> => {
  const { value, ops } = materialize(context)
  return new PreparedImpl(context, base, value, ops)
}

class ChangeImpl<T extends object> implements Change<T> {
  readonly state: Draft<T>
  #context: OverlayContext | undefined
  #preparedStatus: StatusCell | undefined
  readonly #base: T

  constructor(context: OverlayContext, base: T) {
    this.#context = context
    this.#base = base
    this.state = cast(context.root!.proxy)
  }

  readonly prepare = Effect.suspend(
    (): Effect.Effect<Prepared<T>, InvalidWrite | ChangeSettled> => {
      const context = this.#context
      if (context === undefined) {
        return Effect.fail(new ChangeSettled({ message: 'Change has already been settled' }))
      }
      if (!isWritable(context)) {
        return Effect.fail(new ChangeSettled({ message: 'Cannot use a settled overlay' }))
      }
      this.#context = undefined
      if (context.problem !== undefined) {
        context.status.value = 'aborted'
        clearContext(context)
        return Effect.fail(new InvalidWrite({ message: context.problem }))
      }
      context.status.value = 'prepared'
      this.#preparedStatus = context.status
      return Effect.succeed(prepared(context, this.#base))
    },
  )

  readonly abort = Effect.sync(() => {
    const context = this.#context
    if (context === undefined) {
      if (this.#preparedStatus?.value === 'prepared') this.#preparedStatus.value = 'aborted'
      this.#preparedStatus = undefined
      return
    }
    this.#context = undefined
    abortContext(context)
  })
}

const rejected = (reason: AdoptReason, message: string) =>
  Effect.fail(new AdoptRejected({ reason, message }))

class TrackerImpl<T extends object> implements Tracker<T>, TrackerHost {
  readonly #owner = {}
  readonly #contexts = new Set<WeakRef<OverlayContext>>()
  readonly #self: WeakRef<TrackerHost> = new WeakRef<TrackerHost>(this)
  #value: T
  #revision = 0
  #pruneBudget = PRUNE_BUDGET

  constructor(initial: T) {
    this.#value = initial
  }

  readonly value = Effect.sync(() => this.#value)
  readonly revision = Effect.sync(() => this.#revision)

  readonly beginChange = Effect.sync((): Change<T> => {
    const base = this.#value
    const context = this.#open(base, false)
    return new ChangeImpl(context, base)
  })

  readonly prepareReplace = (value: T) =>
    Effect.sync(() => {
      const context = this.#open(value, true)
      return prepared(context, this.#value)
    })

  readonly adopt = (preparation: Prepared<T>) =>
    Effect.suspend(() => {
      const context = PREPARED.get(preparation)
      if (context?.owner !== this.#owner) {
        return rejected('foreign', 'Prepared change belongs to a different tracker')
      }
      const status = context.status.value
      if (status === 'consumed')
        return rejected('consumed', 'Prepared change has already been used')
      if (status === 'aborted') return rejected('aborted', 'Prepared change has been aborted')
      if (status === 'stale') return rejected('stale', 'Prepared change is stale')
      if (status !== 'prepared') return rejected('notReady', 'Prepared change is not ready')
      if (context.baseRevision !== this.#revision || this.#value !== preparation.base) {
        context.status.value = 'stale'
        clearContext(context)
        return rejected('stale', 'Prepared change is stale')
      }
      // Materialization happened during prepare, so adoption is an infallible pointer swap: a
      // storage failure can discard the candidate without touching authority.
      this.#value = preparation.value
      context.status.value = 'consumed'
      this.#revision += 1
      this.#invalidate(context)
      return Effect.void
    })

  releaseContext(context: OverlayContext): void {
    if (context.registryRef !== undefined) this.#contexts.delete(context.registryRef)
    context.registryRef = undefined
  }

  replacementBase(context: OverlayContext): Json | undefined {
    return context.baseRevision === this.#revision ? cast(this.#value) : undefined
  }

  #open(root: T, replacement: boolean): OverlayContext {
    const container: Container = cast(root)
    const base: Container = cast(this.#value)
    const context = createContext(
      this.#self,
      this.#owner,
      this.#revision,
      container,
      replacement,
      base,
    )
    const reference = new WeakRef(context)
    context.registryRef = reference
    this.#contexts.add(reference)
    this.#pruneBudget -= 1
    if (this.#pruneBudget === 0) {
      for (const entry of this.#contexts)
        if (entry.deref() === undefined) this.#contexts.delete(entry)
      this.#pruneBudget = Math.max(PRUNE_BUDGET, this.#contexts.size)
    }
    return context
  }

  /**
   * Stale every competitor of the adopted context. Adoption must not walk every node a competing
   * draft touched, so each competitor drops its references in O(1); retained handles still see
   * the stale status.
   */
  #invalidate(winner: OverlayContext): void {
    for (const reference of this.#contexts) {
      const context = reference.deref()
      if (context === undefined || context === winner) continue
      const status = context.status.value
      if (status === 'open' || status === 'prepared') context.status.value = 'stale'
      releaseOverlayReferences(context)
    }
    this.#contexts.clear()
    this.#pruneBudget = PRUNE_BUDGET
  }
}

export const make = <T extends object>(initial: T): Effect.Effect<Tracker<T>> =>
  Effect.sync(() => new TrackerImpl(initial))
