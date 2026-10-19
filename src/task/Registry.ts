/**
 * The task `Registry`: which definition runs each task kind. The Scheduler reads a snapshot when
 * it reserves a task and again at every phase boundary, and reconsiders blocked tasks whenever the
 * registry changes.
 */
import { Context, Effect, Layer, SubscriptionRef } from 'effect'
import type { Stream } from 'effect'
import type * as Task from './Task.ts'

/** Installed definitions by task kind. A snapshot never changes; installing publishes a new one. */
export type Snapshot = ReadonlyMap<string, Task.Any>

export interface Service {
  /**
   * Install a definition under its name, replacing one installed before. A running task changes
   * to a replacement that can take it at its next phase boundary.
   */
  readonly install: (task: Task.Any) => Effect.Effect<void>
  /** Remove the definition of `name`; its tasks stay live, blocked until it is installed again. */
  readonly uninstall: (name: string) => Effect.Effect<void>
  /** The current snapshot. */
  readonly snapshot: Effect.Effect<Snapshot>
  /** The current snapshot, then every later one. */
  readonly changes: Stream.Stream<Snapshot>
}

export class Registry extends Context.Service<Registry, Service>()(
  'effect-harness/task/Registry',
) {}

/** A registry holding `tasks`; a later task replaces an earlier one with the same name. */
export const make = (tasks: Iterable<Task.Any> = []): Effect.Effect<Service> =>
  Effect.map(
    SubscriptionRef.make<Snapshot>(new Map([...tasks].map((task) => [task.name, task]))),
    (ref): Service => ({
      install: (task) =>
        SubscriptionRef.update(ref, (current) => new Map(current).set(task.name, task)),
      uninstall: (name) =>
        SubscriptionRef.update(ref, (current) => {
          if (!current.has(name)) return current
          const next = new Map(current)
          next.delete(name)
          return next
        }),
      snapshot: SubscriptionRef.get(ref),
      changes: SubscriptionRef.changes(ref),
    }),
  )

/** A `Registry` holding `tasks`. */
export const layer = (tasks: Iterable<Task.Any> = []): Layer.Layer<Registry> =>
  Layer.effect(Registry, make(tasks))
