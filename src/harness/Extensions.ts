/**
 * The extension registry, ported from pi's `createRegistry` (spec §7.1, §7.5): the extensions the
 * loop has installed, in install order. It is process-local and not persisted; conversations
 * select extensions by name, and a restart installs them again.
 *
 * Installing an extension under an installed name replaces it in place, in one publication, which
 * is how extension code reloads: work already running keeps what it resolved until its phase
 * ends, and the next phase resolves the replacement. An extension's task definitions go into the
 * task `Registry` with it, and leave it with it.
 */
import { Context, Effect, Layer, Semaphore, SubscriptionRef } from 'effect'
import type { Stream } from 'effect'
import { Registry } from '../task/Registry.ts'
import type * as Toolkit from '../tool/Toolkit.ts'
import type * as ToolTask from '../tool/ToolTask.ts'
import type { Extension } from './Extension.ts'
import type { InvalidExtension } from './HarnessError.ts'
import { implementations, publish, validate } from './internal/extensions.ts'

/** An installed extension, with the implementations its tools were installed with. */
export interface Installed {
  readonly extension: Extension
  /** Its tools in declaration order. */
  readonly tools: ReadonlyArray<ToolTask.Installed>
}

/** The installed extensions, in install order. A snapshot never changes. */
export type Snapshot = ReadonlyArray<Installed>

export interface Service {
  /**
   * Install `extension`, or replace the installed one of its name in place, with the
   * implementations of its tools from the context. Fails without publishing when the registry
   * would be invalid.
   */
  readonly install: <Names extends string>(
    extension: Extension<Names>,
  ) => Effect.Effect<void, InvalidExtension, Toolkit.Requires<Names>>
  /** Remove the extension named `name`; a later install appends it. */
  readonly uninstall: (name: string) => Effect.Effect<void>
  readonly snapshot: Effect.Effect<Snapshot>
  /** The current snapshot, then every later one. */
  readonly changes: Stream.Stream<Snapshot>
}

export class Extensions extends Context.Service<Extensions, Service>()(
  'effect-harness/harness/Extensions',
) {}

/** An empty registry over the task `Registry`, which gets each extension's tasks. */
export const make: Effect.Effect<Service, never, Registry> = Effect.gen(function* () {
  const registry = yield* Registry
  const ref = yield* SubscriptionRef.make<Snapshot>([])
  const line = yield* Semaphore.make(1)
  const update = (next: (current: Snapshot) => Effect.Effect<Snapshot, InvalidExtension>) =>
    line.withPermit(
      Effect.gen(function* () {
        const current = yield* SubscriptionRef.get(ref)
        const replaced = yield* next(current)
        yield* publish(registry, current, replaced)
        yield* SubscriptionRef.set(ref, replaced)
      }),
    )
  const service: Service = {
    install: (extension) =>
      Effect.flatMap(implementations(extension), (installed) =>
        update((current) => {
          const at = current.findIndex((each) => each.extension.name === extension.name)
          const next =
            at < 0
              ? [...current, installed]
              : current.map((each, index) => (index === at ? installed : each))
          return Effect.as(validate(next, extension.name), next)
        }),
      ),
    uninstall: (name) =>
      Effect.orDie(
        update((current) => Effect.succeed(current.filter((each) => each.extension.name !== name))),
      ),
    snapshot: SubscriptionRef.get(ref),
    changes: SubscriptionRef.changes(ref),
  }
  return service
})

/** An empty `Extensions` registry over the task `Registry`. */
export const layer: Layer.Layer<Extensions, never, Registry> = Layer.effect(Extensions, make)

/** Install `extension` when the layer builds, with its tools' implementations from context. */
export const layerInstall = <Names extends string>(
  extension: Extension<Names>,
): Layer.Layer<never, InvalidExtension, Extensions | Toolkit.Requires<Names>> =>
  Layer.effectDiscard(Effect.flatMap(Extensions, (extensions) => extensions.install(extension)))
