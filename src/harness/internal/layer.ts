/**
 * The default loop: a task `Registry` and an `Extensions` registry of its own, the given
 * extensions, the built-in generation, tool and compaction tasks, and a Harness over them.
 */
import { Effect } from 'effect'
import * as Registry from '../../task/Registry.ts'
import * as Extensions from '../Extensions.ts'
import * as Generation from '../Generation.ts'
import type { LayerOptions } from '../Harness.ts'
import * as Settings from '../Settings.ts'
import { make } from './harness.ts'

const build = Effect.fnUntraced(function* (options: LayerOptions) {
  const registry = yield* Registry.make()
  const extensions = yield* Extensions.make.pipe(Effect.provideService(Registry.Registry, registry))
  for (const extension of options.extensions ?? []) yield* extensions.install(extension)
  const builtIn = yield* Generation.tasks.pipe(
    Effect.provideService(Extensions.Extensions, extensions),
  )
  for (const task of [...builtIn, ...(options.tasks ?? [])]) yield* registry.install(task)
  const harness = yield* make(options).pipe(
    Effect.provideService(Registry.Registry, registry),
    Effect.provideService(Extensions.Extensions, extensions),
  )
  return { harness, extensions }
})

/** The default loop's Harness and Extensions, under `options.settings` when given. */
export const open = (options: LayerOptions) =>
  options.settings === undefined
    ? build(options)
    : Effect.provideService(build(options), Settings.Current, Settings.make(options.settings))
