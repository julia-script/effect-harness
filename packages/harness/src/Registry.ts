import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import * as SubscriptionRef from 'effect/SubscriptionRef'
import * as Agent from './Agent.ts'
import { RegistryError } from './Error.ts'
import type * as Extension from './Extension.ts'
import * as Hook from './Hook.ts'
import { Invocation } from './Invocation.ts'
import type * as Tool from './Tool.ts'

export interface Snapshot {
  readonly revision: number
  readonly extensions: ReadonlyArray<Extension.Extension>
}
export interface Resolved {
  readonly snapshot: Snapshot
  readonly state: Agent.State
  readonly settings: Agent.Settings
  readonly extensions: ReadonlyArray<Extension.Extension>
  readonly tools: ReadonlyArray<Tool.Registration>
  readonly sections: ReadonlyArray<Extension.Section>
  readonly hooks: ReadonlyArray<Hook.Registration>
}
export class Registry extends Context.Service<
  Registry,
  {
    readonly snapshot: Effect.Effect<Snapshot>
    readonly changes: Stream.Stream<Snapshot>
    readonly install: (
      extensions: ReadonlyArray<Extension.Extension>,
    ) => Effect.Effect<void, RegistryError>
    readonly uninstall: (name: string) => Effect.Effect<void>
  }
>()('@effect-harness/harness/Registry') {}
function validate(
  extensions: ReadonlyArray<Extension.Extension>,
): Effect.Effect<void, RegistryError> {
  for (const extension of extensions) {
    if (extension.name === '')
      return Effect.fail(new RegistryError({ message: 'Extension name must be nonempty' }))
    const names = new Set<string>()
    for (const registration of extension.tools ?? []) {
      if (names.has(registration.tool.name))
        return Effect.fail(
          new RegistryError({
            message: `Duplicate tool ${registration.tool.name} in ${extension.name}`,
          }),
        )
      names.add(registration.tool.name)
    }
    const keys = new Set<string>()
    for (const section of extension.sections ?? []) {
      if (!/^[a-z][a-z0-9_-]*$/.test(section.key) || section.key === 'instructions')
        return Effect.fail(
          new RegistryError({ message: `Invalid or reserved section ${section.key}` }),
        )
      if (keys.has(section.key))
        return Effect.fail(
          new RegistryError({ message: `Duplicate section ${section.key} in ${extension.name}` }),
        )
      keys.add(section.key)
    }
  }
  return Effect.void
}
export const make = Effect.fnUntraced(function* (initial: ReadonlyArray<Extension.Extension> = []) {
  yield* validate(initial)
  const merged = new Map(initial.map((extension) => [extension.name, extension]))
  const ref = yield* SubscriptionRef.make<Snapshot>({
    revision: 0,
    extensions: [...merged.values()],
  })
  const lock = yield* Semaphore.make(1)
  return Registry.of({
    snapshot: SubscriptionRef.get(ref),
    changes: SubscriptionRef.changes(ref),
    install: (extensions) =>
      lock.withPermit(
        Effect.gen(function* () {
          yield* validate(extensions)
          const before = yield* SubscriptionRef.get(ref)
          const candidate = new Map(
            before.extensions.map((extension) => [extension.name, extension]),
          )
          for (const extension of extensions) candidate.set(extension.name, extension)
          yield* validate([...candidate.values()])
          if (
            extensions.length === 0 ||
            ([...candidate.values()].every(
              (extension, index) => extension === before.extensions[index],
            ) &&
              candidate.size === before.extensions.length)
          )
            return
          yield* SubscriptionRef.set(ref, {
            revision: before.revision + 1,
            extensions: [...candidate.values()],
          })
        }),
      ),
    uninstall: (name) =>
      lock.withPermit(
        Effect.gen(function* () {
          const before = yield* SubscriptionRef.get(ref)
          const extensions = before.extensions.filter((extension) => extension.name !== name)
          if (extensions.length === before.extensions.length) return
          yield* SubscriptionRef.set(ref, { revision: before.revision + 1, extensions })
        }),
      ),
  })
})
export const layer = (
  extensions: ReadonlyArray<Extension.Extension> = [],
): Layer.Layer<Registry, RegistryError> => Layer.effect(Registry, make(extensions))
/** The builder captures service implementations at Layer construction, not at request execution. */
export const layerEffect = <E, R>(
  extensions: Effect.Effect<ReadonlyArray<Extension.Extension>, E, R>,
): Layer.Layer<Registry, E | RegistryError, R> =>
  Layer.effect(Registry, Effect.flatMap(extensions, make))
export const resolve = Effect.fnUntraced(function* (
  snapshot: Snapshot,
  state: Agent.State,
  settings: Agent.Settings,
) {
  const installed = new Map(snapshot.extensions.map((extension) => [extension.name, extension]))
  const selected = Agent.select(
    state.extensions,
    settings.extensions ?? [...installed.keys()],
  ).flatMap((name) => {
    const extension = installed.get(name)
    return extension === undefined ? [] : [extension]
  })
  const tools = new Map<string, Tool.Registration>()
  const sections = new Map<string, Extension.Section>()
  for (const extension of selected) {
    for (const tool of extension.tools ?? []) tools.set(tool.tool.name, tool)
    for (const section of extension.sections ?? []) sections.set(section.key, section)
  }
  for (const extension of selected) {
    for (const wrapper of extension.toolWraps ?? []) {
      const current = tools.get(wrapper.name)
      if (current === undefined) continue
      const next = yield* Hook.recover(Effect.suspend(() => wrapper.wrap.call(wrapper, current)))
      if (next === undefined) tools.delete(wrapper.name)
      else if (next.tool.name !== wrapper.name) {
        tools.delete(wrapper.name)
        yield* (yield* Invocation).report(
          new RegistryError({ message: `Tool wrapper renamed ${wrapper.name}` }),
        )
      } else tools.set(wrapper.name, next)
    }
    for (const wrapper of extension.sectionWraps ?? []) {
      const current = sections.get(wrapper.key)
      if (current === undefined) continue
      const next = yield* Hook.recover(Effect.suspend(() => wrapper.wrap.call(wrapper, current)))
      if (next === undefined) sections.delete(wrapper.key)
      else if (next.key !== wrapper.key) {
        sections.delete(wrapper.key)
        yield* (yield* Invocation).report(
          new RegistryError({ message: `Section wrapper renamed ${wrapper.key}` }),
        )
      } else sections.set(wrapper.key, next)
    }
  }
  let offered: ReadonlyArray<Tool.Registration> = [...tools.values()]
  if (Array.isArray(state.tools))
    offered = [...new Set(state.tools)].flatMap((name) => {
      const tool = tools.get(name)
      return tool === undefined ? [] : [tool]
    })
  else if (state.tools !== undefined) {
    const remove = new Set((state.tools as { readonly remove: ReadonlyArray<string> }).remove)
    offered = offered.filter((tool) => !remove.has(tool.tool.name))
  }
  if (state.instructions !== undefined) {
    const instructions = state.instructions
    sections.set('instructions', {
      key: 'instructions',
      render: () => Effect.succeed(instructions),
    })
  }
  return {
    snapshot,
    state,
    settings,
    extensions: selected,
    tools: offered,
    sections: [...sections.values()],
    hooks: selected.flatMap((extension) => extension.hooks ?? []),
  } satisfies Resolved
})
export const handlers = (
  agent: Resolved,
  operation: Hook.Operation,
): ReadonlyArray<Hook.Handlers> =>
  agent.hooks.filter((hook) => hook.operation === operation).map((hook) => hook.handlers)
export const render = Effect.fnUntraced(function* (
  agent: Resolved,
  view: import('./Context.ts').View,
  shown: ReadonlyMap<string, string>,
) {
  const invocation = yield* Invocation
  const input = { view, tools: agent.tools, cwd: agent.state.cwd ?? invocation.cwd }
  const desired = new Map<string, string>()
  for (const section of agent.sections) {
    const result = yield* Effect.exit(Effect.suspend(() => section.render.call(section, input)))
    if (result._tag === 'Failure') {
      yield* Hook.recover(Effect.failCause(result.cause))
      const kept = shown.get(section.key)
      if (kept !== undefined) desired.set(section.key, kept)
    } else if (result.value !== undefined)
      desired.set(
        section.key,
        section.tag === false
          ? result.value
          : `<${section.key}>\n${result.value}\n</${section.key}>`,
      )
  }
  return desired
})
