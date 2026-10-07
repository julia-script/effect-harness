/**
 * Extension installation, snapshot resolution and prompt rendering.
 *
 * @since 0.0.0
 */
import { constUndefined } from 'effect/Function'
import * as Exit from 'effect/Exit'
import { dual } from 'effect/Function'
import * as Arr from 'effect/Array'
import * as Option from 'effect/Option'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import * as SubscriptionRef from 'effect/SubscriptionRef'
import * as Agent from './Agent.ts'
import { RegistryError, RegistryFailure } from './RegistryError.ts'
import type * as Extension from './Extension.ts'
import * as Hook from './Hook.ts'
import { Invocation } from './Invocation.ts'
import type * as Tool from './Tool.ts'

/**
 * Registry snapshot contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Snapshot {
  readonly revision: number
  readonly extensions: ReadonlyArray<Extension.Extension>
}
/**
 * Registry resolved contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Resolved {
  readonly snapshot: Snapshot
  readonly state: Agent.State
  readonly settings: Agent.Settings
  readonly extensions: ReadonlyArray<Extension.Extension>
  readonly tools: ReadonlyArray<Tool.Registration>
  readonly sections: ReadonlyArray<Extension.Section>
  readonly hooks: ReadonlyArray<Hook.Registration>
}
/**
 * Service for registry capabilities.
 *
 * @category services
 * @since 0.0.0
 */
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
function validate(self: ReadonlyArray<Extension.Extension>): Effect.Effect<void, RegistryError> {
  for (const extension of self) {
    if (extension.name === '')
      return Effect.fail(
        new RegistryError({
          reason: new RegistryFailure({ message: 'Extension name must be nonempty' }),
        }),
      )
    const names = new Set<string>()
    for (const registration of extension.tools ?? []) {
      if (names.has(registration.tool.name))
        return Effect.fail(
          new RegistryError({
            reason: new RegistryFailure({
              message: `Duplicate tool ${registration.tool.name} in ${extension.name}`,
            }),
          }),
        )
      names.add(registration.tool.name)
    }
    const keys = new Set<string>()
    for (const section of extension.sections ?? []) {
      if (!/^[a-z][a-z0-9_-]*$/.test(section.key) || section.key === 'instructions')
        return Effect.fail(
          new RegistryError({
            reason: new RegistryFailure({ message: `Invalid or reserved section ${section.key}` }),
          }),
        )
      if (keys.has(section.key))
        return Effect.fail(
          new RegistryError({
            reason: new RegistryFailure({
              message: `Duplicate section ${section.key} in ${extension.name}`,
            }),
          }),
        )
      keys.add(section.key)
    }
  }
  return Effect.void
}
/**
 * Creates a validated extension registry with atomic immutable revision snapshots.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = Effect.fnUntraced(function* (
  initial: ReadonlyArray<Extension.Extension> = [],
): Effect.fn.Return<Registry['Service'], RegistryError> {
  yield* validate(initial)
  const merged = new Map(initial.map((extension) => [extension.name, extension]))
  const ref = yield* SubscriptionRef.make<Snapshot>({
    revision: 0,
    extensions: [...merged.values()],
  })
  const lock = yield* Semaphore.make(1)
  return Registry.of({
    snapshot: SubscriptionRef.get(ref).pipe(Effect.withSpan('Registry.snapshot')),
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
            ([...candidate.values()].every((extension, index) =>
              Option.contains(Arr.get(before.extensions, index), extension),
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
/**
 * Layer for Registry capabilities.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (
  extensions: ReadonlyArray<Extension.Extension> = [],
): Layer.Layer<Registry, RegistryError> => Layer.effect(Registry, make(extensions))
/**
 * The builder captures service implementations at Layer construction, not at request execution.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerEffect = <E, R>(
  self: Effect.Effect<ReadonlyArray<Extension.Extension>, E, R>,
): Layer.Layer<Registry, E | RegistryError, R> => Layer.effect(Registry, Effect.flatMap(self, make))
const resolveImpl = Effect.fnUntraced(function* (
  snapshot: Snapshot,
  state: Agent.State,
  settings: Agent.Settings,
): Effect.fn.Return<Resolved, never, Invocation> {
  const installed = new Map(snapshot.extensions.map((extension) => [extension.name, extension]))
  const selected = Agent.select(
    state.extensions,
    settings.extensions ?? [...installed.keys()],
  ).flatMap((name) => {
    return Option.toArray(Option.fromUndefinedOr(installed.get(name)))
  })
  const tools = new Map<string, Tool.Registration>()
  const sections = new Map<string, Extension.Section>()
  for (const extension of selected) {
    for (const tool of extension.tools ?? []) tools.set(tool.tool.name, tool)
    for (const section of extension.sections ?? []) sections.set(section.key, section)
  }
  for (const extension of selected) {
    for (const wrapper of extension.toolWraps ?? []) {
      yield* Option.match(Option.fromUndefinedOr(tools.get(wrapper.name)), {
        onNone: () => Effect.void,
        onSome: (value) =>
          Effect.gen(function* () {
            const next = yield* Hook.recover(
              Effect.suspend(() => wrapper.wrap.call(wrapper, value)),
            )
            if (next === undefined) tools.delete(wrapper.name)
            else if (next.tool.name !== wrapper.name) {
              tools.delete(wrapper.name)
              yield* (yield* Invocation).report(
                new RegistryError({
                  reason: new RegistryFailure({ message: `Tool wrapper renamed ${wrapper.name}` }),
                }),
              )
            } else tools.set(wrapper.name, next)
          }),
      })
    }
    for (const wrapper of extension.sectionWraps ?? []) {
      yield* Option.match(Option.fromUndefinedOr(sections.get(wrapper.key)), {
        onNone: () => Effect.void,
        onSome: (value) =>
          Effect.gen(function* () {
            const next = yield* Hook.recover(
              Effect.suspend(() => wrapper.wrap.call(wrapper, value)),
            )
            if (next === undefined) sections.delete(wrapper.key)
            else if (next.key !== wrapper.key) {
              sections.delete(wrapper.key)
              yield* (yield* Invocation).report(
                new RegistryError({
                  reason: new RegistryFailure({
                    message: `Section wrapper renamed ${wrapper.key}`,
                  }),
                }),
              )
            } else sections.set(wrapper.key, next)
          }),
      })
    }
  }
  let offered: ReadonlyArray<Tool.Registration> = [...tools.values()]
  if (Array.isArray(state.tools))
    offered = Arr.dedupe(state.tools).flatMap((name) => {
      return Option.toArray(Option.fromUndefinedOr(tools.get(name)))
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
/**
 * Resolves the configured extensions, tools and model-facing sections.
 *
 * @category combinators
 * @since 0.0.0
 */
export const resolve: {
  (
    state: Agent.State,
    settings: Agent.Settings,
  ): (self: Snapshot) => Effect.Effect<Resolved, never, Invocation>
  (
    self: Snapshot,
    state: Agent.State,
    settings: Agent.Settings,
  ): Effect.Effect<Resolved, never, Invocation>
} = dual(3, resolveImpl)
const handlersImpl = (self: Resolved, operation: Hook.Operation): Array<Hook.Handlers> =>
  self.hooks.filter((hook) => hook.operation === operation).map((hook) => hook.handlers)
/**
 * Returns registered handlers for the requested lifecycle operation.
 *
 * @category combinators
 * @since 0.0.0
 */
export const handlers: {
  (operation: Hook.Operation): (self: Resolved) => Array<Hook.Handlers>
  (self: Resolved, operation: Hook.Operation): Array<Hook.Handlers>
} = dual(2, handlersImpl)
const renderImpl = Effect.fnUntraced(function* (
  agent: Resolved,
  view: import('./Context.ts').View,
  shown: ReadonlyMap<string, string>,
): Effect.fn.Return<Map<string, string>, never, Invocation> {
  const invocation = yield* Invocation
  const input = { view, tools: agent.tools, cwd: agent.state.cwd ?? invocation.cwd }
  const desired = new Map<string, string>()
  for (const section of agent.sections) {
    const result = yield* Effect.exit(Effect.suspend(() => section.render.call(section, input)))
    yield* Exit.match(result, {
      onFailure: (cause) =>
        Hook.recover(Effect.failCause(cause)).pipe(
          Effect.map(() =>
            Option.match(Option.fromUndefinedOr(shown.get(section.key)), {
              onNone: constUndefined,
              onSome: (self) => {
                desired.set(section.key, self)
              },
            }),
          ),
        ),
      onSuccess: (self) =>
        Effect.sync(() => {
          if (self !== undefined)
            desired.set(
              section.key,
              section.tag === false ? self : `<${section.key}>\n${self}\n</${section.key}>`,
            )
        }),
    })
  }
  return desired
})
/**
 * Renders effective managed sections while retaining prior values after hook failure.
 *
 * @category combinators
 * @since 0.0.0
 */
export const render: {
  (
    view: import('./Context.ts').View,
    shown: ReadonlyMap<string, string>,
  ): (self: Resolved) => Effect.Effect<Map<string, string>, never, Invocation>
  (
    self: Resolved,
    view: import('./Context.ts').View,
    shown: ReadonlyMap<string, string>,
  ): Effect.Effect<Map<string, string>, never, Invocation>
} = dual(3, renderImpl)
