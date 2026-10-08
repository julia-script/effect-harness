/**
 * Extension installation, snapshot resolution and prompt rendering.
 */
import { constUndefined } from 'effect/Function'
import * as Exit from 'effect/Exit'
import { dual } from 'effect/Function'
import * as Arr from 'effect/Array'
import * as Option from 'effect/Option'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Stream from 'effect/Stream'
import * as SubscriptionRef from 'effect/SubscriptionRef'
import * as Agent from './Agent.ts'
import * as Schema from 'effect/Schema'
import * as SchemaField from './SchemaField.ts'
import type * as Extension from './Extension.ts'
import * as Hook from './Hook.ts'
import { Invocation } from './Invocation.ts'
import type * as ToolRegistration from './ToolRegistration.ts'

/**
 * Semantic registry failure with its retained cause.
 *
 * @category errors
 */
export class RegistryFailureError extends Schema.TaggedError<RegistryFailureError>(
  'effect-harness/Registry/RegistryFailureError',
)('RegistryFailureError', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
}) {}
/**
 * Schema for registry error reason.
 *
 * @category schemas
 */
export const RegistryErrorReason = Schema.Union([RegistryFailureError])
/**
 * Decoded value validated by the `RegistryErrorReason` schema.
 *
 * @category models
 */
export type RegistryErrorReason = typeof RegistryErrorReason.Type
/**
 * Semantic registry error with its retained cause.
 *
 * @category errors
 */
export class RegistryError extends Schema.TaggedError<RegistryError>(
  'effect-harness/Registry/RegistryError',
)('RegistryError', { reason: RegistryErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
}

/**
 * Registered named extensions captured by a Registry.
 *
 * @category models
 */
export interface Snapshot {
  readonly revision: number
  readonly extensions: ReadonlyArray<Extension.Extension>
}
/**
 * Selected tools, sections, hooks and model reference for a request.
 *
 * @category models
 */
export interface Resolved {
  readonly snapshot: Snapshot
  readonly state: Agent.State
  readonly settings: Agent.Settings
  readonly extensions: ReadonlyArray<Extension.Extension>
  readonly tools: ReadonlyArray<ToolRegistration.Registration>
  readonly sections: ReadonlyArray<Extension.Extension.Section>
  readonly hooks: ReadonlyArray<Hook.Registration>
}
/**
 * Service resolving named extensions into request-local tools, sections and hooks.
 *
 * **Details**
 *
 * Resolve selections before preparing a request. Registration snapshots keep host
 * dependencies captured while invocation services remain dynamic.
 *
 * @category services
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
>()('effect-harness/Registry') {}
function validate(self: ReadonlyArray<Extension.Extension>): Effect.Effect<void, RegistryError> {
  for (const extension of self) {
    if (extension.name === '')
      return Effect.fail(
        new RegistryError({
          reason: new RegistryFailureError({ message: 'Extension name must be nonempty' }),
        }),
      )
    const names = new Set<string>()
    for (const registration of extension.tools ?? []) {
      if (names.has(registration.tool.name))
        return Effect.fail(
          new RegistryError({
            reason: new RegistryFailureError({
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
            reason: new RegistryFailureError({
              message: `Invalid or reserved section ${section.key}`,
            }),
          }),
        )
      if (keys.has(section.key))
        return Effect.fail(
          new RegistryError({
            reason: new RegistryFailureError({
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
  return Registry.of({
    snapshot: SubscriptionRef.get(ref).pipe(Effect.withSpan('Registry.snapshot')),
    changes: SubscriptionRef.changes(ref),
    install: (extensions) =>
      SubscriptionRef.modifySomeEffect(
        ref,
        Effect.fnUntraced(function* (before) {
          yield* validate(extensions)
          const candidate = new Map(
            before.extensions.map((extension) => [extension.name, extension]),
          )
          for (const extension of extensions) candidate.set(extension.name, extension)
          const next = [...candidate.values()]
          yield* validate(next)
          if (
            extensions.length === 0 ||
            (next.every((extension, index) =>
              Option.contains(Arr.get(before.extensions, index), extension),
            ) &&
              candidate.size === before.extensions.length)
          )
            return [undefined, Option.none<Snapshot>()] as const
          return [
            undefined,
            Option.some({ revision: before.revision + 1, extensions: next }),
          ] as const
        }),
      ),
    uninstall: (name) =>
      SubscriptionRef.modifySome(ref, (before) => {
        const extensions = Arr.filter(before.extensions, (extension) => extension.name !== name)
        return [
          undefined,
          extensions.length === before.extensions.length
            ? Option.none<Snapshot>()
            : Option.some({ revision: before.revision + 1, extensions }),
        ] as const
      }),
  })
})
/**
 * Provides a Registry from already constructed extensions.
 *
 * **Details**
 *
 * Registration validates names and selection conflicts. Build effectful extension values
 * before supplying this Layer.
 *
 * @see {@link layerEffect} for effectful extension construction.
 * @category layers
 */
export const layer = (
  extensions: ReadonlyArray<Extension.Extension> = [],
): Layer.Layer<Registry, RegistryError> => Layer.effect(Registry, make(extensions))
/**
 * Provides a Registry built from effectful extensions.
 *
 * **Details**
 *
 * The builder captures host services when the Layer is constructed; invocation services remain request-local.
 *
 * @category layers
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
  const tools = new Map<string, ToolRegistration.Registration>()
  const sections = new Map<string, Extension.Extension.Section>()
  for (const extension of selected) {
    for (const tool of extension.tools ?? []) tools.set(tool.tool.name, tool)
    for (const section of extension.sections ?? []) sections.set(section.key, section)
  }
  for (const extension of selected) {
    for (const wrapper of extension.toolWraps ?? []) {
      yield* Option.match(Option.fromUndefinedOr(tools.get(wrapper.name)), {
        onNone: () => Effect.void,
        onSome: Effect.fnUntraced(function* (value) {
          const next = yield* Hook.recover(Effect.suspend(() => wrapper.wrap.call(wrapper, value)))
          if (next === undefined) tools.delete(wrapper.name)
          else if (next.tool.name !== wrapper.name) {
            tools.delete(wrapper.name)
            yield* (yield* Invocation).report(
              new RegistryError({
                reason: new RegistryFailureError({
                  message: `Tool wrapper renamed ${wrapper.name}`,
                }),
              }),
            )
          } else tools.set(wrapper.name, next)
        }),
      })
    }
    for (const wrapper of extension.sectionWraps ?? []) {
      yield* Option.match(Option.fromUndefinedOr(sections.get(wrapper.key)), {
        onNone: () => Effect.void,
        onSome: Effect.fnUntraced(function* (value) {
          const next = yield* Hook.recover(Effect.suspend(() => wrapper.wrap.call(wrapper, value)))
          if (next === undefined) sections.delete(wrapper.key)
          else if (next.key !== wrapper.key) {
            sections.delete(wrapper.key)
            yield* (yield* Invocation).report(
              new RegistryError({
                reason: new RegistryFailureError({
                  message: `Section wrapper renamed ${wrapper.key}`,
                }),
              }),
            )
          } else sections.set(wrapper.key, next)
        }),
      })
    }
  }
  let offered: ReadonlyArray<ToolRegistration.Registration> = [...tools.values()]
  if (Array.isArray(state.tools))
    offered = Arr.dedupe(state.tools).flatMap((name) => {
      return Option.toArray(Option.fromUndefinedOr(tools.get(name)))
    })
  else if (state.tools !== undefined) {
    const remove = new Set((state.tools as { readonly remove: ReadonlyArray<string> }).remove)
    offered = Arr.filter(offered, (tool) => !remove.has(tool.tool.name))
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
// effect-nit-allow P1-stdlib-collection-replacements: this public/native array may contain missing indices or inherited numeric accessors; native filter preserves HasProperty/Get and callback order, skips holes, and keeps explicit undefined distinct. Effect Array.filter visits missing slots.
const handlersImpl = (self: Resolved, operation: Hook.Operation): Array<Hook.Handlers> =>
  self.hooks.filter((hook) => hook.operation === operation).map((hook) => hook.handlers)
/**
 * Returns registered handlers for the requested lifecycle operation.
 *
 * @category combinators
 */
export const handlers: {
  (operation: Hook.Operation): (self: Resolved) => Array<Hook.Handlers>
  (self: Resolved, operation: Hook.Operation): Array<Hook.Handlers>
} = dual(2, handlersImpl)
const renderImpl = Effect.fnUntraced(function* (
  agent: Resolved,
  view: import('./Transcript.ts').View,
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
 */
export const render: {
  (
    view: import('./Transcript.ts').View,
    shown: ReadonlyMap<string, string>,
  ): (self: Resolved) => Effect.Effect<Map<string, string>, never, Invocation>
  (
    self: Resolved,
    view: import('./Transcript.ts').View,
    shown: ReadonlyMap<string, string>,
  ): Effect.Effect<Map<string, string>, never, Invocation>
} = dual(3, renderImpl)
