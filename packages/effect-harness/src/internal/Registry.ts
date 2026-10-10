/** Registry-owned installation scopes, snapshot publication and invocation resource pins. */
import * as Context from 'effect/Context'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as PubSub from 'effect/PubSub'
import * as Scope from 'effect/Scope'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import type * as Extension from '../Extension.js'
import type * as Hook from '../Hook.js'
import * as HookReducer from '../HookReducer.js'
import * as TaskHook from '../TaskHook.js'
import type * as ToolWrapper from '../ToolWrapper.js'
import type * as PromptSectionWrapper from '../PromptSectionWrapper.js'
import * as Schema from 'effect/Schema'
import * as ToolResult from '../ToolResult.js'
import type * as PromptSection from '../PromptSection.js'
import type * as Registry from '../Registry.js'
import { RegistryError } from '../RegistryError.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as Task from '../Task.js'
import type * as Tool from '../Tool.js'
import type * as Toolkit from '../Toolkit.js'
import * as TaskResolution from '../TaskResolution.js'
import type * as TaskRegistry from './TaskRegistry.js'
import { ToolExecution } from '../ToolExecution.js'

interface Captured<A> {
  readonly declaration: A
  readonly context: Context.Context<never>
}
export interface Contents {
  readonly extensions?: ReadonlyArray<Captured<Extension.Any>>
  readonly tools: ReadonlyArray<
    Captured<Tool.Any> & {
      readonly invoke: Toolkit.WithHandler<Record<string, Tool.Any>>['invoke']
      readonly codec?: RegistryScope.ToolCodec
    }
  >
  readonly tasks: ReadonlyArray<
    Captured<Task.Any> & { readonly executable: TaskRegistry.Executable }
  >
  readonly hooks: ReadonlyArray<Captured<Hook.Any>>
  readonly sections: ReadonlyArray<Captured<PromptSection.Any>>
  readonly toolWrappers?: ReadonlyArray<ToolWrapper.Compiled>
  readonly sectionWrappers?: ReadonlyArray<PromptSectionWrapper.Compiled>
  readonly taskHooks?: ReadonlyArray<TaskHook.Compiled>
}

// Authentication uses object identity, never a public flag or caller-provided property.
const nativeRegistrations = new WeakSet<object>()
/** Private compiler binding used only by the native runtime, outside public package routes. */
export const nativeRunRegistration = (
  declaration: Task.Any,
  executable: TaskRegistry.Executable,
): Registry.Registration => {
  const registration: Registry.Registration = Object.freeze({
    definition: Object.freeze({ id: 'native-runtime-run', version: 1 }),
    acquire: Effect.succeed({
      tools: [],
      hooks: [],
      sections: [],
      tasks: [{ declaration, context: Context.empty(), executable }],
    }),
  })
  nativeRegistrations.add(registration)
  return registration
}

interface Entry {
  readonly owner: RegistryScope.Owner
  readonly nativeRun: boolean
  readonly scope: Scope.Closeable
  readonly contents: Contents
  leases: number
  retired: boolean
}
interface Publication {
  readonly snapshot: RegistryScope.Snapshot
  readonly tools: ReadonlyArray<ToolWrapper.Item>
  readonly sections: ReadonlyArray<PromptSectionWrapper.Item>
  readonly scope: Scope.Closeable
  leases: number
  retired: boolean
}

const error = (reason: RegistryError['reason'], message: string, registration?: string) =>
  new RegistryError({
    reason,
    operation: 'registry',
    message,
    ...(registration === undefined ? {} : { registration }),
  })

const reserved = new Set(['__proto__', 'prototype', 'constructor'])
const validDefinition = (definition: Registry.Definition): Effect.Effect<void, RegistryError> =>
  definition.id.length === 0 ||
  reserved.has(definition.id) ||
  definition.id.startsWith('effect-harness/') ||
  !Number.isSafeInteger(definition.version) ||
  definition.version < 1
    ? Effect.fail(
        error(
          'invalidRegistration',
          'Registration requires an unreserved ID and positive safe contract version',
          definition.id,
        ),
      )
    : Effect.void

const validate = Effect.fnUntraced(function* (contents: Contents, id: string) {
  const tools = new Set<string>()
  for (const { declaration } of contents.tools) {
    if (declaration.name.length === 0 || tools.has(declaration.name))
      return yield* error('duplicateTool', `Duplicate or empty tool name ${declaration.name}`, id)
    tools.add(declaration.name)
  }
  const sections = new Set<string>()
  for (const { declaration } of contents.sections) {
    if (
      !/^[a-z][a-z0-9_-]*$/.test(declaration.key) ||
      declaration.key === 'instructions' ||
      reserved.has(declaration.key) ||
      sections.has(declaration.key)
    )
      return yield* error(
        'invalidSection',
        `Invalid, duplicate or reserved section key ${declaration.key}`,
        id,
      )
    if (typeof declaration.bind !== 'function')
      return yield* error(
        'invalidSection',
        'Leased sections require typed PromptSection.make/bind; legacy static render remains supported by Harness',
        id,
      )
    sections.add(declaration.key)
  }
  for (const { declaration } of contents.hooks)
    if (typeof declaration.bind !== 'function')
      return yield* error('invalidRegistration', 'Leased hooks require typed Hook.make/bind', id)
  for (const { declaration } of contents.tasks)
    yield* Task.validate(declaration).pipe(
      Effect.mapError(
        (cause) =>
          new RegistryError({
            reason: 'invalidTask',
            operation: 'registry.validate',
            message: `Invalid task ${declaration.kind}`,
            registration: id,
            cause,
          }),
      ),
    )
})

const copy = <A extends object>(value: A): A =>
  Object.freeze(
    Object.create(Object.getPrototypeOf(value), Object.getOwnPropertyDescriptors(value)),
  )
const ownedContents = (contents: Contents): Contents => ({
  toolWrappers: Object.freeze((contents.toolWrappers ?? []).map(copy)),
  sectionWrappers: Object.freeze((contents.sectionWrappers ?? []).map(copy)),
  taskHooks: Object.freeze((contents.taskHooks ?? []).map(copy)),
  extensions: Object.freeze(
    (contents.extensions ?? []).map((item) =>
      Object.freeze({
        ...item,
        declaration: Object.freeze({
          ...item.declaration,
          tools: Object.freeze([...item.declaration.tools]),
          hooks: Object.freeze([...item.declaration.hooks]),
          sections: Object.freeze([...item.declaration.sections]),
        }),
      }),
    ),
  ),
  tools: Object.freeze(
    contents.tools.map((item) => Object.freeze({ ...item, declaration: copy(item.declaration) })),
  ),
  tasks: Object.freeze(
    contents.tasks.map((item) =>
      Object.freeze({
        ...item,
        declaration: Object.freeze({
          ...item.declaration,
          phases: Object.freeze({ ...item.declaration.phases }),
        }),
      }),
    ),
  ),
  hooks: Object.freeze(
    contents.hooks.map((item) => Object.freeze({ ...item, declaration: copy(item.declaration) })),
  ),
  sections: Object.freeze(
    contents.sections.map((item) =>
      Object.freeze({ ...item, declaration: copy(item.declaration) }),
    ),
  ),
})

const snapshot = (
  entries: ReadonlyArray<Entry>,
  generation: number,
  transformedTools?: ReadonlyArray<ToolWrapper.Item>,
  transformedSections?: ReadonlyArray<PromptSectionWrapper.Item>,
): RegistryScope.Snapshot => {
  const items = <A>(entry: Entry, list: ReadonlyArray<Captured<A>>) =>
    list.map((item) =>
      Object.freeze({
        declaration: item.declaration,
        context: item.context as unknown as Context.Context<unknown>,
        owner: entry.owner,
      }),
    )
  const bindings =
    transformedTools === undefined
      ? entries.flatMap((entry) => items(entry, entry.contents.tools))
      : transformedTools.map(({ declaration, owner, context, codec }) =>
          Object.freeze({ declaration, owner, context, ...(codec === undefined ? {} : { codec }) }),
        )
  const canonical = new Map(bindings.map((item) => [item.declaration.name, item]))
  return Object.freeze({
    generation,
    registrations: Object.freeze(entries.map((entry) => entry.owner)),
    extensions: Object.freeze(
      entries.flatMap((entry) => items(entry, entry.contents.extensions ?? [])),
    ),
    tools: Object.freeze([...canonical.values()]),
    bindings: Object.freeze(bindings),
    tasks: Object.freeze(entries.flatMap((entry) => items(entry, entry.contents.tasks))),
    hooks: Object.freeze(entries.flatMap((entry) => items(entry, entry.contents.hooks))),
    sections: Object.freeze(
      transformedSections === undefined
        ? entries.flatMap((entry) => items(entry, entry.contents.sections))
        : transformedSections.map(({ declaration, owner, context }) =>
            Object.freeze({ declaration, owner, context }),
          ),
    ),
    taskHooks: Object.freeze(
      entries.flatMap((entry) =>
        (entry.contents.taskHooks ?? []).map(({ kind, version, phase, stage, context }) =>
          Object.freeze({
            declaration: Object.freeze({ kind, version, phase, stage }),
            context: context as Context.Context<unknown>,
            owner: entry.owner,
          }),
        ),
      ),
    ),
  })
}

const selected = <A extends RegistryScope.Item<unknown>>(
  item: A | undefined,
  name: string,
  selection: RegistryScope.Selection,
): Effect.Effect<A, RegistryError> => {
  if (item === undefined)
    return Effect.fail(error('notFound', `No binding for ${name}`, selection.binding))
  if (!item.owner.enabled || selection.disabled?.includes(item.owner.id))
    return Effect.fail(
      error('disabled', `Registration ${item.owner.id} is disabled`, item.owner.id),
    )
  if (selection.version !== undefined && selection.version !== item.owner.version)
    return Effect.fail(
      error('incompatibleVersion', `Contract version for ${name} changed`, item.owner.id),
    )
  return Effect.succeed(item)
}

/** Each transformation generation has its own Scope; failure closes only unpublished resources. */
const materialize = (
  entries: ReadonlyArray<Entry>,
  generation: number,
): Effect.Effect<Publication, RegistryError> =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const scope = yield* Scope.make()
      let complete = false
      return yield* restore(
        Scope.provide(
          Effect.gen(function* () {
            let tools: ReadonlyArray<ToolWrapper.Item> = entries.flatMap((entry) =>
              entry.contents.tools.map((binding) => ({
                declaration: binding.declaration,
                owner: entry.owner,
                context: binding.context as Context.Context<unknown>,
                ...(binding.codec === undefined ? {} : { codec: binding.codec }),
                invoke: (call: import('../Tool.js').Call) =>
                  Effect.suspend(() => binding.invoke(call)).pipe(
                    Effect.updateContext((input: Context.Context<ToolExecution | Scope.Scope>) =>
                      Context.add(
                        Context.add(
                          binding.context,
                          ToolExecution,
                          Context.get(input, ToolExecution),
                        ),
                        Scope.Scope,
                        Context.get(input, Scope.Scope),
                      ),
                    ),
                  ),
              })),
            )
            const canonicalSections = new Map<string, PromptSectionWrapper.Item>()
            for (const entry of entries)
              for (const binding of entry.contents.sections)
                canonicalSections.set(binding.declaration.key, {
                  declaration: binding.declaration,
                  owner: entry.owner,
                  context: binding.context as Context.Context<unknown>,
                  render: binding.declaration.bind!(binding.context),
                })
            let sections = [...canonicalSections.values()]
            for (const entry of entries) {
              if (!entry.owner.enabled) continue
              for (const wrapper of entry.contents.toolWrappers ?? [])
                tools = yield* Effect.forEach(tools, (item) => {
                  if (item.declaration.name !== wrapper.name) return Effect.succeed(item)
                  return Effect.suspend(() => wrapper.apply(item)).pipe(
                    Effect.flatMap((wrapped) => {
                      if (
                        wrapped.declaration.name !== item.declaration.name ||
                        typeof wrapped.invoke !== 'function' ||
                        !Schema.isSchema(wrapped.declaration.parametersSchema) ||
                        !Schema.isSchema(wrapped.declaration.successSchema) ||
                        !Schema.isSchema(wrapped.declaration.failureSchema)
                      )
                        return Effect.fail(
                          error(
                            'invalidRegistration',
                            `Wrapper must preserve tool ${item.declaration.name} and its executable schemas`,
                            entry.owner.id,
                          ),
                        )
                      return Effect.succeed(
                        Object.freeze({
                          ...wrapped,
                          declaration: copy(wrapped.declaration),
                          owner: item.owner,
                          context: wrapped.context,
                          ...(wrapped.codec === undefined ? {} : { codec: wrapped.codec }),
                        }),
                      )
                    }),
                  )
                })
              for (const wrapper of entry.contents.sectionWrappers ?? [])
                sections = yield* Effect.forEach(sections, (item) => {
                  if (item.declaration.key !== wrapper.key) return Effect.succeed(item)
                  return Effect.suspend(() => wrapper.apply(item)).pipe(
                    Effect.flatMap((wrapped) => {
                      if (
                        wrapped.declaration.key !== item.declaration.key ||
                        typeof wrapped.render !== 'function'
                      )
                        return Effect.fail(
                          error(
                            'invalidRegistration',
                            `Wrapper must preserve section ${item.declaration.key}`,
                            entry.owner.id,
                          ),
                        )
                      return Effect.succeed(
                        Object.freeze({
                          ...wrapped,
                          declaration: copy(wrapped.declaration),
                          owner: item.owner,
                          context: item.context,
                        }),
                      )
                    }),
                  )
                })
            }
            for (const entry of entries)
              for (const hook of entry.contents.taskHooks ?? []) {
                const task = entries
                  .flatMap((owner) => owner.contents.tasks)
                  .find(({ declaration }) => declaration.kind === hook.kind)
                if (
                  hook.kind.length === 0 ||
                  !Number.isSafeInteger(hook.version) ||
                  hook.version < 1 ||
                  !Schema.is(TaskHook.StageSchema)(hook.stage) ||
                  typeof hook.execute !== 'function' ||
                  hook.phase.length === 0 ||
                  (task !== undefined &&
                    (task.declaration.version !== hook.version ||
                      !Object.hasOwn(task.declaration.phases, hook.phase)))
                )
                  return yield* error(
                    'invalidTask',
                    'Task hook must match its task contract version and phase',
                    entry.owner.id,
                  )
              }
            const result = {
              snapshot: snapshot(entries, generation, tools, sections),
              tools: Object.freeze(tools),
              sections: Object.freeze(sections),
              scope,
              leases: 0,
              retired: false,
            }
            complete = true
            return result
          }),
          scope,
        ),
      ).pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterrupts(cause))
            return Effect.failCause(cause as Cause.Cause<RegistryError>)
          return Effect.fail(
            new RegistryError({
              reason: 'invalidRegistration',
              operation: 'registry.wrap',
              message: 'Snapshot transformation failed',
              cause,
            }),
          )
        }),
        Effect.onExit((exit) => (complete ? Effect.void : Scope.close(scope, exit))),
      )
    }),
  )

export const make = Effect.gen(function* (): Effect.fn.Return<
  Registry.Service,
  never,
  Scope.Scope
> {
  const mutex = yield* Semaphore.make(1)
  const publications = yield* PubSub.sliding<RegistryScope.Snapshot>(1)
  let entries: ReadonlyArray<Entry> = []
  let publication = yield* materialize(entries, 0).pipe(Effect.orDie)
  let current = publication.snapshot
  let closed = false
  const locked = <A, E, R>(effect: Effect.Effect<A, E, R>) => mutex.withPermit(effect)
  const closeRetired = (entry: Entry) =>
    Effect.suspend(() =>
      entry.retired && entry.leases === 0 ? Scope.close(entry.scope, Exit.void) : Effect.void,
    )
  const closePublication = (value: Publication) =>
    Effect.suspend(() =>
      value.retired && value.leases === 0 ? Scope.close(value.scope, Exit.void) : Effect.void,
    )
  const release = (list: ReadonlyArray<Entry>, value?: Publication) =>
    Effect.uninterruptible(
      Effect.gen(function* () {
        yield* locked(
          Effect.sync(() => {
            for (const entry of list) entry.leases--
            if (value !== undefined) value.leases--
          }),
        )
        if (value !== undefined) yield* closePublication(value)
        yield* Effect.forEach(list, closeRetired, { discard: true })
      }),
    )
  const publish = (next: ReadonlyArray<Entry>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const nextPublication = yield* restore(materialize(next, current.generation + 1))
        const previousPublication = publication
        entries = next
        publication = nextPublication
        current = publication.snapshot
        previousPublication.retired = true
        yield* PubSub.publish(publications, current)
        return { published: current, previousPublication }
      }),
    )

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      const retired = yield* locked(
        Effect.sync(() => {
          closed = true
          const retired = entries
          entries = []
          publication.retired = true
          for (const entry of retired) entry.retired = true
          return retired
        }),
      )
      yield* PubSub.shutdown(publications)
      yield* closePublication(publication)
      yield* Effect.forEach(retired, closeRetired, { discard: true })
    }),
  )

  const install: Registry.Service['install'] = (registration) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const definition = registration.definition
        const owner = Object.freeze({
          id: definition.id,
          version: definition.version,
          enabled: definition.enabled ?? true,
        })
        yield* validDefinition(owner)
        const scope = yield* Scope.make()
        const memoMap = yield* Layer.makeMemoMap
        let committed = false
        return yield* Effect.gen(function* () {
          const contents = ownedContents(
            yield* restore(
              Scope.provide(registration.acquire, scope).pipe(
                Effect.provideService(Layer.CurrentMemoMap, memoMap),
              ),
            ),
          )
          yield* validate(contents, owner.id)
          const candidate: Entry = {
            owner,
            nativeRun: nativeRegistrations.has(registration),
            scope,
            contents,
            leases: 0,
            retired: false,
          }
          const result = yield* locked(
            Effect.gen(function* () {
              if (closed) return yield* error('revoked', 'Registry Scope has closed')
              const previous = entries.find((entry) => entry.owner.id === candidate.owner.id)
              if (previous?.nativeRun && !candidate.nativeRun)
                return yield* error(
                  'invalidRegistration',
                  'Native runtime registration cannot be replaced by a public registration',
                  owner.id,
                )
              const next =
                previous === undefined
                  ? [...entries, candidate]
                  : entries.map((entry) => (entry === previous ? candidate : entry))
              const kinds = new Set<string>()
              for (const entry of next)
                for (const { declaration } of entry.contents.tasks) {
                  if (
                    kinds.has(declaration.kind) ||
                    declaration.kind.startsWith('effect-harness/') ||
                    reserved.has(declaration.kind) ||
                    (declaration.kind === 'harness.run' &&
                      !(entry.nativeRun && declaration.version === 1))
                  )
                    return yield* error(
                      'duplicateTask',
                      `Task kind ${declaration.kind} is already installed or reserved`,
                      entry.owner.id,
                    )
                  kinds.add(declaration.kind)
                }
              const { published, previousPublication } = yield* restore(publish(next))
              committed = true
              if (previous !== undefined) previous.retired = true
              return { published, previous, previousPublication }
            }),
          )
          yield* closePublication(result.previousPublication)
          if (result.previous !== undefined) yield* closeRetired(result.previous)
          return result.published
        }).pipe(Effect.onExit((exit) => (committed ? Effect.void : Scope.close(scope, exit))))
      }),
    )

  const uninstall: Registry.Service['uninstall'] = (id) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const result = yield* locked(
          Effect.gen(function* () {
            if (closed) return yield* error('revoked', 'Registry Scope has closed')
            const previous = entries.find((entry) => entry.owner.id === id)
            if (previous === undefined)
              return { published: current, previous, previousPublication: undefined }
            const { published, previousPublication } = yield* restore(
              publish(entries.filter((entry) => entry !== previous)),
            )
            previous.retired = true
            return { published, previous, previousPublication }
          }),
        )
        if (result.previousPublication !== undefined)
          yield* closePublication(result.previousPublication)
        if (result.previous !== undefined) yield* closeRetired(result.previous)
        return result.published
      }),
    )

  const acquire = Effect.uninterruptible(
    Effect.gen(function* () {
      const pinned = yield* locked(
        Effect.gen(function* () {
          if (closed) return yield* error('revoked', 'Registry Scope has closed')
          for (const entry of entries) entry.leases++
          publication.leases++
          return { entries, snapshot: current, publication }
        }),
      )
      let active = true
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* locked(
            Effect.sync(() => {
              active = false
            }),
          )
          yield* release(pinned.entries, pinned.publication)
        }),
      )
      const check = <A, E>(effect: Effect.Effect<A, E>) =>
        locked(
          Effect.suspend<A, E | RegistryError, never>(() =>
            active ? effect : Effect.fail(error('revoked', 'Invocation lease has closed')),
          ),
        )
      const tool = (name: string, selection: RegistryScope.Selection = {}) =>
        selected(
          (selection.binding === undefined ? pinned.snapshot.tools : pinned.snapshot.bindings).find(
            (item) =>
              item.declaration.name === name &&
              (selection.binding === undefined || item.owner.id === selection.binding),
          ),
          name,
          selection,
        )
      const task = (kind: string, selection: RegistryScope.Selection = {}) =>
        selected(
          pinned.snapshot.tasks.find(
            (item) =>
              item.declaration.kind === kind &&
              (selection.binding === undefined || item.owner.id === selection.binding),
          ),
          kind,
          selection,
        )
      const useTask = <A, E, R>(
        kind: string,
        use: (executable: TaskRegistry.Executable) => Effect.Effect<A, E, R>,
        selection?: RegistryScope.Selection,
      ) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const binding = yield* check(
              Effect.gen(function* () {
                const item = yield* task(kind, selection)
                const entry = pinned.entries.find((entry) => entry.owner === item.owner)!
                const compiled = entry.contents.tasks.find(
                  (candidate) => candidate.declaration.kind === kind,
                )!
                entry.leases++
                return { entry, compiled }
              }),
            )
            return yield* restore(Effect.suspend(() => use(binding.compiled.executable))).pipe(
              Effect.ensuring(release([binding.entry])),
            )
          }),
        )
      const withPublication = <A, E, R>(effect: Effect.Effect<A, E, R | Scope.Scope>) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            yield* check(
              Effect.sync(() => {
                for (const entry of pinned.entries) entry.leases++
                pinned.publication.leases++
              }),
            )
            return yield* restore(Effect.scoped(effect)).pipe(
              Effect.ensuring(release(pinned.entries, pinned.publication)),
            )
          }),
        )
      return {
        snapshot: pinned.snapshot,
        resolveTool: (name, selection) => check(tool(name, selection)),
        withTool: (name, use, selection) =>
          withPublication(
            Effect.flatMap(tool(name, selection), (item) => Effect.suspend(() => use(item))),
          ),
        resolveTask: (kind, selection) => check(task(kind, selection)),
        prepareTask: (kind, input, selection) =>
          useTask(kind, (executable) => executable.initialize(input), selection),
        inspectTask: (record, failure) => {
          const item = pinned.snapshot.tasks.find((item) => item.declaration.kind === record.kind)
          if (item === undefined) return check(TaskResolution.inspectMissing(record))
          if (
            !item.owner.enabled &&
            record.state.status !== 'terminal' &&
            record.state.status !== 'completing'
          )
            return check(
              Effect.succeed({
                _tag: 'blocked' as const,
                error: TaskResolution.blocked(
                  record,
                  'disabledDefinition',
                  'Task registration is disabled',
                ),
              }),
            )
          if (record.state.status === 'terminal' || record.state.status === 'completing')
            return check(TaskResolution.inspectMissing(record))
          return useTask(record.kind, (executable) => executable.inspect(record, failure))
        },
        upgradeTask: (tx, record) =>
          useTask(record.kind, (executable) => executable.upgrade(tx, record.id)),
        invokeTask: (record, mode) =>
          useTask(record.kind, (executable) => executable.invoke(record, mode)),
        invokeTool: (call, selection) =>
          withPublication(
            Effect.gen(function* () {
              const item = yield* tool(call.name, selection)
              const binding = pinned.publication.tools.find(
                (candidate) =>
                  candidate.owner === item.owner && candidate.declaration.name === call.name,
              )!
              if (item.codec !== undefined) yield* item.codec.validateParameters(call.arguments)
              const result = yield* Effect.suspend(() => binding.invoke(call))
              return yield* Schema.decodeEffect(Schema.toType(ToolResult.ResultSchema))(result, {
                onExcessProperty: 'error',
              })
            }),
          ),
        renderSection: (key, prompt, selection = {}) =>
          withPublication(
            Effect.gen(function* () {
              const item = yield* selected(
                pinned.snapshot.sections.find(
                  (candidate) =>
                    candidate.declaration.key === key &&
                    (selection.binding === undefined || candidate.owner.id === selection.binding),
                ),
                key,
                selection,
              )
              const binding = pinned.publication.sections.find(
                (candidate) => candidate.owner === item.owner && candidate.declaration.key === key,
              )!
              return yield* binding.render(prompt)
            }),
          ),
        dispatchHook: (event, options = {}) =>
          withPublication(
            Effect.gen(function* () {
              const callbacks = pinned.entries
                .filter(
                  (entry) =>
                    entry.owner.enabled &&
                    (options.registrations === undefined ||
                      options.registrations.includes(entry.owner.id)),
                )
                .flatMap((entry) =>
                  entry.contents.hooks.map(({ declaration, context }) => ({
                    event: declaration.event,
                    execute: declaration.bind(context),
                  })),
                )
              const input: Hook.Event = event
              let validateArguments: HookReducer.Options<
                typeof options extends HookReducer.Options<infer E, infer _R>
                  ? E | Schema.SchemaError
                  : never,
                typeof options extends HookReducer.Options<infer _E, infer R> ? R : never
              >['validateArguments'] = options.validateArguments
              if (input._tag === 'beforeTool') {
                const item = yield* tool(input.call.name, options.selection)
                const schema = Schema.toCodecJson(
                  item.declaration.parametersSchema as unknown as Schema.ConstraintCodec<
                    unknown,
                    unknown,
                    never,
                    never
                  >,
                )
                validateArguments = (arguments_) =>
                  (item.codec === undefined
                    ? Schema.decodeEffect(schema)(arguments_).pipe(
                        Effect.provideContext(item.context),
                        Effect.asVoid,
                      )
                    : item.codec.validateParameters(arguments_)
                  ).pipe(
                    Effect.andThen(
                      options.validateArguments === undefined
                        ? Effect.void
                        : options.validateArguments(arguments_),
                    ),
                  )
              }
              return yield* HookReducer.dispatch(callbacks, event, {
                ...options,
                ...(validateArguments === undefined ? {} : { validateArguments }),
              })
            }),
          ),
        dispatchTaskHook: (event, options = {}) =>
          withPublication(
            Effect.gen(function* () {
              const checked = yield* Schema.decodeEffect(TaskHook.EventSchema)(event).pipe(
                Effect.mapError(
                  (cause) =>
                    new HookReducer.HookFailure({
                      event: 'task',
                      reason: 'validation',
                      message: 'Invalid task hook event',
                      cause,
                    }),
                ),
              )
              const definition = yield* task(checked.kind)
              for (const entry of pinned.entries) {
                if (
                  !entry.owner.enabled ||
                  (options.registrations !== undefined &&
                    !options.registrations.includes(entry.owner.id))
                )
                  continue
                for (const hook of entry.contents.taskHooks ?? []) {
                  if (
                    hook.kind !== checked.kind ||
                    hook.phase !== checked.phase ||
                    hook.stage !== checked.stage
                  )
                    continue
                  if (hook.version !== definition.declaration.version)
                    return yield* error(
                      'incompatibleVersion',
                      'Task hook contract version changed',
                      entry.owner.id,
                    )
                  yield* HookReducer.protect(
                    'task.' + checked.stage,
                    Effect.suspend(() => hook.execute(checked)),
                  ).pipe(
                    Effect.catchIf(
                      (failure) => failure.reason !== 'critical' && options.report !== undefined,
                      (failure) => options.report!(failure),
                    ),
                  )
                }
              }
            }),
          ),
      } satisfies RegistryScope.Lease
    }),
  )

  const changes = Stream.fromPubSub(publications)
  return {
    snapshot: Effect.sync(() => current),
    changes,
    install,
    uninstall,
    acquire,
    listen: (listener) =>
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(publications)
        yield* Stream.fromSubscription(subscription).pipe(
          Stream.runForEach(listener),
          Effect.forkScoped,
        )
      }),
  }
})
