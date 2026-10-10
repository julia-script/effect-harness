/** Registry-owned installation scopes, snapshot publication and invocation resource pins. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as PubSub from 'effect/PubSub'
import * as Scope from 'effect/Scope'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import type * as Hook from '../Hook.js'
import type * as PromptSection from '../PromptSection.js'
import type * as Registry from '../Registry.js'
import { RegistryError } from '../RegistryError.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as Task from '../Task.js'
import type * as Tool from '../Tool.js'
import type * as Toolkit from '../Toolkit.js'
import { ToolExecution } from '../ToolExecution.js'

interface Captured<A> {
  readonly declaration: A
  readonly context: Context.Context<never>
}
export interface Contents {
  readonly tools: ReadonlyArray<
    Captured<Tool.Any> & {
      readonly invoke: Toolkit.WithHandler<Record<string, Tool.Any>>['invoke']
    }
  >
  readonly tasks: ReadonlyArray<Captured<Task.Any>>
  readonly hooks: ReadonlyArray<Captured<Hook.Any>>
  readonly sections: ReadonlyArray<Captured<PromptSection.Any>>
}

interface Entry {
  readonly owner: RegistryScope.Owner
  readonly scope: Scope.Closeable
  readonly contents: Contents
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
    sections.add(declaration.key)
  }
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

const snapshot = (entries: ReadonlyArray<Entry>, generation: number): RegistryScope.Snapshot => {
  const items = <A>(entry: Entry, list: ReadonlyArray<Captured<A>>) =>
    list.map((item) =>
      Object.freeze({
        declaration: item.declaration,
        context: item.context as unknown as Context.Context<unknown>,
        owner: entry.owner,
      }),
    )
  const bindings = entries.flatMap((entry) => items(entry, entry.contents.tools))
  const canonical = new Map(bindings.map((item) => [item.declaration.name, item]))
  return Object.freeze({
    generation,
    registrations: Object.freeze(entries.map((entry) => entry.owner)),
    tools: Object.freeze([...canonical.values()]),
    bindings: Object.freeze(bindings),
    tasks: Object.freeze(entries.flatMap((entry) => items(entry, entry.contents.tasks))),
    hooks: Object.freeze(entries.flatMap((entry) => items(entry, entry.contents.hooks))),
    sections: Object.freeze(entries.flatMap((entry) => items(entry, entry.contents.sections))),
  })
}

const selected = <A>(
  item: RegistryScope.Item<A> | undefined,
  name: string,
  selection: RegistryScope.Selection,
): Effect.Effect<RegistryScope.Item<A>, RegistryError> => {
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

export const make = Effect.gen(function* (): Effect.fn.Return<
  Registry.Service,
  never,
  Scope.Scope
> {
  const mutex = yield* Semaphore.make(1)
  const publications = yield* PubSub.sliding<RegistryScope.Snapshot>(1)
  let entries: ReadonlyArray<Entry> = []
  let current = snapshot(entries, 0)
  let closed = false
  const locked = <A, E, R>(effect: Effect.Effect<A, E, R>) => mutex.withPermit(effect)
  const closeRetired = (entry: Entry) =>
    Effect.suspend(() =>
      entry.retired && entry.leases === 0 ? Scope.close(entry.scope, Exit.void) : Effect.void,
    )
  const release = (list: ReadonlyArray<Entry>) =>
    Effect.uninterruptible(
      Effect.gen(function* () {
        yield* locked(
          Effect.sync(() => {
            for (const entry of list) entry.leases--
          }),
        )
        yield* Effect.forEach(list, closeRetired, { discard: true })
      }),
    )
  const publish = (next: ReadonlyArray<Entry>) =>
    Effect.gen(function* () {
      entries = next
      current = snapshot(entries, current.generation + 1)
      yield* PubSub.publish(publications, current)
      return current
    })

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      const retired = yield* locked(
        Effect.sync(() => {
          closed = true
          const retired = entries
          entries = []
          for (const entry of retired) entry.retired = true
          return retired
        }),
      )
      yield* PubSub.shutdown(publications)
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
            scope,
            contents,
            leases: 0,
            retired: false,
          }
          const result = yield* locked(
            Effect.gen(function* () {
              if (closed) return yield* error('revoked', 'Registry Scope has closed')
              const previous = entries.find((entry) => entry.owner.id === candidate.owner.id)
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
                    declaration.kind === 'harness.run'
                  )
                    return yield* error(
                      'duplicateTask',
                      `Task kind ${declaration.kind} is already installed or reserved`,
                      entry.owner.id,
                    )
                  kinds.add(declaration.kind)
                }
              const published = yield* publish(next)
              committed = true
              if (previous !== undefined) previous.retired = true
              return { published, previous }
            }),
          )
          if (result.previous !== undefined) yield* closeRetired(result.previous)
          return result.published
        }).pipe(Effect.onExit((exit) => (committed ? Effect.void : Scope.close(scope, exit))))
      }),
    )

  const uninstall: Registry.Service['uninstall'] = (id) =>
    Effect.uninterruptible(
      Effect.gen(function* () {
        const result = yield* locked(
          Effect.gen(function* () {
            if (closed) return yield* error('revoked', 'Registry Scope has closed')
            const previous = entries.find((entry) => entry.owner.id === id)
            if (previous === undefined) return { published: current, previous }
            const published = yield* publish(entries.filter((entry) => entry !== previous))
            previous.retired = true
            return { published, previous }
          }),
        )
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
          return { entries, snapshot: current }
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
          yield* release(pinned.entries)
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
      return {
        snapshot: pinned.snapshot,
        resolveTool: (name, selection) => check(tool(name, selection)),
        resolveTask: (kind, selection) => check(task(kind, selection)),
        invokeTool: (call, selection) =>
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const selected = yield* check(
                Effect.gen(function* () {
                  const item = yield* tool(call.name, selection)
                  const entry = pinned.entries.find((entry) => entry.owner === item.owner)!
                  const binding = entry.contents.tools.find(
                    (binding) => binding.declaration.name === call.name,
                  )!
                  entry.leases++
                  return { entry, binding }
                }),
              )
              // Captured backend and codec services win over ambient caller services. Only
              // invocation access and Scope are fresh; Toolkit's own merge must see this context.
              const invoke = Effect.scoped(
                Effect.suspend(() => selected.binding.invoke(call)).pipe(
                  Effect.updateContext((input: Context.Context<ToolExecution | Scope.Scope>) =>
                    Context.add(
                      Context.add(
                        Context.merge(input, selected.binding.context),
                        ToolExecution,
                        Context.get(input, ToolExecution),
                      ),
                      Scope.Scope,
                      Context.get(input, Scope.Scope),
                    ),
                  ),
                ),
              )
              return yield* restore(invoke).pipe(Effect.ensuring(release([selected.entry])))
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
