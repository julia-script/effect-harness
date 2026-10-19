/** Caller admission against a single immutable, leased registry generation. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as NativeTool from 'effect/ai/Tool'
import * as Catalog from './ToolCatalog.js'
import { ExecutionError } from './ExecutionError.js'
import type * as Agent from './Agent.js'
import type * as RegistryScope from './RegistryScope.js'
import * as Tool from './Tool.js'
import * as Toolkit from './Toolkit.js'
import * as ToolResult from './ToolResult.js'
import type { RegistryError } from './RegistryError.js'
import type { Failure } from './ExecutionError.js'
import { ToolExecution } from './ToolExecution.js'
import type * as ToolControl from './ToolControl.js'

export const ReasonSchema = Schema.Literals([
  'notFound',
  'registrationNotSelected',
  'disabled',
  'incompatibleVersion',
  'toolFiltered',
  'modelFiltered',
  'callerDenied',
])
export class Denial extends Schema.TaggedError<Denial>()('ToolAdmissionDenied', {
  name: Schema.NonEmptyString,
  caller: Tool.CallerSchema,
  reason: ReasonSchema,
  registration: Schema.optionalKey(Schema.NonEmptyString),
  message: Schema.String,
}) {}
export const InspectionSchema = Schema.Union([
  Schema.TaggedStruct('allowed', {
    name: Schema.NonEmptyString,
    caller: Tool.CallerSchema,
    registration: Schema.NonEmptyString,
    version: Schema.Int,
  }),
  Schema.TaggedStruct('denied', { denial: Denial }),
])
export type Inspection = typeof InspectionSchema.Type

export interface Options {
  readonly agent?: Agent.State
  /** Permission checks ignore presentation only when explicitly requested by runtime control paths. */
  readonly selection?: 'offered' | 'permitted'
  /** The request's admitted generation fences discovery and control application after replacement. */
  readonly preparedTools?: ReadonlyArray<typeof import('./PreparedRequest.js').ToolSchema.Type>
  /** Supplied default registrations; omitted means all installed registrations. */
  readonly inheritedRegistrations?: ReadonlyArray<string>
}

const pinOptions = (options: Options): Options => {
  const agent = options.agent
  const selection = agent?.toolkits
  return {
    ...options,
    ...(options.inheritedRegistrations === undefined
      ? {}
      : { inheritedRegistrations: [...options.inheritedRegistrations] }),
    ...(agent === undefined
      ? {}
      : {
          agent: {
            ...agent,
            ...(agent.extensions === undefined ? {} : { extensions: [...agent.extensions] }),
            ...(agent.tools === undefined ? {} : { tools: [...agent.tools] }),
            ...(agent.modelTools === undefined
              ? {}
              : {
                  modelTools: {
                    ...agent.modelTools,
                    names: [...agent.modelTools.names],
                    ...(agent.modelTools.bindings === undefined
                      ? {}
                      : {
                          bindings: Object.fromEntries(
                            Object.entries(agent.modelTools.bindings).map(([name, binding]) => [
                              name,
                              { ...binding },
                            ]),
                          ),
                        }),
                  },
                }),
            ...(selection === undefined
              ? {}
              : {
                  toolkits: {
                    inherit: selection.inherit,
                    registrations: [...selection.registrations],
                    ...(selection.disabled === undefined
                      ? {}
                      : { disabled: [...selection.disabled] }),
                    ...(selection.bindings === undefined
                      ? {}
                      : {
                          bindings: Object.fromEntries(
                            Object.entries(selection.bindings).map(([name, binding]) => [
                              name,
                              { ...binding },
                            ]),
                          ),
                        }),
                  },
                }),
          },
        }),
  }
}

const registrations = (
  snapshot: RegistryScope.Snapshot,
  options: Options,
): ReadonlyArray<string> => {
  const inherited =
    options.inheritedRegistrations ?? snapshot.registrations.map((owner) => owner.id)
  const selection = options.agent?.toolkits
  return selection === undefined
    ? inherited
    : [...new Set([...(selection.inherit ? inherited : []), ...selection.registrations])]
}

/** Choose ownership before considering enabled flags, caller permissions or name filters. */
const canonical = (snapshot: RegistryScope.Snapshot, options: Options) => {
  const result = new Map<string, RegistryScope.BoundTool>()
  for (const id of registrations(snapshot, options))
    for (const item of snapshot.bindings)
      if (item.owner.id === id) result.set(item.declaration.name, item)
  return result
}

const chosen = (snapshot: RegistryScope.Snapshot, name: string, options: Options) => {
  const bindings = options.agent?.toolkits?.bindings
  const explicit =
    bindings !== undefined && Object.hasOwn(bindings, name) ? bindings[name] : undefined
  if (explicit !== undefined)
    return snapshot.bindings.find(
      (item) => item.owner.id === explicit.binding && item.declaration.name === name,
    )
  return canonical(snapshot, options).get(name)
}

export const inspect = (
  snapshot: RegistryScope.Snapshot,
  name: string,
  caller: Tool.Caller,
  options: Options = {},
): Inspection => {
  const item = chosen(snapshot, name, options)
  const deny = (reason: Denial['reason'], message: string): Inspection => ({
    _tag: 'denied',
    denial: new Denial({
      name,
      caller,
      reason,
      message,
      ...(item === undefined ? {} : { registration: item.owner.id }),
    }),
  })
  if (item === undefined) return deny('notFound', `Tool ${name} has no selected binding`)
  if (!registrations(snapshot, options).includes(item.owner.id))
    return deny('registrationNotSelected', `Registration ${item.owner.id} is not selected`)
  const extensions = snapshot.extensions.filter((extension) => extension.owner.id === item.owner.id)
  if (
    options.agent?.extensions !== undefined &&
    extensions.length > 0 &&
    !extensions.some((extension) => options.agent!.extensions!.includes(extension.declaration.name))
  )
    return deny(
      'registrationNotSelected',
      `Registration ${item.owner.id} is outside the agent extension selection`,
    )
  if (!item.owner.enabled || options.agent?.toolkits?.disabled?.includes(item.owner.id))
    return deny('disabled', `Registration ${item.owner.id} is disabled`)
  const bindings = options.agent?.toolkits?.bindings
  const version =
    bindings !== undefined && Object.hasOwn(bindings, name) ? bindings[name]?.version : undefined
  if (version !== undefined && version !== item.owner.version)
    return deny('incompatibleVersion', `Contract version for ${name} changed`)
  if (options.agent?.tools !== undefined && !options.agent.tools.includes(name))
    return deny('toolFiltered', `Tool ${name} is outside the agent tool inventory`)
  const loaded = caller === 'model' ? options.agent?.modelTools?.bindings : undefined
  const pin = loaded !== undefined && Object.hasOwn(loaded, name) ? loaded[name] : undefined
  if (pin !== undefined && (pin.binding !== item.owner.id || pin.version !== item.owner.version))
    return deny('incompatibleVersion', `Activated binding for ${name} changed`)
  if (options.preparedTools !== undefined) {
    const prepared = options.preparedTools.find((tool) => tool.declaration.name === name)
    if (
      prepared === undefined ||
      prepared.binding.id !== item.owner.id ||
      prepared.binding.version !== item.owner.version ||
      prepared.deferred !== Tool.isDeferred(item.declaration)
    )
      return deny('incompatibleVersion', `Request binding for ${name} changed`)
  }
  if (caller === 'model' && options.selection !== 'permitted') {
    const selection = options.agent?.modelTools
    const selected = selection?.names.includes(name) === true
    if (!selected && (selection?.mode === 'explicit' || Tool.isDeferred(item.declaration)))
      return deny('modelFiltered', `Tool ${name} is outside the model offer inventory`)
  }
  if (!Tool.callers(item.declaration).includes(caller))
    return deny('callerDenied', `Tool ${name} does not allow ${caller} callers`)
  return { _tag: 'allowed', name, caller, registration: item.owner.id, version: item.owner.version }
}

/** Ordered visible inventory. tools filters both callers; modelTools filters only model offers. */
export const inventory = (
  snapshot: RegistryScope.Snapshot,
  caller: Tool.Caller,
  options: Options = {},
): ReadonlyArray<RegistryScope.BoundTool> => {
  const all = canonical(snapshot, options)
  const selection = options.agent?.modelTools
  const base = options.agent?.tools ?? [...all.keys()]
  let names = base
  if (caller === 'model' && options.selection !== 'permitted') {
    if (selection?.mode === 'explicit') names = selection.names
    else
      names = [
        ...base.filter((name) => {
          const item = chosen(snapshot, name, options)
          return item !== undefined && !Tool.isDeferred(item.declaration)
        }),
        ...(selection?.names ?? []),
      ]
  }
  return [...new Set(names)].flatMap((name) => {
    const inspection = inspect(snapshot, name, caller, options)
    const item = chosen(snapshot, name, options)
    return inspection._tag === 'allowed' && item !== undefined ? [item] : []
  })
}

/** Only authorized, deferred, currently unoffered declarations enter the metadata catalog. */
export const catalog = (snapshot: RegistryScope.Snapshot, options: Options = {}) => {
  const offered = new Set(
    inventory(snapshot, 'model', { ...options, selection: 'offered' }).map(
      (item) => item.declaration.name,
    ),
  )
  const candidates = inventory(snapshot, 'model', { ...options, selection: 'permitted' }).filter(
    (item) => Tool.isDeferred(item.declaration) && !offered.has(item.declaration.name),
  )
  return Effect.forEach(candidates, (item) =>
    Effect.try({
      try: (): Catalog.Metadata => ({
        name: item.declaration.name,
        description: NativeTool.getDescription(item.declaration) ?? '',
        parameters: structuredClone(
          NativeTool.getJsonSchema(item.declaration),
        ) as Schema.JsonObject,
      }),
      catch: () =>
        new Catalog.CatalogError({
          name: item.declaration.name,
          message: `Tool ${item.declaration.name} has no searchable parameter schema`,
        }),
    }),
  )
}

/** Admission uses the same lease and exact owner/version that invocation will use. */
export const admit = Effect.fn('ToolInventory.admit')(function* (
  lease: RegistryScope.Lease,
  name: string,
  caller: Tool.Caller,
  options: Options = {},
) {
  const inspection = inspect(lease.snapshot, name, caller, { ...options, selection: 'offered' })
  if (inspection._tag === 'denied') return yield* inspection.denial
  return yield* lease.resolveTool(name, {
    binding: inspection.registration,
    version: inspection.version,
    ...(options.agent?.toolkits?.disabled === undefined
      ? {}
      : { disabled: options.agent.toolkits.disabled }),
  })
})

/** Bound invocation cannot run a denied handler; final channels/control use owner codecs. */
export const invoke = Effect.fn('ToolInventory.invoke')(function* (
  lease: RegistryScope.Lease,
  call: Tool.Call,
  caller: Tool.Caller,
  options: Options = {},
): Effect.fn.Return<
  ToolResult.Result,
  Denial | RegistryError | Failure | Schema.SchemaError | ToolControl.InvalidControl,
  ToolExecution
> {
  const pinned = pinOptions({ ...options, selection: 'offered' })
  const admitted = yield* admit(lease, call.name, caller, pinned)
  return yield* lease.withTool(
    call.name,
    (item) =>
      Effect.gen(function* () {
        // The leased invocation validates outer parameters with the bound capability before delegation.
        const access = yield* ToolExecution
        let active = true
        const deferredTools = Effect.suspend<
          ReadonlyArray<Catalog.Metadata>,
          Catalog.CatalogError | ExecutionError,
          never
        >(() =>
          active
            ? catalog(lease.snapshot, pinned)
            : Effect.fail(
                new ExecutionError({
                  reason: 'revoked',
                  operation: 'tool.catalog',
                  message: 'Tool invocation has ended',
                }),
              ),
        )
        const result = yield* lease
          .invokeTool(call, {
            binding: item.owner.id,
            version: item.owner.version,
          })
          .pipe(
            Effect.provideService(ToolExecution, { ...access, deferredTools }),
            Effect.ensuring(
              Effect.sync(() => {
                active = false
              }),
            ),
          )
        const available = inventory(lease.snapshot, 'model', {
          ...pinned,
          selection: 'permitted',
        }).map((item) => item.declaration.name)
        // Registration already captured every codec service. Erased registry declarations are
        // narrowed only at this schema boundary; validation runs with exactly the owner's context.
        type CapturedCodec = Schema.Codec<unknown, unknown, never, never>
        const captured = item.declaration as unknown as Tool.Tool<
          string,
          CapturedCodec,
          CapturedCodec,
          CapturedCodec
        >
        const final = yield* item.codec === undefined
          ? Toolkit.validateResult(captured, result, available).pipe(
              Effect.setContext(item.context),
            )
          : item.codec.validateResult(result, available)
        if (caller === 'model') return final
        const { control: _control, ...nested } = final
        return nested
      }),
    { binding: admitted.owner.id, version: admitted.owner.version },
  )
})

/** Programs receive typed-data/content fallback and UI channels, never model controls. */
export const invokeNested = Effect.fn('ToolInventory.invokeNested')(function* (
  lease: RegistryScope.Lease,
  call: Tool.Call,
  options: Options = {},
) {
  const pinned = pinOptions({ ...options, selection: 'offered' })
  const item = yield* admit(lease, call.name, 'tools', pinned)
  const result = yield* invoke(lease, call, 'tools', pinned)
  return ToolResult.nested(result, Tool.hasStructuredOutput(item.declaration))
})
