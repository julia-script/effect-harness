/** Caller admission against a single immutable, leased registry generation. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Agent from './Agent.js'
import type * as RegistryScope from './RegistryScope.js'
import * as Tool from './Tool.js'
import * as Toolkit from './Toolkit.js'
import * as ToolResult from './ToolResult.js'
import type { RegistryError } from './RegistryError.js'
import type { Failure } from './ExecutionError.js'
import type { ToolExecution } from './ToolExecution.js'
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
  /** Supplied default registrations; omitted means all installed registrations. */
  readonly inheritedRegistrations?: ReadonlyArray<string>
}

const pinOptions = (options: Options): Options => {
  const agent = options.agent
  const selection = agent?.toolkits
  return {
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
            ...(agent.modelTools === undefined ? {} : { modelTools: [...agent.modelTools] }),
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
  const selected =
    selection === undefined
      ? inherited
      : [...new Set([...(selection.inherit ? inherited : []), ...selection.registrations])]
  const extensions = options.agent?.extensions
  if (extensions === undefined) return selected
  const names = (id: string) =>
    snapshot.extensions.filter((item) => item.owner.id === id).map((item) => item.declaration.name)
  if (selection !== undefined)
    return selected.filter(
      (id) => names(id).length === 0 || names(id).some((name) => extensions.includes(name)),
    )
  // Legacy extension selection retains its supplied order; standalone toolkits remain inherited.
  return [
    ...new Set([
      ...selected.filter((id) => names(id).length === 0),
      ...extensions.flatMap((name) => selected.filter((id) => names(id).includes(name))),
    ]),
  ]
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
  if (!item.owner.enabled || options.agent?.toolkits?.disabled?.includes(item.owner.id))
    return deny('disabled', `Registration ${item.owner.id} is disabled`)
  const bindings = options.agent?.toolkits?.bindings
  const version =
    bindings !== undefined && Object.hasOwn(bindings, name) ? bindings[name]?.version : undefined
  if (version !== undefined && version !== item.owner.version)
    return deny('incompatibleVersion', `Contract version for ${name} changed`)
  if (options.agent?.tools !== undefined && !options.agent.tools.includes(name))
    return deny('toolFiltered', `Tool ${name} is outside the agent tool inventory`)
  if (
    caller === 'model' &&
    options.agent?.modelTools !== undefined &&
    !options.agent.modelTools.includes(name)
  )
    return deny('modelFiltered', `Tool ${name} is outside the model offer inventory`)
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
  const order =
    caller === 'model' ? (options.agent?.modelTools ?? options.agent?.tools) : options.agent?.tools
  const names = order ?? [...all.keys()]
  return [...new Set(names)].flatMap((name) => {
    const inspection = inspect(snapshot, name, caller, options)
    const item = chosen(snapshot, name, options)
    return inspection._tag === 'allowed' && item !== undefined ? [item] : []
  })
}

/** Admission uses the same lease and exact owner/version that invocation will use. */
export const admit = Effect.fn('ToolInventory.admit')(function* (
  lease: RegistryScope.Lease,
  name: string,
  caller: Tool.Caller,
  options: Options = {},
) {
  const inspection = inspect(lease.snapshot, name, caller, options)
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
  const pinned = pinOptions(options)
  const admitted = yield* admit(lease, call.name, caller, pinned)
  return yield* lease.withTool(
    call.name,
    (item) =>
      Effect.gen(function* () {
        // The leased invocation validates outer parameters with the bound capability before delegation.
        const result = yield* lease.invokeTool(call, {
          binding: item.owner.id,
          version: item.owner.version,
        })
        const { modelTools: _modelTools, ...agent } = pinned.agent ?? {}
        const available = inventory(lease.snapshot, 'model', { ...pinned, agent }).map(
          (item) => item.declaration.name,
        )
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
  const pinned = pinOptions(options)
  const item = yield* admit(lease, call.name, 'tools', pinned)
  const result = yield* invoke(lease, call, 'tools', pinned)
  return ToolResult.nested(result, Tool.hasStructuredOutput(item.declaration))
})
