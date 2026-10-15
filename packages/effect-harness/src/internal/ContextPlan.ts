import * as PromptWire from '../PromptWire.js'
/** Read-only declaration planning over a bounded context and one caller-held registry lease. */
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as NativeTool from 'effect/ai/Tool'
import * as Projection from '../ContextProjection.js'
import * as Record from '../Record.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as Inventory from '../ToolInventory.js'
import * as Value from './DocumentValue.js'

export class ContextPlanError extends Schema.TaggedError<ContextPlanError>()('ContextPlanError', {
  message: Schema.String,
}) {}

export const DesiredSchema = Schema.Struct({
  sections: Schema.Array(Projection.SectionSchema),
  tools: Schema.Array(Projection.ToolDeclarationSchema),
})
export type Desired = typeof DesiredSchema.Type

/** These drafts contain declarations and edits only: no handlers, Contexts or captured resources. */
export const DraftSchema = Schema.Struct({
  kind: Schema.Literal('context.change'),
  data: Projection.FactSchema,
  edits: Schema.Array(Record.ContextEdit),
})
export type Draft = typeof DraftSchema.Type
export const PlanSchema = Schema.Struct({
  desired: DesiredSchema,
  drafts: Schema.Array(DraftSchema),
})
export type Plan = typeof PlanSchema.Type

/** RegistryScope.Lease supplies this structural contract. Its scope must outlive preparation. */
export interface Renderer<E, R> {
  readonly snapshot: RegistryScope.Snapshot
  readonly renderSection: (
    key: string,
    prompt: Prompt.Prompt,
    selection?: RegistryScope.Selection,
  ) => Effect.Effect<string | undefined, E, R>
}
export interface RenderOptions extends Inventory.Options {
  /** Omitted means all enabled canonical sections. Selection never reveals a disabled shadow. */
  readonly sectionKeys?: ReadonlyArray<string>
  readonly sectionRegistrations?: ReadonlyArray<string>
  /** Provider-native declaration options, copied without translation or flattening. */
  readonly toolOptions?: Readonly<{ [name: string]: Prompt.ProviderOptions }>
}

const immutable = <A>(value: A): A => {
  if (
    value !== null &&
    typeof value === 'object' &&
    !(value instanceof Uint8Array) &&
    !(value instanceof URL)
  ) {
    for (const child of Object.values(value)) immutable(child)
    Object.freeze(value)
  }
  return value
}
const own = Effect.fnUntraced(function* <S extends Schema.Constraint>(schema: S, input: S['Type']) {
  return immutable(yield* Value.copy(Schema.toCodecJson(schema), input))
})

const unique = (names: ReadonlyArray<string>) => new Set(names).size === names.length
const validateDesired = Effect.fnUntraced(function* (input: Desired) {
  const desired = yield* own(DesiredSchema, input)
  if (
    !unique(desired.sections.map((section) => section.key)) ||
    !unique(desired.tools.map((tool) => tool.name))
  )
    return yield* new ContextPlanError({
      message: 'Desired section keys and tool names must be unique',
    })
  return desired
})

/** Render through the lease's compiled bindings; exact callback E/R remain visible to the caller. */
export const render = Effect.fnUntraced(function* <E, R>(
  input: Projection.ContextView,
  renderer: Renderer<E, R>,
  options: RenderOptions = {},
): Effect.fn.Return<Desired, E | Schema.SchemaError | ContextPlanError, R> {
  const view = yield* own(Projection.ContextViewSchema, input)
  const snapshot = renderer.snapshot
  const tools = Inventory.inventory(snapshot, 'model', options)
  const sectionsByKey = new Map(snapshot.sections.map((item) => [item.declaration.key, item]))
  const keys = [...(options.sectionKeys ?? sectionsByKey.keys())]
  const registrations =
    options.sectionRegistrations === undefined ? undefined : [...options.sectionRegistrations]
  const toolOptions = yield* Value.copy(
    Schema.Record(Schema.String, Prompt.ProviderOptions),
    options.toolOptions ?? {},
  )
  if (!unique(keys))
    return yield* new ContextPlanError({ message: 'Section selection must be unique' })
  const sections: Array<Projection.Section> = []
  for (const key of keys) {
    const item = sectionsByKey.get(key)
    if (item === undefined || !item.owner.enabled) continue
    if (registrations !== undefined && !registrations.includes(item.owner.id)) continue
    const text = yield* renderer.renderSection(key, view.prompt, {
      binding: item.owner.id,
      version: item.owner.version,
    })
    if (text !== undefined) sections.push({ key, text })
  }
  const declarations = yield* Effect.forEach(tools, (item) =>
    Effect.try({
      try: () => ({
        name: item.declaration.name,
        description: NativeTool.getDescription(item.declaration) ?? '',
        parameters: NativeTool.getJsonSchema(item.declaration),
        ...(Object.hasOwn(toolOptions, item.declaration.name)
          ? { options: toolOptions[item.declaration.name]! }
          : {}),
      }),
      catch: () =>
        new ContextPlanError({ message: `Tool ${item.declaration.name} has no JSON schema` }),
    }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Projection.ToolDeclarationSchema))),
  )
  return yield* validateDesired({ sections, tools: declarations })
})

const fact = (
  sections: ReadonlyArray<Projection.SectionChange>,
  tools: ReadonlyArray<Projection.ToolChange>,
  baseline = false,
): Projection.Fact => ({
  _tag: '@effect-harness/ContextFact',
  version: 1,
  ...(baseline ? { baseline: true } : {}),
  sections,
  tools,
})
const draft = (data: Projection.Fact, edits: ReadonlyArray<Record.ContextEdit> = []): Draft => ({
  kind: 'context.change',
  data,
  edits,
})

const sectionChanges = (
  shown: ReadonlyArray<Projection.Section>,
  desired: ReadonlyArray<Projection.Section>,
): ReadonlyArray<ReadonlyArray<Projection.SectionChange>> => {
  const wanted = new Map(desired.map((section) => [section.key, section]))
  const previous = new Map(shown.map((section) => [section.key, section]))
  const order = [
    ...shown.filter((section) => wanted.has(section.key)),
    ...desired.filter((section) => !previous.has(section.key)),
  ]
  if (order.some((section, index) => section.key !== desired[index]?.key))
    return [
      shown.map((section) => ({ _tag: 'remove', key: section.key })),
      desired.map((section) => ({ _tag: 'set', section })),
    ]
  const changes: Array<Projection.SectionChange> = []
  for (const section of shown) {
    const next = wanted.get(section.key)
    if (next === undefined) changes.push({ _tag: 'remove', key: section.key })
    else if (next.text !== section.text) changes.push({ _tag: 'set', section: next })
  }
  for (const section of desired)
    if (!previous.has(section.key)) changes.push({ _tag: 'set', section })
  return changes.length === 0 ? [] : [changes]
}

const toolChanges = (
  shown: ReadonlyArray<Projection.ToolDeclaration>,
  desired: ReadonlyArray<Projection.ToolDeclaration>,
): ReadonlyArray<Projection.ToolChange> => {
  const wanted = new Map(desired.map((tool) => [tool.name, tool]))
  // Effect's native structural equality ignores JSON object key order, retaining array order.
  const kept = shown.filter((tool) => Equal.equals(tool, wanted.get(tool.name)))
  const keptNames = new Set(kept.map((tool) => tool.name))
  const added = desired.filter((tool) => !keptNames.has(tool.name))
  const ordered = [...kept, ...added]
  const reorder = ordered.some((tool, index) => tool.name !== desired[index]?.name)
  const removed = reorder ? shown : shown.filter((tool) => !keptNames.has(tool.name))
  const inserted = reorder ? desired : added
  return [
    ...removed.map((tool): Projection.ToolChange => ({ _tag: 'remove', name: tool.name })),
    ...inserted.map((declaration): Projection.ToolChange => ({ _tag: 'set', declaration })),
  ]
}

/** Plan ordered facts. Commit the returned drafts atomically at the runtime owner's request boundary. */
export const plan = Effect.fnUntraced(function* (
  input: Projection.ContextView,
  inputDesired: Desired,
): Effect.fn.Return<Plan, Schema.SchemaError | ContextPlanError> {
  const view = yield* own(Projection.ContextViewSchema, input)
  const desired = yield* validateDesired(inputDesired)
  const head = view.bounds.head
  const baseline =
    view.positionalFacts.length === 0 ||
    (head !== undefined && !view.positionalFacts.some((item) => item.entryId > head.id))
  if (baseline)
    return yield* own(PlanSchema, {
      desired,
      drafts: [
        draft(
          fact(
            desired.sections.map((section) => ({ _tag: 'set', section })),
            desired.tools.map((declaration) => ({ _tag: 'set', declaration })),
            true,
          ),
          view.positionalFacts.map((item) => ({ _tag: 'omit', target: item.entryId })),
        ),
      ],
    })
  const sections = sectionChanges(view.orderedSections, desired.sections)
  const tools = toolChanges(view.toolDeclarations, desired.tools)
  const drafts = sections.map((changes, index) =>
    draft(fact(changes, index === sections.length - 1 ? tools : [])),
  )
  if (sections.length === 0 && tools.length > 0) drafts.push(draft(fact([], tools)))
  return yield* own(PlanSchema, { desired, drafts })
})

export const PositioningSchema = Schema.Struct({
  sections: Schema.Literals(['positional', 'leading']),
  tools: Schema.Literals(['positional', 'latest']),
})
export type Positioning = typeof PositioningSchema.Type
export const FrameSchema = Schema.Struct({
  entryId: Record.EntryId,
  sections: Schema.Array(Projection.SectionSchema),
  tools: Schema.Array(Projection.ToolDeclarationSchema),
})
export type Frame = typeof FrameSchema.Type
export const ToolPositionSchema = Schema.Struct({
  entryId: Record.EntryId,
  /** Index of the native system marker in the translated prompt, after pairing and baseline positioning. */
  messageIndex: Schema.Natural,
})
export type ToolPosition = typeof ToolPositionSchema.Type
export const TranslationSchema = Schema.Struct({
  positioning: PositioningSchema,
  prompt: PromptWire.Prompt,
  tools: Schema.Array(Projection.ToolDeclarationSchema),
  /** Complete ordered declaration states at each fact, for capable adapters and audit/replay. */
  frames: Schema.Array(FrameSchema),
  /** Empty with latest fallback. Positional consumers correlate these markers with frames by entryId. */
  toolPositions: Schema.Array(ToolPositionSchema),
})
export type Translation = typeof TranslationSchema.Type

/**
 * Translate recorded facts without execution. Positional tools require a provider consumer of frames;
 * latest explicitly falls back to the final native tool list. Leading sections explicitly flatten only
 * declarative sections; ordinary native system messages, media and provider options remain intact.
 */
export const translate = Effect.fnUntraced(function* (
  input: Projection.ContextView,
  inputPositioning: Positioning,
): Effect.fn.Return<Translation, Schema.SchemaError | ContextPlanError> {
  const view = yield* own(Projection.ContextViewSchema, input)
  const positioning = yield* own(PositioningSchema, inputPositioning)
  if (view.activeEntries.length !== view.contributions.length)
    return yield* new ContextPlanError({
      message: 'Context contributions must align with active entries',
    })
  const facts = new Map(view.positionalFacts.map((item) => [item.entryId, item.fact]))
  const sections = new Map<string, Projection.Section>()
  const tools = new Map<string, Projection.ToolDeclaration>()
  const frames: Array<Frame> = []
  const markers: Array<{
    readonly entryId: Record.EntryId
    readonly message: Prompt.SystemMessage
  }> = []
  const messages: Array<Prompt.Message> = []
  for (const [index, entry] of view.activeEntries.entries()) {
    const change = facts.get(entry.id)
    if (change !== undefined) {
      if (change.baseline) {
        sections.clear()
        tools.clear()
      }
      for (const update of change.sections) {
        if (update._tag === 'remove') sections.delete(update.key)
        else sections.set(update.section.key, update.section)
      }
      for (const update of change.tools) {
        if (update._tag === 'remove') tools.delete(update.name)
        else tools.set(update.declaration.name, update.declaration)
      }
      frames.push({
        entryId: entry.id,
        sections: [...sections.values()],
        tools: [...tools.values()],
      })
      const positionedSections =
        positioning.sections === 'positional' && (change.baseline || change.sections.length > 0)
      if (positionedSections || positioning.tools === 'positional') {
        const message = Prompt.systemMessage({
          content: positionedSections
            ? [...sections.values()].map((section) => section.text).join('\n\n')
            : '',
        })
        messages.push(message)
        if (positioning.tools === 'positional') markers.push({ entryId: entry.id, message })
      }
    }
    messages.push(...view.contributions[index]!)
  }
  if (positioning.sections === 'leading' && frames.length > 0)
    messages.unshift(
      Prompt.systemMessage({
        content: [...sections.values()].map((section) => section.text).join('\n\n'),
      }),
    )
  const prompt = Projection.projectMessages(messages)
  return yield* own(TranslationSchema, {
    positioning,
    prompt,
    tools: [...tools.values()],
    frames,
    toolPositions: markers.map(({ entryId, message }) => ({
      entryId,
      messageIndex: prompt.content.indexOf(message),
    })),
  })
})
