import * as PromptWire from '../PromptWire.js'
/** Durable request ownership and compatibility; no callbacks run while checking recovered work. */
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Schema from 'effect/Schema'
import type * as Prompt from 'effect/ai/Prompt'
import * as NativeTool from 'effect/ai/Tool'
import type * as Model from '../Model.js'
import * as Prepared from '../PreparedRequest.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as Value from './DocumentValue.js'
import { HarnessError } from '../HarnessError.js'
import * as Inventory from '../ToolInventory.js'
import * as Tool from '../Tool.js'
import type * as Agent from '../Agent.js'
import * as Projection from '../ContextProjection.js'
import * as Record from '../Record.js'
import * as Plan from './ContextPlan.js'

/** Preview facts with local identities only; no native writes or format activation occur here. */
export const preview = Effect.fnUntraced(function* (
  view: Projection.ContextView,
  entries: ReadonlyArray<Record.Entry>,
  plan: Plan.Plan,
  positioning: Plan.Positioning,
) {
  const drafts = yield* Effect.forEach(plan.drafts, (draft, index) =>
    Schema.decodeEffect(Record.Entry)({
      ...draft,
      id: (view.bounds.tail ?? 0) + index + 1,
      conversationId: view.bounds.conversationId,
    }),
  )
  const projected = yield* Projection.project(view.bounds.conversationId, [...entries, ...drafts], {
    legacyInterpretation: view.bounds.legacyInterpretation,
  }).pipe(
    Effect.mapError(
      (cause) =>
        new HarnessError({
          reason: 'invalid',
          operation: 'request.context',
          message: 'Cannot preview request context',
          cause,
        }),
    ),
  )
  return {
    identities: drafts.map((entry) => entry.id),
    translation: yield* Plan.translate(projected, positioning),
  }
})

/** Keep accepted native messages intact; ambiguous/missing/reordered markers fail explicitly. */
export const align = Effect.fnUntraced(function* (
  translation: Plan.Translation,
  input: Prompt.Prompt,
) {
  const prompt = yield* Value.copy(Schema.toCodecJson(PromptWire.Prompt), input)
  if (Equal.equals(translation.prompt.content, prompt.content)) return { ...translation, prompt }
  const indices = new Set(translation.toolPositions.map((position) => position.messageIndex))
  if (translation.positioning.sections === 'positional')
    translation.prompt.content.forEach((message, index) => {
      if (message.role === 'system') indices.add(index)
    })
  const mapped = new Map<number, number>()
  let previous = -1
  for (const index of [...indices].sort((left, right) => left - right)) {
    const marker = translation.prompt.content[index]
    const matches = prompt.content.flatMap((message, candidate) =>
      Equal.equals(marker, message) ? [candidate] : [],
    )
    const position = matches[0]
    if (
      marker === undefined ||
      matches.length !== 1 ||
      position === undefined ||
      position <= previous
    )
      return yield* new HarnessError({
        reason: 'invalid',
        operation: 'request.positioning',
        message: 'Final prompt must retain unique positional markers in their original order',
      })
    mapped.set(index, position)
    previous = position
  }
  return {
    ...translation,
    prompt,
    toolPositions: translation.toolPositions.map((position) => ({
      ...position,
      messageIndex: mapped.get(position.messageIndex) ?? position.messageIndex,
    })),
  }
})

/** Request preparation shares the admission inventory, including ownership and permission filters. */
export const inventory = (
  snapshot: RegistryScope.Snapshot,
  caller: Tool.Caller,
  agent: Agent.State,
  selection: Inventory.Options['selection'] = 'offered',
) => Inventory.inventory(snapshot, caller, { agent, selection })

export const unavailable = (message: string) =>
  new HarnessError({
    reason: 'invalid',
    operation: 'request.binding',
    message,
  })
export const declaration = (item: RegistryScope.BoundTool) =>
  Effect.try({
    try: () => ({
      declaration: {
        name: item.declaration.name,
        description: NativeTool.getDescription(item.declaration) ?? '',
        parameters: NativeTool.getJsonSchema(item.declaration),
      },
      binding: { id: item.owner.id, version: item.owner.version },
      callers: Tool.callers(item.declaration),
      replay: Tool.policy(item.declaration).replay,
      deferred: Tool.isDeferred(item.declaration),
      ...(NativeTool.getStrictMode(item.declaration) === undefined
        ? {}
        : { strict: NativeTool.getStrictMode(item.declaration) }),
      success: Schema.toJsonSchemaDocument(Schema.toCodecJson(item.declaration.successSchema)),
      failure: Schema.toJsonSchemaDocument(Schema.toCodecJson(item.declaration.failureSchema)),
    }),
    catch: () => unavailable(`Tool ${item.declaration.name} has no native declaration schema`),
  }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Prepared.ToolSchema)))

/** JSON codec copying owns bytes/options and freezes every nested value of the native prompt. */
export const own = Effect.fnUntraced(function* (input: Prepared.Snapshot) {
  const value = yield* Value.copy(Schema.toCodecJson(Prepared.SnapshotSchema), input)
  const freeze = (item: unknown): void => {
    if (item !== null && typeof item === 'object' && !ArrayBuffer.isView(item)) {
      for (const child of Object.values(item)) freeze(child)
      Object.freeze(item)
    }
  }
  freeze(value)
  return value
})

export const selectedModel = (preparation: Prepared.Snapshot, models: ReadonlyArray<Model.Any>) =>
  preparation.model._tag === 'native'
    ? undefined
    : models.find(
        (model) =>
          preparation.model._tag === 'descriptor' &&
          Equal.equals(model.definition.ref, preparation.model.definition.ref),
      )

/** Stable versions attest callback compatibility; declaration and model metadata are checked as well. */
export const compatible = Effect.fnUntraced(function* (
  preparation: Prepared.Snapshot,
  snapshot: RegistryScope.Snapshot,
  models: ReadonlyArray<Model.Any>,
  native: Prepared.Binding,
  hasNative: boolean,
) {
  if (preparation.model._tag === 'native') {
    if (!hasNative || !Equal.equals(preparation.model.binding, native))
      return yield* unavailable('Pinned native model binding is unavailable or incompatible')
  } else {
    const model = selectedModel(preparation, models)
    if (
      model === undefined ||
      (model.version ?? 1) !== preparation.model.binding.version ||
      !Equal.equals(model.definition, preparation.model.definition)
    )
      return yield* unavailable('Pinned model definition is unavailable or incompatible')
    const configuration = model.configuration
    if (preparation.execution === 'deferred' && model.deferred === undefined)
      return yield* unavailable('Pinned deferred model capability is unavailable')
    const binding = configuration?.binding ?? {
      id: 'effect-harness/empty-configuration',
      version: 1,
    }
    if (!Equal.equals(binding, preparation.configurationBinding))
      return yield* unavailable('Pinned model configuration codec is unavailable or incompatible')
  }
  for (const required of preparation.requiredBindings) {
    const owner = snapshot.registrations.find((item) => item.id === required.id)
    if (owner === undefined || !owner.enabled || owner.version !== required.version)
      return yield* unavailable(
        `Pinned registration ${required.id}@${required.version} is unavailable`,
      )
  }
  for (const saved of preparation.allTools) {
    const item = snapshot.bindings.find(
      (item) =>
        item.owner.id === saved.binding.id &&
        item.owner.version === saved.binding.version &&
        item.declaration.name === saved.declaration.name,
    )
    const observed = item === undefined ? undefined : yield* declaration(item)
    // Either saved or current unsafe replay policy fences repetition of started work.
    if (
      item === undefined ||
      !item.owner.enabled ||
      !Equal.equals(
        saved,
        observed === undefined ? undefined : { ...observed, replay: saved.replay },
      )
    )
      return yield* unavailable(`Pinned tool declaration ${saved.declaration.name} changed`)
  }
})
