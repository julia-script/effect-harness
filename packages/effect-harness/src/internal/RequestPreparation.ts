/** Durable request ownership and compatibility; no callbacks run while checking recovered work. */
import * as Effect from 'effect/Effect'
import * as Equal from 'effect/Equal'
import * as Schema from 'effect/Schema'
import * as NativeTool from 'effect/ai/Tool'
import type * as Model from '../Model.js'
import * as Prepared from '../PreparedRequest.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as Value from './DocumentValue.js'
import { HarnessError } from '../HarnessError.js'
import * as Inventory from '../ToolInventory.js'
import * as Tool from '../Tool.js'
import type * as Agent from '../Agent.js'

/** Legacy extension filters deny the canonical owner; explicit toolkit selection defines its own order. */
export const inventory = (
  snapshot: RegistryScope.Snapshot,
  caller: Tool.Caller,
  agent: Agent.State,
) => {
  if (agent.toolkits !== undefined) return Inventory.inventory(snapshot, caller, { agent })
  const { extensions, ...unfiltered } = agent
  return Inventory.inventory(snapshot, caller, { agent: unfiltered }).filter((item) => {
    const extension = snapshot.extensions.find((entry) => entry.owner.id === item.owner.id)
    return (
      extension === undefined ||
      extensions === undefined ||
      extensions.includes(extension.declaration.name)
    )
  })
}

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
