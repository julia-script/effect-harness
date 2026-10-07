import * as Array from 'effect/Array'
import * as Option from 'effect/Option'
import * as AiPrompt from 'effect/ai/Prompt'
import * as AiResponse from 'effect/ai/Response'
import type { Part } from './Executor.ts'
import * as Schema from 'effect/Schema'
const samePart = Schema.toEquivalence(AiPrompt.AssistantMessagePart)
const sameOptions = Schema.toEquivalence(AiPrompt.ProviderOptions)

export interface State {
  readonly parts: ReadonlyArray<AiResponse.AnyPart>
  readonly completed: ReadonlyMap<string, AiResponse.AnyPart>
  readonly other: ReadonlyArray<AiResponse.AnyPart>
  readonly text: ReadonlyMap<string, string>
  readonly reasoning: ReadonlyMap<string, string>
  readonly toolParams: ReadonlyMap<
    string,
    { readonly name: string; readonly raw: string; readonly providerExecuted: boolean }
  >
  readonly order: ReadonlyArray<{
    readonly type: 'text' | 'reasoning' | 'tool-call'
    readonly id: string
  }>
}
export const empty = (): State => ({
  parts: [],
  completed: new Map(),
  other: [],
  text: new Map(),
  reasoning: new Map(),
  toolParams: new Map(),
  order: [],
})
// Streaming/full-part provider augmentations differ; retain their shared JSON metadata protocol.
const metadata = (value: AiResponse.ProviderMetadata): AiResponse.ProviderMetadata =>
  Object.fromEntries(Object.entries(value))
const key = (type: string, id: string): string => JSON.stringify([type, id])
function completed(
  state: State,
  type: State['order'][number]['type'],
  id: string,
  part: AiResponse.AnyPart,
): State {
  const values = new Map(state.completed).set(key(type, id), part)
  const order = state.order.some((item) => item.type === type && item.id === id)
    ? state.order
    : [...state.order, { type, id }]
  return {
    ...state,
    completed: values,
    order,
    parts: [
      ...order.flatMap((item) => {
        return Option.toArray(Option.fromUndefinedOr(values.get(key(item.type, item.id))))
      }),
      ...state.other,
    ],
  }
}
/** Copy streaming data before progress publication; final parts preserve block start order rather than completion order. */
export function append(state: State, part: Part): State {
  switch (part.type) {
    case 'text-start':
      return {
        ...state,
        text: new Map(state.text).set(part.id, ''),
        order: [...state.order, { type: 'text', id: part.id }],
      }
    case 'reasoning-start':
      return {
        ...state,
        reasoning: new Map(state.reasoning).set(part.id, ''),
        order: [...state.order, { type: 'reasoning', id: part.id }],
      }
    case 'text-delta':
      return {
        ...state,
        text: new Map(state.text).set(
          part.id,
          Option.getOrElse(Option.fromUndefinedOr(state.text.get(part.id)), () => '') + part.delta,
        ),
      }
    case 'reasoning-delta':
      return {
        ...state,
        reasoning: new Map(state.reasoning).set(
          part.id,
          Option.getOrElse(Option.fromUndefinedOr(state.reasoning.get(part.id)), () => '') +
            part.delta,
        ),
      }
    case 'text-end':
      return completed(
        state,
        'text',
        part.id,
        AiResponse.makePart('text', {
          text: Option.getOrElse(Option.fromUndefinedOr(state.text.get(part.id)), () => ''),
          metadata: metadata(part.metadata),
        }),
      )
    case 'reasoning-end':
      return completed(
        state,
        'reasoning',
        part.id,
        AiResponse.makePart('reasoning', {
          text: Option.getOrElse(Option.fromUndefinedOr(state.reasoning.get(part.id)), () => ''),
          metadata: metadata(part.metadata),
        }),
      )
    case 'tool-params-start':
      return {
        ...state,
        toolParams: new Map(state.toolParams).set(part.id, {
          name: part.name,
          raw: '',
          providerExecuted: part.providerExecuted,
        }),
        order: [...state.order, { type: 'tool-call', id: part.id }],
      }
    case 'tool-params-delta': {
      const previous = Option.fromUndefinedOr(state.toolParams.get(part.id))
      return Option.isNone(previous)
        ? state
        : {
            ...state,
            toolParams: new Map(state.toolParams).set(part.id, {
              ...previous.value,
              raw: previous.value.raw + part.delta,
            }),
          }
    }
    case 'tool-params-end':
      return state
    case 'tool-call':
      return completed(state, 'tool-call', part.id, part)
    default:
      return { ...state, other: [...state.other, part], parts: [...state.parts, part] }
  }
}
/** Incomplete tool argument JSON is retained as a native params string with a harness partial annotation. It is UI history, never model context. */
export function partial(state: State): Option.Option<AiPrompt.AssistantMessage> {
  const content: AiPrompt.AssistantMessagePart[] = []
  for (const item of state.order) {
    if (item.type === 'tool-call') {
      const final = Option.fromUndefinedOr(state.completed.get(key(item.type, item.id)))
      const pending = Option.fromUndefinedOr(state.toolParams.get(item.id))
      if (Option.isSome(final) && final.value.type === 'tool-call')
        content.push(
          AiPrompt.toolCallPart({
            id: final.value.id,
            name: final.value.name,
            params: final.value.params,
            providerExecuted: final.value.providerExecuted,
          }),
        )
      else if (Option.isSome(pending))
        content.push(
          AiPrompt.toolCallPart({
            id: item.id,
            name: pending.value.name,
            params: pending.value.raw,
            providerExecuted: pending.value.providerExecuted,
            options: { harness: { partial: true } },
          }),
        )
      continue
    }
    const text =
      item.type === 'text'
        ? Option.getOrElse(Option.fromUndefinedOr(state.text.get(item.id)), () => '')
        : Option.getOrElse(Option.fromUndefinedOr(state.reasoning.get(item.id)), () => '')
    if (text !== '')
      content.push(
        item.type === 'text' ? AiPrompt.textPart({ text }) : AiPrompt.reasoningPart({ text }),
      )
  }
  return content.length === 0 ? Option.none() : Option.some(AiPrompt.assistantMessage({ content }))
}
export const message = (state: State): AiPrompt.Prompt => AiPrompt.fromResponseParts(state.parts)
export type Change = {
  readonly path: ReadonlyArray<string | number>
  readonly type: 'append' | 'set'
  readonly value: unknown
}
/** Native message deltas use append where safe; arbitrary replacements fall back to the whole immutable message. */
export function delta(
  before: AiPrompt.AssistantMessage | undefined,
  after: AiPrompt.AssistantMessage,
): ReadonlyArray<Change> {
  if (
    before === undefined ||
    before.content.length > after.content.length ||
    !sameOptions(before.options, after.options)
  )
    return [{ type: 'set', path: [], value: after }]
  const changes: Change[] = []
  for (const [index, part] of after.content.entries()) {
    const found = Array.get(before.content, index)
    if (Option.isNone(found)) {
      changes.push({ type: 'set', path: ['content', index], value: part })
      continue
    }
    const previous = found.value
    if (samePart(previous, part)) continue
    if (
      previous.type === part.type &&
      (part.type === 'text' || part.type === 'reasoning') &&
      (previous.type === 'text' || previous.type === 'reasoning') &&
      part.text.startsWith(previous.text)
    ) {
      changes.push({
        type: 'append',
        path: ['content', index, 'text'],
        value: part.text.slice(previous.text.length),
      })
      if (!sameOptions(previous.options, part.options))
        changes.push({ type: 'set', path: ['content', index, 'options'], value: part.options })
    } else if (
      part.type === 'tool-call' &&
      previous.type === 'tool-call' &&
      part.id === previous.id &&
      part.name === previous.name &&
      part.providerExecuted === previous.providerExecuted
    ) {
      const append =
        typeof part.params === 'string' &&
        typeof previous.params === 'string' &&
        part.params.startsWith(previous.params)
      changes.push({
        type: append ? 'append' : 'set',
        path: ['content', index, 'params'],
        value: append ? String(part.params).slice(String(previous.params).length) : part.params,
      })
      if (!sameOptions(previous.options, part.options))
        changes.push({ type: 'set', path: ['content', index, 'options'], value: part.options })
    } else return [{ type: 'set', path: [], value: after }]
  }
  return changes
}
