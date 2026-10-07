import * as Array from 'effect/Array'
// Context projection adapted from pi-durable (MIT), pinned 636703a0.
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Usage from './Usage.ts'
import * as ToolResult from './ToolResult.ts'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Serialization from './Serialization.ts'
import { EntryId } from './Identity.ts'
import { SystemPatch } from './SystemPatch.ts'
export { ToolDeclaration, SystemPatch } from './SystemPatch.ts'

export const Edit = Schema.Union([
  Schema.Struct({ target: EntryId, action: Schema.Literal('omit') }),
  Schema.Struct({
    target: EntryId,
    action: Schema.Literal('replace'),
    messages: Schema.Array(Prompt.Message),
  }),
])
export type Edit = typeof Edit.Type
/** Transcript inputs are immutable committed values supplied by the durable package. */
export interface Entry {
  readonly id: EntryId
  readonly head?: EntryId | undefined
  readonly kind?: string | undefined
  readonly messages?: ReadonlyArray<Prompt.Message> | undefined
  readonly edits?: ReadonlyArray<Edit> | undefined
  readonly status?: 'stop' | 'length' | 'tool-calls' | 'aborted' | 'error' | 'deferred' | undefined
  readonly usage?: Usage.Usage | undefined
  readonly system?: SystemPatch | undefined
}
export interface View {
  readonly head: Entry | undefined
  readonly entries: ReadonlyArray<Entry>
  readonly contributions: ReadonlyArray<ReadonlyArray<Prompt.Message>>
  readonly messages: ReadonlyArray<Prompt.Message>
  /** Effective managed deltas after context edits, in entry order. */
  readonly systems?: ReadonlyArray<SystemPatch | undefined> | undefined
}
export const empty = (): View => ({ head: undefined, entries: [], contributions: [], messages: [] })
/** Newest head marker precedes non-head range; every range entry's edits count, including removed old markers. */
export function derive(visible: ReadonlyArray<Entry>, at?: EntryId): View {
  const upto = visible.filter((entry) => at === undefined || entry.id <= at)
  const head = Array.findLast(upto, (entry) => entry.head !== undefined)
  const range = upto.filter(
    (entry) => Option.isNone(head) || head.value.head === undefined || entry.id >= head.value.head,
  )
  const edits = new Map<EntryId, Edit>()
  for (const entry of range) for (const edit of entry.edits ?? []) edits.set(edit.target, edit)
  const entries = Option.isNone(head)
    ? range
    : [head.value, ...range.filter((entry) => entry.head === undefined)]
  const contributions = entries.map((entry) => {
    const edit = Option.fromUndefinedOr(edits.get(entry.id))
    if (Option.isSome(edit) && edit.value.action === 'omit') return []
    let messages = entry.messages ?? []
    if (Option.isSome(edit) && edit.value.action === 'replace') messages = edit.value.messages
    else if (entry.system !== undefined)
      messages = [
        ...systemMessages(entry.system),
        ...messages.filter((message) => message.role !== 'system'),
      ]
    return messages.filter(
      (message) =>
        message.role !== 'assistant' ||
        !['aborted', 'error', 'deferred'].includes(entry.status ?? 'stop'),
    )
  })
  return {
    head: Option.getOrUndefined(head),
    entries,
    contributions,
    messages: orderToolResults(contributions.flat()),
    systems: entries.map((entry) => (edits.has(entry.id) ? undefined : entry.system)),
  }
}
/** Managed section/tool deltas that survive the newest context edits. */
export function systemPatches(view: View): ReadonlyArray<SystemPatch> {
  return (view.systems ?? view.entries.map((entry) => entry.system)).filter(
    (patch): patch is SystemPatch => patch !== undefined,
  )
}
/** Native managed messages to remove when projecting the effective named sections. Edited replacements remain normal native messages. */
export function managedMessages(view: View): ReadonlyArray<Prompt.Message> {
  return view.entries.flatMap((entry, index) =>
    Option.isNone(
      view.systems === undefined
        ? Option.fromUndefinedOr(entry.system)
        : Array.get(view.systems, index).pipe(Option.flatMap(Option.fromUndefinedOr)),
    )
      ? []
      : Option.getOrElse(Array.get(view.contributions, index), () => []),
  )
}
/** Move results into call order; first matching result before the next assistant wins; orphan results disappear. */
export function orderToolResults(
  messages: ReadonlyArray<Prompt.Message>,
): ReadonlyArray<Prompt.Message> {
  const ordered: Prompt.Message[] = []
  for (const [index, message] of messages.entries()) {
    if (message.role === 'tool') continue
    ordered.push(message)
    if (message.role !== 'assistant') continue
    const calls = message.content.filter((part) => part.type === 'tool-call')
    const found = new Map<string, Prompt.ToolResultPart>()
    for (const next of messages.slice(index + 1)) {
      if (next.role === 'assistant') break
      if (next.role === 'tool')
        for (const part of next.content)
          if (part.type === 'tool-result' && !found.has(part.id)) found.set(part.id, part)
    }
    for (const call of calls)
      ordered.push(
        Prompt.toolMessage({
          content: [
            Option.getOrElse(Option.fromUndefinedOr(found.get(call.id)), () =>
              Prompt.toolResultPart({
                id: call.id,
                name: call.name,
                isFailure: true,
                providerExecuted: false,
                result: {
                  reason: 'missing_result',
                  message: 'Tool result unavailable: history ends before this call completed.',
                },
              }),
            ),
          ],
        }),
      )
  }
  return ordered
}
/** Provider tokenizers can replace this estimate through Model.Descriptor. Images use the pinned Pi fixed-size heuristic, independent of encoded payload bytes. */
export function estimateMessage(message: Prompt.Message): number {
  if (message.role === 'system') return Math.ceil(message.content.length / 3.5)
  let size = 0
  for (const part of message.content) {
    switch (part.type) {
      case 'text':
      case 'reasoning':
        size += part.text.length
        break
      case 'tool-call':
        size += part.name.length + Serialization.display(part.params).length
        break
      case 'tool-result': {
        const envelope = Result.getOrElse(
          Serialization.attempt(() =>
            Schema.decodeUnknownOption(Schema.toCodecJson(ToolResult.Envelope))(part.result),
          ),
          () => Option.none(),
        )
        if (Option.isSome(envelope))
          for (const block of envelope.value.content)
            size += block.type === 'text' ? block.text.length : 4800
        else
          size +=
            typeof part.result === 'string'
              ? part.result.length
              : Serialization.display(part.result).length
        break
      }
      case 'file':
        size += 4800
        break
      default:
        break
    }
  }
  return Math.ceil(size / 3.5)
}
/** Native message equivalents for a system delta's visible sections and declared tool metadata. */
export function systemMessages(patch: SystemPatch): ReadonlyArray<Prompt.SystemMessage> {
  const text = Object.values(patch.sections ?? {})
    .filter((value): value is string => value !== null)
    .join('\n\n')
  const content = [
    text,
    ...(patch.toolsAdded?.length ? [Serialization.display(patch.toolsAdded)] : []),
    ...(patch.toolsRemoved?.length ? [Serialization.display(patch.toolsRemoved)] : []),
  ]
  return content
    .filter((value) => value !== '')
    .map((value) => Prompt.systemMessage({ content: value }))
}
/** The newest measured assistant after the marker anchors estimates; entry order, not timestamps, determines newest. */
export function estimate(
  view: View,
  extra: ReadonlyArray<Prompt.Message> = [],
  tokenize: (message: Prompt.Message) => number = estimateMessage,
): number {
  let tokens = 0
  let from = 0
  for (let index = view.entries.length - 1; index >= 0; index--) {
    const entry = Array.get(view.entries, index)
    const contribution = Array.get(view.contributions, index)
    if (
      Option.isNone(entry) ||
      entry.value.id <= (view.head?.id ?? -Infinity) ||
      entry.value.usage === undefined ||
      Option.isNone(contribution)
    )
      continue
    const assistant = Array.findLast(contribution.value, (message) => message.role === 'assistant')
    const measured = Usage.contextTokens(entry.value.usage)
    if (Option.isNone(assistant) || measured <= 0) continue
    tokens = measured
    from =
      Option.getOrElse(
        Array.findLastIndex(view.messages, (message) => message === assistant.value),
        () => -1,
      ) + 1
    break
  }
  for (const message of [...view.messages.slice(from), ...extra]) tokens += tokenize(message)
  return tokens
}
export function delta(
  previous: View,
  current: View,
): {
  readonly headChanged: boolean
  readonly removed: ReadonlyArray<EntryId>
  readonly added: ReadonlyArray<Entry>
} {
  const ids = new Set(current.entries.map((entry) => entry.id))
  const before = new Set(previous.entries.map((entry) => entry.id))
  return {
    headChanged: previous.head?.id !== current.head?.id,
    removed: previous.entries.filter((entry) => !ids.has(entry.id)).map((entry) => entry.id),
    added: current.entries.filter((entry) => !before.has(entry.id)),
  }
}

export const isEdit: (input: unknown) => input is Edit = Schema.is(Edit)
