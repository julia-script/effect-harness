/**
 * Committed transcript projection, edit codecs and token estimates.
 */
import * as Number from 'effect/Number'
import * as Function from 'effect/Function'
import * as Predicate from 'effect/Predicate'
import { dual, constTrue, constFalse } from 'effect/Function'
import * as Arr from 'effect/Array'
// Context projection adapted from pi-durable (MIT), pinned 636703a0.
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import type * as AiError from 'effect/ai/AiError'
import * as Usage from './Usage.ts'
import * as ToolResult from './ToolResult.ts'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Serialization from './Serialization.ts'
import { EntryId } from './Identity.ts'
import type { SystemPatch } from './SystemPatch.ts'

const OmitPayload = Schema.Struct({ target: EntryId })
const ReplacePayload = Schema.Struct({
  ...OmitPayload.fields,
  messages: Schema.Array(Prompt.Message),
})
/**
 * Tagged omission and replacement of transcript contributions.
 *
 * @category schemas
 */
export const Edit = Schema.Union([
  Schema.TaggedStruct('omit', OmitPayload.fields),
  Schema.TaggedStruct('replace', ReplacePayload.fields),
])
/**
 * Omission or replacement of an entry contribution in model context.
 *
 * @category models
 */
export type Edit = typeof Edit.Type
/**
 * Transcript inputs are immutable committed values supplied by the durable package.
 *
 * @category models
 */
export interface Entry {
  readonly id: EntryId
  readonly head?: EntryId | undefined
  readonly kind?: string | undefined
  readonly messages?: ReadonlyArray<Prompt.Message> | undefined
  readonly edits?: ReadonlyArray<Edit> | undefined
  readonly status?: 'stop' | 'length' | 'tool-calls' | 'aborted' | 'error' | 'deferred' | undefined
  /** Saved request failure; invalid output contributes generic corrective feedback. */
  readonly error?: AiError.AiError | undefined
  readonly usage?: Usage.Usage | undefined
  readonly system?: SystemPatch | undefined
}
/**
 * Active head, visible entries and ordered model-message contributions.
 *
 * @category models
 */
export interface View {
  readonly head: Entry | undefined
  readonly entries: ReadonlyArray<Entry>
  readonly contributions: ReadonlyArray<ReadonlyArray<Prompt.Message>>
  readonly messages: ReadonlyArray<Prompt.Message>
  /** Effective managed deltas after context edits, in entry order. */
  readonly systems?: ReadonlyArray<SystemPatch | undefined> | undefined
}
/**
 * Creates an empty context state.
 *
 * @category constructors
 */
export const make = (): View => ({ head: undefined, entries: [], contributions: [], messages: [] })
/** Newest head marker precedes non-head range; every range entry's edits count, including removed old markers. */
function deriveImpl(self: ReadonlyArray<Entry>, at?: EntryId): View {
  // effect-nit-allow P1-stdlib-collection-replacements: this public/native array may contain missing indices or inherited numeric accessors; native filter preserves HasProperty/Get and callback order, skips holes, and keeps explicit undefined distinct. Effect Array.filter visits missing slots.

  const upto = self.filter((entry) => at === undefined || entry.id <= at)
  const head = Arr.findLast(upto, (entry) => entry.head !== undefined)
  const range = Arr.filter(upto, (entry) =>
    Option.match(head, {
      onNone: constTrue,
      onSome: (self) => self.head === undefined || entry.id >= self.head,
    }),
  )
  const edits = new Map<EntryId, Edit>()
  for (const entry of range) for (const edit of entry.edits ?? []) edits.set(edit.target, edit)
  const entries = Option.match(head, {
    onNone: Function.constant(range),
    onSome: (self) => [self, ...Arr.filter(range, (entry) => entry.head === undefined)],
  })
  const contributions = entries.map((entry) => {
    const messages = Option.match(Option.fromUndefinedOr(edits.get(entry.id)), {
      onSome: (edit) => (edit._tag === 'omit' ? [] : edit.messages),
      onNone: () => {
        const messages =
          entry.system === undefined
            ? (entry.messages ?? [])
            : [
                ...systemMessages(entry.system),
                ...(entry.messages ?? []).filter((message) => message.role !== 'system'),
              ]
        if (entry.status !== 'error' || entry.error?.reason._tag !== 'InvalidOutputError')
          return messages
        return [
          ...messages,
          Prompt.userMessage({
            content: [
              Prompt.textPart({
                text: 'Your previous response could not be validated. The rejected output is unavailable. Try again using only the tools offered in this request and arguments matching their schemas.',
              }),
            ],
          }),
        ]
      },
    })
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
    messages: orderToolResults(Arr.flatten(contributions)),
    systems: entries.map((entry) => (edits.has(entry.id) ? undefined : entry.system)),
  }
}
/**
 * Projects visible committed entries and applies the newest context edits.
 *
 * @category combinators
 */
export const derive: {
  (at?: EntryId): (self: ReadonlyArray<Entry>) => View
  (self: ReadonlyArray<Entry>, at?: EntryId): View
} = dual((args) => Array.isArray(args[0]), deriveImpl)
/**
 * Returns managed section and tool deltas surviving the newest context edits.
 *
 * @category combinators
 */
export function systemPatches(self: View): Array<SystemPatch> {
  // effect-nit-allow P1-stdlib-collection-replacements: this public/native array may contain missing indices or inherited numeric accessors; native filter preserves HasProperty/Get and callback order, skips holes, and keeps explicit undefined distinct. Effect Array.filter visits missing slots.

  return (self.systems ?? self.entries.map((entry) => entry.system)).filter(
    Predicate.isNotUndefined,
  )
}
/**
 * Returns native managed messages replaced by effective named sections.
 *
 * **Details**
 *
 * Edited replacements remain normal native messages.
 *
 * @category combinators
 */
export function managedMessages(self: View): Array<Prompt.Message> {
  return Arr.flatten(
    Arr.filterMap(self.entries, (entry, index) =>
      Option.isNone(
        self.systems === undefined
          ? Option.fromUndefinedOr(entry.system)
          : Arr.get(self.systems, index).pipe(Option.flatMap(Option.fromUndefinedOr)),
      )
        ? Result.failVoid
        : Result.succeed(Option.getOrElse(Arr.get(self.contributions, index), () => [])),
    ),
  )
}
/**
 * Moves results into call order; first matching result before the next assistant wins; orphan results disappear.
 *
 * @category combinators
 */
export function orderToolResults(self: ReadonlyArray<Prompt.Message>): Array<Prompt.Message> {
  // effect-nit-allow P1-stdlib-collection-replacements: this public/native array may contain missing indices or inherited numeric accessors; native filter preserves HasProperty/Get and callback order, skips holes, and keeps explicit undefined distinct. Effect Array.filter visits missing slots.

  const ordered: Array<Prompt.Message> = []
  for (const [index, message] of self.entries()) {
    if (message.role === 'tool') continue
    ordered.push(message)
    if (message.role !== 'assistant') continue
    const calls = message.content.filter((part) => part.type === 'tool-call')
    const found = new Map<string, Prompt.ToolResultPart>()
    for (const next of self.slice(index + 1)) {
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
/**
 * Estimates the token cost of a native model message.
 *
 * **Details**
 *
 * Provider tokenizers can replace this estimate through Model.Descriptor.
 *
 * Images use the pinned Pi fixed-size heuristic, independent of encoded payload bytes.
 *
 * @category combinators
 */
export function estimateMessage(self: Prompt.Message): number {
  if (self.role === 'system') return Math.ceil(self.content.length / 3.5)
  let size = 0
  for (const part of self.content) {
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
        size += Option.match(envelope, {
          onNone: () =>
            typeof part.result === 'string'
              ? part.result.length
              : Serialization.display(part.result).length,
          onSome: (value) =>
            // Schema decoding above owns a dense Envelope content array.
            Number.sumAll(
              Arr.map(value.content, (block) => (block.type === 'text' ? block.text.length : 4800)),
            ),
        })
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
/**
 * Converts visible system sections and declared tools to native system messages.
 *
 * @category combinators
 */
export function systemMessages(self: SystemPatch): Array<Prompt.SystemMessage> {
  // effect-nit-allow P1-stdlib-collection-replacements: this public record may have accessors that delete or change later own keys. Native reflective enumeration rechecks each descriptor before reading; Record.collect snapshots keys and can read a newly inherited value instead.

  const text = Arr.filter(Object.values(self.sections ?? {}), Predicate.isNotNull).join('\n\n')
  const content = [
    text,
    ...(self.toolsAdded?.length ? [Serialization.display(self.toolsAdded)] : []),
    ...(self.toolsRemoved?.length ? [Serialization.display(self.toolsRemoved)] : []),
  ]
  return Arr.filter(content, (value) => value !== '').map((value) =>
    Prompt.systemMessage({ content: value }),
  )
}
/** The newest measured assistant after the marker anchors estimates; entry order, not timestamps, determines newest. */
function estimateImpl(
  self: View,
  extra: ReadonlyArray<Prompt.Message> = [],
  tokenize: (message: Prompt.Message) => number = estimateMessage,
): number {
  let tokens = 0
  let from = 0
  for (let index = self.entries.length - 1; index >= 0; index--) {
    const known = Option.flatMap(Arr.get(self.entries, index), (entry) => {
      if (entry.id <= (self.head?.id ?? -Infinity) || entry.usage === undefined)
        return Option.none<{ readonly tokens: number; readonly from: number }>()
      const measured = Usage.contextTokens(entry.usage)
      if (measured <= 0) return Option.none<{ readonly tokens: number; readonly from: number }>()
      return Arr.get(self.contributions, index).pipe(
        Option.flatMap((messages) =>
          Arr.findLast(messages, (message) => message.role === 'assistant'),
        ),
        Option.map((assistant) => ({
          tokens: measured,
          from:
            Option.getOrElse(
              Arr.findLastIndex(self.messages, (message) => message === assistant),
              () => -1,
            ) + 1,
        })),
      )
    })
    if (
      Option.match(known, {
        onNone: constFalse,
        onSome: (value) => {
          tokens = value.tokens
          from = value.from
          return true
        },
      })
    )
      break
  }
  for (const message of [...self.messages.slice(from), ...extra]) tokens += tokenize(message)
  return tokens
}
/**
 * Estimates context tokens from the newest measured assistant and later messages.
 *
 * @category combinators
 */
export const estimate: {
  (
    extra?: ReadonlyArray<Prompt.Message>,
    tokenize?: (message: Prompt.Message) => number,
  ): (self: View) => number
  (
    self: View,
    extra?: ReadonlyArray<Prompt.Message>,
    tokenize?: (message: Prompt.Message) => number,
  ): number
} = dual(
  Predicate.mapInput(
    Predicate.and(Predicate.isObjectOrArray, Predicate.hasProperty('entries')),
    (args: IArguments) => args[0],
  ),
  estimateImpl,
)
function deltaImpl(
  self: View,
  that: View,
): {
  readonly headChanged: boolean
  readonly removed: ReadonlyArray<EntryId>
  readonly added: ReadonlyArray<Entry>
} {
  // effect-nit-allow P1-stdlib-collection-replacements: this public/native array may contain missing indices or inherited numeric accessors; native filter preserves HasProperty/Get and callback order, skips holes, and keeps explicit undefined distinct. Effect Array.filter visits missing slots.
  const ids = new Set(that.entries.map((entry) => entry.id))
  const before = new Set(self.entries.map((entry) => entry.id))
  return {
    headChanged: self.head?.id !== that.head?.id,
    removed: self.entries.filter((entry) => !ids.has(entry.id)).map((entry) => entry.id),
    added: that.entries.filter((entry) => !before.has(entry.id)),
  }
}
/**
 * Returns incremental changes between the previous and current values.
 *
 * @category combinators
 */
export const delta: {
  (that: View): (self: View) => {
    readonly headChanged: boolean
    readonly removed: ReadonlyArray<EntryId>
    readonly added: ReadonlyArray<Entry>
  }
  (
    self: View,
    that: View,
  ): {
    readonly headChanged: boolean
    readonly removed: ReadonlyArray<EntryId>
    readonly added: ReadonlyArray<Entry>
  }
} = dual(2, deltaImpl)

/**
 * Checks whether a value satisfies the decoded `Edit` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isEdit: (u: unknown) => u is Edit = Schema.is(Edit)
