/** Immutable, bounded canonical context. This module performs no model or tool execution. */
import * as PromptWire from './PromptWire.js'
import * as ToolResultEntry from './internal/ToolResultEntry.js'
import * as Prompt from 'effect/ai/Prompt'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Record from './Record.js'
import * as Session from './Session.js'
import * as Transaction from './Transaction.js'
import { ContextProjectionError, type Failure } from './ContextProjectionError.js'
import * as Value from './internal/DocumentValue.js'

export { ContextProjectionError } from './ContextProjectionError.js'
export type { Failure } from './ContextProjectionError.js'

/**
 * Decode an entry's original native messages before context edits or tool-pair repair.
 * Tool-result messages are reconstructed from their canonical stored content.
 */
export const messages = (
  entry: Record.Entry,
): Effect.Effect<ReadonlyArray<Prompt.Message>, Schema.SchemaError> =>
  entry.kind === 'tool.result'
    ? ToolResultEntry.messages(entry)
    : Schema.decodeEffect(Schema.Array(Schema.toCodecJson(PromptWire.Message)))(entry.model ?? [])

/** Ordered declarative state; tool handlers and service environments are never serialized. */
export const SectionSchema = Schema.Struct({ key: Schema.NonEmptyString, text: Schema.String })
export type Section = typeof SectionSchema.Type
export const ToolDeclarationSchema = Schema.Struct({
  name: Schema.NonEmptyString,
  description: Schema.String,
  parameters: Schema.JsonObject,
  options: Schema.optionalKey(Prompt.ProviderOptions),
})
export type ToolDeclaration = typeof ToolDeclarationSchema.Type
export const SectionChangeSchema = Schema.Union([
  Schema.TaggedStruct('set', { section: SectionSchema }),
  Schema.TaggedStruct('remove', { key: Schema.NonEmptyString }),
])
export const ToolChangeSchema = Schema.Union([
  Schema.TaggedStruct('set', { declaration: ToolDeclarationSchema }),
  Schema.TaggedStruct('remove', { name: Schema.NonEmptyString }),
])
export type SectionChange = typeof SectionChangeSchema.Type
export type ToolChange = typeof ToolChangeSchema.Type

/** Entry.data payload for the positional planner. A baseline replaces both ordered collections. */
export const FactSchema = Schema.TaggedStruct('@effect-harness/ContextFact', {
  version: Schema.Literal(1),
  baseline: Schema.optionalKey(Schema.Boolean),
  sections: Schema.Array(SectionChangeSchema),
  tools: Schema.Array(ToolChangeSchema),
  assistantStopReason: Schema.optionalKey(Schema.Literals(['aborted', 'error', 'deferred'])),
})
export type Fact = typeof FactSchema.Type

/** Omission is canonical. Compatibility requires an explicit inclusive pre-activation cutoff. */
export const LegacyInterpretationSchema = Schema.Union([
  Schema.TaggedStruct('honorAll', {}),
  Schema.TaggedStruct('preserveManaged', { through: Record.EntryId }),
])
export type LegacyInterpretation = typeof LegacyInterpretationSchema.Type
export const ReadOptionsSchema = Schema.Struct({
  at: Schema.optionalKey(Record.EntryId),
  legacyInterpretation: Schema.optionalKey(LegacyInterpretationSchema),
})
export type ReadOptions = typeof ReadOptionsSchema.Type
export const BoundsSchema = Schema.Struct({
  conversationId: Record.ConversationId,
  tail: Schema.optionalKey(Record.EntryId),
  head: Schema.optionalKey(Record.Entry),
  legacyInterpretation: LegacyInterpretationSchema,
})
export type Bounds = typeof BoundsSchema.Type

/** Contributions align with activeEntries, before tool-result repair and baseline positioning. */
export const ContextViewSchema = Schema.Struct({
  bounds: BoundsSchema,
  activeEntries: Schema.Array(Record.Entry),
  contributions: Schema.Array(Schema.Array(PromptWire.Message)),
  prompt: PromptWire.Prompt,
  positionalFacts: Schema.Array(Schema.Struct({ entryId: Record.EntryId, fact: FactSchema })),
  orderedSections: Schema.Array(SectionSchema),
  toolDeclarations: Schema.Array(ToolDeclarationSchema),
})
export type ContextView = typeof ContextViewSchema.Type

const managedKinds = new Set(['input', 'assistant', 'tool.result', 'agent.configure'])
const excludedAssistant = Schema.is(
  Schema.Struct({
    role: Schema.Literal('assistant'),
    stopReason: Schema.Literals(['aborted', 'error', 'deferred']),
  }),
)
const factTag = Schema.is(Schema.TaggedStruct('@effect-harness/ContextFact', {}))

const isHead = (entry: Record.Entry, legacy: LegacyInterpretation): boolean =>
  entry.head !== undefined &&
  !(
    legacy._tag === 'preserveManaged' &&
    entry.id <= legacy.through &&
    entry.head === entry.id &&
    managedKinds.has(entry.kind)
  )

const invalid = (message: string) => new ContextProjectionError({ reason: 'invalid', message })

/** Freeze only owned values, including nested provider options and JSON tool results. */
const freeze = <A>(value: A): A => {
  if (
    value !== null &&
    typeof value === 'object' &&
    !ArrayBuffer.isView(value) &&
    !Object.isFrozen(value)
  ) {
    for (const child of Object.values(value)) freeze(child)
    Object.freeze(value)
  }
  return value
}

/**
 * Pure service-free projection of ascending visible entries. Input facts are copied, never mutated.
 * Only entries through at participate; head targets must be present. Latest in-range edits win,
 * including edits on superseded heads. Remove then set moves a section/tool to the end.
 * Native assistant messages have no stopReason; a ContextFact carries that durable status.
 */
export const project = Effect.fnUntraced(function* (
  conversationId: Record.ConversationId,
  input: ReadonlyArray<Record.Entry>,
  inputOptions: ReadOptions = {},
): Effect.fn.Return<ContextView, Failure> {
  const id = yield* Schema.decodeEffect(Record.ConversationId)(conversationId)
  const options = yield* Value.copy(ReadOptionsSchema, inputOptions)
  const entries = yield* Value.copy(Schema.Array(Record.Entry), input)
  for (let index = 1; index < entries.length; index++) {
    if (entries[index - 1]!.id >= entries[index]!.id)
      return yield* invalid('Visible entries must have unique ascending identities')
  }
  if (options.at !== undefined && !entries.some((entry) => entry.id === options.at))
    return yield* new ContextProjectionError({
      reason: 'notFound',
      message: 'Cutoff is not visible',
    })
  const bounded = entries.filter((entry) => options.at === undefined || entry.id <= options.at)
  const legacy = options.legacyInterpretation ?? { _tag: 'honorAll' as const }
  const head = bounded.findLast((entry) => isHead(entry, legacy))
  if (head !== undefined && !bounded.some((entry) => entry.id === head.head))
    return yield* invalid('Head target is not visible within the selected bounds')
  if (head !== undefined && head.head! > head.id)
    return yield* invalid('Head target must not follow its marker')
  const range = bounded.filter((entry) => head === undefined || entry.id >= head.head!)
  const edits = new Map<Record.EntryId, Record.ContextEdit>()
  for (const entry of range) for (const edit of entry.edits ?? []) edits.set(edit.target, edit)
  const active =
    head === undefined ? range : [head, ...range.filter((entry) => !isHead(entry, legacy))]
  const contributions: Array<ReadonlyArray<Prompt.Message>> = []
  const positionalFacts: Array<{ readonly entryId: Record.EntryId; readonly fact: Fact }> = []
  const sections = new Map<string, Section>()
  const tools = new Map<string, ToolDeclaration>()
  for (const entry of active) {
    const edit = edits.get(entry.id)
    // Facts describe the original contribution. An omit/replacement removes that
    // contribution; a replacement carries native messages, not the old declaration.
    const fact =
      edit === undefined && factTag(entry.data)
        ? yield* Schema.decodeUnknownEffect(FactSchema)(entry.data)
        : undefined
    if (fact !== undefined) {
      positionalFacts.push({ entryId: entry.id, fact })
      if (fact.baseline) {
        sections.clear()
        tools.clear()
      }
      for (const change of fact.sections) {
        if (change._tag === 'remove') sections.delete(change.key)
        else sections.set(change.section.key, change.section)
      }
      for (const change of fact.tools) {
        if (change._tag === 'remove') tools.delete(change.name)
        else tools.set(change.declaration.name, change.declaration)
      }
    }
    // Inspect durable stop markers before the native message codec discards extra fields.
    const original = entry.kind === 'tool.result' ? yield* messages(entry) : (entry.model ?? [])
    const raw = edit?._tag === 'replace' ? edit.messages : original
    const eligible = raw.filter(
      (message) =>
        !excludedAssistant(message) && !(fact?.assistantStopReason && isAssistant(message)),
    )
    contributions.push(
      edit?._tag === 'omit'
        ? []
        : yield* Schema.decodeUnknownEffect(Schema.Array(Schema.toCodecJson(PromptWire.Message)))(
            eligible,
          ),
    )
  }
  const tail = bounded.at(-1)?.id
  return freeze({
    bounds: {
      conversationId: id,
      legacyInterpretation: legacy,
      ...(tail === undefined ? {} : { tail }),
      ...(head === undefined ? {} : { head }),
    },
    activeEntries: active,
    contributions,
    prompt: projectMessages(contributions.flat()),
    positionalFacts,
    orderedSections: [...sections.values()],
    toolDeclarations: [...tools.values()],
  })
})

/** Repair native tool pairing and position a user-preceded baseline without adding Prompt fields. */
export const projectMessages = (messages: ReadonlyArray<Prompt.Message>): Prompt.Prompt => {
  const ordered = orderToolResults(messages)
  const baselineIndex = ordered.findIndex((message) => message.role !== 'user')
  if (baselineIndex > 0 && ordered[baselineIndex]?.role === 'system') {
    const baseline = ordered.splice(baselineIndex, 1)[0]!
    ordered.unshift(baseline)
  }
  return Prompt.fromMessages(ordered)
}

const isAssistant = Schema.is(Schema.Struct({ role: Schema.Literal('assistant') }))

/**
 * Capture one immutable context under Session coordination. No entries/documents are written.
 * Unknown conversations and cutoffs fail. Fork-inherited facts are supplied by existing scans.
 * Calling inside Session.commit is rejected; request integration must use the Transaction there.
 */
export const read = Effect.fnUntraced(function* (
  session: Session.Session,
  conversationId: Record.ConversationId,
  options: ReadOptions = {},
): Effect.fn.Return<ContextView, Failure> {
  const capturedOptions = yield* Value.copy(ReadOptionsSchema, options)
  return yield* Session.commit(session, (tx) =>
    Effect.gen(function* () {
      const conversation = yield* Transaction.conversation(tx, conversationId)
      if (Option.isNone(conversation))
        return yield* new ContextProjectionError({
          reason: 'notFound',
          message: 'Conversation is absent',
        })
      const entries = yield* Transaction.scanEntries(tx, {
        conversationId,
        order: 'ascending',
        ...(capturedOptions.at === undefined ? {} : { maxEntryId: capturedOptions.at }),
      }).pipe(Stream.runCollect)
      return yield* project(conversationId, entries, capturedOptions)
    }),
  )
})

const missingResult = (call: Prompt.ToolCallPart): Prompt.ToolMessage =>
  Prompt.toolMessage({
    content: [
      Prompt.toolResultPart({
        id: call.id,
        name: call.name,
        providerExecuted: call.providerExecuted,
        isFailure: true,
        result: {
          reason: 'missing_result',
          message: 'Tool result unavailable: history ends before this call completed.',
        },
      }),
    ],
  })

/** Results are paired only until the next assistant, in declared call order. Orphans are dropped. */
const orderToolResults = (messages: ReadonlyArray<Prompt.Message>): Array<Prompt.Message> => {
  const ordered: Array<Prompt.Message> = []
  for (let index = 0; index < messages.length; index++) {
    const message = messages[index]!
    if (message.role === 'tool') {
      const content = message.content.filter((part) => part.type !== 'tool-result')
      if (content.length > 0)
        ordered.push(Prompt.toolMessage({ content, options: message.options }))
      continue
    }
    ordered.push(message)
    if (message.role !== 'assistant') continue
    const calls = message.content.filter((part) => part.type === 'tool-call')
    const results = new Map<
      string,
      Map<string, { readonly part: Prompt.ToolResultPart; readonly message: Prompt.ToolMessage }>
    >()
    for (
      let next = index + 1;
      next < messages.length && messages[next]!.role !== 'assistant';
      next++
    ) {
      const candidate = messages[next]!
      if (candidate.role !== 'tool') continue
      for (const part of candidate.content) {
        if (part.type !== 'tool-result') continue
        let byName = results.get(part.id)
        if (byName === undefined) {
          byName = new Map()
          results.set(part.id, byName)
        }
        if (!byName.has(part.name)) byName.set(part.name, { part, message: candidate })
      }
    }
    for (const call of calls) {
      // Native provider-executed results belong inside the assistant message.
      // Keep them there and do not invent an additional client-side result.
      if (
        message.content.some(
          (part) => part.type === 'tool-result' && part.id === call.id && part.name === call.name,
        )
      )
        continue
      const found = results.get(call.id)?.get(call.name)
      ordered.push(
        found === undefined
          ? missingResult(call)
          : Prompt.toolMessage({ content: [found.part], options: found.message.options }),
      )
    }
  }
  return ordered
}
