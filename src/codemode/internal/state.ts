/**
 * Code mode's documents and what they hold, ported from pi's `codemode.ts`: the run's start data
 * and outcome, its journal, and the conversation's `store()` values.
 */
import { Effect, Schema } from 'effect'
import { cast } from 'effect/Function'
import type { Draft } from '../../delta/Tracker.ts'
import { strictJson } from '../../harness/internal/json.ts'
import * as Document from '../../session/Document.ts'
import { Output } from '../../tool/ToolResult.ts'

/**
 * Version of everything that turns stored results into what a script sees: result mapping,
 * globals, start data. A call started under another version is not replayed.
 */
export const VERSION = 1

/** One tool as the script sees it: `tools.<jsName>`. */
export const DeclaredTool = Schema.Struct({
  name: Schema.String,
  jsName: Schema.String,
  /** Its sample: the description and its declaration. */
  description: Schema.String,
  /** Resolves to its value rather than its text output. */
  structured: Schema.optionalKey(Schema.Literal(true)),
})
export type DeclaredTool = typeof DeclaredTool.Type

/** A namespace as `describeNamespace()` and `searchTools()` see it. */
export const NamespaceInfo = Schema.Struct({
  name: Schema.String,
  description: Schema.optionalKey(Schema.String),
  instructions: Schema.optionalKey(Schema.String),
  tools: Schema.Array(Schema.String),
})
export type NamespaceInfo = typeof NamespaceInfo.Type

/** What a call starts with; a rerun starts the VM from exactly this. */
export const Start = Schema.Struct({
  version: Schema.Int,
  startedAt: Schema.Finite,
  seed: Schema.Int,
  /** The VM's fingerprint, recorded by the first run before any call is admitted. */
  fingerprint: Schema.optionalKey(Schema.String),
  /** `startedAt + timeout_ms` when the script set one. */
  deadline: Schema.optionalKey(Schema.Finite),
  tools: Schema.Array(DeclaredTool),
  namespaces: Schema.Array(NamespaceInfo),
  globals: Schema.Array(Schema.Struct({ name: Schema.String, spread: Schema.Boolean })),
  maxOutputTokens: Schema.Int,
  store: Schema.Record(Schema.String, Schema.Json),
})
export type Start = typeof Start.Type

/** The call's outcome, as the handler returns it. */
export const Stored = Schema.Struct({ isError: Schema.Boolean, output: Output })
export type Stored = typeof Stored.Type

export const RunState = Schema.Struct({
  start: Schema.optionalKey(Start),
  /** The result's files' directory, created once: a rerun writes the same files again. */
  outputDir: Schema.optionalKey(Schema.String),
  /** Decided once. */
  result: Schema.optionalKey(Stored),
})
export type RunState = typeof RunState.Type

/** A call's answer as the VM takes it; no `value` is `undefined`. */
export const Outcome = Schema.Union([
  Schema.Struct({ ok: Schema.Literal(true), value: Schema.optionalKey(Schema.Json) }),
  Schema.Struct({
    ok: Schema.Literal(false),
    message: Schema.String,
    properties: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
  }),
])
export type Outcome = typeof Outcome.Type

export const JournalEntry = Schema.Struct({
  /** The VM's call ID. Its result is the entry's position + 1 (`seq`). */
  id: Schema.Int,
  /** The VM clock while it processes this result. */
  at: Schema.Finite,
  /** Calls the VM had issued after processing this result; absent until acknowledged. */
  issuedAfter: Schema.optionalKey(Schema.Int),
  /** A global's call and outcome; a tool's result is its nested call's. */
  global: Schema.optionalKey(
    Schema.Struct({
      name: Schema.String,
      args: Schema.optionalKey(Schema.Json),
      outcome: Outcome,
    }),
  ),
})
export type JournalEntry = typeof JournalEntry.Type

export const JournalState = Schema.Struct({
  entries: Schema.Array(JournalEntry),
  /** Calls issued by the script's first run, before any result; absent until acknowledged. */
  firstIssued: Schema.optionalKey(Schema.Int),
})
export type JournalState = typeof JournalState.Type

export const StoreState = Schema.Struct({ values: Schema.Record(Schema.String, Schema.Json) })
export type StoreState = typeof StoreState.Type

/** The start data and the outcome; scoped to the call's task, gone when it settles. */
export const run = Document.make<RunState>({
  kind: 'pi.codemode.run',
  version: 1,
  scope: 'task',
  initial: () => Effect.succeed({}),
})

/** One entry per delivered result, in delivery order; scoped to the call's task. */
export const journal = Document.make<JournalState>({
  kind: 'pi.codemode.journal',
  version: 1,
  scope: 'task',
  initial: () => Effect.succeed({ entries: [] }),
})

/**
 * What scripts keep with `store()` and read with `load()`, per conversation. Rewindable and
 * copied as of the fork point, so a fork sees the values as they were at its entry.
 */
export const store = Document.make<StoreState>({
  kind: 'pi.codemode.store',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  initial: () => Effect.succeed({ values: {} }),
})

/** One tool call a script made, as the result's details list it. */
export const Row = Schema.Struct({
  /** The VM's number for the call; its nested call key is `c<id>`. */
  id: Schema.Int,
  name: Schema.String,
  /** Compact JSON of the arguments, cut for display. */
  args: Schema.String,
  status: Schema.Literals(['running', 'ok', 'error', 'cancelled']),
  /** Error text, cut for display. */
  error: Schema.optionalKey(Schema.String),
})
export type Row = typeof Row.Type

/** The result's details: the script's tool calls in call order, and the full output's file. */
export const Details = Schema.Struct({
  calls: Schema.Array(Row),
  fullOutputPath: Schema.optionalKey(Schema.String),
})
export type Details = typeof Details.Type

/** `value` for a document draft: strict JSON, without properties set to `undefined`. */
export const draft = <T>(value: T): Draft<T> => cast(strictJson(cast<T, Schema.Json>(value)))
