/**
 * Durable code mode, ported from pi's `codemode/`: a tool that runs model-written JavaScript in a
 * QuickJS sandbox (`@earendil-works/pi-codemode`), whose script survives a restart.
 *
 * A script is in-memory VM state, so a restart while it waits on the last of many calls would
 * lose it and every call it made. The VM is deterministic instead: given the same start data,
 * clock, seed, and results in the same order, it makes the same calls with the same arguments.
 * The tool is replay-safe: after a restart it starts the script again from the stored start data
 * and hands it the results it got before, in the order it got them, from a journal written before
 * each delivery. Tool calls are nested calls keyed `c<VM call id>`, so they reattach to their
 * tasks and never run twice unless their own replay policy reruns them. Then the script continues
 * live. A rerun refuses to replay, ending the call with an error result, when the version or the
 * VM's fingerprint changed, the deadline passed, or the script diverges from its journal.
 *
 * This module holds the declaration, its extension and its documents, importable anywhere;
 * `CodemodeHandlers` implements it where the loop runs.
 */
import { Effect, Schema } from 'effect'
import * as Extension from '../harness/Extension.ts'
import * as Tool from '../tool/Tool.ts'
import * as Toolkit from '../tool/Toolkit.ts'
import { Output } from '../tool/ToolResult.ts'
import { DEFAULT_INLINE_BUDGET, DESCRIPTION, section } from './internal/describe.ts'
import type { Namespace as NamespaceType } from './internal/describe.ts'
import { OUTPUT_LIMITS } from './internal/output.ts'
import * as state from './internal/state.ts'

export const NAME = 'codemode'

/** Version of the replay rules; a call started under another one is not replayed. */
export const VERSION = state.VERSION

/** A group of tools named by a common prefix, for the section and tool search. */
export type Namespace = NamespaceType

export { DEFAULT_INLINE_BUDGET }

/** The result's details: the script's tool calls in call order, and the full output's file. */
export const Details = state.Details
export type Details = state.Details

/** The start data and outcome of a call; scoped to its task. */
export const RunDocument = state.run
/** One entry per result delivered to the script; scoped to the call's task. */
export const JournalDocument = state.journal
/** What scripts keep with `store()`, per conversation, rewindable and copied as of a fork. */
export const StoreDocument = state.store

export const Codemode = Tool.make(NAME, {
  description: DESCRIPTION,
  parameters: Schema.Struct({
    code: Schema.String.annotate({ description: 'Raw JavaScript source.' }),
  }),
  success: Output,
  failure: Output,
  callers: ['model'],
  replay: 'safe',
  outputLimits: OUTPUT_LIMITS,
})

export const toolkit = Toolkit.make(Codemode)

export interface Options {
  /** Groups of tools by name prefix, such as MCP servers. */
  readonly namespaces?: ReadonlyArray<Namespace>
  /** Estimated tokens the declarations in the `codemode` section may use. Default 3000. */
  readonly inlineBudget?: number
}

/**
 * The `codemode` extension: the tool, and the `codemode` section listing the tools scripts can
 * call. Deferred tools are left out of it; scripts find them with `searchTools()`.
 */
export const extension = (options: Options = {}) =>
  Extension.make('codemode', {
    toolkit,
    sections: [
      Extension.section('codemode', ({ agent }) =>
        Effect.succeed(
          section(
            agent.callable.flatMap(({ tool }) => (tool.deferred ? [] : [tool])),
            options.namespaces ?? [],
            options.inlineBudget ?? DEFAULT_INLINE_BUDGET,
          ),
        ),
      ),
    ],
  })
