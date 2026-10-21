/**
 * A call's start data: what the first attempt records before the VM starts, and what a rerun
 * finds, with the journal and the outcome if one was decided.
 */
import { toCodemodeIdentifier } from '@earendil-works/pi-codemode/declarations'
import type { CodemodeSourceOptions } from '@earendil-works/pi-codemode/source'
import { Clock, Effect, Random, Schema } from 'effect'
import type * as Agent from '../../harness/Agent.ts'
import type * as Call from '../../tool/Call.ts'
import type { Namespace } from './describe.ts'
import { isStructured, namespaceOf, sampleOf } from './describe.ts'
import { GLOBALS as DISCOVERY } from './discover.ts'
import type { Global } from './launch.ts'
import { DEFAULT_MAX_OUTPUT_TOKENS } from './output.ts'
import type { DeclaredTool, NamespaceInfo, Start } from './state.ts'
import {
  draft,
  JournalState,
  journal,
  run,
  Start as StartSchema,
  store,
  Stored,
  VERSION,
} from './state.ts'

const namespacesOf = (
  tools: ReadonlyArray<DeclaredTool>,
  namespaces: ReadonlyArray<Namespace>,
): Array<NamespaceInfo> => {
  const found: Array<{ readonly info: Namespace; readonly tools: Array<string> }> = []
  for (const tool of tools) {
    const namespace = namespaceOf(tool.name, namespaces)
    if (namespace === undefined) continue
    let entry = found.find((candidate) => candidate.info.name === namespace.name)
    if (entry === undefined) {
      entry = { info: namespace, tools: [] }
      found.push(entry)
    }
    entry.tools.push(tool.name)
  }
  return found.map(({ info, tools: names }) =>
    Object.assign(
      { name: info.name, tools: names },
      info.description === undefined ? {} : { description: info.description },
      info.instructions === undefined ? {} : { instructions: info.instructions },
    ),
  )
}

/** The start data a first attempt records, without the store, which the commit reads. */
export const fresh = Effect.fnUntraced(function* (
  agent: Agent.Resolved,
  options: {
    readonly namespaces: ReadonlyArray<Namespace>
    readonly globals: ReadonlyArray<Global>
  },
  source: CodemodeSourceOptions,
) {
  const tools: Array<DeclaredTool> = agent.callable.map(({ tool }) => ({
    name: tool.name,
    jsName: toCodemodeIdentifier(tool.name),
    description: sampleOf(tool),
    ...(isStructured(tool) ? { structured: true as const } : {}),
  }))
  const startedAt = yield* Clock.currentTimeMillis
  const start: Omit<Start, 'store'> = {
    version: VERSION,
    startedAt,
    seed: yield* Random.nextIntBetween(0, 0xff_ff_ff_ff),
    ...(source.timeoutMs === undefined ? {} : { deadline: startedAt + source.timeoutMs }),
    tools,
    namespaces: namespacesOf(tools, options.namespaces),
    globals: [
      ...DISCOVERY.map((name) => ({ name, spread: true })),
      ...options.globals.map(({ name, spread }) => ({ name, spread: spread === true })),
    ],
    maxOutputTokens: source.maxOutputTokens ?? DEFAULT_MAX_OUTPUT_TOKENS,
  }
  return start
})

/** What a first attempt or a rerun finds. */
const Found = Schema.Union([
  Schema.Struct({ done: Stored }),
  Schema.Struct({ start: StartSchema, journal: JournalState, rerun: Schema.Boolean }),
])
export type Found = typeof Found.Type

const decodeFound = Schema.decodeEffect(Schema.fromJsonString(Found))

/** The outcome if one was decided; else the start data, recorded now by a first attempt. */
export const load = (call: Call.Service, start: Omit<Start, 'store'>) =>
  call
    .commit((tx) =>
      Effect.gen(function* () {
        const state = yield* tx.doc(run, call.taskId)
        if (state.result !== undefined) return JSON.stringify({ done: state.result })
        const rerun = state.start !== undefined
        if (state.start === undefined) {
          const values = (yield* tx.doc(store, call.conversationId)).values
          state.start = draft({ ...start, store: values })
        }
        const entries = yield* tx.doc(journal, call.taskId)
        return JSON.stringify({ start: state.start, journal: entries, rerun })
      }),
    )
    .pipe(Effect.flatMap((text) => Effect.orDie(decodeFound(text))))
