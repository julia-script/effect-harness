/**
 * Test support: a Harness running the built-in generation and tool tasks over the `Faux` model
 * and the scripted tools of `tool/test/support.ts`.
 */
import { Effect, Layer } from 'effect'
import type { Scope } from 'effect'
import type { ModelThinkingLevel } from '../model/Message.ts'
import { Faux, Model } from '../model/index.ts'
import type { Service as StorageService } from '../storage/Storage.ts'
import { Storage } from '../storage/Storage.ts'
import type { EntryRecord } from '../storage/schema/Entry.ts'
import type { Hook } from '../task/index.ts'
import { Registry } from '../task/index.ts'
import type { Tool } from '../tool/index.ts'
import type { Script } from './tool.ts'
import { script, slowHandlers, toolkit } from './tool.ts'
import * as Extension from '../harness/Extension.ts'
import * as Extensions from '../harness/Extensions.ts'
import * as Generation from '../harness/Generation.ts'
import * as Harness from '../harness/Harness.ts'
import * as Settings from '../harness/Settings.ts'
import type { Settled } from '../protocol/Submission.ts'

export interface Opened {
  readonly harness: Harness.Service
  readonly extensions: Extensions.Service
  readonly faux: Faux.Faux
  readonly script: Script
}

/** The test extension: some tools of the toolkit, sections, hooks and wrappers. */
export interface Agent {
  /** Default: the faux model; `null` for none. */
  readonly model?: Model.Ref | null
  readonly fallbacks?: ReadonlyArray<Model.Ref>
  readonly reasoning?: ModelThinkingLevel
  readonly sections?: ReadonlyArray<Extension.Section>
  /** Default: every tool of the toolkit. */
  readonly tools?: ReadonlyArray<Tool.Any>
  readonly hooks?: ReadonlyArray<Hook.Registration>
  readonly wraps?: ReadonlyArray<Extension.Wrap>
}

export interface Options {
  readonly agent?: (faux: Faux.Faux) => Agent
  readonly settings?: Settings.Input
  readonly faux?: Faux.Options
}

/**
 * Open a Harness over `storage` in the scope, with the built-in tasks installed and the test
 * extension, `test`, over the scripted toolkit.
 */
export const open = (
  storage: StorageService,
  options: Options = {},
): Effect.Effect<Opened, never, Scope.Scope> =>
  Effect.gen(function* () {
    const faux = yield* Faux.make(options.faux)
    const current = yield* script
    const agent = options.agent?.(faux) ?? {}
    const model = agent.model === undefined ? faux.ref() : agent.model
    const settings = Settings.make({
      retry: { baseDelayMs: 1, maxDelayMs: 1 },
      ...(model === null ? {} : { model }),
      ...(agent.fallbacks === undefined ? {} : { fallbacks: agent.fallbacks }),
      ...(agent.reasoning === undefined ? {} : { reasoning: agent.reasoning }),
      ...options.settings,
    })
    const registry = yield* Registry.make()
    const extensions = yield* Extensions.make.pipe(
      Effect.provideService(Registry.Registry, registry),
    )
    const extension = Extension.make('test', {
      toolkit,
      sections: agent.sections ?? [],
      hooks: agent.hooks ?? [],
      wraps: agent.wraps ?? [],
    })
    const implementations = yield* Layer.build(toolkit.toLayer(slowHandlers(current)))
    yield* extensions
      .install(agent.tools === undefined ? extension : { ...extension, tools: agent.tools })
      .pipe(Effect.provideContext(implementations))
    yield* Generation.layer.pipe(
      Layer.provide(Model.layer([faux.provider])),
      Layer.provide(Layer.succeed(Extensions.Extensions, extensions)),
      Layer.provide(Layer.succeed(Registry.Registry, registry)),
      Layer.provide(Layer.succeed(Settings.Current, settings)),
      Layer.build,
    )
    const harness = yield* Harness.make.pipe(
      Effect.provideService(Storage, storage),
      Effect.provideService(Registry.Registry, registry),
      Effect.provideService(Extensions.Extensions, extensions),
      Effect.provideService(Settings.Current, settings),
    )
    const opened: Opened = { harness, extensions, faux, script: current }
    return opened
  }).pipe(Effect.orDie)

/** The first model message of an entry. */
export const messageOf = (entry: EntryRecord): Record<string, unknown> => entry.model?.[0] ?? {}

/** Every entry of the conversation, oldest first. */
export const entriesOf = (
  harness: Harness.Service,
  conversationId: EntryRecord['conversationId'],
) =>
  Effect.orDie(
    Effect.flatMap(harness.conversation(conversationId), (conversation) =>
      conversation === undefined
        ? Effect.succeed([])
        : Effect.map(conversation.entries({ order: 'ascending' }, 1000), (page) => page.items),
    ),
  )

/** Each entry as `kind`, with the text or tool calls of its message. */
export const describe = (entry: EntryRecord): string => {
  const message = entry.model?.[0]
  const content: unknown = message?.['content']
  const blocks = Array.isArray(content) ? content : [{ type: 'text', text: content }]
  const parts = blocks.flatMap((block: unknown) => {
    if (typeof block !== 'object' || block === null) return []
    if ('text' in block && typeof block.text === 'string' && block.text !== '') return [block.text]
    if ('name' in block && typeof block.name === 'string') return [`call ${block.name}`]
    return []
  })
  const stop = message?.['stopReason']
  const suffix = typeof stop === 'string' && stop !== 'stop' ? ` (${stop})` : ''
  return `${entry.kind}${parts.length > 0 ? `: ${parts.join(' | ')}` : ''}${suffix}`
}

/** The settled submission, unanswered or done. */
export const reasonOf = (settled: Settled): string =>
  settled.status === 'unanswered' ? settled.reason : settled.status

export { eventually, liveOf, ownerless, runTest } from './harness.ts'
