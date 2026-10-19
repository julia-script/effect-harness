/**
 * Test support: a Harness over memory storage, with a scripted stand-in for the generation task.
 *
 * The stand-in takes `$generation` runs where a test scripts the run itself rather than a model
 * (`generationSupport.ts` runs the built-in task). Each run task waits for the next scripted step: an answer ends the run at the final boundary, a tool
 * round passes the `postTools` boundary and hands the run to a fresh task, and a failure faults
 * the task. Its abort handler ends the run `unanswered` with reason `aborted`.
 */
import { expect } from 'bun:test'
import { Effect, Exit, Queue, Scope } from 'effect'
import { cast } from 'effect/Function'
import type { Schema } from 'effect'
import type { Service as StorageService } from '../../storage/Storage.ts'
import { Storage } from '../../storage/Storage.ts'
import type { Op } from '../../storage/schema/Document.ts'
import type { EntryRecord } from '../../storage/schema/Entry.ts'
import type { ConversationId, EntryId } from '../../storage/schema/Ids.ts'
import { Registry, Task } from '../../task/index.ts'
import type { Conversation } from '../Conversation.ts'
import * as Extensions from '../Extensions.ts'
import * as Harness from '../Harness.ts'
import * as Inbox from '../Inbox.ts'
import * as Live from '../Live.ts'
import * as Settings from '../Settings.ts'
import type { Settled } from '../Submission.ts'
import { prepareBoundary } from '../internal/inbox.ts'
import { continueRun, endRun, finishRun, RUN_KIND } from '../internal/run.ts'

export const ASSISTANT_KIND = '$assistant'

/** What the next run task does. */
export type Step =
  | { readonly kind: 'answer'; readonly text: string }
  | { readonly kind: 'tools'; readonly text: string }
  | { readonly kind: 'fail' }

type Checkpoint = { readonly phase: 'prepare'; readonly attempt: number }
type Result = { readonly entryId: EntryId }

/** An assistant entry draft with one text part. */
const assistant = (text: string) => ({
  kind: ASSISTANT_KIND,
  model: [{ role: 'assistant', content: [{ type: 'text', text }] }],
})

/** The scripted stand-in for `$generation`. */
export const standIn = (steps: Queue.Queue<Step>, settings: Effect.Effect<Settings.Settings>) =>
  Task.make<Record<string, never>, Checkpoint, Result>({
    name: RUN_KIND,
    version: 1,
    initial: () => ({ phase: 'prepare', attempt: 1 }),
    phases: {
      prepare: (_task, runtime) =>
        Effect.gen(function* () {
          const step = yield* Queue.take(steps)
          if (step.kind === 'fail') return yield* Effect.fail('scripted failure')
          return yield* runtime.commit((tx) =>
            Effect.gen(function* () {
              const { conversationId, taskId } = runtime
              // Queue modes are read on the line, when the boundary is decided.
              const boundary = yield* prepareBoundary(tx, conversationId, yield* settings)
              const entry = yield* tx.appendEntry(conversationId, assistant(step.text))
              if (step.kind === 'answer') yield* finishRun(tx, boundary, taskId, entry.id)
              else yield* continueRun(tx, boundary, taskId)
              return {
                status: 'terminal',
                outcome: { status: 'completed', result: { entryId: entry.id } },
              } as const
            }),
          )
        }),
    },
    abort: (_task, runtime) =>
      runtime.commit((tx) =>
        Effect.gen(function* () {
          const live = yield* tx.doc(Live.document, runtime.conversationId)
          yield* endRun(tx, live, runtime.taskId, { status: 'unanswered', reason: 'aborted' })
          return { status: 'terminal', outcome: { status: 'aborted' } } as const
        }),
      ),
  })

/** Harness options, and the settings it runs with. */
export interface Options extends Harness.Options {
  readonly settings?: Settings.Input | Effect.Effect<Settings.Input>
}

export interface Opened {
  readonly harness: Harness.Service
  readonly steps: Queue.Queue<Step>
  readonly answer: (text: string) => Effect.Effect<void>
  readonly tools: (text: string) => Effect.Effect<void>
  readonly fail: Effect.Effect<void>
}

/** Open a Harness over `storage` in the scope, with the stand-in installed. */
export const open = (
  storage: StorageService,
  options: Options = {},
  tasks: ReadonlyArray<Task.Any> = [],
): Effect.Effect<Opened, never, Scope.Scope> =>
  Effect.gen(function* () {
    const steps = yield* Queue.unbounded<Step>()
    const settings = Settings.make(options.settings ?? {})
    const registry = yield* Registry.make([standIn(steps, settings), ...tasks])
    const extensions = yield* Extensions.make.pipe(
      Effect.provideService(Registry.Registry, registry),
    )
    const harness = yield* Harness.makeWith(options).pipe(
      Effect.provideService(Extensions.Extensions, extensions),
      Effect.provideService(Storage, storage),
      Effect.provideService(Registry.Registry, registry),
      Effect.provideService(Settings.Current, settings),
    )
    const offer = (step: Step) => Effect.asVoid(Queue.offer(steps, step))
    const opened: Opened = {
      harness,
      steps,
      answer: (text) => offer({ kind: 'answer', text }),
      tools: (text) => offer({ kind: 'tools', text }),
      fail: offer({ kind: 'fail' }),
    }
    return opened
  }).pipe(Effect.orDie)

/** `open` in a scope of its own, closed by `close`: a later `open` over `storage` reopens. */
export const openClosable = (storage: StorageService, options: Options = {}) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const opened = yield* Scope.provide(open(storage, options), scope)
    return { ...opened, close: Scope.close(scope, Exit.void) }
  })

/** A fresh ownerless conversation. */
export const ownerless = (harness: Harness.Service) =>
  Effect.orDie(harness.createConversation({ ownership: { kind: 'ownerless' } }))

/** The text of an entry's first model message, or its kind when it has none. */
export const textOf = (entry: EntryRecord): string => {
  const content: Schema.Json | undefined = entry.model?.[0]?.['content']
  if (typeof content === 'string') return content
  if (Array.isArray(content)) {
    const first: unknown = content[0]
    if (typeof first === 'object' && first !== null && 'text' in first) return String(first.text)
  }
  return ''
}

/** Every entry of the conversation, oldest first, as `kind: text`. */
export const transcript = (conversation: Conversation) =>
  Effect.map(Effect.orDie(conversation.entries({ order: 'ascending' }, 1000)), (page) =>
    page.items.map((entry) => `${entry.kind}: ${textOf(entry)}`),
  )

/** The committed `$inbox` items of a conversation. */
export const inboxOf = (harness: Harness.Service, id: ConversationId) =>
  Effect.map(
    Effect.orDie(harness.session.snapshot(Inbox.document, id)),
    (state) => state?.items ?? [],
  )

/** The committed `$live` of a conversation. */
export const liveOf = (harness: Harness.Service, id: ConversationId) =>
  Effect.map(Effect.orDie(harness.session.snapshot(Live.document, id)), (state) => state ?? {})

/** Retry `check` on a short interval until it holds. */
export { eventually, runTest, settle } from '../../task/test/support.ts'

/** The op batches of `$inbox` changes, collected from every later commit in the scope. */
export const inboxOps = (harness: Harness.Service) =>
  Effect.gen(function* () {
    const batches: Array<ReadonlyArray<Op>> = []
    yield* harness.session.observeCommits((publication) =>
      Effect.sync(() => {
        for (const change of publication.changes) {
          if (change.type !== 'document' || change.record.kind !== Inbox.document.definition.kind)
            continue
          if (change.ops.length > 0) batches.push(change.ops)
        }
      }),
    )
    return batches
  })

/** A write that affects model context, so it queues while busy. */
export const queuedNote = (text: string) =>
  ({ kind: 'note', model: [{ role: 'user', content: text, timestamp: 0 }] }) as const

/** Any number, for matchers; `expect.any` is untyped. */
export const anyNumber = cast<unknown, number>(expect.any(Number))

/** Any entry ID, for matchers. */
export const anyEntryId = cast<unknown, EntryId>(expect.any(Number))

/** `settled` as a placed submission settles: with some entry. */
export const placedAs = <A extends object>(settled: A) =>
  cast<unknown, A>({ ...settled, entry: anyEntryId })

/** Any string containing `text`, for matchers. */
export const containing = (text: string) => cast<unknown, string>(expect.stringContaining(text))

/** The answer of a settled submission, when it settled done. */
export const answerOf = (settled: Settled): unknown =>
  settled.status === 'done' && settled.type === 'input' ? settled.answer : null

/** A Storage that counts its commits. */
export const counting = (backend: StorageService) => {
  let commits = 0
  const storage: StorageService = {
    ...backend,
    commit: (writes) =>
      Effect.suspend(() => {
        commits++
        return backend.commit(writes)
      }),
  }
  return { storage, commits: () => commits }
}
