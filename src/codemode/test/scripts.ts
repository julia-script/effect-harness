/**
 * Test support: scripts run in a Harness with code mode, to their answer or across a restart.
 */
import { Deferred, Effect } from 'effect'
import { ownerless } from '../../test/generation.ts'
import type * as Harness from '../../harness/Harness.ts'
import { Faux } from '../../model/index.ts'
import * as Session from '../../session/Session.ts'
import { Storage } from '../../storage/Storage.ts'
import type { Service as StorageService } from '../../storage/Storage.ts'
import type { TaskId } from '../../storage/schema/Ids.ts'
import { ToolTask } from '../../tool/index.ts'
import { Codemode } from '../index.ts'
import type { Options, World } from './support.ts'
import { isNested, openChat, resultOf, scriptTask } from './support.ts'

export const DONE = Faux.message('done')
export const script = (code: string) =>
  Faux.message([Faux.toolCall('codemode', { code }, 's1')], { stopReason: 'toolUse' })

/** Run `code` to its answer in one Harness. */
export const runScript = (
  storage: StorageService,
  current: World,
  code: string,
  options: Options = {},
) =>
  Effect.gen(function* () {
    const opened = yield* openChat(storage, current, options)
    yield* opened.faux.setResponses([script(code), DONE])
    const conversation = yield* ownerless(opened.harness)
    const submission = yield* Effect.orDie(conversation.submit({ type: 'input', content: 'go' }))
    yield* submission.wait
    const result = yield* resultOf(opened.harness, conversation.id)
    return { ...opened, conversation, result }
  })

/** Change stored state through a Session of its own, while no Harness is open. */
const offline = (
  storage: StorageService,
  change: (session: Session.Service, task: TaskId) => Effect.Effect<void>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const session = yield* Session.make
      const page = yield* Effect.orDie(
        session.commit((tx) => tx.scanTasks({ kind: ToolTask.name }, 100)),
      )
      const task = page.items.find((each) => !isNested(each.input))
      if (task !== undefined) yield* change(session, task.id)
    }),
  ).pipe(Effect.provideService(Storage, storage))

export interface Restart extends Options {
  /** Change the stored state while no Harness is open. */
  readonly between?: (session: Session.Service, task: TaskId) => Effect.Effect<void>
  /** Wait for something before the first Harness closes. */
  readonly beforeClose?: (harness: Harness.Service, task: TaskId) => Effect.Effect<void>
}

/**
 * Run `code` until `hold` is reached, close the Harness (a restart), let `between` change the
 * stored state, then reopen with `hold` no longer blocking and wait for the answer.
 */
export const restart = (
  storage: StorageService,
  current: World,
  code: string,
  options: Restart = {},
) =>
  Effect.gen(function* () {
    const first = yield* openChat(storage, current, options)
    yield* first.faux.setResponses([script(code)])
    const conversation = yield* ownerless(first.harness)
    const submission = yield* Effect.orDie(conversation.submit({ type: 'input', content: 'go' }))
    yield* Deferred.await(current.reached)
    if (options.beforeClose !== undefined)
      yield* options.beforeClose(first.harness, yield* scriptTask(first.harness))
    yield* first.close
    current.block = false
    if (options.between !== undefined) yield* offline(storage, options.between)
    const second = yield* openChat(storage, current, options)
    yield* second.faux.setResponses([DONE])
    const again = yield* Effect.orDie(second.harness.submission(submission.id))
    if (again !== undefined) yield* again.wait
    const result = yield* resultOf(second.harness, conversation.id)
    return { ...second, conversation, result }
  })

/** Change the call's start data or journal in one commit. */
export const edit =
  (change: {
    readonly run?: (state: {
      start?: { store: unknown; fingerprint?: string; version: number }
    }) => void
    readonly journal?: (state: {
      entries: Array<{ id: number; issuedAfter?: number; global?: { args?: unknown } }>
    }) => void
  }) =>
  (session: Session.Service, task: TaskId) =>
    Effect.orDie(
      session.commit((tx) =>
        Effect.gen(function* () {
          if (change.run !== undefined) change.run(yield* tx.doc(Codemode.RunDocument, task))
          if (change.journal !== undefined)
            change.journal(yield* tx.doc(Codemode.JournalDocument, task))
        }),
      ),
    )
