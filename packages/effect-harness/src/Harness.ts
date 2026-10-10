/** Application client. Its backend can live in this process or behind a transport. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import type * as Record from './Record.js'
import type * as Conversation from './Conversation.js'
import type * as Submission from './Submission.js'
import { HarnessBackend } from './HarnessBackend.js'
import type { CreateOptions } from './HarnessBackend.js'
import type { HarnessError } from './HarnessError.js'
import * as ConversationHost from './internal/ConversationHost.js'
import * as HarnessRuntime from './HarnessRuntime.js'
import type * as Tool from './Tool.js'
import type * as Hook from './Hook.js'
import type * as Extension from './Extension.js'
import type * as Model from './Model.js'

export { CreateOptionsSchema, type CreateOptions } from './HarnessBackend.js'
export { HarnessError } from './HarnessError.js'
export interface HarnessService {
  /** Opens the root and automatically starts pending work in the local runtime. */
  readonly root: Effect.Effect<Conversation.Conversation, HarnessError>
  readonly create: (
    options?: CreateOptions,
  ) => Effect.Effect<Conversation.Conversation, HarnessError>
  readonly conversation: (
    id: Record.ConversationId,
  ) => Effect.Effect<Option.Option<Conversation.Conversation>, HarnessError>
  /**
   * Reacquires a saved input submission using this client's backend.
   * Validates immediately through backend.read, including settled submissions;
   * missing IDs fail with HarnessError. Creates no durable records and retains
   * the original conversation ID. Open that conversation to resume pending work
   * after a local runtime restart, then use Submission.read, wait or withdraw.
   */
  readonly submission: (
    id: Record.SubmissionId,
  ) => Effect.Effect<Submission.Submission, HarnessError>
  readonly waitForIdle: Effect.Effect<void, HarnessError>
}
export class Harness extends Context.Service<Harness, HarnessService>()('effect-harness/Harness') {}
export const make = Effect.gen(function* () {
  const backend = yield* HarnessBackend
  return Harness.of({
    root: backend.root.pipe(Effect.map((id) => ConversationHost.conversation(backend, id))),
    create: (options) =>
      backend.create(options).pipe(Effect.map((id) => ConversationHost.conversation(backend, id))),
    conversation: (id) =>
      backend
        .conversation(id)
        .pipe(Effect.map(Option.map((value) => ConversationHost.conversation(backend, value)))),
    submission: (id) =>
      backend
        .read(id)
        .pipe(
          Effect.map((record) => ConversationHost.submission(backend, id, record.conversationId)),
        ),
    waitForIdle: backend.waitForIdle,
  })
})
export const layer = Layer.effect(Harness, make)

/** Runs the client and durable backend in one process; Scope owns runtime shutdown. */
export const layerLocal = <
  const T extends Record<string, Tool.Any> = {},
  const H extends ReadonlyArray<Hook.Any> = readonly [],
  const X extends ReadonlyArray<Extension.Any> = readonly [],
  const M extends ReadonlyArray<Model.Any> = readonly [],
>(
  options: HarnessRuntime.Options<T, H, X, M> = {},
) => layer.pipe(Layer.provide(HarnessRuntime.layer(options)))
