/**
 * Test support: declared tools with scripted handlers, and calls made the way a generation's tool
 * round makes them, an assistant entry holding the call and a `$tool` task for it.
 */
import { Deferred, Effect, Exit, Layer, Schema } from 'effect'
import type { Scope } from 'effect'
import { Faux } from '../../model/index.ts'
import type { Session } from '../../session/index.ts'
import type { Ids, Task } from '../../storage/schema/index.ts'
import { Tool, Toolkit, ToolResult, ToolTask } from '../index.ts'

export const Read = Tool.make('read', {
  description: 'Read a file',
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.String,
  failure: Schema.Struct({ reason: Schema.String }),
  config: Schema.Struct({ root: Schema.optionalKey(Schema.String) }),
})

/** Prints `lines` numbered lines; the model sees the last three, at most 40 bytes. */
export const Lines = Tool.make('lines', {
  description: 'Print lines',
  parameters: Schema.Struct({ lines: Schema.Finite }),
  success: Schema.String,
  outputLimits: { maxBytes: 40, maxLines: 3, retain: 'tail' },
})

/** Waits until released; not replay-safe. */
export const Slow = Tool.make('slow', {
  description: 'Wait',
  parameters: Schema.Struct({}),
  success: Schema.String,
})

/** Waits until released; replay-safe. */
export const SlowSafe = Tool.make('slow_safe', {
  description: 'Wait',
  parameters: Schema.Struct({}),
  success: Schema.String,
  replay: 'safe',
})

export const toolkit = Toolkit.make(Read, Lines, Slow, SlowSafe)

/** What the slow tools wait on, and how many times each handler started. */
export interface Script {
  readonly started: Deferred.Deferred<void>
  readonly release: Deferred.Deferred<void>
  readonly runs: Array<string>
}

export const script: Effect.Effect<Script> = Effect.gen(function* () {
  return { started: yield* Deferred.make<void>(), release: yield* Deferred.make<void>(), runs: [] }
})

const slow = (name: string, { started, release, runs }: Script) =>
  Effect.gen(function* () {
    runs.push(name)
    yield* Deferred.done(started, Exit.void)
    yield* Deferred.await(release)
    return `${name} done`
  })

/** Scripted handlers for `toolkit`. */
export const slowHandlers = (current: Script): Toolkit.Handlers<typeof toolkit.tools> => ({
  read: ({ path }, { config }) => {
    if (path === 'missing') return Effect.fail({ reason: `${path} not found` })
    if (path === 'boom') return Effect.die(new Error('disk on fire'))
    return Effect.succeed(`contents of ${config.root ?? ''}${path}`)
  },
  lines: ({ lines }) =>
    Effect.succeed(Array.from({ length: lines }, (_, index) => `line ${index + 1}`).join('\n')),
  slow: () => slow('slow', current),
  slow_safe: () => slow('slow_safe', current),
})

/** The tool task over `toolkit` with scripted handlers. */
export const tasks = (
  current: Script,
  options: ToolTask.Options<typeof toolkit.tools> = {},
): Effect.Effect<ToolTask.ToolTask, never, Scope.Scope> =>
  Effect.map(Layer.build(toolkit.toLayer(slowHandlers(current))), (implementations) =>
    ToolTask.make(toolkit, implementations, options),
  )

/** Commit an assistant entry calling `name` with `args`, and the tool task for the call. */
export const call = (
  session: Session.Service,
  conversationId: Ids.ConversationId,
  name: string,
  args: Record<string, Schema.Json>,
) =>
  Effect.orDie(
    session.commit((tx) =>
      Effect.gen(function* () {
        const response = Faux.message([Faux.toolCall(name, args, 'call-1')], {
          stopReason: 'toolUse',
        })
        const entry = yield* tx.appendEntry(conversationId, {
          kind: '$assistant',
          model: [{ ...response }],
        })
        return yield* tx.createTask(
          ToolTask.kind,
          { assistant: entry.id, callId: 'call-1' },
          { ownership: { kind: 'conversation' }, conversationId },
        )
      }),
    ),
  )

/** The result entry a tool task's outcome names: the entry, its message and diagnostics. */
export const answer = (session: Session.Service, outcome: Task.TaskOutcome) =>
  Effect.orDie(
    Effect.gen(function* () {
      const result = 'result' in outcome ? outcome.result : undefined
      const { entryId } = yield* Schema.decodeUnknownEffect(ToolTask.ModelResult)(result)
      const entry = yield* session.commit((tx) => tx.entry(entryId))
      const data = yield* Schema.decodeUnknownEffect(ToolResult.Data)(entry?.data)
      return { entry, message: entry?.model?.[0], diagnostics: data.diagnostics }
    }),
  )
