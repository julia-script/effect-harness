import { Context, Deferred, Effect, Exit, Layer, Schema, Scope, Stream } from 'effect'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as Conversation from 'effect-harness/Conversation'
import * as ContextProjection from 'effect-harness/ContextProjection'
import * as Harness from 'effect-harness/Harness'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import { ToolExecution } from 'effect-harness/ToolExecution'

export const Snapshot = Schema.Struct({
  running: Schema.Boolean,
  answering: Schema.Boolean,
  messages: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      role: Schema.Literals(['user', 'assistant', 'tool']),
      text: Schema.String,
    }),
  ),
  requestId: Schema.String,
  canRetry: Schema.Boolean,
  canFork: Schema.Boolean,
  pending: Schema.String,
  emitted: Schema.String,
  partial: Schema.String,
  requests: Schema.Natural,
  toolCalls: Schema.Natural,
  submissions: Schema.Natural,
  reused: Schema.Boolean,
  branch: Schema.String,
  commit: Schema.String,
  status: Schema.String,
  entries: Schema.Array(
    Schema.Struct({ id: Schema.String, kind: Schema.String, text: Schema.String }),
  ),
  tasks: Schema.Array(Schema.Struct({ kind: Schema.String, status: Schema.String })),
})
export type Snapshot = typeof Snapshot.Type
export const empty: Snapshot = {
  running: false,
  answering: false,
  messages: [],
  requestId: '',
  canRetry: false,
  canFork: false,
  pending: '',
  emitted: '',
  partial: '',
  requests: 0,
  toolCalls: 0,
  submissions: 0,
  reused: false,
  branch: 'root',
  commit: '0',
  status: 'Ready',
  entries: [],
  tasks: [],
}

const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 0, uncached: 0, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 0, text: 0, reasoning: undefined },
  },
  response: undefined,
})

const textOf = (messages: ReadonlyArray<Prompt.Message>): string =>
  messages
    .flatMap((message) => {
      if (message.role === 'system') return [message.content]
      return message.content.flatMap((part) => {
        if (part.type === 'text') return [part.text]
        if (part.type === 'tool-call') return [`${part.name}(${JSON.stringify(part.params)})`]
        if (part.type === 'tool-result') return [JSON.stringify(part.result)]
        return []
      })
    })
    .join('')

const make = Effect.gen(function* () {
  // Storage outlives each runtime scope, but ends with the browser application.
  const ownerScope = yield* Scope.Scope
  let storage = yield* Layer.build(Storage.layerMemory)
  let snapshot = empty
  let runtimeScope: Scope.Closeable | undefined
  let conversation: Conversation.Conversation | undefined
  let root: Conversation.Conversation | undefined
  let gate: Deferred.Deferred<void> | undefined
  let requestNumber = 0
  let lastSubmission: Submission.Submission | undefined
  let lastDraft: Submission.Draft | undefined
  let forks = 0

  const waitForStep = (label: string) =>
    Effect.gen(function* () {
      const current = yield* Deferred.make<void>()
      gate = current
      snapshot = { ...snapshot, pending: label }
      yield* Deferred.await(current).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (gate === current) {
              gate = undefined
              snapshot = { ...snapshot, pending: '' }
            }
          }),
        ),
      )
    })

  const response = ({ prompt }: LanguageModel.ProviderOptions): Response.StreamPartEncoded[] => {
    const lastUser = prompt.content.findLastIndex((message) => message.role === 'user')
    const result = prompt.content
      .slice(lastUser + 1)
      .flatMap((message) => (message.role === 'tool' ? message.content : []))
      .find((part) => part.type === 'tool-result')
    if (result === undefined)
      return [
        {
          type: 'tool-call',
          id: `inspect-${snapshot.requests}`,
          name: 'inspect_test',
          params: { file: 'login.test.ts' },
          providerExecuted: false,
        },
        finish('tool-calls'),
      ]
    const question = textOf(prompt.content.slice(lastUser, lastUser + 1))
    let answer = result.isFailure
      ? 'The inspection was interrupted. Check the external outcome before trying again.'
      : 'The login test reads the session before authentication finishes. Await signIn() before asserting the user.'
    if (!result.isFailure && /fix|change|code/i.test(question)) {
      answer =
        'Add await before signIn():\n\nawait signIn()\nexpect(session.user).toBeDefined()\n\nNow the assertion runs after authentication finishes.'
    }
    return [
      { type: 'text-start', id: 'answer' },
      ...(answer.match(/\S+\s*/g)?.map((delta): Response.StreamPartEncoded => ({
        type: 'text-delta',
        id: 'answer',
        delta,
      })) ?? []),
      { type: 'text-end', id: 'answer' },
      finish('stop'),
    ]
  }

  const model = Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () =>
        Effect.succeed([{ type: 'text', text: 'Controlled model' }, finish('stop')]),
      streamText: (request) =>
        Stream.suspend(() => {
          snapshot = { ...snapshot, requests: snapshot.requests + 1 }
          return Stream.fromIterable(response(request)).pipe(
            Stream.mapEffect((part) =>
              Effect.gen(function* () {
                if (part.type === 'text-delta' || part.type === 'tool-call') {
                  yield* waitForStep(part.type === 'tool-call' ? 'Tool intent' : 'Next text chunk')
                }
                if (part.type === 'text-start') snapshot = { ...snapshot, emitted: '' }
                if (part.type === 'text-delta')
                  snapshot = { ...snapshot, emitted: snapshot.emitted + part.delta }
                return part
              }),
            ),
          )
        }),
    }),
  )
  const tools = Toolkit.make(
    Tool.make('inspect_test', {
      description: 'Read a fixed test fixture in this offline demo.',
      parameters: Schema.Struct({ file: Schema.String }),
      success: Schema.String,
      replay: 'safe',
    }),
  )
  const handlers = tools.toLayer({
    inspect_test: Effect.fn('demo.inspectTest')(function* ({ file }) {
      snapshot = { ...snapshot, toolCalls: snapshot.toolCalls + 1 }
      const execution = yield* ToolExecution
      yield* execution.output(`Reading ${file}\n`)
      yield* waitForStep('Tool result')
      return 'signIn() returns a Promise; the test does not await it.'
    }),
  })
  const live = () =>
    Harness.layerLocal({ tools }).pipe(
      Layer.provide(handlers),
      Layer.provide(model),
      Layer.provide(Layer.succeedContext(storage)),
    )

  const read = Effect.gen(function* () {
    if (conversation === undefined || runtimeScope === undefined) return snapshot
    const current = yield* Conversation.view(conversation)
    const value = current.value
    const messages: Array<Snapshot['messages'][number]> = []
    const entries = yield* Effect.forEach(value.entries, (entry) =>
      ContextProjection.messages(entry).pipe(
        Effect.map((decoded) => {
          for (const [index, message] of decoded.entries()) {
            if (message.role === 'system') continue
            const text = message.content
              .flatMap((part) => {
                if (part.type === 'text') return [part.text]
                if (part.type === 'tool-result')
                  return [
                    typeof part.result === 'string' ? part.result : JSON.stringify(part.result),
                  ]
                return []
              })
              .join('')
            if (text) messages.push({ id: `${entry.id}-${index}`, role: message.role, text })
          }
          return {
            id: String(entry.id),
            kind: entry.kind,
            text: textOf(decoded) || JSON.stringify(entry.data),
          }
        }),
      ),
    )
    const partial = value.live.generation?.partial
    let partialText = ''
    if (partial !== undefined) {
      partialText = textOf(partial.message.content)
    }
    const latest = lastSubmission === undefined ? undefined : yield* Submission.read(lastSubmission)
    const statuses = value.submissions.map((submission) => submission._tag)
    snapshot = {
      ...snapshot,
      entries,
      messages,
      answering:
        latest !== undefined && latest._tag !== 'InputDone' && latest._tag !== 'InputUnanswered',
      partial: partialText,
      submissions: value.submissions.length,
      requestId: lastDraft?.requestId ?? '',
      canRetry: lastDraft !== undefined,
      canFork: latest?._tag === 'InputDone',
      commit: String(current.commitSeq ?? '0'),
      tasks: value.graph.tasks.map((task) => ({ kind: task.kind, status: task.status })),
      status: statuses.at(-1) ?? 'Ready',
    }
    return snapshot
  })

  const stop = Effect.gen(function* () {
    const scope = runtimeScope
    runtimeScope = undefined
    if (scope !== undefined) yield* Scope.close(scope, Exit.void)
    snapshot = { ...snapshot, running: false, pending: '', status: 'Runtime stopped' }
  })
  yield* Effect.addFinalizer(() => stop)

  const start = Effect.gen(function* () {
    if (runtimeScope !== undefined) return
    const scope = yield* Scope.make()
    runtimeScope = scope
    const context = yield* Layer.buildWithScope(live(), scope)
    const harness = Context.get(context, Harness.Harness)
    root = yield* harness.root
    if (lastSubmission !== undefined) lastSubmission = yield* harness.submission(lastSubmission.id)
    if (conversation === undefined) conversation = root
    else {
      const previous = yield* harness.conversation(conversation.id)
      conversation = previous._tag === 'Some' ? previous.value : root
    }
    snapshot = { ...snapshot, running: true, status: 'Runtime open' }
  })
  const submit = (content: string) =>
    Effect.gen(function* () {
      yield* start
      if (conversation === undefined) return
      requestNumber += 1
      lastDraft = {
        type: 'input',
        content,
        requestId: `demo-${requestNumber}`,
      }
      lastSubmission = yield* Conversation.submit(conversation, lastDraft)
      snapshot = { ...snapshot, emitted: '', reused: false }
    })
  const retry = Effect.gen(function* () {
    if (conversation === undefined || lastDraft === undefined || lastSubmission === undefined)
      return
    const again = yield* Conversation.submit(conversation, lastDraft)
    snapshot = { ...snapshot, reused: again.id === lastSubmission.id }
  })
  const step = Effect.gen(function* () {
    if (gate !== undefined) yield* Deferred.succeed(gate, undefined)
  })
  const fork = Effect.gen(function* () {
    if (conversation === undefined || lastSubmission === undefined) return
    const record = yield* Submission.read(lastSubmission)
    if (record._tag !== 'InputDone') return
    conversation = yield* Conversation.fork(conversation, { at: record.answer })
    forks += 1
    snapshot = { ...snapshot, branch: `fork-${forks}`, emitted: '', partial: '', reused: false }
    lastDraft = undefined
    lastSubmission = undefined
  })
  const returnToRoot = Effect.sync(() => {
    conversation = root
    snapshot = { ...snapshot, branch: 'root', emitted: '', reused: false }
    lastDraft = undefined
    lastSubmission = undefined
  })
  const abort = Effect.gen(function* () {
    if (conversation !== undefined) yield* Conversation.abort(conversation)
  })
  const reset = Effect.gen(function* () {
    yield* stop
    storage = yield* Layer.buildWithScope(Storage.layerMemory, ownerScope)
    conversation = undefined
    root = undefined
    lastSubmission = undefined
    lastDraft = undefined
    requestNumber = 0
    forks = 0
    snapshot = empty
  })
  return { read, start, stop, submit, retry, step, fork, returnToRoot, abort, reset }
})

export class Lab extends Context.Service<Lab, Effect.Success<typeof make>>()('docs/Lab') {
  static readonly layer = Layer.effect(Lab, make)
}
