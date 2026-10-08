/** Commit typed document state with transcript events, then fork an earlier revision. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Pull from 'effect/Pull'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Conversation from 'effect-harness/Conversation'
import * as Document from 'effect-harness/Document'
import * as Extension from 'effect-harness/Extension'
import * as HookError from 'effect-harness/HookError'
import * as Observation from 'effect-harness/Observation'
import * as Record from 'effect-harness/Record'
import * as Runtime from './Runtime.ts'

export const TodoItem = Schema.Struct({ text: Schema.String, done: Schema.Boolean })
export const TodoData = Schema.Struct({ items: Schema.Array(TodoItem) })
export const TodoDoc = Document.defineUnsafe({
  kind: 'tour.todos',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: TodoData,
  initial: () => ({ items: [] }),
})

export const ProgressData = Schema.Struct({ stage: Schema.String, completed: Schema.Int })
export const ProgressDoc = Document.defineUnsafe({
  kind: 'tour.progress',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: ProgressData,
  initial: () => ({ stage: 'planning', completed: 0 }),
})

export const TodoEvent = Schema.Struct({ action: Schema.String, text: Schema.String })
export const Result = Schema.Struct({
  rootTodos: TodoData,
  forkTodos: TodoData,
  progress: ProgressData,
  forkAt: Record.EntryId,
  atomicEntryId: Record.EntryId,
  atomicCommit: Schema.Boolean,
  managedSection: Schema.String,
  modelReply: Schema.String,
  wireChanges: Schema.Array(Schema.Json),
})

export const run = Effect.scoped(
  Effect.gen(function* () {
    const current = yield* Ref.make(Option.none<Conversation.Conversation>())
    const rendered = yield* Ref.make('')
    const todos = Extension.make({
      name: 'tour.todos',
      sections: [
        {
          key: 'todos',
          render: Effect.fn('tour.todos.section')(function* () {
            const conversation = yield* Ref.get(current)
            if (Option.isNone(conversation)) return undefined
            const snapshot = yield* Conversation.document(conversation.value, TodoDoc).pipe(
              Effect.mapError(
                (cause) =>
                  new HookError.HookError({
                    reason: new HookError.HookFailureError({ message: cause.message, cause }),
                  }),
              ),
            )
            if (Option.isNone(snapshot)) return undefined
            const text = snapshot.value.value.items
              .map((item) => `${item.done ? '[x]' : '[ ]'} ${item.text}`)
              .join('\n')
            yield* Ref.set(rendered, text)
            return text
          }),
        },
      ],
    })
    const { harness } = yield* Runtime.open({
      extensions: [todos],
      provider: {
        generateText: () =>
          Effect.succeed([{ type: 'text', text: 'Todo summary' }, Runtime.finish('stop')]),
        streamText: ({ prompt }) => {
          const system = prompt.content
            .filter((message) => message.role === 'system')
            .map((message) => message.content)
            .join('\n')
          return Runtime.answer(
            system.includes('[x] Read the article') && system.includes('[ ] Build the example')
              ? 'I can see the completed reading and the remaining example.'
              : 'Todo section is missing.',
          )
        },
      },
    })
    const root = yield* harness.root
    yield* Ref.set(current, Option.some(root))
    yield* Conversation.awaitIdle(root)
    const first = yield* harness.transaction(
      Effect.fn('tour.todos.initialize')(function* (tx) {
        const draft = yield* tx.doc(TodoDoc, { owner: root.id })
        draft.items.push({ text: 'Read the article', done: false })
        yield* tx.doc(ProgressDoc, { owner: root.id })
        return yield* tx.appendEntry(root.id, {
          kind: 'tour.todo',
          data: yield* Schema.encodeEffect(TodoEvent)({ action: 'add', text: 'Read the article' }),
        })
      }),
    )

    // A watch starts with a coherent snapshot; the following frame contains both edits.
    const watched = yield* Effect.scoped(
      Effect.gen(function* () {
        const pull = yield* Stream.toPull(Conversation.watch(root))
        const next = Pull.catchDone(pull, () =>
          Effect.fail(new Runtime.ExampleError({ message: 'Watch ended early' })),
        )
        const initial = yield* next
        const edited = yield* harness.transaction(
          Effect.fn('tour.todos.update')(function* (tx) {
            const draft = yield* tx.doc(TodoDoc, { owner: root.id })
            const reading = draft.items[0]
            if (reading !== undefined) reading.done = true
            draft.items.push({ text: 'Build the example', done: false })
            const progress = yield* tx.doc(ProgressDoc, { owner: root.id })
            progress.stage = 'building'
            progress.completed = 1
            return yield* tx.appendEntry(root.id, {
              kind: 'tour.todo',
              data: yield* Schema.encodeEffect(TodoEvent)({
                action: 'complete',
                text: 'Read the article',
              }),
            })
          }),
        )
        const committed = yield* next
        const atomicCommit = committed.some(
          (change) =>
            change._tag === 'commit' &&
            change.frame.writes.some(
              (write) => write._tag === 'entry' && write.value.id === edited.id,
            ) &&
            change.frame.documents.some((doc) => doc.record.kind === 'tour.todos') &&
            change.frame.documents.some((doc) => doc.record.kind === 'tour.progress'),
        )
        yield* Runtime.check(
          initial[0]._tag === 'snapshot' && atomicCommit,
          'Watch must publish the entry and both document values together',
        )
        const wireChanges = yield* Effect.forEach([...initial, ...committed], (change) =>
          Schema.encodeEffect(Schema.toCodecJson(Observation.Change))(change).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
          ),
        )
        return { atomicEntryId: edited.id, atomicCommit, wireChanges }
      }),
    )

    const fork = yield* Conversation.fork(root, first.id)
    const rootTodos = yield* Conversation.document(root, TodoDoc)
    const forkTodos = yield* Conversation.document(fork, TodoDoc)
    const progress = yield* Conversation.document(root, ProgressDoc)
    if (Option.isNone(rootTodos) || Option.isNone(forkTodos) || Option.isNone(progress))
      return yield* new Runtime.ExampleError({ message: 'Expected committed document snapshots' })
    yield* Runtime.check(
      forkTodos.value.value.items.length === 1 && !forkTodos.value.value.items[0]?.done,
      'An asOf fork must inherit the Todo value at its transcript cutoff',
    )
    const reply = yield* Runtime.ask(root, 'What remains on my todo list?')
    yield* Runtime.check(
      reply.text === 'I can see the completed reading and the remaining example.',
      'The native model must receive the committed document through a managed section',
    )
    return yield* Schema.decodeEffect(Result)({
      ...watched,
      rootTodos: rootTodos.value.value,
      forkTodos: forkTodos.value.value,
      progress: progress.value.value,
      forkAt: first.id,
      managedSection: yield* Ref.get(rendered),
      modelReply: reply.text,
    })
  }),
)
