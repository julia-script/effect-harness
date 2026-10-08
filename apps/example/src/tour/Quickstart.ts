/** SQLite, portable coding tools and a host-owned working directory. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Conversation from 'effect-harness/Conversation'
import * as Identity from 'effect-harness/Identity'
import * as MutationLocks from 'effect-harness/MutationLocks'
import * as NodeEnv from 'effect-harness/NodeEnv'
import { Persistence } from 'effect-harness/Persistence'
import * as SqliteBun from 'effect-harness/storage/SqliteBun'
import * as CodingTools from 'effect-harness/tools/CodingTools'
import * as Runtime from './Runtime.ts'

export const Result = Schema.Struct({
  answer: Schema.String,
  sameSubmission: Schema.Boolean,
  toolEntries: Schema.Int,
  storage: Schema.Literal('SQLite'),
  selectedTools: Schema.Array(Schema.String),
})

export const run = Effect.scoped(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-tour-start-' })
    yield* fs.writeFileString(
      path.join(directory, 'notes.txt'),
      'The deployment needs a smoke test.\n',
    )
    const environment = NodeEnv.layer({ id: 'tour-workspace', cwd: directory })
    const capabilities = yield* Layer.build(Layer.merge(environment, MutationLocks.layer))
    const tools = yield* CodingTools.make().pipe(Effect.provideContext(capabilities))
    const storage = yield* Layer.build(
      SqliteBun.layer({ filename: path.join(directory, 'agent.sqlite') }),
    )
    const provider: Runtime.Provider = {
      generateText: () =>
        Effect.succeed([{ type: 'text', text: 'summary' }, Runtime.finish('stop')]),
      streamText: ({ prompt }) => {
        const read = prompt.content.some((message) => message.role === 'tool')
        return read
          ? Runtime.answer('The deployment needs a smoke test.')
          : Stream.fromIterable([
              {
                type: 'tool-call' as const,
                id: 'read-notes',
                name: 'read',
                params: { path: 'notes.txt' },
                providerExecuted: false,
              },
              Runtime.finish('tool-calls'),
            ])
      },
    }
    const { harness } = yield* Runtime.open({
      store: Context.get(storage, Persistence),
      extensions: [tools],
      provider,
      agent: { cwd: directory, tools: ['read'] },
    })
    const root = yield* harness.root
    const requestId = Identity.RequestId.make('read-notes-v1')
    const first = yield* Runtime.ask(root, 'Read notes.txt', { requestId })
    const same = yield* Runtime.ask(root, 'Read notes.txt', { requestId })
    const snapshot = yield* Conversation.snapshot(root)
    yield* Runtime.check(
      first.submission.id === same.submission.id,
      'A retry created a new submission',
    )
    const reads = snapshot.entries.filter((entry) => entry.kind === 'harness.tool')
    yield* Runtime.check(
      reads.length === 1 && JSON.stringify(reads[0]?.model).includes('smoke test'),
      'The coding tool did not read the workspace file',
    )
    return yield* Schema.decodeEffect(Result)({
      answer: first.text,
      sameSubmission: first.submission.id === same.submission.id,
      toolEntries: reads.length,
      storage: 'SQLite',
      selectedTools: ['read'],
    })
  }),
)
