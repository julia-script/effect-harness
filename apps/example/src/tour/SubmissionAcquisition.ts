import { NodeRuntime, NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import * as Record from 'effect-harness/Record'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as DemoModel from '../DemoModel.js'

// From the workspace root: bun run build && node apps/example/dist/tour/SubmissionAcquisition.js
// Each provide builds a new scoped runtime and SQLite connection. Only the ID is saved.
export const program = Effect.scoped(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'saved-submission-' })
    const idFile = path.join(directory, 'submission.json')
    const idCodec = Schema.fromJsonString(Record.SubmissionId)
    const client = Harness.layerLocal().pipe(
      Layer.provide(DemoModel.layer),
      Layer.provide(
        Storage.layerSql.pipe(
          Layer.provide(SqliteClient.layer({ filename: path.join(directory, 'agent.sqlite') })),
        ),
      ),
    )
    const savedId = yield* Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* Harness.Harness
        const root = yield* harness.root
        const submission = yield* Conversation.submit(root, { type: 'input', content: 'hello' })
        yield* Submission.wait(submission)
        return submission.id
      }).pipe(Effect.provide(client)),
    )
    yield* fs.writeFileString(idFile, yield* Schema.encodeEffect(idCodec)(savedId))

    return yield* Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* Harness.Harness
        const id = yield* Schema.decodeEffect(idCodec)(yield* fs.readFileString(idFile))
        const submission = yield* harness.submission(id)
        // Opening its original conversation also resumes pending work after a restart.
        Option.getOrThrow(yield* harness.conversation(submission.conversationId))
        return yield* Submission.wait(submission)
      }).pipe(Effect.provide(client)),
    )
  }),
).pipe(Effect.provide(NodeServices.layer))

NodeRuntime.runMain(program.pipe(Effect.flatMap(Effect.log)))
