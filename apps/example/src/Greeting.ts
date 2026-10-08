import * as Task from 'effect-harness/Task'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Serialization from 'effect-harness/Serialization'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'

const Input = Schema.Struct({ name: Schema.String })
const Checkpoint = Schema.Struct({ phase: Schema.Literals(['prepare', 'greet']) })

/** The checkpoint commits before the next phase runs. */
export const make: Effect.Effect<Task.BoundDefinition> = Task.bind(
  Task.define<typeof Input, typeof Checkpoint, typeof Schema.String, never, never>({
    name: 'example.greeting',
    version: 1,
    input: Input,
    checkpoint: Checkpoint,
    result: Schema.String,
    initial: () => ({ phase: 'prepare' }),
    run: ({ input, checkpoint }) =>
      Effect.succeed(
        checkpoint.phase === 'prepare'
          ? Task.continueWith({ phase: 'greet' as const })
          : Task.complete(`Hello, ${input.name}`),
      ),
  }),
)

/** Remember the admitted task identity in the same commit that creates it. */
export const RunDoc = Document.defineUnsafe({
  kind: 'example.greeting',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Serialization.object(Schema.Struct({ taskId: Schema.optionalKey(Record.TaskId) })),
  initial: (): { readonly taskId?: Record.TaskId } => ({}),
})
export const Outcome = Task.outcome(Schema.String)
