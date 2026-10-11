/** One registered supervisor; the existing scheduler executes and drains its owned work. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Agent from '../Agent.js'
import * as Record from '../Record.js'
import * as Subagent from '../Subagent.js'
import * as Task from '../Task.js'
import { TaskExecution } from '../TaskExecution.js'
import * as Invocation from './ConversationInvocation.js'

export const kind = 'harness.subagent'
export const InputSchema = Schema.Struct({
  originTaskId: Record.TaskId,
  originConversationId: Record.ConversationId,
  key: Schema.NonEmptyString,
  mode: Schema.Literals(['foreground', 'background']),
  draft: Subagent.RequestSchema.fields.draft,
  options: Schema.optionalKey(Subagent.OptionsSchema),
  inherited: Agent.StateSchema,
  restartPolicy: Schema.Literals(['resume', 'abandon']),
})
export type Input = typeof InputSchema.Type
export const CheckpointSchema = Schema.Union([
  Schema.Struct({ phase: Schema.tag('create') }),
  Schema.Struct({ phase: Schema.tag('observe'), ...Subagent.AttachmentSchema.fields }),
])
export type Definition = Task.Any & {
  readonly input: typeof InputSchema
  readonly checkpoint: typeof CheckpointSchema
  readonly success: typeof Subagent.ReceiptSchema
  readonly failure: typeof Subagent.ReceiptSchema
}
export const contract = {
  kind,
  version: 1,
  input: InputSchema,
  checkpoint: CheckpointSchema,
  success: Subagent.ReceiptSchema,
  failure: Subagent.ReceiptSchema,
} as const
export const make = () =>
  Task.make({
    ...contract,
    initial: () => ({ phase: 'create' as const }),
    phases: {
      create: () =>
        Effect.gen(function* () {
          const execution = yield* TaskExecution
          yield* Invocation.initializeOwned(execution)
        }),
      observe: ({ checkpoint }) =>
        Effect.gen(function* () {
          const execution = yield* TaskExecution
          const receipt = yield* Invocation.observeOwned(execution, checkpoint)
          yield* execution.commit((tx) =>
            execution.progress(tx, contract, {
              status: 'completing',
              outcome:
                receipt.submission.status === 'done'
                  ? { status: 'completed', result: receipt }
                  : {
                      status: 'failed',
                      error: { message: 'The child submission was unanswered' },
                      result: receipt,
                    },
            }),
          )
        }),
    },
    abort: () =>
      Effect.gen(function* () {
        const execution = yield* TaskExecution
        yield* execution.commit((tx) =>
          execution.progress(tx, contract, {
            status: 'completing',
            outcome: { status: 'aborted' },
          }),
        )
      }),
  })
