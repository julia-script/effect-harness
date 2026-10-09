/** Schema-backed durable checkpoints. A started tool intent is persisted before its handler runs. */
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Record from '../Record.js'
import * as Agent from '../Agent.js'
import * as Submission from '../Submission.js'
import * as Tool from '../Tool.js'
import * as ToolResult from '../ToolResult.js'

const shared = { submissionId: Record.SubmissionId }
const running = { ...shared, prompt: Prompt.Prompt, agent: Agent.StateSchema }
export const RunStateSchema = Schema.Union([
  Schema.TaggedStruct('queued', {
    phase: Schema.tag('queued'),
    ...shared,
    draft: Submission.DraftSchema,
  }),
  Schema.TaggedStruct('request', { phase: Schema.tag('request'), ...running }),
  Schema.TaggedStruct('tools', {
    phase: Schema.tag('tools'),
    ...running,
    calls: Schema.Array(
      Schema.Struct({ call: Tool.CallSchema, replay: Tool.ReplaySchema, started: Schema.Boolean }),
    ),
    index: Schema.Natural,
    results: Schema.Array(ToolResult.ResultSchema),
  }),
])
export type RunState = typeof RunStateSchema.Type
export const encode = Schema.encodeEffect(Schema.toCodecJson(RunStateSchema))
export const decode = Schema.decodeUnknownEffect(Schema.toCodecJson(RunStateSchema))
export const Messages = Schema.Array(Schema.toCodecJson(Prompt.Message))
