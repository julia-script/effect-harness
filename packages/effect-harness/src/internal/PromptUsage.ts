/** Latest successful input measurement. A rewritten prefix or tool set invalidates it. */
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Document from '../Document.js'
import * as Projection from '../ContextProjection.js'
import * as Model from '../Model.js'
import type * as Prepared from '../PreparedRequest.js'
import { target } from './ProviderState.js'
export { target }
export const Baseline = Schema.Struct({
  identity: Schema.String,
  model: Schema.optionalKey(Model.SelectionSchema),
  prompt: Prompt.Prompt,
  tools: Schema.Array(Projection.ToolDeclarationSchema),
  inputTokens: Schema.Natural,
})
export type Baseline = typeof Baseline.Type
export const document = Document.define({
  kind: 'harness.promptUsage',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Schema.Struct({ baseline: Schema.optionalKey(Schema.toCodecJson(Baseline)) }),
  initial: () => ({}),
})
const sameMessages = Schema.toEquivalence(Schema.Array(Prompt.Message))
const sameTools = Schema.toEquivalence(Schema.Array(Projection.ToolDeclarationSchema))
/** Provider input totals already include cache reads/writes. Never add their subcounts again. */
export const applicable = (
  baseline: Baseline,
  prompt: Prompt.Prompt,
  tools: ReadonlyArray<Projection.ToolDeclaration>,
): boolean =>
  sameTools(baseline.tools, tools) &&
  baseline.prompt.content.length <= prompt.content.length &&
  sameMessages(baseline.prompt.content, prompt.content.slice(0, baseline.prompt.content.length))

/** Include strictness and binding changes, even when visible declaration JSON is unchanged. */
export const identity = (
  providerIdentity: string,
  tools: ReadonlyArray<Prepared.Snapshot['tools'][number]>,
): string => JSON.stringify([providerIdentity, tools])
