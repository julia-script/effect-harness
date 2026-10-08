/**
 * Provider prompt projections that retain native message roles and opaque protocol data.
 */
import * as Prompt from 'effect/ai/Prompt'
/**
 * Collects system messages into one leading group in transcript order.
 *
 * **When to use**
 *
 * Use when an application wants leading instructions.
 *
 * **Details**
 *
 * Keeps each original block and its provider options, including cache breakpoints. User,
 * assistant and tool messages retain their data and relative order. Native models still
 * decide which history features they support.
 *
 * @category combinators
 */
export function normalize(self: Prompt.Prompt): Prompt.Prompt {
  // effect-nit-allow P1-stdlib-collection-replacements: this public/native array may contain missing indices or inherited numeric accessors; native filter preserves HasProperty/Get and callback order, skips holes, and keeps explicit undefined distinct. Effect Array.filter visits missing slots.

  return Prompt.fromMessages([
    ...self.content.filter((message) => message.role === 'system'),
    ...self.content.filter((message) => message.role !== 'system'),
  ])
}

/**
 * Captured native Anthropic models and API-key transport composition.
 *
 * @category re-exports
 */
export { make, layer, layerConfig } from './AnthropicLanguageModel.ts'
