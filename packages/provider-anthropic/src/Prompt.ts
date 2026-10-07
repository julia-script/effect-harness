/**
 * Provider prompt projections that retain native message roles and opaque protocol data.
 *
 * @since 0.0.0
 */
import * as Prompt from 'effect/ai/Prompt'
/**
 * Collects system messages into one leading group in transcript order.
 *
 * **When to use**
 *
 * Use this projection when an application wants leading instructions.
 *
 * **Details**
 *
 * Native models handle their own history capabilities. Keeping each original
 * block preserves provider options, including cache breakpoints, while
 * all user, assistant and tool messages retain their original data and ordering.
 *
 * @category combinators
 * @since 0.0.0
 */
export function normalize(self: Prompt.Prompt): Prompt.Prompt {
  return Prompt.fromMessages([
    ...self.content.filter((message) => message.role === 'system'),
    ...self.content.filter((message) => message.role !== 'system'),
  ])
}

/**
 * Forwards the supported public declarations from their owning concept.
 *
 * @category exports
 * @since 0.0.0
 */
export { make, layer, layerConfig } from './AnthropicLanguageModel.ts'
