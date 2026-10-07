/**
 * Compatibility constructors and transport identity for Anthropic account clients and models.
 *
 * @since 0.0.0
 */
/**
 * Forwards the supported public declarations from their owning concept.
 *
 * @category exports
 * @since 0.0.0
 */
export {
  identity,
  cliVersion,
  betas,
  layer as layerClient,
  layerConfig as layerClientConfig,
} from './AnthropicAccountClient.ts'
/**
 * Reexports the native account client construction options.
 *
 * @category exports
 * @since 0.0.0
 */
export type { ClientOptions } from './AnthropicAccountClient.ts'
/**
 * Forwards the supported public declarations from their owning concept.
 *
 * @category exports
 * @since 0.0.0
 */
export { layer, layerConfig } from './AnthropicAccountLanguageModel.ts'
