/**
 * Compatibility constructors and transport identity for Anthropic account clients and models.
 */
/**
 * Authenticated native Anthropic clients with account protocol adaptation.
 *
 * @category re-exports
 */
export {
  identity,
  cliVersion,
  betas,
  layer as layerClient,
  layerConfig as layerClientConfig,
} from './AnthropicAccountClient.ts'
/**
 * Authenticated native Anthropic clients with account protocol adaptation.
 *
 * @category re-exports
 */
export type { ClientOptions } from './AnthropicAccountClient.ts'
/**
 * Anthropic account model construction sharing its captured native client.
 *
 * @category re-exports
 */
export { layer, layerConfig } from './AnthropicAccountLanguageModel.ts'
