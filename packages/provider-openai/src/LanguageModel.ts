/**
 * Compatibility forwarding paths for concrete provider language models.
 *
 * @since 0.0.0
 */
/**
 * Forwards the supported public declarations from their owning concept.
 *
 * @category exports
 * @since 0.0.0
 */
export { make, layer, layerApiKey, layerApiKeyConfig, layerConfig } from './OpenAiLanguageModel.ts'
/**
 * Forwards the supported public declarations from their owning concept.
 *
 * @category exports
 * @since 0.0.0
 */
export {
  layer as layerChatGptClient,
  layerConfig as layerChatGptClientConfig,
} from './ChatGptClient.ts'
/**
 * Forwards the supported public declarations from their owning concept.
 *
 * @category exports
 * @since 0.0.0
 */
export { layer as layerChatGpt, layerConfig as layerChatGptConfig } from './ChatGptLanguageModel.ts'
