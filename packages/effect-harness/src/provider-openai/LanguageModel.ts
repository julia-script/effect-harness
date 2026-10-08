/**
 * Compatibility forwarding paths for concrete provider language models.
 */
/**
 * Captured native OpenAI models and API-key transport composition.
 *
 * @category re-exports
 */
export { make, layer, layerApiKey, layerApiKeyConfig, layerConfig } from './OpenAiLanguageModel.ts'
/**
 * Authenticated ChatGPT Responses clients with semantic terminal-event validation.
 *
 * @category re-exports
 */
export {
  layer as layerChatGptClient,
  layerConfig as layerChatGptClientConfig,
} from './ChatGptClient.ts'
/**
 * ChatGPT account model construction sharing its captured Responses client.
 *
 * @category re-exports
 */
export { layer as layerChatGpt, layerConfig as layerChatGptConfig } from './ChatGptLanguageModel.ts'
