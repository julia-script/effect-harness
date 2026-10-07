/**
 * Node public contracts.
 */
/**
 * Node environment construction with injected platform services and decoded host
 * configuration.
 *
 * @category re-exports
 */
export * from '../NodeEnv.ts'
/**
 * Scoped Node filesystem capabilities absent from portable FileSystem.
 *
 * @category re-exports
 */
export { layerNative, fileError } from '../NodeNativeFiles.ts'
