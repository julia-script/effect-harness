/** Failures owned by model discovery and selection; source errors retain their original types. */
import * as Data from 'effect/Data'

export class ModelCatalogError extends Data.TaggedError('ModelCatalogError')<{
  readonly reason: 'providerNotFound' | 'modelNotFound' | 'invalidMetadata' | 'unsupportedOptions'
  readonly message: string
  readonly provider?: string
  readonly modelId?: string
  readonly cause?: unknown
}> {}
