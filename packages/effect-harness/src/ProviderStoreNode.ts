/** Public provider configuration uses its own file; credentials are referenced, never embedded. */
import * as Layer from 'effect/Layer'
import * as Collection from './internal/FileCollectionStore.js'
import * as ProviderInstance from './ProviderInstance.js'
import { ProviderStore } from './ProviderStore.js'
import { ProviderError } from './ProviderError.js'
export const make = (options: { readonly path: string; readonly lockTimeoutMs?: number }) =>
  Collection.make(ProviderInstance.Stored, (reason) => new ProviderError({ reason }), options)
export const layer = (options: Parameters<typeof make>[0]) =>
  Layer.effect(ProviderStore, make(options))
