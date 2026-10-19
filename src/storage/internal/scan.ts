import { Effect } from 'effect'
import { StorageRequestError } from '../StorageError.ts'
import type { Cursor, ScanOrder } from '../schema/Scan.ts'

/** Where a built-in storage scan starts: its order and the last ID a previous page returned. */
export interface ScanStart {
  readonly order: ScanOrder
  readonly after: number | undefined
}

const isScanOrder = (value: unknown): value is ScanOrder =>
  value === 'ascending' || value === 'descending'

const invalid = (message: string) => Effect.fail(new StorageRequestError({ message }))

/**
 * Resolve a scan's order and position. A cursor continues in the order it was created with,
 * whether the query repeats that order or omits it; a different `order` is rejected. A cursor
 * without an order continues in the scan's default order.
 */
export const scanStart = (
  requested: ScanOrder | undefined,
  cursor: Cursor | undefined,
  fallback: ScanOrder,
): Effect.Effect<ScanStart, StorageRequestError> => {
  if (requested !== undefined && !isScanOrder(requested)) {
    return invalid(`Invalid scan order: ${String(requested)}`)
  }
  if (cursor === undefined)
    return Effect.succeed({ order: requested ?? fallback, after: undefined })
  const { after, order: stored } = cursor
  if (typeof after !== 'number' || !Number.isSafeInteger(after)) {
    return invalid('Invalid storage cursor')
  }
  if (stored !== undefined && !isScanOrder(stored)) return invalid('Invalid storage cursor')
  const order = stored ?? fallback
  if (requested !== undefined && requested !== order) {
    return invalid(`The cursor continues a ${order} scan; the query asks for ${requested}`)
  }
  return Effect.succeed({ order, after })
}

/** The continuation of a scan whose last returned item has `id`. */
export const nextCursor = (id: number, order: ScanOrder): Cursor => ({ after: id, order })
