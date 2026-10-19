import type { Schema } from 'effect'
import { cast } from 'effect/Function'

/**
 * `value` as strict JSON: object properties set to `undefined` are left out, as hosts may pass
 * them for optional fields while document drafts take strict JSON only.
 */
export const strictJson = <T extends Schema.Json>(value: T): T => cast(strict(value))

const strict = (value: Schema.Json): Schema.Json => {
  if (typeof value !== 'object' || value === null) return value
  if (Array.isArray(value)) {
    return cast<Schema.Json, ReadonlyArray<Schema.Json>>(value).map((child) => strict(child))
  }
  const copy: Record<string, Schema.Json> = {}
  for (const [key, child] of Object.entries(value)) {
    if (child !== undefined) copy[key] = strict(child)
  }
  return copy
}
