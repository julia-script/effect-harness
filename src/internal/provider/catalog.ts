/**
 * Catalog entries of the stock providers: a `Definition` plus what a provider needs to map the
 * request options for that model.
 */
import type { ModelThinkingLevel } from '../../model/Message.ts'
import type { Definition } from '../../model/Model.ts'

export interface Entry extends Definition {
  /**
   * The provider's effort for a thinking level where it differs from the level's own name; `null`
   * where the model does not have the level.
   */
  readonly efforts?: Partial<Record<ModelThinkingLevel, string | null>>
  /** Thinks adaptively: it takes an effort, never a token budget. */
  readonly adaptive?: boolean
  /** `false` when the model rejects `temperature`. */
  readonly temperature?: boolean
}

const LEVELS = ['minimal', 'low', 'medium', 'high', 'xhigh', 'max'] as const

/**
 * The thinking levels `entry` takes, as pi-ai's `getSupportedThinkingLevels`: `off` alone without
 * reasoning; otherwise every level but those mapped to `null`, with `xhigh` and `max` only where
 * mapped.
 */
export const thinkingLevels = (entry: Entry): ReadonlyArray<ModelThinkingLevel> =>
  entry.reasoning === true
    ? (['off', ...LEVELS] as const).filter((level) => {
        const mapped = entry.efforts?.[level]
        if (mapped === null) return false
        return level === 'xhigh' || level === 'max' ? mapped !== undefined : true
      })
    : ['off']

/**
 * The effort `entry` names for `level`, or for the nearest level it takes: lower first, then
 * higher. `undefined` for a model the catalog does not know.
 */
export const effort = (
  entry: Entry | undefined,
  level: (typeof LEVELS)[number],
): string | undefined => {
  if (entry === undefined) return undefined
  const supported = thinkingLevels(entry)
  const at = LEVELS.indexOf(level)
  const order = [...LEVELS.slice(0, at + 1).toReversed(), ...LEVELS.slice(at + 1)]
  const found = order.find((candidate) => supported.includes(candidate))
  if (found === undefined) return undefined
  const mapped = entry.efforts?.[found]
  return typeof mapped === 'string' ? mapped : found
}

/** The entry of `id` among `models`. */
export const find = (models: ReadonlyArray<Entry>, id: string): Entry | undefined =>
  models.find((entry) => entry.id === id)
