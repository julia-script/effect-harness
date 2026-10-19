import { arrayOverlay, entryValueAt } from './arrays.ts'
import { isSettledContext, nodeBase, nodeForTarget } from './context.ts'
import type { OverlayContext, OverlayNode } from './context.ts'
import { copyJson, defineData, emptyLike, Invalid, isArray, isContainer } from './json.ts'
import type { Json } from './json.ts'
import { objectKeys, objectValue } from './objects.ts'
import { piecesOf } from './pieces.ts'

/**
 * Copies out of an overlay. A placement copies its value so the draft never aliases it; a
 * materialization copies only dirty containers and shares the rest with the base.
 */

type CloneEntry = (value: Json, context: OverlayContext) => Json

/** Copy a node's current content, cloning each entry with `entry`. */
const cloneWith = (node: OverlayNode, entry: CloneEntry): Json => {
  const context = node.context
  if (isArray(nodeBase(node))) {
    const result: Array<Json> = []
    for (const piece of piecesOf(arrayOverlay(node))) {
      for (let offset = 0; offset < piece.length; offset++) {
        result.push(entry(entryValueAt(node, piece, piece.start + piece.step * offset), context))
      }
    }
    return result
  }
  const result = emptyLike(nodeBase(node))
  for (const key of objectKeys(node))
    defineData(result, key, entry(objectValue(node, key), context))
  return result
}

/** A copy of trusted strict JSON, keeping null prototypes. */
const cloneTrusted = (value: Json): Json => {
  if (!isContainer(value)) return value
  if (isArray(value)) return value.map((item) => cloneTrusted(item))
  const result = emptyLike(value)
  for (const key of Object.keys(value)) defineData(result, key, cloneTrusted(value[key]!))
  return result
}

/** A materialized entry: clean containers are shared, dirty ones copied. */
export const cloneStored: CloneEntry = (value, context) => {
  if (!isContainer(value)) return value
  const node = context.rawNodes!.get(value)
  return node === undefined || (!node.dirty && !node.subtreeDirty) ? value : cloneNode(node)
}

export const cloneNode = (node: OverlayNode): Json => cloneWith(node, cloneStored)

/** A placed entry: always a fresh copy. */
export const clonePlacementStored: CloneEntry = (value, context) => {
  if (!isContainer(value)) return value
  const node = context.rawNodes!.get(value)
  return node === undefined ? cloneTrusted(value) : cloneWith(node, clonePlacementStored)
}

/**
 * The value a placement stores: a validated copy of an external value, or the current content of
 * a draft handle. A settled handle cannot be placed.
 */
export const clonePlacement = (value: unknown): Json | Invalid => {
  const node = isContainer(value) ? nodeForTarget(value) : undefined
  if (node === undefined) return copyJson(value)
  if (isSettledContext(node.context)) return new Invalid('Cannot use a settled overlay')
  return cloneWith(node, clonePlacementStored)
}
