import { handler } from './handler.ts'
import { isContainer } from './json.ts'
import type { Container, Json, JsonObject } from './json.ts'
import type { Op, Path } from './ops.ts'

/**
 * The overlay of one change, ported from Chord's `delta/tracker.ts`.
 *
 * A change never copies its base. Each container it reads gets an `OverlayNode` whose proxy reads
 * through to the base and records writes beside it: object writes and deletions by key, array
 * edits as a piece table over the base elements and inserted values. Values placed into the
 * draft live in the context's `stored` table and are referenced by index, so releasing a context
 * drops them in O(1). `prepare()` turns the recorded edits into ops and materializes the next
 * revision from them.
 */

type Primitive = null | boolean | number | string
export type StoredRef = Primitive | { readonly index: number }
export type Status = 'open' | 'prepared' | 'consumed' | 'aborted' | 'stale'
export interface StatusCell {
  value: Status
}

/** Values inserted by one array edit; pieces reference ranges of it. */
export interface InsertSource {
  readonly refs: Array<StoredRef>
}
/** Where a node sits in its parent: an object key, a base-array entry, or an inserted entry. */
export type ParentKind = 0 | 1 | 2

export interface BasePiece {
  readonly kind: 'base'
  start: number
  length: number
  step: 1 | -1
}
export interface InsertPiece {
  readonly kind: 'insert'
  readonly source: InsertSource
  start: number
  length: number
  step: 1 | -1
}
export type Piece = BasePiece | InsertPiece

/** A treap node over array pieces, keyed by element count. */
export interface PieceNode {
  piece: Piece
  left: PieceNode | undefined
  right: PieceNode | undefined
  readonly priority: number
  elements: number
}

export interface PieceLocation {
  readonly piece: Piece
  readonly logicalStart: number
  readonly minimum: number
  readonly maximum: number
}

export interface ArrayPlan {
  /** Unretained base runs from the end backwards, as `start, length` pairs. */
  readonly removeRuns: ReadonlyArray<number>
  readonly permutation: Array<number> | undefined
  /** Runs of insert pieces as `logical index, first piece, end piece` triples. */
  readonly insertRuns: ReadonlyArray<number>
}

export interface ArrayOverlay {
  root: PieceNode | undefined
  pieces: Array<Piece> | undefined
  baseOverrides: Map<number, StoredRef> | undefined
  insertOverrides: Map<InsertSource, Map<number, StoredRef>> | undefined
  structural: boolean
  generation: number
  plan: ArrayPlan | undefined
  seed: number
  /** Offset into the piece the last `locatePiece` returned. */
  locatedOffset: number
  baseLocations: Array<PieceLocation> | undefined
  insertLocations: Map<InsertSource, Array<PieceLocation>> | undefined
}

/** The tracker as its contexts see it. */
export interface TrackerHost {
  releaseContext(context: OverlayContext): void
  replacementBase(context: OverlayContext): Json | undefined
}

export interface OverlayContext {
  readonly owner: object
  readonly tracker: WeakRef<TrackerHost>
  readonly baseRevision: number
  readonly status: StatusCell
  root: OverlayNode | undefined
  bases: Array<Container> | undefined
  stored: Array<Json> | undefined
  readonly dirty: Array<OverlayNode>
  readonly nodes: Array<OverlayNode>
  rawNodes: WeakMap<object, OverlayNode> | undefined
  ops: Array<Op> | undefined
  readonly replacement: boolean
  replacementNoop: boolean
  baseValue: Container | undefined
  overlayReleased: boolean
  simpleObjectMaterialization: boolean
  registryRef: WeakRef<OverlayContext> | undefined
  /** The first write the draft rejected; `prepare()` fails with it. */
  problem: string | undefined
}

interface NodeInit {
  readonly context: OverlayContext
  readonly baseIndex: number
  readonly parentIndex: number
  readonly parentKind: ParentKind
  readonly parentKey: string | number
  readonly parentSource: InsertSource | undefined
  readonly parentPlacement: boolean
  readonly target: object
}

export class OverlayNode {
  readonly context: OverlayContext
  readonly baseIndex: number
  readonly parentIndex: number
  readonly parentKind: ParentKind
  readonly parentKey: string | number
  parentSource: InsertSource | undefined
  /** Whether the node's container was placed by this change rather than read from the base. */
  readonly parentPlacement: boolean
  target: object
  proxy: object
  writeKey: string | undefined = undefined
  writeValue: StoredRef | undefined = undefined
  writes: Map<string, StoredRef> | undefined = undefined
  deleteKey: string | undefined = undefined
  deletes: Set<string> | undefined = undefined
  readded: Set<string> | undefined = undefined
  array: ArrayOverlay | undefined = undefined
  dirty = false
  subtreeDirty = false
  preparedPath: Path | undefined = undefined

  constructor(init: NodeInit) {
    this.context = init.context
    this.baseIndex = init.baseIndex
    this.parentIndex = init.parentIndex
    this.parentKind = init.parentKind
    this.parentKey = init.parentKey
    this.parentSource = init.parentSource
    this.parentPlacement = init.parentPlacement
    this.target = init.target
    this.proxy = init.target
  }
}

export const NODE = Symbol('effect-harness/delta/node')
export const RELEASED: JsonObject = {}

export interface Placement {
  readonly kind?: ParentKind
  readonly key?: string | number
  readonly source?: InsertSource | undefined
  readonly placed?: boolean
}

/** The node of `base`, created with its proxy on first access. */
export const createNode = (
  context: OverlayContext,
  base: Container,
  parent?: OverlayNode,
  placement: Placement = {},
): OverlayNode => {
  const existing = context.rawNodes!.get(base)
  if (existing !== undefined) return existing
  const target: object = Array.isArray(base) ? [] : {}
  const node = new OverlayNode({
    context,
    baseIndex: context.bases!.length,
    parentIndex: parent?.baseIndex ?? -1,
    parentKind: placement.kind ?? 0,
    parentKey: placement.key ?? '',
    parentSource: placement.source,
    parentPlacement: placement.placed ?? false,
    target,
  })
  context.bases!.push(base)
  Object.defineProperty(target, NODE, { value: node, writable: true, configurable: true })
  node.proxy = new Proxy(target, handler)
  context.rawNodes!.set(base, node)
  context.nodes.push(node)
  return node
}

export const nodeBase = (node: OverlayNode): Container => node.context.bases![node.baseIndex]!

export const nodeParent = (node: OverlayNode): OverlayNode | undefined =>
  node.parentIndex < 0 ? undefined : node.context.nodes[node.parentIndex]

export const nodeForTarget = (target: object): OverlayNode | undefined => {
  const node: unknown = Reflect.get(target, NODE)
  return node instanceof OverlayNode ? node : undefined
}

const isReference = (reference: StoredRef): reference is { readonly index: number } =>
  typeof reference === 'object' && reference !== null

export const storeValue = (context: OverlayContext, value: Json): StoredRef => {
  if (!isContainer(value)) return value
  const index = context.stored!.length
  context.stored!.push(value)
  return { index }
}

export const storedValue = (context: OverlayContext, reference: StoredRef): Json =>
  isReference(reference) ? context.stored![reference.index]! : reference

export const replaceStoredValue = (
  context: OverlayContext,
  reference: StoredRef,
  value: Json,
): StoredRef => {
  if (isContainer(value)) {
    if (!isReference(reference)) return storeValue(context, value)
    context.stored![reference.index] = value
    return reference
  }
  if (isReference(reference)) context.stored![reference.index] = RELEASED
  return value
}

export const releaseStoredValue = (context: OverlayContext, reference: StoredRef): void => {
  if (isReference(reference)) context.stored![reference.index] = RELEASED
}

export const isSettledContext = (context: OverlayContext): boolean =>
  context.overlayReleased ||
  context.status.value === 'consumed' ||
  context.status.value === 'aborted' ||
  context.status.value === 'stale'

export const isWritable = (context: OverlayContext): boolean =>
  !isSettledContext(context) && context.status.value === 'open'

/**
 * Reject a write the draft cannot represent: the draft stays unchanged and `prepare()` fails with
 * the first rejection. Returns the trap result that reports the write as handled.
 */
export const reject = (context: OverlayContext, problem: string): true => {
  context.problem ??= problem
  return true
}

export const markDirty = (node: OverlayNode): void => {
  if (node.dirty) return
  node.dirty = true
  node.context.dirty.push(node)
  for (let parent = nodeParent(node); parent !== undefined; parent = nodeParent(parent)) {
    parent.subtreeDirty = true
  }
}
