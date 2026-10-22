/**
 * Where everything in a diagram goes. Graphs are laid out by ELK's layered algorithm; sequences
 * by hand, since they are columns and rows. Labels are set in a monospace font, so their width is
 * their length times the advance and the build needs no font metrics.
 */
import ELK from 'elkjs/lib/elk.bundled.js'
import type { ElkExtendedEdge, ElkLabel, ElkNode } from 'elkjs/lib/elk-api'
import type { Graph, GraphNode, Line, Sequence, Shape, Step } from './parse.ts'

export const FONT_SIZE = 12
export const LABEL_SIZE = 11
export const LINE_HEIGHT = 16
const ADVANCE = 0.6

export const textWidth = (text: string, size = FONT_SIZE): number =>
  Math.ceil(text.length * size * ADVANCE)

const widest = (lines: ReadonlyArray<Line>): number =>
  Math.max(0, ...lines.map((line) => textWidth(line.text)))

export interface Point {
  readonly x: number
  readonly y: number
}

export interface Box extends Point {
  readonly width: number
  readonly height: number
}

// GRAPHS

export interface PlacedNode extends Box {
  readonly id: string
  readonly lines: ReadonlyArray<Line>
  readonly shape: Shape
}

export interface PlacedGroup extends Box {
  readonly label: string
}

export interface PlacedEdge {
  readonly from: string
  readonly to: string
  readonly points: ReadonlyArray<Point>
  readonly label: (Box & { readonly text: string }) | undefined
  readonly isDashed: boolean
  readonly isBidirectional: boolean
}

export interface PlacedGraph {
  readonly _tag: 'Graph'
  readonly width: number
  readonly height: number
  readonly nodes: ReadonlyArray<PlacedNode>
  readonly groups: ReadonlyArray<PlacedGroup>
  readonly edges: ReadonlyArray<PlacedEdge>
}

const sizeOf = (node: GraphNode): { readonly width: number; readonly height: number } => {
  const text = { width: widest(node.lines), height: node.lines.length * LINE_HEIGHT }
  switch (node.shape) {
    case 'start':
    case 'end':
      return { width: 16, height: 16 }
    case 'text':
      return { width: text.width + 8, height: text.height + 6 }
    case 'diamond':
      return { width: text.width + 56, height: text.height + 36 }
    case 'circle': {
      const diameter = Math.max(text.width + 28, text.height + 28)
      return { width: diameter, height: diameter }
    }
    case 'cylinder':
      return { width: text.width + 32, height: text.height + 34 }
    default:
      return { width: text.width + 28, height: text.height + 18 }
  }
}

const GROUP_PADDING = '[top=34,left=16,bottom=16,right=16]'

/** Options every level of a graph shares; ELK does not pass them down to groups. */
const LAYERED = {
  'elk.algorithm': 'layered',
  // Keep the order the diagram was written in: it decides which edges of a cycle point back,
  // and the order of siblings.
  'elk.layered.cycleBreaking.strategy': 'DEPTH_FIRST',
  'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
  'elk.edgeRouting': 'ORTHOGONAL',
  'elk.spacing.nodeNode': '28',
  'elk.layered.spacing.nodeNodeBetweenLayers': '56',
  'elk.layered.spacing.edgeNodeBetweenLayers': '24',
  'elk.spacing.edgeLabel': '4',
  'elk.edgeLabels.placement': 'CENTER',
  'elk.layered.nodePlacement.strategy': 'BRANDES_KOEPF',
} as const

/**
 * Bun defines `self` but not `document`, so ELK's worker file takes itself for a real Web Worker and
 * skips the in-process fallback ELK needs; `self` is hidden while ELK starts.
 */
const startElk = () => {
  const self = Object.getOwnPropertyDescriptor(globalThis, 'self')
  if (self === undefined || 'document' in globalThis) {
    return new ELK()
  }
  Reflect.deleteProperty(globalThis, 'self')
  try {
    return new ELK()
  } finally {
    Object.defineProperty(globalThis, 'self', self)
  }
}

const elk = startElk()

/** The chain of groups a node sits in, innermost first. */
const ancestry = (graph: Graph, id: string | undefined): ReadonlyArray<string> => {
  const parent =
    graph.nodes.find((node) => node.id === id)?.parent ??
    graph.groups.find((group) => group.id === id)?.parent
  return parent === undefined ? [] : [parent, ...ancestry(graph, parent)]
}

/** ELK expects an edge in the innermost group holding both ends. */
const containerOf = (graph: Graph, from: string, to: string): string | undefined => {
  const toAncestors = ancestry(graph, to)
  return ancestry(graph, from).find((group) => toAncestors.includes(group))
}

export const layoutGraph = async (graph: Graph): Promise<PlacedGraph> => {
  const edgeIds = graph.edges.map((_, index) => `e${index}`)
  const crossesGroups = graph.edges.some(
    (edge) => ancestry(graph, edge.from)[0] !== ancestry(graph, edge.to)[0],
  )

  const edgesIn = (container: string | undefined): Array<ElkExtendedEdge> =>
    graph.edges.flatMap((edge, index) =>
      containerOf(graph, edge.from, edge.to) === container
        ? [
            {
              id: edgeIds[index] ?? `e${index}`,
              sources: [edge.from],
              targets: [edge.to],
              labels:
                edge.label === undefined
                  ? []
                  : [
                      {
                        text: edge.label,
                        width: textWidth(edge.label, LABEL_SIZE) + 12,
                        height: LINE_HEIGHT + 4,
                      },
                    ],
            },
          ]
        : [],
    )

  const childrenOf = (parent: string | undefined): Array<ElkNode> => [
    ...graph.groups
      .filter((group) => group.parent === parent)
      .map((group): ElkNode => ({
        id: group.id,
        layoutOptions: {
          'elk.padding': GROUP_PADDING,
          ...(crossesGroups
            ? {}
            : { ...LAYERED, 'elk.direction': group.direction ?? graph.direction }),
        },
        children: childrenOf(group.id),
        edges: edgesIn(group.id),
      })),
    ...graph.nodes
      .filter((node) => node.parent === parent)
      .map((node): ElkNode => ({ id: node.id, ...sizeOf(node) })),
  ]

  const input: ElkNode = {
    id: 'root',
    layoutOptions: {
      'elk.direction': graph.direction,
      // Edges across groups need one layout of the whole hierarchy; without them, each group
      // is laid out on its own and can keep its own direction.
      'elk.hierarchyHandling': crossesGroups ? 'INCLUDE_CHILDREN' : 'SEPARATE_CHILDREN',
      ...LAYERED,
      'elk.padding': '[top=8,left=8,bottom=8,right=8]',
    },
    children: childrenOf(undefined),
    edges: edgesIn(undefined),
  }
  const laidOut = await elk.layout(input)

  const nodes: Array<PlacedNode> = []
  const groups: Array<PlacedGroup> = []
  const edges: Array<PlacedEdge> = []

  const visit = (elkNode: ElkNode, origin: Point) => {
    ;(elkNode.edges ?? []).forEach((elkEdge) => {
      const index = edgeIds.indexOf(elkEdge.id)
      const edge = graph.edges[index]
      const section = elkEdge.sections?.[0]
      if (!edge || !section) {
        return
      }
      const shift = (point: Point): Point => ({ x: point.x + origin.x, y: point.y + origin.y })
      const label: ElkLabel | undefined = elkEdge.labels?.[0]
      edges.push({
        from: edge.from,
        to: edge.to,
        points: [section.startPoint, ...(section.bendPoints ?? []), section.endPoint].map(shift),
        label:
          label && edge.label !== undefined
            ? {
                text: edge.label,
                x: (label.x ?? 0) + origin.x,
                y: (label.y ?? 0) + origin.y,
                width: label.width ?? 0,
                height: label.height ?? 0,
              }
            : undefined,
        isDashed: edge.isDashed,
        isBidirectional: edge.isBidirectional,
      })
    })
    ;(elkNode.children ?? []).forEach((child) => {
      const box = {
        x: (child.x ?? 0) + origin.x,
        y: (child.y ?? 0) + origin.y,
        width: child.width ?? 0,
        height: child.height ?? 0,
      }
      const group = graph.groups.find(({ id }) => id === child.id)
      if (group) {
        groups.push({ ...box, label: group.label })
        visit(child, box)
        return
      }
      const node = graph.nodes.find(({ id }) => id === child.id)
      if (node) {
        nodes.push({ ...box, id: node.id, lines: node.lines, shape: node.shape })
      }
    })
  }
  visit(laidOut, { x: 0, y: 0 })

  return {
    _tag: 'Graph',
    width: Math.ceil(laidOut.width ?? 0),
    height: Math.ceil(laidOut.height ?? 0),
    nodes,
    groups,
    edges,
  }
}

// SEQUENCES

export interface PlacedParticipant extends Box {
  readonly label: string
}

export type PlacedStep =
  | {
      readonly _tag: 'Message'
      readonly fromX: number
      readonly toX: number
      readonly y: number
      readonly text: string
      readonly isReply: boolean
    }
  | (Box & { readonly _tag: 'Note'; readonly text: string })

export interface PlacedSequence {
  readonly _tag: 'Sequence'
  readonly width: number
  readonly height: number
  readonly isNumbered: boolean
  readonly participants: ReadonlyArray<PlacedParticipant>
  readonly lifelineEnd: number
  readonly steps: ReadonlyArray<PlacedStep>
}

const MARGIN = 8
const HEAD_HEIGHT = 34
const MIN_GAP = 120
const BADGE = 22
const MESSAGE_ROW = 44
const SELF_ROW = 58
const NOTE_ROW = 44

export const layoutSequence = (sequence: Sequence): PlacedSequence => {
  const widths = sequence.participants.map(({ label }) => textWidth(label) + 28)
  const indexOf = (id: string) =>
    sequence.participants.findIndex((participant) => participant.id === id)
  const gaps = widths
    .slice(1)
    .map((width, index) => Math.max(MIN_GAP, ((widths[index] ?? 0) + width) / 2 + 28))
  const labelRoom = (step: Step) =>
    step._tag === 'Message'
      ? textWidth(step.text, LABEL_SIZE) + (sequence.isNumbered ? BADGE + 8 : 0) + 40
      : textWidth(step.text, LABEL_SIZE) + 40
  const spanOf = (step: Step): readonly [number, number] => {
    const ends = step._tag === 'Message' ? [step.from, step.to] : step.over
    const indices = ends.map(indexOf)
    return [Math.min(...indices), Math.max(...indices)]
  }
  // Widen the gaps until every label fits over its arrow, narrowest spans first.
  const trailing = [0]
  ;[...sequence.steps]
    .sort((a, b) => spanOf(a)[1] - spanOf(a)[0] - (spanOf(b)[1] - spanOf(b)[0]))
    .forEach((step) => {
      const [low, high] = spanOf(step)
      const room = labelRoom(step)
      if (low === high) {
        if (step._tag === 'Note') {
          return
        }
        if (low < gaps.length) {
          gaps[low] = Math.max(gaps[low] ?? 0, room + 16)
        } else {
          trailing[0] = Math.max(trailing[0] ?? 0, room)
        }
        return
      }
      const current = gaps.slice(low, high).reduce((sum, gap) => sum + gap, 0)
      if (current < room) {
        gaps[high - 1] = (gaps[high - 1] ?? 0) + room - current
      }
    })

  const firstCenter = MARGIN + (widths[0] ?? 0) / 2
  const centers = gaps.reduce<Array<number>>(
    (all, gap) => [...all, (all.at(-1) ?? firstCenter) + gap],
    [firstCenter],
  )
  const participants = sequence.participants.map(({ label }, index): PlacedParticipant => ({
    label,
    x: (centers[index] ?? 0) - (widths[index] ?? 0) / 2,
    y: MARGIN,
    width: widths[index] ?? 0,
    height: HEAD_HEIGHT,
  }))

  const top = MARGIN + HEAD_HEIGHT + 28
  const placed = sequence.steps.reduce<{
    readonly y: number
    readonly steps: ReadonlyArray<PlacedStep>
  }>(
    (state, step) => {
      if (step._tag === 'Note') {
        const [low, high] = spanOf(step)
        const left = (centers[low] ?? 0) - 48
        const right = (centers[high] ?? 0) + 48
        const width = Math.max(right - left, textWidth(step.text, LABEL_SIZE) + 24)
        const middle = (left + right) / 2
        return {
          y: state.y + NOTE_ROW,
          steps: [
            ...state.steps,
            { _tag: 'Note', text: step.text, x: middle - width / 2, y: state.y, width, height: 28 },
          ],
        }
      }
      const fromX = centers[indexOf(step.from)] ?? 0
      const toX = centers[indexOf(step.to)] ?? 0
      const row = fromX === toX ? SELF_ROW : MESSAGE_ROW
      return {
        y: state.y + row,
        steps: [
          ...state.steps,
          { _tag: 'Message', fromX, toX, y: state.y + 18, text: step.text, isReply: step.isReply },
        ],
      }
    },
    { y: top, steps: [] },
  )

  const right = Math.max(
    ...participants.map((participant) => participant.x + participant.width),
    (centers.at(-1) ?? 0) + (trailing[0] ?? 0),
    ...placed.steps.map((step) => (step._tag === 'Note' ? step.x + step.width : 0)),
  )
  const left = Math.min(0, ...placed.steps.map((step) => (step._tag === 'Note' ? step.x : 0)))
  const shift = left < 0 ? MARGIN - left : 0
  return {
    _tag: 'Sequence',
    width: Math.ceil(right + MARGIN + shift),
    height: Math.ceil(placed.y + MARGIN),
    isNumbered: sequence.isNumbered,
    participants: participants.map((participant) => ({ ...participant, x: participant.x + shift })),
    lifelineEnd: placed.y - 8,
    steps: placed.steps.map((step) =>
      step._tag === 'Note'
        ? { ...step, x: step.x + shift }
        : { ...step, fromX: step.fromX + shift, toX: step.toX + shift },
    ),
  }
}
