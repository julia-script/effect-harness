/**
 * Draws a laid-out diagram as SVG markup. Colors and motion live in the site's stylesheet (the
 * `dg-` classes); what depends on one diagram, such as which edges a hovered node lights up and
 * how long a sequence's walk-through takes, goes in a small style block of its own.
 */
import { createHash } from 'node:crypto'
import type { Line } from './parse.ts'
import {
  FONT_SIZE,
  LABEL_SIZE,
  LINE_HEIGHT,
  type PlacedGraph,
  type PlacedNode,
  type PlacedSequence,
  type Point,
} from './layout.ts'

const escape = (text: string): string =>
  text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')

const round = (value: number): number => Math.round(value * 10) / 10

const svg = (id: string, width: number, height: number, style: string, body: string): string =>
  `<svg xmlns="http://www.w3.org/2000/svg" class="dg" id="${id}" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="img" aria-label="Diagram"><style>${style}</style>${body}</svg>`

export const idOf = (source: string): string =>
  `dg-${createHash('sha256').update(source).digest('hex').slice(0, 8)}`

const TONE_CLASS: Readonly<Record<Line['tone'], string>> = {
  strong: 'dg-strong',
  normal: 'dg-text',
  muted: 'dg-muted',
}

const textBlock = (lines: ReadonlyArray<Line>, cx: number, cy: number): string => {
  const first = cy - ((lines.length - 1) * LINE_HEIGHT) / 2
  return `<text x="${round(cx)}" text-anchor="middle" font-size="${FONT_SIZE}">${lines
    .map(
      (line, index) =>
        `<tspan class="${TONE_CLASS[line.tone]}" x="${round(cx)}" y="${round(first + index * LINE_HEIGHT)}" dy="0.35em">${escape(line.text)}</tspan>`,
    )
    .join('')}</text>`
}

// EDGES

const towards = (from: Point, to: Point, distance: number): Point => {
  const length = Math.hypot(to.x - from.x, to.y - from.y) || 1
  return {
    x: from.x + ((to.x - from.x) / length) * distance,
    y: from.y + ((to.y - from.y) / length) * distance,
  }
}

const ARROW_LENGTH = 7
const ARROW_HALF = 3.5

/** A filled arrowhead with its tip on `tip`, pointing away from `from`. */
const arrowhead = (from: Point, tip: Point): string => {
  const base = towards(tip, from, ARROW_LENGTH)
  const length = Math.hypot(tip.x - from.x, tip.y - from.y) || 1
  const nx = (-(tip.y - from.y) / length) * ARROW_HALF
  const ny = ((tip.x - from.x) / length) * ARROW_HALF
  return `<path class="dg-head" d="M${round(tip.x)} ${round(tip.y)}L${round(base.x + nx)} ${round(base.y + ny)}L${round(base.x - nx)} ${round(base.y - ny)}Z"/>`
}

/** An orthogonal route with its corners rounded. */
const routePath = (points: ReadonlyArray<Point>): string => {
  const [start, ...rest] = points
  if (!start) {
    return ''
  }
  const corners = rest.slice(0, -1).map((corner, index) => {
    const before = points[index] ?? corner
    const after = points[index + 2] ?? corner
    const radius = Math.min(
      8,
      Math.hypot(corner.x - before.x, corner.y - before.y) / 2,
      Math.hypot(after.x - corner.x, after.y - corner.y) / 2,
    )
    const enter = towards(corner, before, radius)
    const leave = towards(corner, after, radius)
    return `L${round(enter.x)} ${round(enter.y)}Q${round(corner.x)} ${round(corner.y)} ${round(leave.x)} ${round(leave.y)}`
  })
  const end = rest.at(-1) ?? start
  return `M${round(start.x)} ${round(start.y)}${corners.join('')}L${round(end.x)} ${round(end.y)}`
}

/** The route, pulled back from each end that carries an arrowhead. */
const trimmed = (points: ReadonlyArray<Point>, atStart: boolean): ReadonlyArray<Point> => {
  const first = points[0]
  const second = points[1]
  const last = points.at(-1)
  const beforeLast = points.at(-2)
  if (!first || !second || !last || !beforeLast) {
    return points
  }
  const middle = points.slice(1, -1)
  const head = atStart ? towards(first, second, ARROW_LENGTH - 1) : first
  const tail = towards(last, beforeLast, ARROW_LENGTH - 1)
  return points.length === 2 ? [head, tail] : [head, ...middle, tail]
}

// GRAPHS

/**
 * ELK routes to a node's bounding box. For a circle or a diamond, slide the end of the route
 * along its last segment until it meets the outline.
 */
const onOutline = (point: Point, neighbor: Point, node: PlacedNode | undefined): Point => {
  if (!node || (node.shape !== 'circle' && node.shape !== 'diamond')) {
    return point
  }
  const cx = node.x + node.width / 2
  const cy = node.y + node.height / 2
  const rx = node.width / 2
  const ry = node.height / 2
  const reach = (offset: number, along: number, across: number) =>
    node.shape === 'circle'
      ? along * Math.sqrt(Math.max(0, 1 - (offset / across) ** 2))
      : along * Math.max(0, 1 - Math.abs(offset) / across)
  if (point.y === neighbor.y) {
    return { x: cx + Math.sign(point.x - cx) * reach(point.y - cy, rx, ry), y: point.y }
  }
  if (point.x === neighbor.x) {
    return { x: point.x, y: cy + Math.sign(point.y - cy) * reach(point.x - cx, ry, rx) }
  }
  return point
}

const shapeOf = (node: PlacedNode): string => {
  const { x, y, width, height } = node
  const cx = x + width / 2
  const cy = y + height / 2
  switch (node.shape) {
    case 'start':
      return `<circle class="dg-dot" cx="${round(cx)}" cy="${round(cy)}" r="6"/>`
    case 'end':
      return `<circle class="dg-shape" cx="${round(cx)}" cy="${round(cy)}" r="7.5"/><circle class="dg-dot" cx="${round(cx)}" cy="${round(cy)}" r="4"/>`
    case 'text':
      return `<rect class="dg-hit" x="${x}" y="${y}" width="${width}" height="${height}"/>`
    case 'diamond':
      return `<path class="dg-shape" d="M${round(cx)} ${y}L${x + width} ${round(cy)}L${round(cx)} ${y + height}L${x} ${round(cy)}Z"/>`
    case 'circle':
      return `<ellipse class="dg-shape dg-accent" cx="${round(cx)}" cy="${round(cy)}" rx="${width / 2}" ry="${height / 2}"/>`
    case 'cylinder': {
      const rim = 6
      return `<path class="dg-shape" d="M${x} ${y + rim}a${width / 2} ${rim} 0 0 1 ${width} 0v${height - rim * 2}a${width / 2} ${rim} 0 0 1 ${-width} 0Z"/><path class="dg-rim" d="M${x} ${y + rim}a${width / 2} ${rim} 0 0 0 ${width} 0"/>`
    }
    case 'stadium':
      return `<rect class="dg-shape" x="${x}" y="${y}" width="${width}" height="${height}" rx="${round(height / 2)}"/>`
    case 'round':
      return `<rect class="dg-shape" x="${x}" y="${y}" width="${width}" height="${height}" rx="12"/>`
    case 'rect':
      return `<rect class="dg-shape" x="${x}" y="${y}" width="${width}" height="${height}" rx="6"/>`
  }
}

const nodeMarkup = (node: PlacedNode, key: string): string => {
  const cx = node.x + node.width / 2
  const cy = node.y + node.height / 2 + (node.shape === 'cylinder' ? 3 : 0)
  return `<g class="dg-node" data-node="${key}">${shapeOf(node)}${textBlock(node.lines, cx, cy)}</g>`
}

export const renderGraph = (id: string, graph: PlacedGraph): string => {
  const keys = new Map(graph.nodes.map((node, index) => [node.id, `n${index}`]))
  const keyOf = (nodeId: string) => keys.get(nodeId) ?? nodeId

  const groups = graph.groups
    .map(
      (group) =>
        `<g class="dg-group"><rect x="${group.x}" y="${group.y}" width="${group.width}" height="${group.height}" rx="10"/><text x="${group.x + 14}" y="${group.y + 20}" font-size="${LABEL_SIZE}">${escape(group.label)}</text></g>`,
    )
    .join('')

  const nodeById = new Map(graph.nodes.map((node) => [node.id, node]))
  const edges = graph.edges
    .map((edge) => {
      const points = edge.points.map((point, index, all) => {
        if (index === 0 && all[1]) {
          return onOutline(point, all[1], nodeById.get(edge.from))
        }
        const previous = all[index - 1]
        return index === all.length - 1 && previous
          ? onOutline(point, previous, nodeById.get(edge.to))
          : point
      })
      const route = routePath(trimmed(points, edge.isBidirectional))
      const last = points.at(-1)
      const beforeLast = points.at(-2)
      const first = points[0]
      const second = points[1]
      const heads = [
        last && beforeLast ? arrowhead(beforeLast, last) : '',
        edge.isBidirectional && first && second ? arrowhead(second, first) : '',
      ].join('')
      const label = edge.label
        ? `<g class="dg-edge-label"><rect x="${round(edge.label.x)}" y="${round(edge.label.y)}" width="${round(edge.label.width)}" height="${round(edge.label.height)}" rx="4"/><text x="${round(edge.label.x + edge.label.width / 2)}" y="${round(edge.label.y + edge.label.height / 2)}" dy="0.35em" text-anchor="middle" font-size="${LABEL_SIZE}">${escape(edge.label.text)}</text></g>`
        : ''
      return `<g class="dg-edge${edge.isDashed ? ' dg-dashed' : ''}" data-touch="${keyOf(edge.from)} ${keyOf(edge.to)}"><path class="dg-line" d="${route}"/><path class="dg-flow" d="${route}"/>${heads}${label}</g>`
    })
    .join('')

  const nodes = graph.nodes.map((node) => nodeMarkup(node, keyOf(node.id))).join('')

  // Hovering a node lights its edges and quiets the rest.
  const style = [...keys.values()]
    .map(
      (key) =>
        `#${id}:has([data-node="${key}"]:hover) .dg-edge:not([data-touch~="${key}"]){opacity:.25}#${id}:has([data-node="${key}"]:hover) [data-touch~="${key}"]{color:var(--dg-bright)}`,
    )
    .join('')

  return svg(id, graph.width, graph.height, style, `${groups}${edges}${nodes}`)
}

// SEQUENCES

const STEP_SECONDS = 1.3

export const renderSequence = (id: string, sequence: PlacedSequence): string => {
  const participants = sequence.participants
    .map((participant) => {
      const cx = participant.x + participant.width / 2
      return `<g class="dg-node"><line class="dg-lifeline" x1="${round(cx)}" y1="${participant.y + participant.height}" x2="${round(cx)}" y2="${round(sequence.lifelineEnd)}"/><rect class="dg-shape" x="${round(participant.x)}" y="${participant.y}" width="${participant.width}" height="${participant.height}" rx="6"/>${textBlock([{ text: participant.label, tone: 'strong' }], cx, participant.y + participant.height / 2)}</g>`
    })
    .join('')

  const steps = sequence.steps
    .map((step, index) => {
      const delay = `style="animation-delay:${round(index * STEP_SECONDS)}s"`
      if (step._tag === 'Note') {
        return `<g class="dg-step dg-note" ${delay}><rect x="${round(step.x)}" y="${round(step.y)}" width="${round(step.width)}" height="${step.height}" rx="4"/><text x="${round(step.x + step.width / 2)}" y="${round(step.y + step.height / 2)}" dy="0.35em" text-anchor="middle" font-size="${LABEL_SIZE}">${escape(step.text)}</text></g>`
      }
      const number =
        index + 1 - sequence.steps.slice(0, index).filter((s) => s._tag === 'Note').length
      const badge = (x: number) =>
        sequence.isNumbered
          ? `<circle class="dg-badge" cx="${round(x)}" cy="${round(step.y)}" r="9"/><text class="dg-badge-text" x="${round(x)}" y="${round(step.y)}" dy="0.35em" text-anchor="middle" font-size="10">${number}</text>`
          : ''
      if (step.fromX === step.toX) {
        const x = step.fromX
        const loop = [
          { x, y: step.y - 6 },
          { x: x + 36, y: step.y - 6 },
          { x: x + 36, y: step.y + 14 },
          { x, y: step.y + 14 },
        ]
        return `<g class="dg-step${step.isReply ? ' dg-reply' : ''}" ${delay}><path class="dg-line" d="${routePath(trimmed(loop, false))}"/>${arrowhead({ x: x + 36, y: step.y + 14 }, { x, y: step.y + 14 })}${badge(x)}<text class="dg-step-text" x="${x + 46}" y="${round(step.y + 4)}" dy="0.35em" font-size="${LABEL_SIZE}">${escape(step.text)}</text></g>`
      }
      const direction = Math.sign(step.toX - step.fromX)
      const start = { x: step.fromX + (sequence.isNumbered ? direction * 9 : 0), y: step.y }
      const tip = { x: step.toX, y: step.y }
      const middle = (step.fromX + step.toX) / 2 + (sequence.isNumbered ? direction * 6 : 0)
      return `<g class="dg-step${step.isReply ? ' dg-reply' : ''}" ${delay}><path class="dg-line" d="${routePath(trimmed([start, tip], false))}"/>${arrowhead(start, tip)}${badge(step.fromX)}<text class="dg-step-text" x="${round(middle)}" y="${round(step.y - 10)}" text-anchor="middle" font-size="${LABEL_SIZE}">${escape(step.text)}</text></g>`
    })
    .join('')

  // One step at a time lights up, in order, and the walk-through repeats.
  const total = sequence.steps.length * STEP_SECONDS
  const share = round(100 / Math.max(1, sequence.steps.length))
  const style = `#${id} .dg-step{animation:${id}-walk ${round(total)}s infinite}@keyframes ${id}-walk{0%,${share}%{color:var(--dg-bright)}${round(Math.min(100, share + 4))}%,100%{color:var(--dg-step)}}`

  return svg(id, sequence.width, sequence.height, style, `${participants}${steps}`)
}
