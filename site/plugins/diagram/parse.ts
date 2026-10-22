/**
 * Reads the Mermaid the guides are written in, so GitHub keeps drawing them, into a diagram the
 * site lays out and draws itself. It covers the subset the guides use: flowcharts with subgraphs,
 * state diagrams, sequence diagrams and mind maps. Anything else fails the build.
 */

/** One line of a label. `<b>` and `<i>` around a whole line set its tone. */
export interface Line {
  readonly text: string
  readonly tone: 'strong' | 'normal' | 'muted'
}

export type Shape =
  | 'rect'
  | 'round'
  | 'stadium'
  | 'cylinder'
  | 'diamond'
  | 'circle'
  | 'start'
  | 'end'
  | 'text'

export interface GraphNode {
  readonly id: string
  readonly lines: ReadonlyArray<Line>
  readonly shape: Shape
  readonly parent: string | undefined
}

export interface GraphEdge {
  readonly from: string
  readonly to: string
  readonly label: string | undefined
  readonly isDashed: boolean
  readonly isBidirectional: boolean
}

export interface GraphGroup {
  readonly id: string
  readonly label: string
  readonly parent: string | undefined
  readonly direction: Direction | undefined
}

export type Direction = 'RIGHT' | 'DOWN'

export interface Graph {
  readonly _tag: 'Graph'
  readonly direction: Direction
  readonly nodes: ReadonlyArray<GraphNode>
  readonly edges: ReadonlyArray<GraphEdge>
  readonly groups: ReadonlyArray<GraphGroup>
}

export type Step =
  | {
      readonly _tag: 'Message'
      readonly from: string
      readonly to: string
      readonly text: string
      readonly isReply: boolean
    }
  | { readonly _tag: 'Note'; readonly over: ReadonlyArray<string>; readonly text: string }

export interface Sequence {
  readonly _tag: 'Sequence'
  readonly isNumbered: boolean
  readonly participants: ReadonlyArray<{ readonly id: string; readonly label: string }>
  readonly steps: ReadonlyArray<Step>
}

export type Diagram = Graph | Sequence

const fail = (message: string): never => {
  throw new Error(`Diagram: ${message}`)
}

// LABELS

const stripTags = (text: string): string => text.replace(/<[^>]+>/g, '')

export const linesOf = (raw: string): ReadonlyArray<Line> =>
  raw
    .replace(/^"(.*)"$/s, '$1')
    .split(/<br\s*\/?>/)
    .map((part) => part.trim())
    .map((part): Line => {
      const strong = /^<b>(.*)<\/b>$/.exec(part)
      if (strong) {
        return { text: stripTags(strong[1] ?? ''), tone: 'strong' }
      }
      const muted = /^<i>(.*)<\/i>$/.exec(part)
      if (muted) {
        return { text: stripTags(muted[1] ?? ''), tone: 'muted' }
      }
      return { text: stripTags(part), tone: 'normal' }
    })

const directionOf = (word: string): Direction => (word === 'LR' || word === 'RL' ? 'RIGHT' : 'DOWN')

// FLOWCHARTS

const SHAPES: ReadonlyArray<readonly [open: string, close: string, shape: Shape]> = [
  ['([', '])', 'stadium'],
  ['[(', ')]', 'cylinder'],
  ['((', '))', 'circle'],
  ['[', ']', 'rect'],
  ['{', '}', 'diamond'],
  ['(', ')', 'round'],
]

interface NodeRef {
  readonly id: string
  readonly label: string | undefined
  readonly shape: Shape
}

/** A node reference such as `A`, `A[label]` or `A[("label")]`, and where it ends. */
const readNode = (line: string, start: number): readonly [NodeRef, number] => {
  const idMatch = /\s*([A-Za-z0-9_$]+)/y
  idMatch.lastIndex = start
  const found = idMatch.exec(line)
  if (!found?.[1]) {
    return fail(`expected a node at "${line.slice(start)}"`)
  }
  const id = found[1]
  const after = idMatch.lastIndex
  const opener = SHAPES.find(([open]) => line.startsWith(open, after))
  if (!opener) {
    return [{ id, label: undefined, shape: 'rect' }, after]
  }
  const [open, close, shape] = opener
  const contentStart = after + open.length
  if (line[contentStart] === '"') {
    const quoteEnd = line.indexOf('"', contentStart + 1)
    const closeAt = line.indexOf(close, quoteEnd)
    if (quoteEnd < 0 || closeAt < 0) {
      return fail(`unclosed label in "${line}"`)
    }
    return [{ id, label: line.slice(contentStart, quoteEnd + 1), shape }, closeAt + close.length]
  }
  const closeAt = line.indexOf(close, contentStart)
  if (closeAt < 0) {
    return fail(`unclosed label in "${line}"`)
  }
  return [{ id, label: line.slice(contentStart, closeAt), shape }, closeAt + close.length]
}

/** A group of nodes joined by `&`. */
const readNodes = (line: string, start: number): readonly [ReadonlyArray<NodeRef>, number] => {
  const [first, end] = readNode(line, start)
  const amp = /\s*&/y
  amp.lastIndex = end
  if (amp.exec(line)) {
    const [rest, restEnd] = readNodes(line, amp.lastIndex)
    return [[first, ...rest], restEnd]
  }
  return [[first], end]
}

interface Link {
  readonly label: string | undefined
  readonly isDashed: boolean
  readonly isBidirectional: boolean
}

const LINKS: ReadonlyArray<readonly [RegExp, (match: RegExpExecArray) => Link]> = [
  [
    /\s*(<)?--\s+(.+?)\s+-->/y,
    (match) => ({ label: match[2], isDashed: false, isBidirectional: Boolean(match[1]) }),
  ],
  [
    /\s*(<)?-->(?:\|(.*?)\|)?/y,
    (match) => ({ label: match[2], isDashed: false, isBidirectional: Boolean(match[1]) }),
  ],
  [
    /\s*(<)?-\.(?:(.+?)\.)?->(?:\|(.*?)\|)?/y,
    (match) => ({
      label: match[2] ?? match[3],
      isDashed: true,
      isBidirectional: Boolean(match[1]),
    }),
  ],
]

const readLink = (line: string, start: number): readonly [Link, number] | undefined =>
  LINKS.reduce<readonly [Link, number] | undefined>((found, [pattern, toLink]) => {
    if (found) {
      return found
    }
    pattern.lastIndex = start
    const match = pattern.exec(line)
    return match ? [toLink(match), pattern.lastIndex] : undefined
  }, undefined)

const edgeLabel = (raw: string | undefined): string | undefined =>
  raw === undefined ? undefined : stripTags(raw.trim().replace(/^"(.*)"$/s, '$1')) || undefined

const parseFlowchart = (header: string, body: ReadonlyArray<string>): Graph => {
  const nodes = new Map<string, GraphNode>()
  const edges: Array<GraphEdge> = []
  const groups: Array<GraphGroup> = []
  const stack: Array<string> = []

  const declare = ({ id, label, shape }: NodeRef) => {
    const parent = stack.at(-1)
    const existing = nodes.get(id)
    nodes.set(id, {
      id,
      lines: label === undefined ? (existing?.lines ?? linesOf(id)) : linesOf(label),
      shape: label === undefined ? (existing?.shape ?? 'rect') : shape,
      parent: existing?.parent ?? parent,
    })
  }

  body.forEach((line) => {
    const subgraph = /^subgraph\s+(.+)$/.exec(line)
    if (subgraph?.[1]) {
      const titled = /^([\w$-]+)\s*\[\s*"?(.*?)"?\s*\]$/.exec(subgraph[1])
      const id = titled?.[1] ?? subgraph[1]
      groups.push({
        id,
        label: titled?.[2] ?? subgraph[1],
        parent: stack.at(-1),
        direction: undefined,
      })
      stack.push(id)
      return
    }
    if (line === 'end') {
      stack.pop()
      return
    }
    const direction = /^direction\s+(\w+)$/.exec(line)
    if (direction?.[1]) {
      const current = stack.at(-1)
      const index = groups.findIndex((group) => group.id === current)
      const group = groups[index]
      if (group) {
        groups[index] = { ...group, direction: directionOf(direction[1]) }
      }
      return
    }
    const [first, firstEnd] = readNodes(line, 0)
    first.forEach(declare)
    const chain = (previous: ReadonlyArray<NodeRef>, position: number): void => {
      const link = readLink(line, position)
      if (!link) {
        if (line.slice(position).trim() !== '') {
          fail(`could not read "${line.slice(position)}"`)
        }
        return
      }
      const [{ label, isDashed, isBidirectional }, linkEnd] = link
      const [next, nextEnd] = readNodes(line, linkEnd)
      next.forEach(declare)
      previous.forEach((from) =>
        next.forEach((to) =>
          edges.push({
            from: from.id,
            to: to.id,
            label: edgeLabel(label),
            isDashed,
            isBidirectional,
          }),
        ),
      )
      chain(next, nextEnd)
    }
    chain(first, firstEnd)
  })

  return {
    _tag: 'Graph',
    direction: directionOf(header.split(/\s+/)[1] ?? 'TB'),
    nodes: [...nodes.values()],
    edges,
    groups,
  }
}

// STATE DIAGRAMS

const START = '__start'
const END = '__end'

const parseStates = (body: ReadonlyArray<string>): Graph => {
  const nodes = new Map<string, GraphNode>()
  const state = (name: string, isSource: boolean): string => {
    if (name === '[*]') {
      const id = isSource ? START : END
      nodes.set(id, { id, lines: [], shape: isSource ? 'start' : 'end', parent: undefined })
      return id
    }
    if (!nodes.has(name)) {
      nodes.set(name, { id: name, lines: linesOf(name), shape: 'round', parent: undefined })
    }
    return name
  }
  const directionLine = body.find((line) => line.startsWith('direction '))
  const edges = body.flatMap((line): ReadonlyArray<GraphEdge> => {
    if (line.startsWith('direction ')) {
      return []
    }
    const transition = /^(\[\*\]|[\w$-]+)\s*-->\s*(\[\*\]|[\w$-]+)\s*(?::\s*(.*))?$/.exec(line)
    if (!transition?.[1] || !transition[2]) {
      return fail(`could not read state line "${line}"`)
    }
    return [
      {
        from: state(transition[1], true),
        to: state(transition[2], false),
        label: transition[3]?.trim() || undefined,
        isDashed: false,
        isBidirectional: false,
      },
    ]
  })
  return {
    _tag: 'Graph',
    direction: directionOf(directionLine?.split(/\s+/)[1] ?? 'TB'),
    nodes: [...nodes.values()],
    edges,
    groups: [],
  }
}

// MIND MAPS

const parseMindmap = (rawBody: ReadonlyArray<string>): Graph => {
  const lines = rawBody.filter((line) => line.trim() !== '')
  const indentOf = (line: string) => line.length - line.trimStart().length
  const [rootLine, ...rest] = lines
  if (!rootLine) {
    return fail('an empty mind map')
  }
  const [root] = readNode(rootLine.trim(), 0)
  const nodes: Array<GraphNode> = [
    { id: root.id, lines: linesOf(root.label ?? root.id), shape: 'circle', parent: undefined },
  ]
  const edges: Array<GraphEdge> = []
  const stack: Array<{ readonly id: string; readonly indent: number }> = [
    { id: root.id, indent: indentOf(rootLine) },
  ]
  rest.forEach((line, index) => {
    const indent = indentOf(line)
    while (stack.length > 1 && (stack.at(-1)?.indent ?? 0) >= indent) {
      stack.pop()
    }
    const parent = stack.at(-1)
    const id = `m${index}`
    const depth = stack.length
    nodes.push({
      id,
      lines: linesOf(line.trim()).map((part) => (depth === 1 ? { ...part, tone: 'strong' } : part)),
      shape: depth === 1 ? 'rect' : 'text',
      parent: undefined,
    })
    if (parent) {
      edges.push({
        from: parent.id,
        to: id,
        label: undefined,
        isDashed: false,
        isBidirectional: false,
      })
    }
    stack.push({ id, indent })
  })
  return { _tag: 'Graph', direction: 'RIGHT', nodes, edges, groups: [] }
}

// SEQUENCE DIAGRAMS

const parseSequence = (body: ReadonlyArray<string>): Sequence => {
  const participants = new Map<string, string>()
  const participant = (id: string) => {
    if (!participants.has(id)) {
      participants.set(id, id)
    }
    return id
  }
  const steps = body.flatMap((line): ReadonlyArray<Step> => {
    if (line === 'autonumber') {
      return []
    }
    const declared = /^participant\s+([\w$]+)(?:\s+as\s+(.+))?$/.exec(line)
    if (declared?.[1]) {
      participants.set(declared[1], declared[2]?.trim() ?? declared[1])
      return []
    }
    const message = /^([\w$]+)\s*(-->>|->>|-->|->)\s*([\w$]+)\s*:\s*(.*)$/.exec(line)
    if (message?.[1] && message[2] && message[3]) {
      return [
        {
          _tag: 'Message',
          from: participant(message[1]),
          to: participant(message[3]),
          text: message[4]?.trim() ?? '',
          isReply: message[2].startsWith('--'),
        },
      ]
    }
    const note = /^Note\s+(?:over|left of|right of)\s+([^:]+):\s*(.*)$/i.exec(line)
    if (note?.[1]) {
      return [
        {
          _tag: 'Note',
          over: note[1].split(',').map((name) => participant(name.trim())),
          text: note[2]?.trim() ?? '',
        },
      ]
    }
    return fail(`could not read sequence line "${line}"`)
  })
  return {
    _tag: 'Sequence',
    isNumbered: body.includes('autonumber'),
    participants: [...participants].map(([id, label]) => ({ id, label })),
    steps,
  }
}

// DIAGRAMS

export const parse = (source: string): Diagram => {
  const raw = source
    .split('\n')
    .filter((line) => line.trim() !== '' && !line.trim().startsWith('%%'))
  const [header = '', ...rawBody] = raw
  const body = rawBody.map((line) => line.trim())
  const kind = header.trim().split(/\s+/)[0]
  if (kind === 'flowchart' || kind === 'graph') {
    return parseFlowchart(header.trim(), body)
  }
  if (kind === 'stateDiagram-v2' || kind === 'stateDiagram') {
    return parseStates(body)
  }
  if (kind === 'sequenceDiagram') {
    return parseSequence(body)
  }
  if (kind === 'mindmap') {
    return parseMindmap(rawBody)
  }
  return fail(`unsupported diagram "${kind}"`)
}
