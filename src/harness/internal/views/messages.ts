import { Schema } from 'effect'
import type { AssistantContent, AssistantMessage } from '../../../model/Message.ts'
import type { Op } from '../../../storage/schema/Document.ts'
import type { MessageChange } from '../../AgentEvent.ts'

type Path = ReadonlyArray<string | number>

/** Where `pi.live` holds the partial in a conversation view. */
const PARTIAL_PATH: Path = ['docs', 'pi.live', 'generation', 'message']

export const startsWith = (path: Path, prefix: Path): boolean =>
  prefix.length <= path.length && prefix.every((segment, index) => path[index] === segment)

/** The path an operation changes; the root for a replacement. */
export const pathOf = (op: Op): Path => (op[0] === 'r' ? [] : op[1])

const startType = (block: AssistantContent) => {
  if (block.type === 'text') return 'text_start' as const
  return block.type === 'thinking' ? ('thinking_start' as const) : ('toolcall_start' as const)
}

const isBlocks = Schema.is(Schema.Array(Schema.Json))

/** Blocks inserted at the end of the content, as message changes; `undefined` when not that. */
const inserted = (op: Op, message: AssistantMessage): ReadonlyArray<MessageChange> | undefined => {
  if (op[0] !== 'p' || op[3] !== 0 || !isBlocks(op[4])) return undefined
  const start = op[2]
  const changes: Array<MessageChange> = []
  for (const offset of op[4].keys()) {
    const block = message.content[start + offset]
    if (block === undefined) return undefined
    changes.push({ type: startType(block), contentIndex: start + offset, block })
  }
  return changes
}

/** The change one operation inside a content block makes, or `undefined` for the whole block. */
const blockChange = (op: Op, rest: Path, index: number): MessageChange | undefined => {
  if (op[0] !== 'a') return undefined
  const field = rest[2]
  if (rest.length === 3 && (field === 'text' || field === 'thinking')) {
    const type = field === 'text' ? ('text_delta' as const) : ('thinking_delta' as const)
    return { type, contentIndex: index, delta: op[2] }
  }
  return field === 'arguments'
    ? { type: 'toolcall_delta', contentIndex: index, path: rest.slice(3), delta: op[2] }
    : undefined
}

/**
 * The view operations on the in-flight message, as message changes. Anything they do not express
 * replaces the message whole.
 */
export const messageChanges = (
  viewOps: ReadonlyArray<Op>,
  message: AssistantMessage,
): ReadonlyArray<MessageChange> => {
  const whole: ReadonlyArray<MessageChange> = [{ type: 'message', message }]
  const changes: Array<MessageChange> = []
  // A block sent whole already holds every later change to it in this batch.
  const sent = new Set<number>()
  for (const op of viewOps) {
    const path = pathOf(op)
    if (!startsWith(path, PARTIAL_PATH)) {
      // The whole message, or the generation holding it, was replaced.
      if (startsWith(PARTIAL_PATH, path)) return whole
      continue
    }
    const rest = path.slice(PARTIAL_PATH.length)
    if (rest[0] === 'usage') continue
    if (rest[0] !== 'content') return whole
    const index = rest[1]
    if (typeof index !== 'number') {
      const added = rest.length === 1 ? inserted(op, message) : undefined
      if (added === undefined) return whole
      changes.push(...added)
      continue
    }
    if (sent.has(index)) continue
    const change = blockChange(op, rest, index)
    if (change !== undefined) {
      changes.push(change)
      continue
    }
    const block = message.content[index]
    if (block === undefined) return whole
    sent.add(index)
    changes.push({ type: 'block', contentIndex: index, block })
  }
  return changes
}
