import type * as Prompt from 'effect/ai/Prompt'
import * as Projection from '../ContextProjection.js'

/** Explicit approximation: 3.5 characters/token, 4800 characters for each media part. */
export const estimateMessage = (message: Prompt.Message): number => {
  if (message.role === 'system') return Math.ceil(message.content.length / 3.5)
  let characters = 0
  for (const part of message.content) {
    switch (part.type) {
      case 'text':
      case 'reasoning':
        characters += part.text.length
        break
      case 'file':
        characters += 4800
        break
      case 'tool-call':
        characters += part.name.length + JSON.stringify(part.params).length
        break
      case 'tool-result':
        characters += part.name.length + JSON.stringify(part.result).length
        break
      case 'tool-approval-request':
      case 'tool-approval-response':
        characters += JSON.stringify(part).length
        break
    }
  }
  return Math.ceil(characters / 3.5)
}

/**
 * Linear analysis of the bounded canonical contributions. Boundaries may start only
 * with a user/assistant, and never cross an exact ID/name call/result group. Assistant
 * windows match the canonical projector; reused IDs cannot borrow older results.
 */
export const candidates = (view: Projection.ContextView): ReadonlyArray<number> => {
  const { contributions } = view
  const start = view.bounds.head === undefined ? 0 : 1
  const differences = Array.from({ length: contributions.length + 1 }, () => 0)
  const calls = new Map<string, number>()
  const approvals = new Map<string, number>()
  const block = (from: number, through: number) => {
    if (from >= through) return
    differences[from + 1]!++
    differences[through + 1]!--
  }
  for (const [index, messages] of contributions.entries()) {
    for (const message of messages) {
      if (message.role === 'system') continue
      if (message.role === 'assistant') calls.clear()
      for (const part of message.content) {
        if (part.type === 'tool-call') calls.set(JSON.stringify([part.id, part.name]), index)
        if (part.type === 'tool-result') {
          const key = JSON.stringify([part.id, part.name])
          const from = calls.get(key)
          if (from !== undefined) {
            block(from, index)
            calls.delete(key)
          }
        }
        if (part.type === 'tool-approval-request') approvals.set(part.approvalId, index)
        if (part.type === 'tool-approval-response') {
          const from = approvals.get(part.approvalId)
          if (from !== undefined) {
            block(from, index)
            approvals.delete(part.approvalId)
          }
        }
      }
    }
  }
  let blocked = 0
  let prefix = false
  const safe: Array<number> = []
  for (let index = 0; index < contributions.length; index++) {
    blocked += differences[index]!
    const first = contributions[index]![0]
    if (
      index >= start &&
      prefix &&
      blocked === 0 &&
      (first?.role === 'user' || first?.role === 'assistant')
    )
      safe.push(index)
    // The projector drops orphan result parts. They cannot be a summary source by themselves.
    if (
      index >= start &&
      contributions[index]!.some(
        (message) =>
          message.role !== 'tool' || message.content.some((part) => part.type !== 'tool-result'),
      )
    )
      prefix = true
  }
  return safe
}

/** Most recent safe cut that retains at least the declared recent suffix. */
export const selectCut = (
  view: Projection.ContextView,
  keepRecentTokens: number,
): number | undefined => {
  const safe = candidates(view)
  for (let index = safe.length - 1; index >= 0; index--) {
    const candidate = safe[index]!
    const suffix = Projection.projectMessages(view.contributions.slice(candidate).flat())
    const kept = suffix.content.reduce((total, message) => total + estimateMessage(message), 0)
    if (kept >= keepRecentTokens) return candidate
  }
  return undefined
}
