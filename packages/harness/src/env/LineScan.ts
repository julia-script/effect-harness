// Adapted from pi-durable (MIT), pinned 636703a0; see ../LICENSE.pi.txt.
import { rangeDecoder, startsWithBom } from './Decode.ts'
import { FileError, FileInvalid, type LineScan } from '../Env.ts'
import * as Result from 'effect/Result'
const NEWLINE = 10
const encoder = new TextEncoder()
const decodedBytes = (text: string): number => encoder.encode(text).length
export interface State {
  readonly startLine: number
  readonly endLine: number
  position: number
  newlines: number
  lineStart: number
  start: number | undefined
  end: number | undefined
  firstLineEnd: number | undefined
  lastLineStart: number | undefined
  selectedBytes: number
  firstLineBytes: number
  selection: TextDecoder | undefined
  firstLine: TextDecoder | undefined
  head: number[] | undefined
  bom: boolean
}
export function make(startLine: number, endLine = Infinity): Result.Result<State, FileError> {
  if (
    !Number.isSafeInteger(startLine) ||
    startLine < 0 ||
    !(endLine > startLine) ||
    (endLine !== Infinity && !Number.isSafeInteger(endLine))
  )
    return Result.fail(
      new FileError({ reason: new FileInvalid({ message: 'Invalid line range' }) }),
    )
  const state: State = {
    startLine,
    endLine,
    position: 0,
    newlines: 0,
    lineStart: 0,
    start: undefined,
    end: undefined,
    firstLineEnd: undefined,
    lastLineStart: undefined,
    selectedBytes: 0,
    firstLineBytes: 0,
    selection: undefined,
    firstLine: undefined,
    head: [],
    bom: false,
  }
  if (startLine === 0) begin(state, 0)
  return Result.succeed(state)
}
export function push(state: State, chunk: Uint8Array): void {
  if (state.head !== undefined) {
    const take = Math.min(3 - state.head.length, chunk.length)
    state.head.push(...chunk.subarray(0, take))
    if (state.head.length < 3) return
    releaseHead(state)
    chunk = chunk.subarray(take)
  }
  process(state, chunk)
}
function releaseHead(state: State): void {
  const head = Uint8Array.from(state.head ?? [])
  state.head = undefined
  state.bom = startsWithBom(head)
  process(state, head)
}
function process(state: State, chunk: Uint8Array): void {
  const base = state.position
  let from = 0
  for (
    let index = chunk.indexOf(NEWLINE);
    index !== -1;
    index = chunk.indexOf(NEWLINE, index + 1)
  ) {
    // The newline ends line `state.newlines`. It belongs to the selection between selected lines only.
    feed(state, chunk, base, from, index)
    const line = state.newlines
    const position = base + index
    if (line === state.startLine) endFirstLine(state, position)
    if (line === state.endLine - 1) endSelection(state, position)
    feed(state, chunk, base, index, index + 1)
    from = index + 1
    state.newlines++
    state.lineStart = position + 1
    if (state.newlines === state.startLine) begin(state, state.lineStart)
    if (state.newlines === state.endLine - 1) state.lastLineStart = state.lineStart
  }
  feed(state, chunk, base, from, chunk.length)
  state.position += chunk.length
}
export function finish(state: State): LineScan {
  if (state.head !== undefined) releaseHead(state)
  const size = state.position
  if (state.start === undefined) {
    return {
      newlines: state.newlines,
      start: size,
      end: size,
      firstLineEnd: size,
      lastLineStart: size,
      selectedBytes: 0,
      firstLineBytes: 0,
    }
  }
  if (state.firstLineEnd === undefined) endFirstLine(state, size)
  if (state.end === undefined) endSelection(state, size)
  return {
    newlines: state.newlines,
    start: state.start,
    end: state.end ?? size,
    firstLineEnd: state.firstLineEnd ?? size,
    // A selection that reaches past the last line ends with the last line.
    lastLineStart: state.lastLineStart ?? state.lineStart,
    selectedBytes: state.selectedBytes,
    firstLineBytes: state.firstLineBytes,
  }
}
function begin(state: State, start: number): void {
  state.start = start
  if (state.startLine === state.endLine - 1) state.lastLineStart = start
  state.selection = rangeDecoder()
  state.firstLine = rangeDecoder()
}
function endFirstLine(state: State, position: number): void {
  state.firstLineEnd = position
  if (state.firstLine !== undefined) state.firstLineBytes += decodedBytes(state.firstLine.decode())
  state.firstLine = undefined
}
function endSelection(state: State, position: number): void {
  state.end = position
  if (state.selection !== undefined) state.selectedBytes += decodedBytes(state.selection.decode())
  state.selection = undefined
}
function feed(state: State, chunk: Uint8Array, base: number, from: number, to: number): void {
  // Decoding the whole file drops a leading byte-order mark.
  if (state.bom && base + from < 3) from = Math.min(to, 3 - base)
  if (to <= from) return
  const bytes = chunk.subarray(from, to)
  if (state.selection !== undefined) {
    state.selectedBytes += decodedBytes(state.selection.decode(bytes, { stream: true }))
  }
  if (state.firstLine !== undefined) {
    state.firstLineBytes += decodedBytes(state.firstLine.decode(bytes, { stream: true }))
  }
}
