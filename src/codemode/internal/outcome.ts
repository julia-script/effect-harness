/**
 * What a script gets for its calls, and the small helpers replay compares calls with.
 */
import type { CodemodeHostCall, CodemodeOutcome } from '@earendil-works/pi-codemode/portable'
import { Schema } from 'effect'
import type * as Call from '../../tool/Call.ts'
import { Output } from '../../tool/ToolResult.ts'
import type { Outcome, Stored } from './state.ts'
import { text } from '../../tool/ToolResult.ts'

const isOutput = Schema.is(Output)

/** The text blocks of an encoded `Output`, joined by lines. */
export const textOf = (value: unknown): string =>
  isOutput(value)
    ? value.content.flatMap((block) => (block.type === 'text' ? [block.text] : [])).join('\n')
    : ''

const errorsOf = (outcome: Call.Outcome) =>
  outcome.diagnostics
    .filter((diagnostic) => diagnostic.severity === 'error')
    .map((diagnostic) => diagnostic.message)
    .join('\n')

/**
 * What a script gets for a nested call. A structured tool resolves to its value; a tool whose
 * success is an `Output` resolves to its text, images left out, as its declaration says. An error
 * rejects with its text, a structured tool's failure as JSON, else its error diagnostics; a
 * structured tool's declared failure also rides along as the error's `failure` property.
 */
export const outcomeOf = (name: string, structured: boolean, outcome: Call.Outcome): Outcome => {
  const { value } = outcome
  if (!outcome.isError) {
    if (!structured) return { ok: true, value: textOf(value) }
    return value === undefined ? { ok: true } : { ok: true, value }
  }
  const failure = value === undefined ? '' : JSON.stringify(value)
  const own = structured ? failure : textOf(value)
  const errors = errorsOf(outcome)
  const message = [own, errors].find((each) => each !== '') ?? `Tool ${name} failed`
  return structured && value !== undefined
    ? { ok: false, message, properties: { failure: value } }
    : { ok: false, message }
}

/** An outcome as the VM takes it. */
export const toSandbox = (outcome: Outcome): CodemodeOutcome =>
  outcome.ok
    ? { ok: true, value: outcome.value }
    : {
        ok: false,
        message: outcome.message,
        ...(outcome.properties === undefined ? {} : { properties: outcome.properties }),
      }

/** `tools.read({"path":"a"})`, `next([])`. */
export const describeCall = (call: CodemodeHostCall): string =>
  `${call.target === 'tool' ? 'tools.' : ''}${call.name}(${JSON.stringify(call.args) ?? ''})`

export const cut = (value: string, max: number): string =>
  value.length > max ? `${value.slice(0, max - 3)}...` : value

/** An error result the call decides itself. */
export const errorResult = (message: string): Stored => ({ isError: true, output: text(message) })
