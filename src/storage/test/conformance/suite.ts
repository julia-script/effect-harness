import { test } from 'bun:test'
import { addressCases } from './addresses.ts'
import { documentCases } from './documents.ts'
import { entryCases } from './entries.ts'
import { recordCases } from './records.ts'
import type { RunCase } from '../support.ts'

const cases = [...recordCases, ...entryCases, ...documentCases, ...addressCases]

/** Register every conformance case as a test against one backend. */
export const runConformance = (backend: string, run: RunCase): void => {
  for (const { name, body } of cases) test(`${backend}: ${name}`, () => run(body))
}
