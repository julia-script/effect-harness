import { assertFailure } from '@effect/vitest/utils'

import { describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Document from 'effect-harness/Document'

import * as Record from 'effect-harness/Record'

const definition: Document.Document.DefinitionInput<{ count: number }> = {
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
}
// The original expected-input regression jointly pins Document and Record definition boundaries; its single registration and complete assertions remain together.

describe('DocumentDefinitions', () => {
  it.effect('returns typed definition failures instead of throwing expected input errors', () =>
    Effect.sync(() => {
      const singleton = Document.define({ ...definition, kind: '' })
      assertFailure(
        singleton,
        new Document.DocumentDefinitionError({
          message: 'Document kind/version must be nonempty and a positive safe integer',
        }),
      )
      const family = Document.family({
        ...definition,
        scope: 'conversation',
        history: 'latest',
        fork: 'asOf',
      })
      assertFailure(
        family,
        new Document.DocumentDefinitionError({
          message: 'Conversation documents require compatible history/fork policies',
        }),
      )
      const entry = Record.defineEntry('', Record.Entry)
      assertFailure(
        entry,
        new Record.EntryDefinitionError({ message: 'Entry kind must be nonempty' }),
      )
    }),
  )
})
