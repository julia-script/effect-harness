import { assert, describe, it } from '@effect/vitest'

import * as Schema from 'effect/Schema'

import * as Record from 'effect-harness/durable/Record'

import * as Submission from 'effect-harness/durable/workflow/Submission'

describe('RecordDocumentSchema', () => {
  it('derives exact document and entry variants without accepting explicit undefined', () => {
    assert.deepStrictEqual(Object.keys(Record.DocumentCreate.fields), [
      'id',
      'kind',
      'scope',
      'key',
      'history',
      'fork',
    ])
    assert.strictEqual(Submission.EntryDraft.fields.data, Record.Entry.fields.data)
    assert.isTrue(Schema.is(Submission.EntryDraft)({ kind: 'custom', head: 'self' }))
    assert.isFalse(Schema.is(Submission.EntryDraft)({ kind: 'custom', head: undefined }))
    assert.isFalse(Schema.is(Submission.EntryDraft)({ kind: 'custom', model: undefined }))
    assert.isFalse(
      Schema.is(Record.DocumentCreate)({
        id: 2,
        kind: 'x',
        scope: { _tag: 'session' as const },
        history: undefined,
      }),
    )
  })
})
