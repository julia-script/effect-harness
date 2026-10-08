import { assert, describe, it } from '@effect/vitest'

// effect-nit-allow P9-namespace-alias-equals-module: effect-harness/Identity and effect-harness/durable/Identity both own Identity; SharedIdentity keeps their distinct native/harness APIs available together for these constructor, service and declaration assertions.
import * as SharedIdentity from 'effect-harness/Identity'

import * as SystemPatch from 'effect-harness/SystemPatch'

import * as Schema from 'effect/Schema'

import * as Entry from 'effect-harness/durable/Entry'

import * as Identity from 'effect-harness/durable/Identity'

import * as Record from 'effect-harness/durable/Record'

describe('IdentitySchema', () => {
  it('shares exact canonical numeric identities and required system patches', () => {
    assert.strictEqual(Record.ConversationId, SharedIdentity.ConversationId)
    assert.strictEqual(Record.EntryId, SharedIdentity.EntryId)
    assert.strictEqual(Entry.SystemData.fields.harness.fields.system, SystemPatch.SystemPatch)
    assert.isFalse(Schema.is(Entry.SystemData)({ harness: {} }))
    for (const schema of [
      Record.ConversationId,
      Record.EntryId,
      Record.TaskId,
      Record.SubmissionId,
      Record.DocumentId,
      Record.Seq,
    ]) {
      assert.isTrue(Schema.is(schema)(Number.MAX_SAFE_INTEGER))
      assert.isFalse(Schema.is(schema)(0))
      assert.isFalse(Schema.is(schema)(Number.MAX_SAFE_INTEGER + 1))
    }
    const session = Identity.SessionId.make('')
    assert.strictEqual(session, '')
    assert.strictEqual(Identity.RequestId.make('\ud800'), '\ud800')
  })
})
