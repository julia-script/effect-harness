import { expect, test } from 'tstyche'
import * as HarnessPackage from 'effect-harness'
import type * as Harness from 'effect-harness/Harness'
import type * as HarnessBackend from 'effect-harness/HarnessBackend'
import type * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type * as Conversation from 'effect-harness/Conversation'
import type * as Submission from 'effect-harness/Submission'
import type * as Storage from 'effect-harness/Storage'
import type * as Session from 'effect-harness/Session'
import type * as Tool from 'effect-harness/Tool'
import type * as Toolkit from 'effect-harness/Toolkit'
import type * as Document from 'effect-harness/Document'
import type * as Transaction from 'effect-harness/Transaction'

test('root namespaces and stable leaf imports expose the promoted API', () => {
  expect(HarnessPackage.Harness.layerLocal).type.toBe<typeof Harness.layerLocal>()
  expect(HarnessPackage.HarnessBackend.HarnessBackend).type.toBe<
    typeof HarnessBackend.HarnessBackend
  >()
  expect(HarnessPackage.HarnessRuntime.make).type.toBe<typeof HarnessRuntime.make>()
  expect(HarnessPackage.Conversation.submit).type.toBe<typeof Conversation.submit>()
  expect(HarnessPackage.Submission.wait).type.toBe<typeof Submission.wait>()
  expect(HarnessPackage.Storage.layerSql).type.toBe<typeof Storage.layerSql>()
  expect(HarnessPackage.Session.commit).type.toBe<typeof Session.commit>()
  expect(HarnessPackage.Tool.make).type.toBe<typeof Tool.make>()
  expect(HarnessPackage.Toolkit.merge).type.toBe<typeof Toolkit.merge>()
  expect(HarnessPackage.Document.define).type.toBe<typeof Document.define>()
  expect(HarnessPackage.Transaction.updateDocument).type.toBe<typeof Transaction.updateDocument>()
})
