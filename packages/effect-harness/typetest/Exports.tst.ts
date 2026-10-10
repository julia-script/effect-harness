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
import type * as DocumentRetirement from 'effect-harness/DocumentRetirement'
import type * as OutputRetention from 'effect-harness/OutputRetention'
import type * as StorageFormat from 'effect-harness/StorageFormat'
import type * as TaskScheduler from 'effect-harness/TaskScheduler'
import type * as ToolSpill from 'effect-harness/ToolSpill'
import type * as Bash from 'effect-harness/coding/Bash'
import type * as Edit from 'effect-harness/coding/Edit'
import type * as ImageRead from 'effect-harness/coding/ImageRead'
import type * as ImageReadPolicy from 'effect-harness/coding/ImageReadPolicy'
import type * as PowerShell from 'effect-harness/coding/PowerShell'
import type * as ShellProcess from 'effect-harness/coding/ShellProcess'

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

test('coding and durable execution namespaces retain their public contracts', () => {
  expect(HarnessPackage.DocumentRetirement.target).type.toBe<typeof DocumentRetirement.target>()
  expect(HarnessPackage.OutputRetention.make).type.toBe<typeof OutputRetention.make>()
  expect(HarnessPackage.StorageFormat.requireSupportedFormat).type.toBe<
    typeof StorageFormat.requireSupportedFormat
  >()
  expect(HarnessPackage.TaskScheduler.make).type.toBe<typeof TaskScheduler.make>()
  expect(HarnessPackage.ToolSpill.layer).type.toBe<typeof ToolSpill.layer>()
  expect(HarnessPackage.Coding.Bash.run).type.toBe<typeof Bash.run>()
  expect(HarnessPackage.Coding.Edit.edit).type.toBe<typeof Edit.edit>()
  expect(HarnessPackage.Coding.ImageRead.Outcome).type.toBe<typeof ImageRead.Outcome>()
  expect(HarnessPackage.Coding.ImageReadPolicy.layer).type.toBe<typeof ImageReadPolicy.layer>()
  expect(HarnessPackage.Coding.PowerShell.run).type.toBe<typeof PowerShell.run>()
  expect(HarnessPackage.Coding.ShellProcess.layerNative).type.toBe<
    typeof ShellProcess.layerNative
  >()
})
