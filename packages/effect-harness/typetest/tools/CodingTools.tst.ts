import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Layer from 'effect/Layer'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as CodingTools from 'effect-harness/tools/CodingTools'
import * as Bash from 'effect-harness/tools/Bash'
import * as Read from 'effect-harness/tools/Read'
import * as Write from 'effect-harness/tools/Write'
import * as Edit from 'effect-harness/tools/Edit'
// effect-nit-allow P8-tests-import-public-specifiers: path is private tool wiring; only its internal effect channels are asserted here.
// effect-nit-allow P9-no-internal-cross-import: path is private tool wiring; only its internal effect channels are asserted here.
import * as path from '../../src/tools/internal/path.ts'
import * as Image from 'effect-harness/tools/Image'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import type { Env } from 'effect-harness/Env'
import type { FileError } from 'effect-harness/FileError'
import type { MutationLocks } from 'effect-harness/MutationLocks'
import type { Invocation, ToolCall, Result } from 'effect-harness/Invocation'
import type { ToolError } from 'effect-harness/ToolError'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Option from 'effect/Option'

declare const registration: ToolRegistration.Registration
declare const model: LanguageModel.LanguageModel
const known = Tool.make('known', {
  parameters: Schema.Struct({ value: Schema.Finite }),
  success: Schema.String,
})
const toolkit = Toolkit.make(known)

test('native definition-only requests retain toolkit and binding types', () => {
  expect(model.generateText).type.toBeCallableWith({
    prompt: 'request',
    toolkit,
    disableToolCallResolution: true,
  })
  expect(ToolRegistration.bind(toolkit)).type.toBe<
    Effect.Effect<
      Array<ToolRegistration.Registration>,
      never,
      Tool.HandlersFor<{ known: typeof known }>
    >
  >()
})

test('coding toolkit binds at the host edge while invocation services remain invocation-time', () => {
  expect(CodingTools.layerHandlers()).type.toBe<
    Layer.Layer<Tool.HandlersFor<Toolkit.Tools<typeof CodingTools.toolkit>>>
  >()
  expect(CodingTools.make()).type.toBe<
    Effect.Effect<import('effect-harness/Extension').Extension, never, Env | MutationLocks>
  >()
  expect(Bash.handler()({ command: 'true' })).type.toBe<
    Effect.Effect<Result, ToolError, Env | Invocation | ToolCall>
  >()
  expect(Bash.powerShellHandler()({ command: 'true' })).type.toBe<
    Effect.Effect<Result, ToolError, Env | Invocation | ToolCall>
  >()
  expect(Read.handler({ path: 'file' })).type.toBe<
    Effect.Effect<Result, ToolError, Env | Invocation>
  >()
  expect(Write.handler({ path: 'file', content: 'data' })).type.toBe<
    Effect.Effect<
      { readonly content: Array<Prompt.TextPart> },
      ToolError,
      Env | Invocation | MutationLocks
    >
  >()
  expect(Edit.handler({ path: 'file', edits: [] })).type.toBe<
    Effect.Effect<
      {
        readonly content: Array<Prompt.TextPart>
        readonly details: {
          readonly diff: string
          readonly patch: string
          readonly firstChangedLine?: number
        }
      },
      ToolError,
      Env | Invocation | MutationLocks
    >
  >()
  expect(path.resolve('file')).type.toBe<Effect.Effect<string, FileError, Env | Invocation>>()
  expect(path.resolveRead('file')).type.toBe<Effect.Effect<string, FileError, Env | Invocation>>()
  expect(ToolRegistration.makeIntent(registration, { id: 'c', decoded: {} })).type.toBe<
    Effect.Effect<ToolRegistration.Intent, ToolError>
  >()
  expect(Image.detectSupportedImageMimeType(new Uint8Array())).type.toBe<Option.Option<string>>()
  expect(
    Image.detectSupportedImageMimeTypeOf({ size: 0, read: () => Effect.fail('read') }),
  ).type.toBe<Effect.Effect<Option.Option<string>, string>>()
  expect(Read.handler).type.not.toBeCallableWith({ path: 1 })
  expect(Write.handler).type.not.toBeCallableWith({ path: 'file' })
})
