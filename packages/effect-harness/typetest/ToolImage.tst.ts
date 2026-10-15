import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import type * as Images from 'effect-harness/ImagePreparation'
import type { ImagePreparationError } from 'effect-harness/ImagePreparationError'
import type { ToolExecution } from 'effect-harness/ToolExecution'
import * as ToolImage from 'effect-harness/ToolImage'
import type * as ToolResult from 'effect-harness/ToolResult'
import type * as Policy from 'effect-harness/coding/ImageReadPolicy'

const prepared = ToolImage.prepare(new Uint8Array(3), 'image/png')

test('custom image preparation retains codec, policy and per-call execution requirements', () => {
  expect<Effect.Services<typeof prepared>>().type.toBe<
    Images.ImagePreparation | Policy.ImageReadPolicy | ToolExecution
  >()
  expect<Effect.Error<typeof prepared>>().type.toBe<
    ImagePreparationError | ToolImage.ImageModelUnsupported | ToolImage.InvalidImageOutput
  >()
  expect<Effect.Success<typeof prepared>>().type.toBeAssignableTo<
    ToolResult.StrictOutput<ToolImage.Metadata>
  >()
})
