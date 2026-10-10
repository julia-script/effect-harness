import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import type * as Scope from 'effect/Scope'
import type sharp from 'sharp'
import type * as Images from '../src/ImagePreparation.js'
import type { ImagePreparationError } from '../src/ImagePreparationError.js'
import * as NodeImages from '../src/images-node/index.js'
import * as WasmImages from '../src/images-wasm/index.js'

declare const photon: WasmImages.PhotonApi
declare const native: typeof sharp
const portable = WasmImages.makeAdapter(photon)
const node = NodeImages.makeAdapter(native, portable)

test('strict validation and native leases preserve scoped adapter types', () => {
  for (const adapter of [portable, node]) {
    expect(adapter.decode(new Uint8Array(), 'image/png', 100)).type.toBe<
      Effect.Effect<Images.DecodedImage, ImagePreparationError, Scope.Scope>
    >()
  }
})
