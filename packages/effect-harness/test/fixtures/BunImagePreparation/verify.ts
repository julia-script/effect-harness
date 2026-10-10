import assert from 'node:assert/strict'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import sharp from 'sharp'
import * as Images from '../../../src/ImagePreparation.js'
import * as Native from '../../../src/images-bun/index.js'

assert.equal(typeof Bun.Image, 'function', 'This matrix requires Bun.Image, never a silent skip')
const backend = Bun.Image.backend
const adapter = await Effect.runPromise(Native.adapter)
const limits: Images.Limits = {
  maxWidth: 100,
  maxHeight: 100,
  maxBytes: 100000,
  maxInputPixels: 10000,
}
const prepare = (bytes: Uint8Array, mime: string, overrides: Partial<Images.Limits> = {}) =>
  Effect.runPromise(
    Images.prepare(bytes, mime, { ...limits, ...overrides }).pipe(Effect.provide(Native.layer)),
  )
const failure = async (
  bytes: Uint8Array,
  mime: string,
  tag: string,
  overrides: Partial<Images.Limits> = {},
) => {
  const result = await Effect.runPromise(
    Images.prepare(bytes, mime, { ...limits, ...overrides }).pipe(
      Effect.provide(Native.layer),
      Effect.result,
    ),
  )
  assert.equal(result._tag, 'Failure')
  if (result._tag === 'Failure') assert.equal(result.failure._tag, tag)
}
const rgba = new Uint8Array([
  255, 0, 0, 255, 0, 255, 0, 255, 0, 0, 255, 255, 255, 255, 0, 255, 255, 0, 255, 255, 0, 255, 255,
  255,
])
const raw = { width: 3, height: 2, channels: 4 as const }
const png = await sharp(rgba, { raw }).png().toBuffer()
const jpeg = await sharp(rgba, { raw })
  .jpeg({ quality: 100, chromaSubsampling: '4:4:4' })
  .toBuffer()
const webp = await sharp(rgba, { raw }).webp({ lossless: true }).toBuffer()
const gif = await sharp(rgba, { raw }).gif().toBuffer()

// Independent minimal 24-bit BMP encoder: bottom-up padded BGR rows.
const bmp = new Uint8Array(54 + 12 * 2)
const bv = new DataView(bmp.buffer)
bmp.set([66, 77])
bv.setUint32(2, bmp.length, true)
bv.setUint32(10, 54, true)
bv.setUint32(14, 40, true)
bv.setInt32(18, 3, true)
bv.setInt32(22, 2, true)
bv.setUint16(26, 1, true)
bv.setUint16(28, 24, true)
for (let y = 0; y < 2; y++)
  for (let x = 0; x < 3; x++) {
    const p = (y * 3 + x) * 4,
      q = 54 + (1 - y) * 12 + x * 3
    bmp.set([rgba[p + 2] ?? 0, rgba[p + 1] ?? 0, rgba[p] ?? 0], q)
  }
const fixtures: ReadonlyArray<readonly [Uint8Array, Images.InputMimeType]> = [
  [png, 'image/png'],
  [jpeg, 'image/jpeg'],
  [gif, 'image/gif'],
  [webp, 'image/webp'],
  [bmp, 'image/bmp'],
]
for (const [bytes, mime] of fixtures) {
  console.log(`Checking ${mime} decode, resize, malformed input and limits`)
  const original = Uint8Array.from(bytes)
  const result = await prepare(bytes, mime)
  assert.deepEqual(result.original, { width: 3, height: 2 })
  assert.equal(result.passthrough, mime !== 'image/bmp')
  if (result.passthrough) assert.deepEqual(result.bytes, original)
  assert.notEqual(result.bytes.buffer, bytes.buffer)
  assert.deepEqual(Uint8Array.from(bytes), original)
  const small = await prepare(bytes, mime, { maxWidth: 2, maxHeight: 1 })
  const header = await new Bun.Image(small.bytes).metadata()
  assert.deepEqual(small.dimensions, { width: 2, height: 1 })
  assert.equal(header.width, 2)
  assert.equal(header.height, 1)
  await failure(bytes, mime, 'ImageInputPixelsExceeded', { maxInputPixels: 5 })
  await failure(bytes, mime, 'ImageCannotFit', { maxBytes: 4, maxWidth: 1, maxHeight: 1 })
  await failure(
    bytes.subarray(0, Math.max(16, Math.floor(bytes.length / 2))),
    mime,
    'ImageDecodeFailed',
  )
}
await failure(png, 'image/jpeg', 'ImageDecodeFailed')
await failure(png, 'image/tiff', 'UnsupportedImage')
await failure(png, 'image/png', 'InvalidImageLimits', { maxWidth: 0 })

// Allocation guard must run before any full native terminal, even with a small
// file whose valid header declares a much larger canvas than its compressed data.
const huge = Uint8Array.from(png)
const hv = new DataView(huge.buffer)
hv.setUint32(16, 100000)
hv.setUint32(20, 100000)
hv.setUint32(29, crc(huge.subarray(12, 29)))
const guardBytes = Bun.Image.prototype.bytes
let allocationTerminals = 0
Bun.Image.prototype.bytes = function () {
  allocationTerminals++
  return guardBytes.call(this)
}
try {
  await failure(huge, 'image/png', 'ImageInputPixelsExceeded')
  assert.equal(allocationTerminals, 0)
} finally {
  Bun.Image.prototype.bytes = guardBytes
}

let seed = 739
const noise = Uint8Array.from({ length: 64 * 32 * 4 }, (_, i) => {
  if (i % 4 === 3) return 128
  seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0
  return seed >>> 24
})
const noisy = await sharp(noise, { raw: { width: 64, height: 32, channels: 4 } })
  .png()
  .toBuffer()
const budgeted = await prepare(noisy, 'image/png', { maxBytes: 400 })
assert.equal(budgeted.passthrough, false)
assert.ok(Images.base64Length(budgeted.bytes.length) <= 400)
assert.ok(budgeted.dimensions.width < 64 && budgeted.dimensions.height < 32)
const actualBudgetDimensions = await new Bun.Image(budgeted.bytes).metadata()
assert.equal(actualBudgetDimensions.width, budgeted.dimensions.width)
assert.equal(actualBudgetDimensions.height, budgeted.dimensions.height)
assert.equal(budgeted.transparency, 'preserved')
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const image = yield* adapter.decode(png, 'image/png', 6)
      yield* image.encode({
        mimeType: 'image/jpeg',
        quality: 40,
        transparency: 'preserve',
        dimensions: { width: 1, height: 1 },
        orientation: 1,
        frame: 'first',
      })
      const restored = yield* image.encode({
        mimeType: 'image/png',
        transparency: 'preserve',
        dimensions: { width: 3, height: 2 },
        orientation: 1,
        frame: 'first',
      })
      const pixels = yield* Effect.promise(() => sharp(restored).ensureAlpha().raw().toBuffer())
      assert.deepEqual([...pixels], [...rgba])
    }),
  ),
)

// Inject genuine TIFF EXIF, with no reliance on the implementation's mapping.
const tiff = (value: Images.Orientation) => {
  const bytes = new Uint8Array(26),
    v = new DataView(bytes.buffer)
  bytes.set([73, 73])
  v.setUint16(2, 42, true)
  v.setUint32(4, 8, true)
  v.setUint16(8, 1, true)
  v.setUint16(10, 0x112, true)
  v.setUint16(12, 3, true)
  v.setUint32(14, 1, true)
  v.setUint16(18, value, true)
  return bytes
}
function crc(bytes: Uint8Array) {
  let value = 0xffffffff
  for (const b of bytes) {
    value ^= b
    for (let i = 0; i < 8; i++) value = (value >>> 1) ^ (value & 1 ? 0xedb88320 : 0)
  }
  return (value ^ 0xffffffff) >>> 0
}
const pngChunk = (kind: string, payload: Uint8Array) => {
  const bytes = new Uint8Array(payload.length + 12),
    v = new DataView(bytes.buffer)
  v.setUint32(0, payload.length)
  for (let i = 0; i < 4; i++) bytes[i + 4] = kind.charCodeAt(i)
  bytes.set(payload, 8)
  v.setUint32(bytes.length - 4, crc(bytes.subarray(4, bytes.length - 4)))
  return bytes
}
const imageData = (input: Uint8Array) => {
  const view = new DataView(input.buffer, input.byteOffset, input.byteLength),
    parts: Array<number> = []
  for (let p = 8; p < input.length;) {
    const length = view.getUint32(p)
    if (String.fromCharCode(...input.subarray(p + 4, p + 8)) === 'IDAT')
      parts.push(...input.subarray(p + 8, p + 8 + length))
    p += length + 12
  }
  return new Uint8Array(parts)
}
const withPngExif = (value: Images.Orientation) => {
  const chunk = new Uint8Array(38),
    v = new DataView(chunk.buffer)
  v.setUint32(0, 26)
  chunk.set([101, 88, 73, 102], 4)
  chunk.set(tiff(value), 8)
  v.setUint32(34, crc(chunk.subarray(4, 34)))
  return new Uint8Array([...png.subarray(0, 33), ...chunk, ...png.subarray(33)])
}
const withJpegExif = (value: Images.Orientation) => {
  const payload = new Uint8Array([69, 120, 105, 102, 0, 0, ...tiff(value)])
  return new Uint8Array([
    255,
    216,
    255,
    225,
    0,
    payload.length + 2,
    ...payload,
    ...jpeg.subarray(2),
  ])
}
const orders: ReadonlyArray<ReadonlyArray<number>> = [
  [],
  [0, 1, 2, 3, 4, 5],
  [2, 1, 0, 5, 4, 3],
  [5, 4, 3, 2, 1, 0],
  [3, 4, 5, 0, 1, 2],
  [0, 3, 1, 4, 2, 5],
  [3, 0, 4, 1, 5, 2],
  [5, 2, 4, 1, 3, 0],
  [2, 5, 1, 4, 0, 3],
]
for (const orientation of [1, 2, 3, 4, 5, 6, 7, 8] as const) {
  console.log(`Checking EXIF orientation ${orientation}`)
  const orientedWebp = await sharp(webp)
    .withMetadata({ orientation })
    .webp({ lossless: true })
    .toBuffer()
  for (const [input, mime] of [
    [withPngExif(orientation), 'image/png'],
    [withJpegExif(orientation), 'image/jpeg'],
    [orientedWebp, 'image/webp'],
  ] as const) {
    const source = await sharp(input).ensureAlpha().raw().toBuffer()
    const size = orientation >= 5 ? { width: 2, height: 3 } : { width: 3, height: 2 }
    const output = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const image = yield* adapter.decode(input, mime, 100)
          assert.equal(image.metadata.orientation, orientation)
          assert.deepEqual(image.metadata.original, { width: 3, height: 2 })
          return yield* image.encode({
            mimeType: 'image/png',
            transparency: 'preserve',
            dimensions: size,
            orientation,
            frame: 'first',
          })
        }),
      ),
    )
    const decoded = await sharp(output).ensureAlpha().raw().toBuffer({ resolveWithObject: true })
    assert.equal(decoded.info.width, size.width)
    assert.equal(decoded.info.height, size.height)
    const expected = (orders[orientation] ?? []).flatMap((p) => [
      ...source.subarray(p * 4, p * 4 + 4),
    ])
    for (let i = 0; i < expected.length; i++)
      assert.ok(
        Math.abs((decoded.data[i] ?? 0) - (expected[i] ?? 0)) <= 3,
        `orientation ${orientation}, ${mime}, pixel byte ${i}`,
      )
    assert.equal((await sharp(output).metadata()).orientation, undefined)
    const result = await prepare(input, mime)
    assert.equal(result.orientation, orientation)
    assert.deepEqual(result.dimensions, size)
    assert.equal(result.passthrough, orientation === 1)
  }
}

// Native versions differ in warning handling. Record the real outcome for each
// warning class; strict adapter rejection is required regardless of the runtime.
const nativeDamage = async (input: Uint8Array, label: string) => {
  try {
    assert.ok((await new Bun.Image(input).png().bytes()).length > 0)
    console.log(`Native Bun accepted ${label}; strict adapter must reject`)
  } catch (cause) {
    assert.ok(typeof cause === 'object' && cause !== null && 'code' in cause)
    assert.equal(cause.code, 'ERR_IMAGE_DECODE_FAILED')
    console.log(`Native Bun rejected ${label}; strict adapter must also reject`)
  }
}
const missingEnd = jpeg.subarray(0, jpeg.length - 2)
console.log('Checking native damaged JPEG behavior')
await nativeDamage(missingEnd, 'missing JPEG end marker')
await failure(missingEnd, 'image/jpeg', 'ImageDecodeFailed')
const sos = jpeg.findIndex((value, i) => value === 255 && jpeg[i + 1] === 0xda)
assert.ok(sos > 0)
const scanStart = sos + 2 + ((jpeg[sos + 2] ?? 0) * 256 + (jpeg[sos + 3] ?? 0))
const truncated = new Uint8Array([...jpeg.subarray(0, scanStart + 2), 255, 217])
await nativeDamage(truncated, 'truncated JPEG entropy with intact end marker')
await failure(truncated, 'image/jpeg', 'ImageDecodeFailed')
const stray = new Uint8Array([...jpeg.subarray(0, sos), 12, 34, ...jpeg.subarray(sos)])
await nativeDamage(stray, 'stray bytes before JPEG scan')
await failure(stray, 'image/jpeg', 'ImageDecodeFailed')
const corruptPng = Uint8Array.from(png)
corruptPng[corruptPng.length - 5] = (corruptPng[corruptPng.length - 5] ?? 0) ^ 1
await failure(corruptPng, 'image/png', 'ImageDecodeFailed')

const transparent = await sharp({
  create: { width: 4, height: 2, channels: 4, background: { r: 255, g: 0, b: 0, alpha: 0.5 } },
})
  .png()
  .toBuffer()
const alpha = await prepare(transparent, 'image/png', { maxWidth: 2, maxHeight: 1 })
assert.equal(alpha.transparency, 'preserved')
const ar = await sharp(alpha.bytes).ensureAlpha().raw().toBuffer()
assert.ok((ar[3] ?? 255) < 255)
const flattened = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const image = yield* adapter.decode(transparent, 'image/png', 100)
      assert.equal(image.metadata.hasAlpha, true)
      return yield* image.encode({
        mimeType: 'image/jpeg',
        quality: 100,
        transparency: 'flatten-white',
        dimensions: { width: 4, height: 2 },
        orientation: 1,
        frame: 'first',
      })
    }),
  ),
)
const fr = await sharp(flattened).raw().toBuffer()
assert.ok((fr[0] ?? 0) > 245 && (fr[1] ?? 0) > 120 && (fr[2] ?? 0) > 120)

const frameData = new Uint8Array([...rgba, ...rgba.map((v, i) => (i % 4 === 3 ? v : 255 - v))])
const animatedGif = await sharp(frameData, {
  raw: { width: 3, height: 4, channels: 4, pageHeight: 2 },
})
  .gif({ loop: 0, delay: [100, 100] })
  .toBuffer()
const animatedWebp = await sharp(frameData, {
  raw: { width: 3, height: 4, channels: 4, pageHeight: 2 },
})
  .webp({ lossless: true, loop: 0, delay: [100, 100] })
  .toBuffer()
for (const [input, mime] of [
  [animatedGif, 'image/gif'],
  [animatedWebp, 'image/webp'],
] as const) {
  const pass = await prepare(input, mime, { maxInputPixels: 6 })
  assert.equal(pass.animation, 'unchanged')
  assert.deepEqual(pass.bytes, Uint8Array.from(input))
  const single = await prepare(input, mime, { maxWidth: 2, maxHeight: 1 })
  assert.equal(single.animation, 'first-frame')
  assert.equal((await sharp(single.bytes).metadata()).pages, undefined)
  const fullFrame = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const decoded = yield* adapter.decode(input, mime, 6)
        return yield* decoded.encode({
          mimeType: 'image/png',
          transparency: 'preserve',
          dimensions: { width: 3, height: 2 },
          orientation: 1,
          frame: 'first',
        })
      }),
    ),
  )
  const firstFrame = await sharp(fullFrame).ensureAlpha().raw().toBuffer()
  assert.deepEqual([...firstFrame], [...rgba])
}

const brokenWebp = Uint8Array.from(animatedWebp)
const wv = new DataView(brokenWebp.buffer)
let webpFrame = 0
for (let p = 12; p < brokenWebp.length;) {
  const length = wv.getUint32(p + 4, true)
  if (String.fromCharCode(...brokenWebp.subarray(p, p + 4)) === 'ANMF' && ++webpFrame === 2) {
    // Leave frame and VP8L dimensions intact while corrupting the entropy data.
    brokenWebp.fill(0, p + 8 + 16 + 8 + 5, p + 8 + length)
    break
  }
  p += 8 + length + (length & 1)
}
assert.equal(webpFrame, 2)
await failure(brokenWebp, 'image/webp', 'ImageDecodeFailed')
const brokenGif = Uint8Array.from(animatedGif)
let gifPosition = 13
const gifPacked = brokenGif[10] ?? 0
if (gifPacked & 128) gifPosition += 3 * 2 ** ((gifPacked & 7) + 1)
let gifFrame = 0
while (gifPosition < brokenGif.length) {
  const marker = brokenGif[gifPosition++]
  if (marker === 0x3b) break
  if (marker === 0x21) gifPosition++
  else if (marker === 0x2c) {
    const packed = brokenGif[gifPosition + 8] ?? 0
    gifPosition += 9
    if (packed & 128) gifPosition += 3 * 2 ** ((packed & 7) + 1)
    gifPosition++
    if (++gifFrame === 2) {
      const length = brokenGif[gifPosition++] ?? 0
      brokenGif.fill(255, gifPosition, gifPosition + length)
      break
    }
  }
  for (;;) {
    const length = brokenGif[gifPosition++] ?? 0
    if (length === 0) break
    gifPosition += length
  }
}
assert.equal(gifFrame, 2)
await failure(brokenGif, 'image/gif', 'ImageDecodeFailed')

// Two-frame APNG; then an APNG whose default still is outside its animation.
const control = (seq: number, width = 3, height = 2, left = 0, top = 0) => {
  const data = new Uint8Array(26),
    v = new DataView(data.buffer)
  v.setUint32(0, seq)
  v.setUint32(4, width)
  v.setUint32(8, height)
  v.setUint32(12, left)
  v.setUint32(16, top)
  v.setUint16(20, 1)
  v.setUint16(22, 10)
  return pngChunk('fcTL', data)
}
const animation = (count: number) => {
  const d = new Uint8Array(8)
  new DataView(d.buffer).setUint32(0, count)
  return pngChunk('acTL', d)
}
const frameDataChunk = (seq: number, input: Uint8Array) => {
  const d = new Uint8Array(input.length + 4)
  new DataView(d.buffer).setUint32(0, seq)
  d.set(input, 4)
  return pngChunk('fdAT', d)
}
const animatedPng = new Uint8Array([
  ...png.subarray(0, 33),
  ...animation(2),
  ...control(0),
  ...pngChunk('IDAT', imageData(png)),
  ...control(1),
  ...frameDataChunk(2, imageData(png)),
  ...pngChunk('IEND', new Uint8Array(0)),
])
assert.equal((await prepare(animatedPng, 'image/png')).animation, 'unchanged')
assert.equal(
  (await prepare(animatedPng, 'image/png', { maxWidth: 2, maxHeight: 1 })).animation,
  'first-frame',
)
const green = await sharp({
  create: { width: 1, height: 1, channels: 4, background: { r: 0, g: 255, b: 0, alpha: 1 } },
})
  .png()
  .toBuffer()
const outsideDefault = new Uint8Array([
  ...png.subarray(0, 33),
  ...animation(1),
  ...pngChunk('IDAT', imageData(png)),
  ...control(0, 1, 1, 1, 1),
  ...frameDataChunk(1, imageData(green)),
  ...pngChunk('IEND', new Uint8Array(0)),
])
const canvas = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const decoded = yield* adapter.decode(outsideDefault, 'image/png', 6)
      return yield* decoded.encode({
        mimeType: 'image/png',
        transparency: 'preserve',
        dimensions: { width: 3, height: 2 },
        orientation: 1,
        frame: 'first',
      })
    }),
  ),
)
const cr = await sharp(canvas).ensureAlpha().raw().toBuffer()
for (let i = 0; i < 6; i++) {
  assert.equal(cr[i * 4 + 3], i === 4 ? 255 : 0)
  if (i === 4) assert.equal(cr[i * 4 + 1], 255)
}
const brokenSecondData = Uint8Array.from(imageData(png))
brokenSecondData[0] = 0
const brokenSecond = new Uint8Array([
  ...png.subarray(0, 33),
  ...animation(2),
  ...control(0),
  ...pngChunk('IDAT', imageData(png)),
  ...control(1),
  ...frameDataChunk(2, brokenSecondData),
  ...pngChunk('IEND', new Uint8Array(0)),
])
await failure(brokenSecond, 'image/png', 'ImageDecodeFailed')

// Force a native terminal to remain pending after a real native decode. The lease
// must defer interruption/finalizers until the borrowed-byte terminal settles.
const actualBytes = Bun.Image.prototype.bytes
let started!: () => void, release!: () => void
const began = new Promise<void>((resolve) => {
  started = resolve
})
const gate = new Promise<void>((resolve) => {
  release = resolve
})
let complete = false,
  finalized = false,
  once = true
Bun.Image.prototype.bytes = function () {
  const actual = actualBytes.call(this)
  if (!once) return actual
  once = false
  started()
  return actual.then(async (bytes) => {
    await gate
    complete = true
    return bytes
  })
}
try {
  const fiber = Effect.runFork(
    Effect.scoped(adapter.decode(png, 'image/png', 100)).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          assert.equal(complete, true)
          finalized = true
        }),
      ),
    ),
  )
  await began
  fiber.interruptUnsafe(123)
  await new Promise<void>((resolve) => setTimeout(resolve, 10))
  assert.equal(finalized, false)
  assert.equal(fiber.pollUnsafe(), undefined)
  release()
  await Effect.runPromise(Fiber.await(fiber))
  assert.equal(finalized, true)
} finally {
  Bun.Image.prototype.bytes = actualBytes
}

// Independently-running encodes are covered by the decoded source's scope lease.
const scope = await Effect.runPromise(Scope.make())
const scopedImage = await Effect.runPromise(
  adapter.decode(png, 'image/png', 100).pipe(Effect.provideService(Scope.Scope, scope)),
)
let encodeStarted!: () => void, encodeRelease!: () => void
const encodeBegan = new Promise<void>((resolve) => {
  encodeStarted = resolve
})
const encodeGate = new Promise<void>((resolve) => {
  encodeRelease = resolve
})
Bun.Image.prototype.bytes = function () {
  const actual = actualBytes.call(this)
  encodeStarted()
  return actual.then(async (output) => {
    await encodeGate
    return output
  })
}
try {
  const encoding = Effect.runFork(
    scopedImage.encode({
      mimeType: 'image/png',
      transparency: 'preserve',
      dimensions: { width: 3, height: 2 },
      orientation: 1,
      frame: 'first',
    }),
  )
  await encodeBegan
  const closing = Effect.runFork(Scope.close(scope, Exit.void))
  await new Promise<void>((resolve) => setTimeout(resolve, 10))
  assert.equal(closing.pollUnsafe(), undefined)
  encoding.interruptUnsafe(123)
  assert.equal(encoding.pollUnsafe(), undefined)
  encodeRelease()
  await Effect.runPromise(Fiber.await(encoding))
  await Effect.runPromise(Fiber.await(closing))
  const closed = await Effect.runPromise(
    scopedImage
      .encode({
        mimeType: 'image/png',
        transparency: 'preserve',
        dimensions: { width: 3, height: 2 },
        orientation: 1,
        frame: 'first',
      })
      .pipe(Effect.result),
  )
  assert.equal(closed._tag, 'Failure')
  if (closed._tag === 'Failure') assert.equal(closed.failure._tag, 'ImageEncodeFailed')
} finally {
  Bun.Image.prototype.bytes = actualBytes
}

const parallel = await Promise.all([
  prepare(png, 'image/png', { maxWidth: 2, maxHeight: 1 }),
  prepare(jpeg, 'image/jpeg'),
])
assert.deepEqual(parallel[0]?.dimensions, { width: 2, height: 1 })
assert.deepEqual(parallel[1]?.dimensions, { width: 3, height: 2 })
assert.equal(Bun.Image.backend, backend)
const nativeConstructor = Bun.Image
Object.defineProperty(Bun, 'Image', { value: undefined })
try {
  const unavailable = await Effect.runPromise(Native.adapter)
  assert.deepEqual(unavailable.capabilities.decode, [])
  await failure(png, 'image/png', 'UnsupportedImage')
} finally {
  Object.defineProperty(Bun, 'Image', { value: nativeConstructor })
}
console.log('Bun native matrix passed')
