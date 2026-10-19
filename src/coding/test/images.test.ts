/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Image processors: the fitting algorithm over each stock codec, and `read` with one.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, FileSystem, Path } from 'effect'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'
import sharp from 'sharp'
import { BunImageProcessor } from '../bun/index.ts'
import * as ImageProcessor from '../ImageProcessor.ts'
import { read } from '../internal/read.ts'
import * as Workspace from '../internal/workspace.ts'
import { SharpImageProcessor } from '../sharp/index.ts'
import { notesOf, run } from './support.ts'

const RED = [255, 0, 0]
const BLUE = [0, 0, 255]

/** A `width`x`height` image, left half red and right half blue, as raw RGB. */
const halves = (width: number, height: number) =>
  Buffer.from(
    Array.from({ length: width * height }, (_, index) =>
      index % width < width / 2 ? RED : BLUE,
    ).flat(),
  )

const encode = (width: number, height: number, format: 'png' | 'jpeg') => {
  const image = sharp(halves(width, height), { raw: { width, height, channels: 3 } })
  return (format === 'png' ? image.png() : image.jpeg())
    .toBuffer()
    .then((buffer) => new Uint8Array(buffer))
}

/** A `width`x`height` 24-bit BMP, all red: rows bottom-up, BGR, padded to 4 bytes. */
const bitmap = (width: number, height: number) => {
  const row = Math.ceil((width * 3) / 4) * 4
  const size = 54 + row * height
  const bytes = new Uint8Array(size)
  const view = new DataView(bytes.buffer)
  bytes.set([0x42, 0x4d])
  view.setUint32(2, size, true)
  view.setUint32(10, 54, true)
  view.setUint32(14, 40, true)
  view.setInt32(18, width, true)
  view.setInt32(22, height, true)
  view.setUint16(26, 1, true)
  view.setUint16(28, 24, true)
  for (let y = 0; y < height; y++)
    for (let x = 0; x < width; x++) bytes.set([0, 0, 255], 54 + y * row + x * 3)
  return bytes
}

/** Noise, which compresses badly. */
const noise = (width: number, height: number) =>
  sharp(
    Buffer.from(Array.from({ length: width * height * 3 }, () => Math.floor(Math.random() * 256))),
    { raw: { width, height, channels: 3 } },
  )
    .png()
    .toBuffer()
    .then((buffer) => new Uint8Array(buffer))

/** An APP1 segment whose EXIF says `orientation`, after the JPEG's start marker. */
const withOrientation = (jpeg: Uint8Array, orientation: number) => {
  const tiff = [0x4d, 0x4d, 0, 0x2a, 0, 0, 0, 8, 0, 1, 0x01, 0x12, 0, 3, 0, 0, 0, 1]
  const entry = [0, orientation, 0, 0, 0, 0, 0, 0]
  const exif = [0x45, 0x78, 0x69, 0x66, 0, 0, ...tiff, ...entry]
  const length = exif.length + 2
  const segment = [0xff, 0xe1, length >> 8, length & 0xff, ...exif]
  return Uint8Array.from([...jpeg.subarray(0, 2), ...segment, ...jpeg.subarray(2)])
}

const decodeRaw = (base64: string) =>
  sharp(Buffer.from(base64, 'base64')).removeAlpha().raw().toBuffer({ resolveWithObject: true })

const isRed = (pixel: ReadonlyArray<number>) =>
  (pixel[0] ?? 0) > 200 && (pixel[1] ?? 0) < 60 && (pixel[2] ?? 0) < 60

const processors = [
  ['Bun.Image', BunImageProcessor.layer, { bmp: true }],
  ['sharp', SharpImageProcessor.layer, { bmp: false }],
] as const

const limits = ImageProcessor.DEFAULT_LIMITS

describe.each(processors)('%s', (_name, layer, supports) => {
  const prepare = (bytes: Uint8Array, mimeType: string, within = limits) =>
    Effect.runPromise(
      Effect.flatMap(
        ImageProcessor.ImageProcessor,
        (processor) => processor.prepare(bytes, mimeType, within),
        // Each test is an entry point that provides its own processor.
        // oxlint-disable-next-line effecttsgo/strict-effect-provide
      ).pipe(Effect.provide(layer)),
    )

  test('returns an upright image within the limits as it is', async () => {
    const png = await encode(4, 2, 'png')
    expect(await prepare(png, 'image/png')).toEqual({
      data: Buffer.from(png).toString('base64'),
      mimeType: 'image/png',
    })
  })

  test('scales a large image to the maximum size and says so', async () => {
    const prepared = await prepare(await encode(3000, 1000, 'png'), 'image/png')
    expect(prepared?.resized).toEqual({
      from: { width: 3000, height: 1000 },
      to: { width: 2000, height: 667 },
    })
    const { info } = await decodeRaw(prepared?.data ?? '')
    expect([info.width, info.height]).toEqual([2000, 667])
  })

  test('turns an image upright by its EXIF orientation', async () => {
    // Stored 40x20, red on the left; orientation 6 rotates it clockwise: red on top.
    const jpeg = withOrientation(await encode(40, 20, 'jpeg'), 6)
    const prepared = await prepare(jpeg, 'image/jpeg')
    expect(prepared?.mimeType).toBe('image/jpeg')
    expect(prepared?.resized).toBeUndefined()
    const { data, info } = await decodeRaw(prepared?.data ?? '')
    expect([info.width, info.height]).toEqual([20, 40])
    const at = (x: number, y: number) => [...data.subarray((y * 20 + x) * 3, (y * 20 + x) * 3 + 3)]
    expect(isRed(at(10, 5))).toBe(true)
    expect(isRed(at(10, 35))).toBe(false)
  })

  test.if(supports.bmp)('converts BMP to PNG', async () => {
    const bmp = bitmap(4, 2)
    const prepared = await prepare(bmp, 'image/bmp')
    expect(prepared?.mimeType).toBe('image/png')
    expect(prepared?.convertedFrom).toBe('image/bmp')
  })

  test('falls back to JPEG, then smaller sizes, until the bytes fit', async () => {
    const prepared = await prepare(await noise(200, 200), 'image/png', {
      ...limits,
      maxBytes: 8000,
    })
    expect(prepared?.mimeType).toBe('image/jpeg')
    expect(prepared?.convertedFrom).toBe('image/png')
    expect(prepared?.resized?.to.width).toBeLessThan(200)
    expect(Buffer.from(prepared?.data ?? '', 'base64').length).toBeLessThanOrEqual(6000)
  })

  test('cannot prepare bytes that do not decode', async () => {
    const png = await encode(4, 2, 'png')
    const broken = Uint8Array.from([...png.subarray(0, 33), ...Array.from({ length: 40 }, () => 7)])
    expect(await prepare(broken, 'image/png')).toBeUndefined()
  })
})

/** `run` with the workspace `CodingHandlers` builds when an image processor is in context. */
const withImages = <A, E>(f: Parameters<typeof run<A, E>>[0]) =>
  run((fixture) =>
    Workspace.make.pipe(
      Effect.provideService(FileSystem.FileSystem, fixture.workspace.fs),
      Effect.provideService(Path.Path, fixture.workspace.path),
      Effect.provideService(ChildProcessSpawner, fixture.workspace.spawner),
      Effect.provideService(
        ImageProcessor.ImageProcessor,
        ImageProcessor.make(SharpImageProcessor.codec),
      ),
      Effect.flatMap((workspace) => f({ ...fixture, workspace })),
    ),
  )

describe('read with an image processor', () => {
  test('reports a resize as a diagnostic', () =>
    withImages(({ workspace, config, write }) =>
      Effect.gen(function* () {
        yield* write('wide.png', yield* Effect.promise(() => encode(4000, 100, 'png')))
        const result = yield* read(workspace, { path: 'wide.png' }, config)
        expect(result.content[0]?.type).toBe('image')
        expect(notesOf(result)).toBe(
          'Read image file [image/png]. Resized from 4000x100 to 2000x50. Multiply coordinates by 2.00 to map them to the original.',
        )
      }),
    ))

  test('refuses an image it cannot prepare', () =>
    withImages(({ workspace, config, write }) =>
      Effect.gen(function* () {
        const png = yield* Effect.promise(() => encode(4, 2, 'png'))
        yield* write('broken.png', Uint8Array.from([...png.subarray(0, 33), 7, 7, 7, 7]))
        const failure = yield* Effect.flip(read(workspace, { path: 'broken.png' }, config))
        expect(notesOf(failure)).toBe(
          'broken.png is an image (image/png) that cannot be prepared for the model',
        )
      }),
    ))
})
