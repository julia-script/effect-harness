import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Images from '../ImagePreparation.js'
import {
  ImageDecodeFailed,
  ImageEncodeFailed,
  ImageInputPixelsExceeded,
  UnsupportedImage,
} from '../ImagePreparationError.js'
import * as Container from './internal/Container.js'
import * as Png from './internal/Png.js'
import type { Sharp } from 'sharp'

const unavailable = (mimeType: string, message: string) =>
  new UnsupportedImage({ mimeType, operation: 'decode', message })

// Bun terminals borrow fixed input buffers and expose no abort API. Mask only the
// foreign terminal: interruption is delivered after settlement, before scope release.
const completionLease = <A, E>(
  work: () => Promise<A>,
  onError: (cause: unknown) => E,
  pending?: Set<Promise<void>>,
) =>
  Effect.uninterruptible(
    Effect.tryPromise({
      try: () => {
        const promise = work()
        const settled = promise.then(
          () => undefined,
          () => undefined,
        )
        pending?.add(settled)
        void settled.then(() => pending?.delete(settled))
        return promise
      },
      catch: onError,
    }),
  )

const code = (cause: unknown): unknown =>
  typeof cause === 'object' && cause !== null && 'code' in cause ? cause.code : undefined

// Dispose auxiliary native handles only after their foreign terminal settles.
const withSharp = async <A>(image: Sharp, terminal: (image: Sharp) => Promise<A>): Promise<A> => {
  try {
    return await terminal(image)
  } finally {
    image.destroy()
  }
}

/** Construct the native Bun adapter, loading its auxiliary validator only on evaluation.
 * Bun.Image must exist at evaluation time. Older Bun and other runtimes expose a
 * typed unavailable decoder instead of failing during module import.
 *
 * Sharp is an explicit auxiliary validator: failOn:warning fully validates JPEG
 * entropy that Bun may repair, inspects first-frame alpha and flattens white.
 * Animated WebP uses its explicit first-frame decoder fallback, because Bun's
 * static WebP decoder rejects animated containers on supported runtimes.
 * APNG uses an explicit frame validator/compositor, including an animation whose
 * default still image is excluded from the animation. Every compressed frame is validated.
 * Source decoding, orientation, resize and final encoding use real Bun.Image.
 * Neither construction nor evaluation changes Bun.Image.backend.
 */
export const adapter: Effect.Effect<Images.Adapter, UnsupportedImage> = Effect.gen(function* () {
  const Native =
    typeof Bun !== 'undefined' && typeof Bun.Image === 'function' ? Bun.Image : undefined
  if (Native === undefined)
    return {
      capabilities: {
        decode: [],
        encode: [],
        orientations: [],
        preserveAlpha: false,
        flattenWhite: false,
      },
      decode: (_bytes, mimeType) =>
        Effect.fail(unavailable(mimeType, 'This runtime has no Bun.Image')),
    } satisfies Images.Adapter
  const sharp = yield* completionLease(
    () => import('sharp').then((module) => module.default),
    () =>
      unavailable(
        'image/*',
        'The auxiliary strict validator requires the optional sharp dependency',
      ),
  )
  return {
    capabilities: {
      decode: ['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/bmp'],
      encode: ['image/png', 'image/jpeg', 'image/webp'],
      orientations: [1, 2, 3, 4, 5, 6, 7, 8],
      preserveAlpha: true,
      flattenWhite: true,
    },
    decode: Effect.fn('BunImagePreparation.decode')(function* (
      bytes: Uint8Array,
      mimeType: Images.InputMimeType,
      maxInputPixels: number,
    ) {
      const decodeError = (cause: unknown) =>
        code(cause) === 'ERR_IMAGE_FORMAT_UNSUPPORTED'
          ? unavailable(mimeType, 'Native decoder is unavailable on this machine')
          : new ImageDecodeFailed({ mimeType, message: 'Native image decode failed', cause })
      // Documented header-only terminal: no pixel allocation. Read original axes
      // before applying the requested pixel cap to every full-decode terminal.
      const header = yield* completionLease(
        () =>
          new Native(bytes, { autoOrient: false, maxPixels: Number.MAX_SAFE_INTEGER }).metadata(),
        decodeError,
      )
      if (`image/${header.format}` !== mimeType || header.width <= 0 || header.height <= 0)
        return yield* new ImageDecodeFailed({
          mimeType,
          message: 'Source MIME or dimensions do not match',
        })
      if (header.width > Math.floor(maxInputPixels / header.height))
        return yield* new ImageInputPixelsExceeded({
          width: header.width,
          height: header.height,
          maxInputPixels,
        })
      const container = yield* Container.inspect(bytes, mimeType)
      const pngFrames = mimeType === 'image/png' ? yield* Png.read(bytes) : []
      // Full source JPEG decode with warning rejection, never metadata-only.
      // GIF/WebP validation includes every frame before animated passthrough.
      if (mimeType === 'image/jpeg' || mimeType === 'image/gif' || mimeType === 'image/webp') {
        const pages = yield* completionLease(
          () =>
            withSharp(
              sharp(bytes, { failOn: 'warning', limitInputPixels: maxInputPixels }),
              (image) => image.metadata(),
            ),
          decodeError,
        )
        // Decode each canvas separately: animation length must not accidentally
        // multiply the first-frame pixel cap or allocate all frames together.
        for (let page = 0; page < (pages.pages ?? 1); page++)
          yield* completionLease(
            () =>
              withSharp(
                sharp(bytes, {
                  failOn: 'warning',
                  limitInputPixels: maxInputPixels,
                  page,
                  pages: 1,
                }),
                (image) => image.raw().toBuffer(),
              ),
            decodeError,
          )
      }
      const nativeInput =
        mimeType === 'image/webp' && container.animated
          ? yield* completionLease(
              () =>
                withSharp(
                  sharp(bytes, {
                    failOn: 'warning',
                    limitInputPixels: maxInputPixels,
                    page: 0,
                    pages: 1,
                  }),
                  (image) => image.png().toBuffer(),
                ),
              decodeError,
            )
          : bytes
      let png = yield* completionLease(
        () =>
          new Native(nativeInput, { autoOrient: false, maxPixels: maxInputPixels }).png().bytes(),
        decodeError,
      )
      // APNG may have a default still image outside the animation. Validate all
      // compressed frame data and compose the actual first animation frame onto
      // its declared transparent canvas, rather than silently using that still.
      for (const frame of pngFrames)
        yield* completionLease(
          () =>
            withSharp(
              sharp(frame.png, { failOn: 'warning', limitInputPixels: maxInputPixels }),
              (image) => image.raw().toBuffer(),
            ),
          decodeError,
        )
      const firstPngFrame = pngFrames[0]
      if (firstPngFrame !== undefined) {
        const canvas = yield* completionLease(
          () =>
            withSharp(
              sharp({
                create: {
                  width: header.width,
                  height: header.height,
                  channels: 4,
                  background: { r: 0, g: 0, b: 0, alpha: 0 },
                },
              }),
              (image) =>
                image
                  .composite([
                    {
                      input: Buffer.from(firstPngFrame.png),
                      left: firstPngFrame.left,
                      top: firstPngFrame.top,
                    },
                  ])
                  .png()
                  .toBuffer(),
            ),
          decodeError,
        )
        png = yield* completionLease(
          () => new Native(canvas, { autoOrient: false, maxPixels: maxInputPixels }).png().bytes(),
          decodeError,
        )
      }
      const pixels = yield* completionLease(
        () =>
          withSharp(sharp(png, { failOn: 'warning', limitInputPixels: maxInputPixels }), (image) =>
            image.ensureAlpha().raw().toBuffer({ resolveWithObject: true }),
          ),
        decodeError,
      )
      if (pixels.info.width !== header.width || pixels.info.height !== header.height)
        return yield* new ImageDecodeFailed({
          mimeType,
          message: 'Decoded canvas differs from source header',
        })
      let hasAlpha = false
      for (let p = 3; p < pixels.data.length; p += 4)
        if (pixels.data[p] !== 255) {
          hasAlpha = true
          break
        }
      const source = yield* Effect.acquireRelease(
        Effect.succeed({
          png: Uint8Array.from(png),
          open: true,
          pending: new Set<Promise<void>>(),
        }),
        (state) =>
          Effect.promise(async () => {
            state.open = false
            await Promise.all(state.pending)
            state.png = new Uint8Array(0)
          }),
      )
      return {
        metadata: {
          mimeType,
          original: { width: header.width, height: header.height },
          ...container,
          hasAlpha,
        },
        encode: Effect.fn('BunImagePreparation.encode')(function* (
          request: Images.EncodingRequest,
        ) {
          if (!source.open)
            return yield* new ImageEncodeFailed({
              mimeType: request.mimeType,
              message: 'Decoded image scope has closed',
            })
          const encodeError = (cause: unknown) =>
            new ImageEncodeFailed({
              mimeType: request.mimeType,
              message: 'Native image encode failed',
              cause,
            })
          // One lease spans auxiliary flattening and the native terminal, so an
          // independently-running encode cannot outlive scope-owned source bytes.
          const original = source.png
          return yield* completionLease(
            async () => {
              let input = original
              if (request.transparency === 'flatten-white' && hasAlpha)
                input = await withSharp(sharp(original), (image) =>
                  image
                    .flatten({ background: { r: 255, g: 255, b: 255 } })
                    .png()
                    .toBuffer(),
                )
              const image = new Native(input, { autoOrient: false, maxPixels: maxInputPixels })
              // Bun executes rotate before flip/flop. These pairs implement the
              // policy's integer map, including transpose/transverse (5 and 7).
              switch (request.orientation) {
                case 2:
                  image.flop()
                  break
                case 3:
                  image.rotate(180)
                  break
                case 4:
                  image.flip()
                  break
                case 5:
                  image.rotate(90).flop()
                  break
                case 6:
                  image.rotate(90)
                  break
                case 7:
                  image.rotate(90).flip()
                  break
                case 8:
                  image.rotate(270)
                  break
              }
              image.resize(request.dimensions.width, request.dimensions.height, {
                fit: 'fill',
                withoutEnlargement: true,
              })
              switch (request.mimeType) {
                case 'image/png':
                  image.png()
                  break
                case 'image/jpeg':
                  image.jpeg(request.quality === undefined ? {} : { quality: request.quality })
                  break
                case 'image/webp':
                  image.webp(request.quality === undefined ? {} : { quality: request.quality })
                  break
              }
              return image.bytes()
            },
            encodeError,
            source.pending,
          )
        }),
      } satisfies Images.DecodedImage
    }),
  } satisfies Images.Adapter
})

/** Toolkit-local native image service; codec availability is checked when this Layer is built. */
export const layer: Layer.Layer<Images.ImagePreparation, UnsupportedImage> = Layer.effect(
  Images.ImagePreparation,
  Effect.map(adapter, Images.make),
)
