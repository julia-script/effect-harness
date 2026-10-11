/** Image conformance consumes an injected preparation service and an independent pixel inspector. */
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import type * as Scope from 'effect/Scope'
import type * as Images from '../ImagePreparation.js'
import type { ImagePreparationError } from '../ImagePreparationError.js'
import type { assertions } from '../internal/ServiceConformance.js'
import {
  scopedCase,
  type ConformanceCase,
  type ScopedFixture,
  type ServiceConformanceError,
} from '../internal/ServiceConformance.js'

export interface ImageSample {
  readonly bytes: Uint8Array
  readonly mimeType: Images.InputMimeType
  readonly dimensions: Images.Dimensions
}
export interface OrientationSample extends ImageSample {
  readonly orientation: Images.Orientation
  readonly upright: Images.Dimensions
  /** Independent expected upright RGBA grid; never compute with the production orientation helper. */
  readonly rgba: ReadonlyArray<number>
}

export interface ImagesFixture<E = never, R = never> extends ScopedFixture<E, R> {
  readonly service: Images.Service
  /** Upright opaque nonanimated PNG/JPEG/WebP with at least two pixels. */
  readonly sample: ImageSample
  /** Plausible header with a broken body, not merely an unsupported MIME. */
  readonly malformed: Uint8Array
  readonly inspect: (bytes: Uint8Array) => Effect.Effect<
    {
      readonly dimensions: Images.Dimensions
      readonly rgba: ReadonlyArray<number>
      readonly orientation: number | undefined
    },
    E,
    R
  >
  readonly orientations?: ReadonlyArray<OrientationSample>
  /** Controlled noncancellable decode for this same service, armed only for the lease case. */
  readonly lease?: {
    readonly arm: Effect.Effect<void, E, R>
    readonly started: Effect.Effect<void, E, R>
    readonly settle: Effect.Effect<void, E, R>
    readonly released: Effect.Effect<boolean, E, R>
  }
}

export interface ImagesOptions<E = never, R = never, FE = E> {
  readonly make: Effect.Effect<ImagesFixture<E, R>, FE, R | Scope.Scope>
  readonly capabilities?: {
    readonly orientations?: boolean
    readonly lease?: boolean
    /** An unavailable platform still has an explicit unsupported case; no silent green omission. */
    readonly unavailable?: boolean
  }
}

const limits = (sample: ImageSample): Images.Limits => ({
  maxWidth: sample.dimensions.width,
  maxHeight: sample.dimensions.height,
  maxBytes: Math.max(10000, sample.bytes.length * 4),
  maxInputPixels: sample.dimensions.width * sample.dimensions.height,
})

/**
 * Core cases verify decoded output, padded base64 budgets, detached bytes, malformed bodies and
 * pixel guards. Optional orientation cases require exactly all eight independently specified grids.
 * Lease checks arrange a pending real adapter operation and interrupt before settling it; cleanup
 * must not release borrowed resources early. No codec, filesystem or platform is imported here.
 */
export const imagesConformance = <E = never, R = never, FE = E>(
  options: ImagesOptions<E, R, FE>,
): ReadonlyArray<
  ConformanceCase<E | FE | ImagePreparationError | ServiceConformanceError, Exclude<R, Scope.Scope>>
> => {
  type Failure = E | ImagePreparationError | ServiceConformanceError
  const cases: Array<
    ConformanceCase<
      E | FE | ImagePreparationError | ServiceConformanceError,
      Exclude<R, Scope.Scope>
    >
  > = []
  const add = (
    name: string,
    run: (
      f: ImagesFixture<E, R>,
      a: ReturnType<typeof assertions>,
    ) => Effect.Effect<void, Failure, R | Scope.Scope>,
  ) =>
    cases.push(
      scopedCase<ImagesFixture<E, R>, E, R, FE, Failure, R | Scope.Scope>(name, options.make, run),
    )
  const caps = { ...options.capabilities }
  if (caps.unavailable) {
    if (caps.orientations || caps.lease)
      add('images: unavailable adapter cannot select supported capabilities', (_f, { same }) =>
        same('orientation or lease checks require an available adapter', false, true),
      )
    add('images: unavailable platform is explicit', (f, { failure }) =>
      failure(
        'typed unavailable',
        f.service.prepare(f.sample.bytes, f.sample.mimeType, limits(f.sample)),
        (e) => e._tag === 'UnsupportedImage',
      ),
    )
    return cases
  }

  add('images: validated passthrough owns bytes', (f, { same }) =>
    Effect.gen(function* () {
      const before = Uint8Array.from(f.sample.bytes)
      const prepared = yield* f.service.prepare(f.sample.bytes, f.sample.mimeType, limits(f.sample))
      yield* same('eligible passthrough', prepared.passthrough, true)
      yield* same('identical bytes', [...prepared.bytes], [...before])
      yield* same('detached input', prepared.bytes.buffer === f.sample.bytes.buffer, false)
      yield* same('input untouched', [...f.sample.bytes], [...before])
      yield* same('dimensions', prepared.dimensions, f.sample.dimensions)
      const pixels = yield* f.inspect(prepared.bytes)
      yield* same('actual decoded dimensions', pixels.dimensions, prepared.dimensions)
      yield* same(
        'valid RGBA allocation',
        pixels.rgba.length,
        prepared.dimensions.width * prepared.dimensions.height * 4,
      )
    }),
  )

  add('images: malformed body and input allocation guard', (f, { failure }) =>
    Effect.gen(function* () {
      const policy = limits(f.sample)
      yield* failure(
        'broken body cannot pass header-only validation',
        f.service.prepare(f.malformed, f.sample.mimeType, policy),
        (e) => e._tag === 'ImageDecodeFailed',
      )
      yield* failure(
        'pixel allocation refused',
        f.service.prepare(f.sample.bytes, f.sample.mimeType, { ...policy, maxInputPixels: 1 }),
        (e) => e._tag === 'ImageInputPixelsExceeded',
      )
      yield* failure(
        'invalid caller limits',
        f.service.prepare(f.sample.bytes, f.sample.mimeType, { ...policy, maxWidth: 0 }),
        (e) => e._tag === 'InvalidImageLimits',
      )
      yield* failure(
        'unsupported MIME remains explicit',
        f.service.prepare(f.sample.bytes, 'image/unknown', policy),
        (e) => e._tag === 'UnsupportedImage',
      )
    }),
  )

  add('images: dimensions and padded base64 budget', (f, { same, failure }) =>
    Effect.gen(function* () {
      const padded = 4 * Math.ceil(f.sample.bytes.length / 3)
      const exact = yield* f.service.prepare(f.sample.bytes, f.sample.mimeType, {
        ...limits(f.sample),
        maxBytes: padded,
      })
      yield* same('exact padded passthrough budget', exact.passthrough, true)
      const reduced = yield* f.service.prepare(f.sample.bytes, f.sample.mimeType, {
        ...limits(f.sample),
        maxBytes: padded - 1,
      })
      yield* same('one byte below padded budget cannot passthrough', reduced.passthrough, false)
      yield* same('reencoded padded budget', 4 * Math.ceil(reduced.bytes.length / 3) < padded, true)
      yield* same(
        'reencoded output decodes',
        (yield* f.inspect(reduced.bytes)).dimensions,
        reduced.dimensions,
      )
      const prepared = yield* f.service.prepare(f.sample.bytes, f.sample.mimeType, {
        ...limits(f.sample),
        maxWidth: 1,
        maxHeight: 1,
        maxBytes: 10000,
      })
      yield* same('bounded dimensions', prepared.dimensions, { width: 1, height: 1 })
      yield* same(
        'actual encoded dimensions',
        (yield* f.inspect(prepared.bytes)).dimensions,
        prepared.dimensions,
      )
      yield* same('padded base64 bound', 4 * Math.ceil(prepared.bytes.length / 3) <= 10000, true)
      yield* failure(
        'impossible budget',
        f.service.prepare(f.sample.bytes, f.sample.mimeType, { ...limits(f.sample), maxBytes: 1 }),
        (e) => e._tag === 'ImageCannotFit',
      )
    }),
  )

  if (caps.orientations)
    add('images: all EXIF orientations match independent pixels', (f, { same }) =>
      Effect.gen(function* () {
        if (f.orientations === undefined)
          return yield* same('orientation fixtures required', false, true)
        yield* same(
          'all eight fixtures',
          f.orientations.map((s) => s.orientation).sort((a, b) => a - b),
          [1, 2, 3, 4, 5, 6, 7, 8],
        )
        for (const sample of f.orientations) {
          const prepared = yield* f.service.prepare(sample.bytes, sample.mimeType, {
            ...limits(sample),
            maxWidth: 30,
            maxHeight: 30,
          })
          const output = yield* f.inspect(prepared.bytes)
          yield* same(
            `orientation ${sample.orientation} metadata`,
            prepared.orientation,
            sample.orientation,
          )
          yield* same(
            `orientation ${sample.orientation} dimensions`,
            output.dimensions,
            sample.upright,
          )
          yield* same(`orientation ${sample.orientation} RGBA`, output.rgba, sample.rgba)
          if (!prepared.passthrough)
            yield* same(
              `orientation ${sample.orientation} stripped stale EXIF`,
              output.orientation,
              undefined,
            )
        }
      }),
    )

  if (caps.lease)
    add('images: interrupted codec retains completion lease', (f, { same }) =>
      Effect.gen(function* () {
        if (f.lease === undefined)
          return yield* same('controlled lease fixture required', false, true)
        const lease = f.lease
        yield* Effect.gen(function* () {
          yield* lease.arm
          const operation = yield* f.service
            .prepare(f.sample.bytes, f.sample.mimeType, limits(f.sample))
            .pipe(Effect.forkChild)
          yield* lease.started.pipe(
            Effect.timeoutOrElse({
              duration: '3 seconds',
              orElse: () => same('codec started within deadline', false, true),
            }),
          )
          const interrupting = yield* Fiber.interrupt(operation).pipe(Effect.forkChild)
          yield* Effect.yieldNow
          yield* same('pending borrowed resource retained', yield* lease.released, false)
          yield* lease.settle
          yield* Fiber.join(interrupting)
          yield* same('resource released after settlement', yield* lease.released, true)
        }).pipe(Effect.onExit(() => lease.settle))
      }),
    )
  return cases
}
