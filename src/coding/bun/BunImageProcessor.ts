/**
 * An `ImageProcessor` on `Bun.Image`, built into Bun: no install, and the work runs on Bun's own
 * worker threads. Bun only.
 */
import type { Layer } from 'effect'
import { codec as bunCodec } from '../internal/bunCodec.ts'
import * as ImageProcessor from '../ImageProcessor.ts'

/** The codec, to build on with `ImageProcessor.make`. */
export const codec: ImageProcessor.Codec = bunCodec

export const layer: Layer.Layer<ImageProcessor.ImageProcessor> = ImageProcessor.layer(codec)
