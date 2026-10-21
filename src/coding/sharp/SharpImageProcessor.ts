/**
 * An `ImageProcessor` on `sharp`, an optional peer dependency: native libvips, for Node and Bun.
 */
import type { Layer } from 'effect'
import { codec as sharpCodec } from './internal/codec.ts'
import * as ImageProcessor from '../ImageProcessor.ts'

/** The codec, to build on with `ImageProcessor.make`. */
export const codec: ImageProcessor.Codec = sharpCodec

export const layer: Layer.Layer<ImageProcessor.ImageProcessor> = ImageProcessor.layer(codec)
