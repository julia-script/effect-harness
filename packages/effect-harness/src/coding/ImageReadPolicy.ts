import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as ImagePreparation from '../ImagePreparation.js'
import type * as Model from '../Model.js'
import type { ToolExecution } from '../ToolExecution.js'

/** Explicit model input policy. maxBytes counts padded base64, not source bytes. */
export type Policy =
  | { readonly vision: false }
  | { readonly vision: true; readonly limits: ImagePreparation.Limits }

/**
 * Evaluated once per read invocation, after the handler receives ToolExecution.
 * The resolver may use that call's execution access to select current model policy.
 * Binding this service does not install runtime model selection: the request owner
 * must supply that resolver. No implicit vision support or default image limits.
 */
export class ImageReadPolicy extends Context.Service<
  ImageReadPolicy,
  { readonly current: Effect.Effect<Policy, never, ToolExecution> }
>()('effect-harness/coding/ImageReadPolicy') {}

/** A fixed policy for applications with a fixed model; read at invocation time. */
export const layer = (policy: Policy): Layer.Layer<ImageReadPolicy> =>
  Layer.succeed(ImageReadPolicy, { current: Effect.succeed(policy) })

/** Harness models declare support; limits remain explicit application input. */
export const fromCapabilities = (
  capabilities: Pick<Model.Capabilities, 'images'>,
  limits: ImagePreparation.Limits,
): Policy => (capabilities.images ? { vision: true, limits } : { vision: false })
