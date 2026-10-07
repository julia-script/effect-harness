/**
 * Semantic native AI error mappings for the Claude Code provider.
 *
 * @since 0.0.0
 */
import * as AiError from 'effect/ai/AiError'

/**
 * Returns the native invalid-request error for an unsupported CLI capability.
 *
 * @category constructors
 * @since 0.0.0
 */
export const unsupported = (capability: string): AiError.AiError =>
  new AiError.AiError({
    module: 'ClaudeCode',
    method: 'request',
    reason: new AiError.InvalidRequestError({
      description: `Claude Code CLI does not support ${capability} through this adapter`,
    }),
  })
/**
 * Returns the native invalid-output error for a malformed CLI event.
 *
 * @category constructors
 * @since 0.0.0
 */
export const protocol = (description: string): AiError.AiError =>
  new AiError.AiError({
    module: 'ClaudeCode',
    method: 'protocol',
    reason: new AiError.InvalidOutputError({ description }),
  })
/**
 * Returns the native unknown error for a CLI process failure.
 *
 * @category constructors
 * @since 0.0.0
 */
export const processError = (description: string): AiError.AiError =>
  new AiError.AiError({
    module: 'ClaudeCode',
    method: 'process',
    reason: new AiError.UnknownError({ description }),
  })
/**
 * Returns the native missing-key error for an unsigned-in installed CLI.
 *
 * @category constructors
 * @since 0.0.0
 */
export const authentication = (): AiError.AiError =>
  new AiError.AiError({
    module: 'ClaudeCode',
    method: 'account',
    reason: new AiError.AuthenticationError({
      kind: 'MissingKey',
      description: 'Sign in to the installed Claude Code CLI with your Claude account',
    }),
  })
