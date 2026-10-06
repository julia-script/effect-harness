import * as AiError from 'effect/ai/AiError'

export const unsupported = (capability: string) =>
  new AiError.AiError({
    module: 'ClaudeCode',
    method: 'request',
    reason: new AiError.InvalidRequestError({
      description: `Claude Code CLI does not support ${capability} through this adapter`,
    }),
  })
export const protocol = (description: string) =>
  new AiError.AiError({
    module: 'ClaudeCode',
    method: 'protocol',
    reason: new AiError.InvalidOutputError({ description }),
  })
export const processError = (description: string) =>
  new AiError.AiError({
    module: 'ClaudeCode',
    method: 'process',
    reason: new AiError.UnknownError({ description }),
  })
export const authentication = () =>
  new AiError.AiError({
    module: 'ClaudeCode',
    method: 'account',
    reason: new AiError.AuthenticationError({
      kind: 'MissingKey',
      description: 'Sign in to the installed Claude Code CLI with your Claude account',
    }),
  })
