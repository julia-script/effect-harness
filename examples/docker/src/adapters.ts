/**
 * What clients share with the container: the providers people can add accounts of, and the RPCs
 * each loop serves. A client can import this; it holds no implementation.
 */
import { Adapter, AccountsRpcs } from 'effect-harness/accounts'
import { Anthropic } from 'effect-harness/anthropic/adapter'
import { HarnessRpcs } from 'effect-harness/client'
import { OpenAi, OpenAiCodex } from 'effect-harness/openai/adapter'

export const adapters = Adapter.group(OpenAiCodex.adapter, OpenAi.adapter, Anthropic.adapter)

export const Rpcs = HarnessRpcs.HarnessRpcs.merge(AccountsRpcs.AccountsRpcs)
