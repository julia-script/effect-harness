import { expect, test } from 'tstyche'
import * as Harness from 'effect-harness'
import * as Durable from 'effect-harness/durable'
import * as Auth from 'effect-harness/auth'
import * as OpenAI from 'effect-harness/provider-openai'
import * as Anthropic from 'effect-harness/provider-anthropic'
import * as ClaudeCode from 'effect-harness/provider-claude-code'
import * as Model from 'effect-harness/Model'
import * as Session from 'effect-harness/durable/Session'
import * as CredentialStore from 'effect-harness/auth/CredentialStore'
import * as OpenAiCatalog from 'effect-harness/provider-openai/Catalog'
import * as AnthropicCatalog from 'effect-harness/provider-anthropic/Catalog'
import * as Cli from 'effect-harness/provider-claude-code/Cli'

test('root and directory imports retain the same concept types as leaf imports', () => {
  expect(Harness.Model.Catalog).type.toBe<typeof Model.Catalog>()
  expect(Durable.Session.Session).type.toBe<typeof Session.Session>()
  expect(Auth.CredentialStore.layerMemory).type.toBe<typeof CredentialStore.layerMemory>()
  expect(OpenAI.Catalog.layerApiKey).type.toBe<typeof OpenAiCatalog.layerApiKey>()
  expect(Anthropic.Catalog.layerApiKey).type.toBe<typeof AnthropicCatalog.layerApiKey>()
  expect(ClaudeCode.Cli.layer).type.toBe<typeof Cli.layer>()
})
