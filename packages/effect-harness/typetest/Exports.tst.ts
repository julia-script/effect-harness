import { expect, test } from 'tstyche'
import * as Harness from 'effect-harness'
import * as OpenAI from 'effect-harness/provider-openai'
import * as Anthropic from 'effect-harness/provider-anthropic'
import type * as Model from 'effect-harness/Model'
import type * as Runtime from 'effect-harness/Harness'
// effect-nit-allow P9-namespace-alias-equals-module: effect-harness/provider-openai/Catalog and effect-harness/provider-anthropic/Catalog both own Catalog; OpenAiCatalog keeps their distinct native/harness APIs available together for these constructor, service and declaration assertions.
import type * as OpenAiCatalog from 'effect-harness/provider-openai/Catalog'
// effect-nit-allow P9-namespace-alias-equals-module: effect-harness/provider-anthropic/Catalog and effect-harness/provider-openai/Catalog both own Catalog; AnthropicCatalog keeps their distinct native/harness APIs available together for these constructor, service and declaration assertions.
import type * as AnthropicCatalog from 'effect-harness/provider-anthropic/Catalog'

test('root and directory imports retain the same concept types as leaf imports', () => {
  expect(Harness.Model.Catalog).type.toBe<typeof Model.Catalog>()
  expect(Harness.Harness.Harness).type.toBe<typeof Runtime.Harness>()
  expect(OpenAI.Catalog.layerApiKey).type.toBe<typeof OpenAiCatalog.layerApiKey>()
  expect(Anthropic.Catalog.layerApiKey).type.toBe<typeof AnthropicCatalog.layerApiKey>()
})
