/** Opt-in portable discovery. Activation belongs to the durable successful-round barrier. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from './Tool.js'
import * as Toolkit from './Toolkit.js'
import * as Catalog from './ToolCatalog.js'
import * as Ranking from './ToolSearchRanking.js'

export const Parameters = Schema.Struct({
  query: Schema.String.check(Schema.isPattern(/\S/)).annotate({
    description: 'English keywords describing the tools to discover.',
  }),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 }))),
})
export const Loaded = Schema.Struct({ loaded: Schema.Array(Schema.NonEmptyString) })
export const tool = Tool.makeResult('tool_search', {
  description:
    'Search permitted deferred tool names, descriptions and parameters. Matching tools become available on the next model request. Use the discovered tool in a subsequent call to perform the operation.',
  parameters: Parameters,
  success: Loaded,
  failure: Catalog.CatalogError,
  resultValidation: 'strict',
  replay: 'safe',
  callers: ['model'],
})
export const toolkit = Toolkit.make(tool)
/** Install with the toolkit; each invocation reads its own leased, permission-filtered catalog. */
export const layer = toolkit.toLayer({
  tool_search: ({ query, limit }) =>
    Effect.gen(function* () {
      const candidates = yield* Catalog.deferred.pipe(
        Effect.mapError((error) => {
          if (error._tag !== 'ToolCatalogError') return error
          return { content: [Prompt.textPart({ text: error.message })], structuredOutput: error }
        }),
      )
      const loaded = Ranking.rankDocuments(query, candidates.map(Ranking.document), limit)
      const lines = loaded.map((name) => {
        const description =
          candidates
            .find((item) => item.name === name)!
            .description.trim()
            .split(/\r?\n/)[0] ?? ''
        return `- ${name}: ${description.slice(0, 240)}`
      })
      return {
        content: [
          Prompt.textPart({
            text:
              loaded.length === 0
                ? 'No matching tools found.'
                : `Available on the next request:\n${lines.join('\n')}`,
          }),
        ],
        structuredOutput: { loaded },
        details: { loaded },
        ...(loaded.length === 0 ? {} : { control: { addTools: loaded } }),
      }
    }),
})
