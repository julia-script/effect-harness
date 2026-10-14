import { assert, describe, it } from '@effect/vitest'
import * as Ranking from '../src/ToolSearchRanking.js'

const documents = [
  { name: 'createIssue', description: 'Open a repository issue', parameters: {} },
  {
    name: 'readFile',
    description: 'Read local file content',
    parameters: { properties: { path: { description: 'Filesystem location' } } },
  },
  {
    name: 'listIssues',
    description: 'List repository issues',
    parameters: { properties: { milestone: { description: 'Release milestone filter' } } },
  },
].map(Ranking.document)

describe('local BM25 discovery', () => {
  it('ranks name, description and parameter matches with independent expected order', () => {
    assert.deepStrictEqual(Ranking.rankDocuments('create issues', documents), [
      'createIssue',
      'listIssues',
    ])
    assert.deepStrictEqual(Ranking.rankDocuments('filesystem', documents), ['readFile'])
    assert.deepStrictEqual(Ranking.rankDocuments('milestones', documents), ['listIssues'])
    assert.deepStrictEqual(Ranking.rankDocuments('repository', documents), [
      'createIssue',
      'listIssues',
    ])
    assert.deepStrictEqual(Ranking.tokenize('HTTPRequests searchQueries searches classes'), [
      'http',
      'request',
      'search',
      'query',
      'search',
      'class',
    ])
  })
  it('keeps input ties, distinct query terms, limits and empty results deterministic', () => {
    const tied = ['z', 'a', 'b'].map((name) => ({ name, text: 'issue' }))
    assert.deepStrictEqual(Ranking.rankDocuments('issues issue', tied, 2), ['z', 'a'])
    assert.deepStrictEqual(Ranking.rankDocuments('the and of', documents), [])
    assert.deepStrictEqual(Ranking.rankDocuments('quasar', documents), [])
    assert.deepStrictEqual(Ranking.rankDocuments('issue', []), [])
    for (const limit of [0, -1, 1.2, 65, Infinity, NaN])
      assert.throws(() => Ranking.rankDocuments('issue', documents, limit), RangeError)
    assert.throws(() => Ranking.rankDocuments(' \n', documents), RangeError)
  })
  it('indexes local shared and recursive definitions, arrays and variants without remote access', () => {
    const parameters = {
      properties: { nodes: { items: { $ref: '#/$defs/Node' } } },
      $defs: {
        Node: {
          description: 'branch',
          properties: {
            next: { $ref: '#/$defs/Node' },
            value: {
              anyOf: [{ description: 'emerald' }, { allOf: [{ description: 'sapphire' }] }],
            },
          },
        },
        Unused: { description: 'forbidden' },
      },
      oneOf: [{ $ref: 'https://example.invalid/no-network' }],
    }
    const doc = Ranking.document({ name: 'tree', description: '', parameters })
    assert.deepStrictEqual(Ranking.rankDocuments('emerald sapphire', [doc]), ['tree'])
    assert.deepStrictEqual(Ranking.rankDocuments('forbidden', [doc]), [])
    assert.strictEqual(Ranking.schemaText(parameters).truncated, false)
    const escaped = { $ref: '#/$defs/a~1b~0c', $defs: { 'a/b~c': { description: 'escaped' } } }
    assert.include(Ranking.schemaText(escaped).text, 'escaped')
  })
  it('bounds cycles, depth, width and indexed text', () => {
    const cyclic: { description: string; items?: unknown } = { description: 'cycle' }
    cyclic.items = cyclic
    assert.strictEqual(Ranking.schemaText(cyclic).text, 'cycle')
    let deep: unknown = { description: 'unreachable' }
    for (let i = 0; i < 100; i++) deep = { items: deep }
    assert.deepStrictEqual(Ranking.schemaText(deep), { text: '', truncated: true })
    assert.isTrue(
      Ranking.schemaText({ anyOf: Array.from({ length: 10000 }, () => ({ description: 'node' })) })
        .truncated,
    )
    const huge = Ranking.schemaText({ description: 'x'.repeat(40000) })
    assert.strictEqual(huge.text.length, 32768)
    assert.isTrue(huge.truncated)
  })
})
