/** Local English lexical ranking. Tokenization/BM25 adapted from earendil-works/pi
 * packages/durable/src/search.ts at b45e0ebcc5ae2fc42a6f2f23104d10e656e6bedb.
 *
 * MIT License
 *
 * Copyright (c) 2025 Mario Zechner
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
export type SearchDocument = { readonly name: string; readonly text: string }

const STOP_WORDS: ReadonlySet<string> = new Set([
  'a',
  'an',
  'and',
  'are',
  'as',
  'at',
  'be',
  'by',
  'for',
  'from',
  'in',
  'is',
  'it',
  'of',
  'on',
  'or',
  'that',
  'the',
  'this',
  'to',
  'with',
])

/** Naive singular form, so `issues` matches `issue` and `searches` matches `search`. */
function stem(term: string): string {
  if (term.length > 4 && term.endsWith('ies')) return `${term.slice(0, -3)}y`
  if (term.length > 4 && /(ches|shes|sses|xes|zes)$/.test(term)) return term.slice(0, -2)
  if (term.length > 3 && term.endsWith('s') && !term.endsWith('ss')) return term.slice(0, -1)
  return term
}

/** Lowercase terms, split at camelCase boundaries and non-alphanumerics, without stop words. */
export function tokenize(text: string): string[] {
  return text
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((term) => term.length > 0 && !STOP_WORDS.has(term))
    .map(stem)
}

/** Okapi BM25 (k1 1.2, b 0.75): the names of the best matches, best first; ties keep document order. */
export function rankDocuments(
  query: string,
  documents: readonly SearchDocument[],
  limit = 8,
): string[] {
  if (query.trim().length === 0) throw new RangeError('query must not be blank')
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 64)
    throw new RangeError('limit must be an integer from 1 to 64')
  const k1 = 1.2
  const b = 0.75
  const queryTerms = [...new Set(tokenize(query))]
  if (queryTerms.length === 0 || documents.length === 0) return []
  const termCounts = documents.map((document) => {
    const counts = new Map<string, number>()
    for (const term of tokenize(document.text)) counts.set(term, (counts.get(term) ?? 0) + 1)
    return counts
  })
  const lengths = termCounts.map((counts) =>
    [...counts.values()].reduce((sum, count) => sum + count, 0),
  )
  const averageLength = lengths.reduce((sum, length) => sum + length, 0) / documents.length || 1
  const idf = new Map(
    queryTerms.map((term) => {
      const frequency = termCounts.filter((counts) => counts.has(term)).length
      return [term, Math.log(1 + (documents.length - frequency + 0.5) / (frequency + 0.5))] as const
    }),
  )
  const matches: { name: string; score: number; index: number }[] = []
  documents.forEach((document, index) => {
    let score = 0
    for (const term of queryTerms) {
      const count = termCounts[index]!.get(term)
      if (!count) continue
      const norm = k1 * (1 - b + (b * lengths[index]!) / averageLength)
      score += (idf.get(term) ?? 0) * ((count * (k1 + 1)) / (count + norm))
    }
    if (score > 0) matches.push({ name: document.name, score, index })
  })
  return matches
    .sort((left, right) => right.score - left.score || left.index - right.index)
    .slice(0, limit)
    .map((match) => match.name)
}

const object = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

/** Index only schema text reachable from the root. Local references are resolved once.
 * Work is bounded to 2,048 nodes, depth 64 and 32,768 text characters per declaration.
 * Remote references are never fetched. Unreachable definitions do not affect ranking.
 */
export const schemaText = (
  schema: unknown,
): { readonly text: string; readonly truncated: boolean } => {
  const parts: string[] = []
  const seen = new Set<object>()
  const pending: Array<{ readonly value: unknown; readonly depth: number }> = [
    { value: schema, depth: 0 },
  ]
  let nodes = 0
  let characters = 0
  let truncated = false
  const add = (text: string) => {
    const separator = parts.length === 0 ? 0 : 1
    const remaining = Math.max(0, 32768 - characters - separator)
    if (text.length > remaining) truncated = true
    const part = text.slice(0, remaining)
    if (part.length > 0) {
      parts.push(part)
      characters += part.length + separator
    }
  }
  const resolve = (ref: string): unknown => {
    if (ref === '#') return schema
    if (!ref.startsWith('#/')) return undefined
    let current: unknown = schema
    for (const token of ref.slice(2).split('/')) {
      const key = token.replaceAll('~1', '/').replaceAll('~0', '~')
      if (!object(current) || !Object.hasOwn(current, key)) return undefined
      current = current[key]
    }
    return current
  }
  while (pending.length > 0 && nodes < 2048 && characters < 32768) {
    const { value, depth } = pending.pop()!
    nodes++
    if (!object(value) || seen.has(value)) continue
    if (depth > 64) {
      truncated = true
      continue
    }
    seen.add(value)
    if (typeof value.description === 'string') add(value.description)
    const children: unknown[] = []
    if (typeof value.$ref === 'string') children.push(resolve(value.$ref))
    if (object(value.properties)) {
      for (const key in value.properties) {
        if (!Object.hasOwn(value.properties, key)) continue
        if (children.length >= 2048 || characters >= 32768) {
          truncated = true
          break
        }
        add(key)
        children.push(value.properties[key])
      }
    }
    if (value.items !== undefined) children.push(value.items)
    for (const key of ['prefixItems', 'anyOf', 'oneOf', 'allOf']) {
      const variants = value[key]
      if (Array.isArray(variants)) {
        const remaining = Math.max(0, 2048 - children.length)
        children.push(...variants.slice(0, remaining))
        if (variants.length > remaining) truncated = true
      }
    }
    for (let index = children.length - 1; index >= 0; index--) {
      if (pending.length >= 2048) {
        truncated = true
        break
      }
      pending.push({ value: children[index], depth: depth + 1 })
    }
  }
  return { text: parts.join(' '), truncated: truncated || pending.length > 0 }
}

/** Names receive two lexical occurrences; schemas contribute property names and descriptions. */
export const document = (tool: {
  readonly name: string
  readonly description: string
  readonly parameters: unknown
}): SearchDocument => ({
  name: tool.name,
  text: [
    tool.name,
    tool.name.replaceAll('_', ' '),
    tool.description,
    schemaText(tool.parameters).text,
  ].join(' '),
})
