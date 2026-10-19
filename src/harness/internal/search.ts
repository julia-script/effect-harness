/**
 * BM25 ranking for `tool_search`, ported from pi's `search.ts`, a copy of the coding agent's tool
 * search ranking.
 */

export interface Document {
  readonly name: string
  readonly text: string
}

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
const stem = (term: string) => {
  if (term.length > 4 && term.endsWith('ies')) return `${term.slice(0, -3)}y`
  if (term.length > 4 && /(?:ches|shes|sses|xes|zes)$/u.test(term)) return term.slice(0, -2)
  if (term.length > 3 && term.endsWith('s') && !term.endsWith('ss')) return term.slice(0, -1)
  return term
}

/** Lowercase terms, split at camelCase boundaries and non-alphanumerics, without stop words. */
export const tokenize = (text: string): ReadonlyArray<string> =>
  text
    .replaceAll(/([a-z0-9])([A-Z])/gu, '$1 $2')
    .replaceAll(/([A-Z]+)([A-Z][a-z])/gu, '$1 $2')
    .toLowerCase()
    .split(/[^a-z0-9]+/u)
    .filter((term) => term.length > 0 && !STOP_WORDS.has(term))
    .map((term) => stem(term))

const K1 = 1.2
const B = 0.75

const countsOf = (text: string) => {
  const counts = new Map<string, number>()
  for (const term of tokenize(text)) counts.set(term, (counts.get(term) ?? 0) + 1)
  return counts
}

/** Okapi BM25 (k1 1.2, b 0.75): the names of the best matches, best first; ties keep order. */
export const rank = (
  query: string,
  documents: ReadonlyArray<Document>,
  limit: number,
): ReadonlyArray<string> => {
  const queryTerms = [...new Set(tokenize(query))]
  if (queryTerms.length === 0 || documents.length === 0 || limit <= 0) return []
  const termCounts = documents.map((document) => countsOf(document.text))
  const lengths = termCounts.map((counts) => [...counts.values()].reduce((sum, n) => sum + n, 0))
  const averageLength = lengths.reduce((sum, length) => sum + length, 0) / documents.length || 1
  const idf = new Map(
    queryTerms.map((term) => {
      const frequency = termCounts.filter((counts) => counts.has(term)).length
      return [term, Math.log(1 + (documents.length - frequency + 0.5) / (frequency + 0.5))]
    }),
  )
  const matches: Array<{ readonly name: string; readonly score: number }> = []
  for (const [index, document] of documents.entries()) {
    const counts = termCounts[index] ?? new Map<string, number>()
    const norm = K1 * (1 - B + (B * (lengths[index] ?? 0)) / averageLength)
    let score = 0
    for (const term of queryTerms) {
      const count = counts.get(term) ?? 0
      if (count > 0) score += (idf.get(term) ?? 0) * ((count * (K1 + 1)) / (count + norm))
    }
    if (score > 0) matches.push({ name: document.name, score })
  }
  return matches
    .toSorted((left, right) => right.score - left.score)
    .slice(0, limit)
    .map((match) => match.name)
}
