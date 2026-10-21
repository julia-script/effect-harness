/**
 * Test support: the rules a provider's strict mode holds a tool's JSON Schema to. Strict mode takes
 * only closed objects whose properties are all required, so a schema with an optional property or
 * open object is refused, as OpenAI refuses it.
 */

type Json = Readonly<Record<string, unknown>>

const isJson = (value: unknown): value is Json =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const children = (schema: Json): ReadonlyArray<readonly [string, unknown]> => [
  ...Object.entries(isJson(schema['properties']) ? schema['properties'] : {}).map(
    ([key, value]) => [`properties/${key}/`, value] as const,
  ),
  ...Object.entries(isJson(schema['$defs']) ? schema['$defs'] : {}).map(
    ([key, value]) => [`$defs/${key}/`, value] as const,
  ),
  ...(schema['items'] === undefined ? [] : [['items/', schema['items']] as const]),
  ...['anyOf', 'oneOf', 'allOf'].flatMap((key) => {
    const options = schema[key]
    return Array.isArray(options)
      ? options.map((value, at) => [`${key}/${at}/`, value] as const)
      : []
  }),
]

/** Why strict mode refuses `schema`, one reason per offending node; empty when it takes it. */
export const violations = (schema: unknown, path = '/'): ReadonlyArray<string> => {
  if (!isJson(schema)) return []
  const own: Array<string> = []
  if (schema['type'] === 'object' || isJson(schema['properties'])) {
    if (schema['additionalProperties'] !== false) {
      own.push(`At schema path ${path}, additionalProperties must be false for a strict schema`)
    }
    const required = new Set<unknown>(Array.isArray(schema['required']) ? schema['required'] : [])
    const missing = Object.keys(isJson(schema['properties']) ? schema['properties'] : {}).filter(
      (key) => !required.has(key),
    )
    if (missing.length > 0) {
      own.push(
        `At schema path ${path}, required must list every property: missing ${missing.join(', ')}`,
      )
    }
  }
  return [...own, ...children(schema).flatMap(([key, child]) => violations(child, `${path}${key}`))]
}
