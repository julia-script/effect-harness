/** Reversible aliases avoid persisting the same legacy value in independent result channels. */
import * as Equal from 'effect/Equal'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as Struct from 'effect/Struct'
import * as ToolResult from '../ToolResult.js'
import * as Invocation from '../ToolInvocation.js'

const envelope = Schema.toCodecJson(ToolResult.Envelope)
const contentEqual = Schema.toEquivalence(ToolResult.ContentSchema)
const detailsAreContent = (result: ToolResult.Result): boolean => {
  const decoded = Schema.decodeUnknownOption(envelope)(result.details)
  return (
    Option.isSome(decoded) &&
    contentEqual(decoded.value.content, result.content) &&
    Equal.equals(result.details, Schema.encodeSync(envelope)(decoded.value))
  )
}
export const result = ToolResult.ResultSchema.mapFields((fields) => ({
  ...fields,
  detailsFromContent: Schema.optionalKey(Schema.Literal(true)),
})).pipe(
  Schema.decodeTo(
    Schema.toType(ToolResult.ResultSchema),
    SchemaTransformation.transform({
      decode: (stored) =>
        stored.detailsFromContent === true
          ? {
              ...Struct.omit(stored, ['detailsFromContent']),
              details: Schema.encodeSync(envelope)({
                _tag: '@effect-harness/ToolContent',
                content: stored.content,
              }),
            }
          : Struct.omit(stored, ['detailsFromContent']),
      encode: (value) =>
        detailsAreContent(value)
          ? { ...Struct.omit(value, ['details']), detailsFromContent: true as const }
          : value,
    }),
  ),
)

const contentValue = (details: Schema.Json | undefined): Schema.Json | undefined => {
  const decoded = Schema.decodeUnknownOption(envelope)(details)
  if (Option.isNone(decoded)) return undefined
  const encoded = Schema.encodeSync(Schema.toCodecJson(ToolResult.NestedSchema))({
    valueKind: 'content',
    structuredOutput: ToolResult.contentValue(decoded.value.content),
    isError: false,
    diagnostics: [],
  })
  return Schema.is(Schema.JsonObject)(encoded) ? encoded['structuredOutput'] : undefined
}

/** Aliases are wire-only; the public receipt keeps its original independent channels. */
export const receipt = Schema.Json.pipe(
  Schema.decodeTo(
    Schema.toCodecJson(Invocation.WireReceiptSchema),
    SchemaTransformation.transform({
      decode: (stored: Schema.Json): Schema.Json => {
        if (
          !Schema.is(Schema.JsonObject)(stored) ||
          !Schema.is(Schema.JsonObject)(stored['result'])
        )
          return stored
        const result = stored['result']
        let expanded = result
        if (result['valueFromDetails'] === true)
          expanded = {
            ...Struct.omit(result, ['valueFromDetails']),
            structuredOutput: result['details']!,
          }
        else if (result['valueFromContent'] === true)
          expanded = {
            ...Struct.omit(result, ['valueFromContent']),
            structuredOutput: contentValue(result['details'])!,
          }
        return stored['legacyFromDetails'] === true
          ? {
              ...Struct.omit(stored, ['legacyFromDetails']),
              result: expanded,
              legacyValue: expanded['details']!,
            }
          : Object.assign({}, stored, { result: expanded })
      },
      encode: (value: Schema.Json): Schema.Json => {
        if (!Schema.is(Schema.JsonObject)(value) || !Schema.is(Schema.JsonObject)(value['result']))
          return value
        const result = value['result']
        let compact = result
        if (result['structuredOutput'] !== undefined) {
          if (Equal.equals(result['structuredOutput'], result['details']))
            compact = { ...Struct.omit(result, ['structuredOutput']), valueFromDetails: true }
          else if (
            result['valueKind'] === 'content' &&
            Equal.equals(result['structuredOutput'], contentValue(result['details']))
          )
            compact = { ...Struct.omit(result, ['structuredOutput']), valueFromContent: true }
        }
        return value['legacyValue'] !== undefined &&
          Equal.equals(value['legacyValue'], result['details'])
          ? { ...Struct.omit(value, ['legacyValue']), result: compact, legacyFromDetails: true }
          : Object.assign({}, value, { result: compact })
      },
    }),
  ),
)
