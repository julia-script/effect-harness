/** One canonical result per transcript entry; provider messages are derived at projection time. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import type * as Record from '../Record.js'
import * as ToolResult from '../ToolResult.js'
import * as ResultRecord from './ResultRecord.js'

export const EntrySchema = ToolResult.ResultSchema.mapFields((fields) => ({
  ...fields,
  callId: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  explicitEnvelope: Schema.Boolean,
}))
export const StoredSchema = Schema.Struct({
  callId: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  explicitEnvelope: Schema.Boolean,
  result: ResultRecord.result,
})
export const encode = (value: typeof EntrySchema.Type) =>
  Schema.encodeEffect(Schema.toCodecJson(StoredSchema))({
    callId: value.callId,
    name: value.name,
    explicitEnvelope: value.explicitEnvelope,
    result: value,
  })
const RecordSchema = Schema.Struct({
  kind: Schema.Literal('tool.result'),
  data: Schema.toCodecJson(StoredSchema),
})
const record = (entry: Record.Entry) =>
  Schema.decodeUnknownEffect(RecordSchema)(entry).pipe(Effect.map((value) => value.data))
export const read = (entry: Record.Entry) => record(entry).pipe(Effect.map((value) => value.result))
export const messages = Effect.fnUntraced(function* (entry: Record.Entry) {
  const { result, ...call } = yield* record(entry)
  const content = [...result.content]
  if (result.diagnostics.length > 0)
    content.push(
      Prompt.textPart({
        text: `<harness>\n${result.diagnostics.map((diagnostic) => `[${diagnostic.severity}] ${diagnostic.message}`).join('\n')}\n</harness>`,
      }),
    )
  const plain = content.flatMap((part) =>
    part.type === 'text' && Object.keys(part.options).length === 0 ? [part.text] : [],
  )
  const value =
    !call.explicitEnvelope && plain.length === content.length
      ? plain.join('\n')
      : yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
          _tag: '@effect-harness/ToolContent',
          content,
        })
  return [
    Prompt.toolMessage({
      content: [
        Prompt.toolResultPart({
          id: call.callId,
          name: call.name,
          providerExecuted: false,
          isFailure: result.isError,
          result: value,
        }),
      ],
    }),
  ]
})
