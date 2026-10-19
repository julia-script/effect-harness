/** Lossless native prompt codecs. Wire controls occur only at typed file-data positions. */
import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as Native from 'effect/ai/Prompt'

const Bytes = Schema.Uint8Array.annotate({
  toCodecJson: () =>
    Schema.link<Uint8Array>()(
      Schema.TaggedStruct('@effect-harness/Bytes', { data: Schema.Uint8ArrayFromBase64 }),
      SchemaTransformation.transform({
        decode: (encoded) => encoded.data,
        encode: (data) => ({ _tag: '@effect-harness/Bytes' as const, data }),
      }),
    ),
})
const Url = Schema.URL.annotate({
  toCodecJson: () =>
    Schema.link<URL>()(
      Schema.TaggedStruct('@effect-harness/URL', { href: Schema.URLFromString }),
      SchemaTransformation.transform({
        decode: (encoded) => encoded.href,
        encode: (href) => ({ _tag: '@effect-harness/URL' as const, href }),
      }),
    ),
})

/** Preserves bytes, URL instances, literal strings, filename and provider options independently. */
export const FilePart = Native.FilePart.mapFields((fields) => ({
  ...fields,
  data: Schema.Union([Schema.String, Bytes, Url]),
}))
export const UserMessagePart = Schema.Union([Native.TextPart, FilePart])
export const Message = Schema.Union([
  Native.SystemMessage,
  Native.UserMessage.mapFields((fields) => ({
    ...fields,
    content: Schema.Union([fields.content.members[0], Schema.Array(UserMessagePart)]),
  })),
  Native.AssistantMessage.mapFields((fields) => ({
    ...fields,
    content: Schema.Union([
      fields.content.members[0],
      Schema.Array(
        Schema.Union([
          Native.TextPart,
          FilePart,
          Native.ReasoningPart,
          Native.ToolCallPart,
          Native.ToolResultPart,
          Native.ToolApprovalRequestPart,
        ]),
      ),
    ]),
  })),
  Native.ToolMessage,
])
/**
 * Restores a native Prompt instance after a JSON round trip, without a storage service.
 * Use `Schema.toCodecJson(PromptWire.Prompt)` for JSON values, or wrap that codec in
 * `Schema.fromJsonString` for strings. Tags are interpreted only in typed file data;
 * tool parameters, tool results and provider options remain ordinary application JSON.
 */
export const Prompt = Schema.Struct({ content: Schema.Array(Message) }).pipe(
  Schema.decodeTo(
    Schema.toType(Native.Prompt),
    SchemaTransformation.transform({
      decode: ({ content }) => Native.fromMessages(content),
      encode: (prompt) => ({ content: prompt.content }),
    }),
  ),
)

export type Prompt = Native.Prompt
