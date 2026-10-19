import { Schema } from 'effect'

/** Root of every durable document, and the shape of opaque structured payloads. */
export const JsonObject = Schema.Record(Schema.String, Schema.Json)
export type JsonObject = typeof JsonObject.Type

/**
 * One model-facing message. Storage never inspects messages, so they stay opaque JSON objects here;
 * the model layer owns their schema.
 */
export const ModelMessage = JsonObject.annotate({ identifier: 'ModelMessage' })
export type ModelMessage = typeof ModelMessage.Type
