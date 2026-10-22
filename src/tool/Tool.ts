/**
 * Tool declarations. A declaration names a tool and holds the Schemas of its parameters, success,
 * failure and config, plus what the tool task needs to know to run it durably: whether a call a
 * crash interrupted may run again, and how much of its output the model sees. It holds no code, so
 * the client and the loop can both import it. The implementation is a handler, given to
 * `Toolkit.toLayer()` where the loop runs.
 *
 * Config is the tool's own settings, such as a timeout or a working directory, set by whoever
 * installs the tool rather than by the model. Its Schema decodes the value the task is given, so a
 * config with defaults can be left out.
 */
import { Option, Schema, SchemaAST } from 'effect'
import type { ImageContent, TextContent, ToolDefinition } from '../model/Message.ts'
import { JsonObject } from '../storage/schema/Json.ts'

/**
 * A Schema the tool task can use without services: arguments, results and config cross storage
 * and transports encoded, and decode on their own.
 */
export type Codec = Schema.ConstraintCodec<unknown>

/** How much of a result the model sees: whole lines, from the start (`head`) or the end. */
export interface OutputLimits {
  readonly maxBytes: number
  readonly maxLines: number
  readonly retain: 'head' | 'tail'
}

/** pi's defaults: 50 KB or 2,000 lines, from the start. */
export const defaultOutputLimits: OutputLimits = {
  maxBytes: 50 * 1024,
  maxLines: 2000,
  retain: 'head',
}

/** What the model sees of a result. */
export type Content = ReadonlyArray<TextContent | ImageContent>

export const TypeId = '~effect-harness/tool/Tool'
export type TypeId = typeof TypeId

/** A declared tool. */
export interface Tool<
  Name extends string,
  Params extends Codec,
  Ok extends Codec,
  Err extends Codec,
  Cfg extends Codec,
> {
  readonly [TypeId]: TypeId
  readonly name: Name
  /** What the model reads to decide when to call the tool. */
  readonly description: string
  readonly parameters: Params
  readonly success: Ok
  /** What the handler fails with; the model sees it as an error result. */
  readonly failure: Err
  readonly config: Cfg
  /**
   * `safe`: a call a crash interrupted after it started runs again on recovery. `unsafe`, the
   * default: it ends as interrupted, since it may have partly run.
   */
  readonly replay: 'safe' | 'unsafe'
  readonly outputLimits: OutputLimits
  /**
   * Not offered to the model until a conversation loads it, as `tool_search` does; until then a
   * conversation's agent lists it as deferred.
   */
  readonly deferred: boolean
  /**
   * Who may call it: the model, other tools through `Call.execute`, or both, the default. Code
   * mode is `['model']`; a tool meant only for scripts is `['tools']`.
   */
  readonly callers: ReadonlyArray<Caller>
}

/** Who calls a tool: the model, or another tool's handler. */
export type Caller = 'model' | 'tools'

/** Any declared tool, with its types erased. */
export interface Any extends Tool<string, Codec, Codec, Codec, Codec> {}

export type Parameters<T extends Any> = T['parameters']['Type']
export type Success<T extends Any> = T['success']['Type']
export type Failure<T extends Any> = T['failure']['Type']
export type Config<T extends Any> = T['config']['Type']

const NoConfig = Schema.Struct({})

interface Options<Params, Ok, Err, Cfg> {
  readonly description: string
  readonly parameters: Params
  readonly success?: Ok
  readonly failure?: Err
  readonly config?: Cfg
  readonly replay?: 'safe' | 'unsafe'
  readonly outputLimits?: Partial<OutputLimits>
  readonly deferred?: boolean
  readonly callers?: ReadonlyArray<Caller>
}

/** Declare a tool. Without `success` it returns nothing, without `failure` it never fails. */
export function make<
  const Name extends string,
  Params extends Codec,
  Ok extends Codec = typeof Schema.Void,
  Err extends Codec = typeof Schema.Never,
  Cfg extends Codec = typeof NoConfig,
>(name: Name, options: Options<Params, Ok, Err, Cfg>): Tool<Name, Params, Ok, Err, Cfg>
export function make(name: string, options: Options<Codec, Codec, Codec, Codec>): Any {
  return {
    [TypeId]: TypeId,
    name,
    description: options.description,
    parameters: options.parameters,
    success: options.success ?? Schema.Void,
    failure: options.failure ?? Schema.Never,
    config: options.config ?? NoConfig,
    replay: options.replay ?? 'unsafe',
    outputLimits: { ...defaultOutputLimits, ...options.outputLimits },
    deferred: options.deferred ?? false,
    callers: options.callers ?? ['model', 'tools'],
  }
}

const jsonObject = Schema.decodeUnknownOption(JsonObject)

/**
 * `Schema.Struct({})` accepts any value but null, so its JSON Schema has no `type`. Providers want
 * an object for a tool's input, and Anthropic refuses the request without one.
 */
const isEmptyObject = (ast: SchemaAST.AST) =>
  SchemaAST.isObjects(ast) &&
  ast.propertySignatures.length === 0 &&
  ast.indexSignatures.length === 0

const emptyParameters = { type: 'object', properties: {}, additionalProperties: false }

/** The tool as the transcript offers it to the model, with its parameters as JSON Schema. */
export const definition = (tool: Any): ToolDefinition => {
  if (isEmptyObject(SchemaAST.toEncoded(tool.parameters.ast)))
    return { name: tool.name, description: tool.description, parameters: emptyParameters }
  const document = Schema.toJsonSchemaDocument(tool.parameters)
  const schema =
    Object.keys(document.definitions).length === 0
      ? document.schema
      : { ...document.schema, $defs: document.definitions }
  return {
    name: tool.name,
    description: tool.description,
    parameters: Option.getOrElse(jsonObject(schema), () => ({ type: 'object' })),
  }
}
