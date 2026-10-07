/**
 * Native model and tool execution with admitted progress settlement.
 *
 * @since 0.0.0
 */
import * as Result from 'effect/Result'
import * as Record from 'effect/Record'
import { constUndefined } from 'effect/Function'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as Data from 'effect/Data'
import * as Arr from 'effect/Array'
import * as Option from 'effect/Option'
import * as SchemaField from './SchemaField.ts'
import * as Serialization from './Serialization.ts'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import type * as AiError from 'effect/ai/AiError'
import * as AiPrompt from 'effect/ai/Prompt'
// effect-review-allow P9-namespace-alias-equals-module: effect/ai/Prompt and ./Prompt.ts both bind Prompt; AiPrompt preserves the checked imported-name collision.
import * as Response from 'effect/ai/Response'
import * as AiTool from 'effect/ai/Tool'
// effect-review-allow P9-namespace-alias-equals-module: effect/ai/Tool and ./Tool.ts both bind Tool; AiTool preserves the checked imported-name collision.
import type * as Toolkit from 'effect/ai/Toolkit'
import * as Agent from './Agent.ts'
import * as Compaction from './Compaction.ts'
import * as ConversationContext from './Context.ts'
// effect-review-allow P9-namespace-alias-equals-module: ./Context.ts and effect/Context both bind Context; ConversationContext preserves the checked imported-name collision.
import { ModelError, ModelInvalidResponse, ModelNoModel, ModelUnsupported } from './ModelError.ts'
import {
  ToolError,
  ToolBlocked,
  ToolExecution,
  ToolInvalidParameters,
  ToolInvalidResult,
  ToolUnavailable,
} from './ToolError.ts'
import * as Hook from './Hook.ts'
import {
  Invocation,
  ToolCall,
  Result as ToolResultSchema,
  type ToolResult,
  type Diagnostic,
} from './Invocation.ts'
import * as Model from './Model.ts'
import * as Output from './Output.ts'
import * as Prompt from './Prompt.ts'
import * as Registry from './Registry.ts'
import * as Tool from './Tool.ts'
import * as Usage from './Usage.ts'
import * as Progress from './Progress.ts'
import * as Exit from 'effect/Exit'
import * as Json from './Json.ts'
import { EntryId } from './Identity.ts'

/**
 * Schema for request.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Request = Schema.Struct({
  model: Agent.ModelRef,
  prompt: AiPrompt.Prompt,
  options: Model.RequestOptions,
  tools: Schema.Array(ConversationContext.ToolDeclaration),
  tail: SchemaField.optional(EntryId),
})
/**
 * Executor request contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Request = typeof Request.Type
/**
 * Executor preparation contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Preparation = Executor.Preparation
/**
 * Schema for summary request.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SummaryRequest = Schema.Struct({
  request: Request,
  firstKept: EntryId,
  tail: EntryId,
  attempt: Schema.Int,
})
/**
 * Executor summary request contract.
 *
 * @category models
 * @since 0.0.0
 */
export type SummaryRequest = typeof SummaryRequest.Type
/**
 * Schema for summary.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Summary = Schema.Struct({ summary: Schema.String, usage: Usage.Usage })
/**
 * Executor summary contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Summary = typeof Summary.Type
/**
 * Executor compaction preparation contract.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactionPreparation = Data.TaggedEnum<{
  none: {}
  summary: { readonly firstKept: EntryId; readonly summary: string }
  request: { readonly request: SummaryRequest }
}>
/**
 * Constructors and matchers for declined, completed and requested compaction.
 *
 * @category constants
 * @since 0.0.0
 */
export const CompactionPreparation = Data.taggedEnum<CompactionPreparation>()
/**
 * Executor part contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Part = Response.StreamPart<Record<string, AiTool.Any>, 'encoded'>
/**
 * Executor prepare input contract.
 *
 * @category models
 * @since 0.0.0
 */
export type PrepareInput = Executor.PrepareInput
/**
 * Executor tool options contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolOptions = Executor.ToolOptions
/**
 * Executor compact input contract.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactInput = Executor.CompactInput
const Call = Schema.Struct({
  ...Response.ToolCallPart('', Schema.Unknown).fields,
  name: Schema.String,
})
const DispositionWire = Schema.Union([
  Schema.Struct({ type: Schema.tag('deferred'), decision: Model.DeferredDecision }),
  Schema.Struct({
    type: Schema.Literals(['answer', 'tools']),
    prompt: AiPrompt.Prompt,
    usage: Usage.Usage,
    calls: Schema.Array(Call),
  }),
  Schema.Struct({
    type: Schema.tag('failure'),
    prompt: AiPrompt.Prompt,
    usage: Usage.Usage,
    message: Schema.String,
    retryable: Schema.Boolean,
    overflow: Schema.Boolean,
  }),
])
const DispositionDomain = Schema.Union([
  Schema.TaggedStruct('deferred', { decision: Model.DeferredDecision }),
  ...(['answer', 'tools'] as const).map((tag) =>
    Schema.TaggedStruct(tag, {
      prompt: AiPrompt.Prompt,
      usage: Usage.Usage,
      calls: Schema.Array(Call),
    }),
  ),
  Schema.TaggedStruct('failure', {
    prompt: AiPrompt.Prompt,
    usage: Usage.Usage,
    message: Schema.String,
    retryable: Schema.Boolean,
    overflow: Schema.Boolean,
  }),
])
/**
 * Codec for tagged execution dispositions with the legacy type discriminator on the wire.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Disposition = DispositionWire.pipe(
  Schema.decodeTo(
    Schema.toType(DispositionDomain),
    SchemaTransformation.transform({
      decode: (value): typeof DispositionDomain.Type => {
        switch (value.type) {
          case 'deferred':
            return { _tag: 'deferred', decision: value.decision }
          case 'answer':
          case 'tools':
            return {
              _tag: value.type,
              prompt: value.prompt,
              usage: value.usage,
              calls: value.calls,
            }
          case 'failure':
            return {
              _tag: 'failure',
              prompt: value.prompt,
              usage: value.usage,
              message: value.message,
              retryable: value.retryable,
              overflow: value.overflow,
            }
        }
      },
      encode: (value) => {
        switch (value._tag) {
          case 'deferred':
            return { type: 'deferred', decision: value.decision }
          case 'answer':
          case 'tools':
            return {
              type: value._tag,
              prompt: value.prompt,
              usage: value.usage,
              calls: value.calls,
            }
          case 'failure':
            return {
              type: 'failure',
              prompt: value.prompt,
              usage: value.usage,
              message: value.message,
              retryable: value.retryable,
              overflow: value.overflow,
            }
        }
      },
    }),
  ),
)
/**
 * Executor disposition contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Disposition = typeof Disposition.Type

/**
 * Service for executor capabilities.
 *
 * @category services
 * @since 0.0.0
 */
export class Executor extends Context.Service<
  Executor,
  {
    readonly resolve: (
      state: Agent.State,
      settings: Agent.Settings,
    ) => Effect.Effect<Registry.Resolved, never, Invocation>
    readonly prepare: (
      input: PrepareInput,
    ) => Effect.Effect<Preparation, ModelError | Schema.SchemaError, Invocation>
    readonly generate: (
      request: Request,
      agent: Registry.Resolved,
    ) => Stream.Stream<Part, ModelError | AiError.AiError, Invocation>
    readonly fetchDeferred: (
      request: Request,
      handle: Schema.Json,
    ) => Stream.Stream<Part, ModelError | AiError.AiError>
    readonly cancelDeferred: (
      request: Request,
      handle: Schema.Json,
    ) => Effect.Effect<void, ModelError | AiError.AiError>
    readonly classifyResponse: (
      request: Request,
      agent: Registry.Resolved,
      parts: ReadonlyArray<Response.AnyPart>,
    ) => Effect.Effect<Disposition, ModelError, Invocation>
    readonly classifyFailure: (
      request: Request,
      error: unknown,
    ) => Effect.Effect<{ readonly retryable: boolean; readonly overflow: boolean }, ModelError>
    readonly prepareTool: (
      agent: Registry.Resolved,
      input: Hook.ToolInput,
    ) => Effect.Effect<Tool.Intent, ToolError, Invocation>
    readonly tool: (
      intent: Tool.Intent,
      agent: Registry.Resolved,
      options?: ToolOptions,
    ) => Effect.Effect<Tool.Execution, ToolError, Invocation>
    readonly prepareCompaction: (
      input: CompactInput,
    ) => Effect.Effect<CompactionPreparation, ModelError | Schema.SchemaError, Invocation>
    readonly compact: (
      request: SummaryRequest,
    ) => Effect.Effect<Summary, ModelError | AiError.AiError>
  }
>()('@effect-harness/harness/Executor') {}
const requireModel = (state: Agent.State): Effect.Effect<Agent.ModelRef, ModelError> =>
  state.model === undefined
    ? Effect.fail(
        new ModelError({ reason: new ModelNoModel({ message: 'No model is configured' }) }),
      )
    : Effect.succeed(state.model)
/** Definitions-only toolkit: native AI validates/declaratively offers tools but never executes them in this boundary. */
function nativeDeclaration(declaration: Request['tools'][number]) {
  if (declaration.provider === undefined)
    return AiTool.dynamic(declaration.name, {
      parameters: declaration.parameters,
      ...(declaration.description === undefined ? {} : { description: declaration.description }),
    })
  const provider = declaration.provider
  // Request's schema validates the provider namespace separator before this conditional string type assertion.
  return AiTool.providerDefined({
    id: provider.id as `${string}.${string}`,
    providerName: provider.name,
    customName: declaration.name,
    args: Schema.Json,
    parameters: Schema.Unknown,
    success: Schema.Unknown,
  })(provider.args)
}
function definitions(
  tools: Request['tools'],
): Toolkit.WithHandler<Record<string, ReturnType<typeof nativeDeclaration>>> {
  return {
    tools: Record.fromIterableWith(tools, (declaration) => [
      declaration.name,
      nativeDeclaration(declaration),
    ]),
    handle: () => Effect.die('Tool resolution must be disabled at the model request boundary'),
  }
}
/**
 * Layer for Executor capabilities.
 *
 * @category combinators
 * @since 0.0.0
 */
export const layer: Layer.Layer<Executor, never, Registry.Registry | Model.Catalog> = Layer.effect(
  Executor,
  Effect.gen(function* () {
    const registry = yield* Registry.Registry
    const catalog = yield* Model.Catalog
    const resolve = (
      state: Agent.State,
      settings: Agent.Settings,
    ): Effect.Effect<Registry.Resolved, never, Invocation> =>
      Effect.flatMap(registry.snapshot, (snapshot) => Registry.resolve(snapshot, state, settings))
    const prepare = Effect.fnUntraced(function* (input: PrepareInput) {
      const ref = yield* requireModel(input.state)
      yield* catalog.resolve(ref)
      const agent = yield* resolve(input.state, input.settings)
      const patches = ConversationContext.systemPatches(input.view)
      const sections = yield* Registry.render(agent, input.view, Prompt.replaySections(patches))
      const declarations = yield* Effect.forEach(agent.tools, Tool.declaration)
      const plan = Prompt.plan(input.view, sections, declarations)
      const options = yield* Schema.decodeUnknownEffect(Model.RequestOptions)({
        thinking: input.state.thinking ?? 'off',
        options: input.settings.stream,
        ...(input.sessionId === undefined ? {} : { sessionId: input.sessionId }),
      })
      const request: Request = {
        model: ref,
        prompt: Prompt.toPrompt(input.view.messages, sections, {
          managedSystemMessages: ConversationContext.managedMessages(input.view),
        }),
        options,
        tools: declarations,
        ...(Option.isNone(Arr.last(input.view.entries))
          ? {}
          : {
              tail: yield* Schema.decodeEffect(EntryId)(
                Math.max(...input.view.entries.map((entry) => entry.id)),
              ),
            }),
      }
      return { request, agent, plan }
    })
    const generate = (
      request: Request,
      agent: Registry.Resolved,
    ): Stream.Stream<Part, ModelError | AiError.AiError, Invocation> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const descriptor = yield* catalog.resolve(request.model)
          const context = yield* descriptor.configure(request.options)
          const prompt = yield* Hook.beforeRequest(
            Registry.handlers(agent, 'generation'),
            request.prompt,
          )
          const toolkit = definitions(request.tools)
          return descriptor.model
            .streamText({
              prompt: descriptor.normalizePrompt?.(prompt) ?? prompt,
              toolkit,
              disableToolCallResolution: true,
              allowUnknownToolCalls: true,
            })
            .pipe(Stream.provideContext(context))
        }),
      )
    const fetchDeferred = (
      request: Request,
      handle: Schema.Json,
    ): Stream.Stream<Part, ModelError | AiError.AiError> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const descriptor = yield* catalog.resolve(request.model)
          if (descriptor.deferred === undefined)
            return yield* new ModelError({
              reason: new ModelUnsupported({
                message: 'This model does not support deferred fetch',
              }),
            })
          return descriptor.deferred.fetch(handle, request.options)
        }),
      )
    const cancelDeferred = Effect.fnUntraced(function* (request: Request, handle: Schema.Json) {
      const descriptor = yield* catalog.resolve(request.model)
      if (descriptor.deferred === undefined)
        return yield* new ModelError({
          reason: new ModelUnsupported({
            message: 'This model does not support deferred cancellation',
          }),
        })
      yield* descriptor.deferred.cancel(handle, request.options)
    })
    const classifyResponse = Effect.fnUntraced(function* (
      request: Request,
      agent: Registry.Resolved,
      parts: ReadonlyArray<Response.AnyPart>,
    ): Effect.fn.Return<Disposition, ModelError, Invocation> {
      const descriptor = yield* catalog.resolve(request.model)
      const deferred = Option.fromUndefinedOr(descriptor.deferred).pipe(
        Option.flatMap((capability) => capability.inspect(parts)),
      )
      return yield* Option.match(deferred, {
        onSome: (decision) => Effect.succeed<Disposition>({ _tag: 'deferred', decision }),
        onNone: () =>
          Effect.gen(function* () {
            yield* Hook.afterResponse(Registry.handlers(agent, 'generation'), parts)
            const finish = Arr.findLast(parts, (part) => part.type === 'finish')
            const usage = Option.match(finish, {
              onNone: Usage.zero,
              onSome: (self) =>
                descriptor.usage?.(self.usage, self.metadata) ?? Usage.fromResponse(self.usage),
            })
            const prompt = AiPrompt.fromResponseParts(parts)
            const calls = parts.filter(
              (part): part is Response.ToolCallPart<string, unknown> =>
                part.type === 'tool-call' && !part.providerExecuted,
            )
            const failure = (): Disposition => {
              const errors = Arr.filterMap(parts, (part) =>
                part.type === 'error' ? Result.succeed(part.error) : Result.failVoid,
              )
              const message =
                errors.map(Model.errorText).join('\n') ||
                `Model finished with ${Option.getOrElse(
                  Option.map(finish, (part) => part.reason),
                  () => 'no finish part',
                )}`
              const policies = errors.map(
                (error) =>
                  descriptor.classify?.(error) ?? Model.classify(error, request.model.provider),
              )
              const policy = {
                overflow: policies.some((value) => value.overflow),
                retryable: policies.length > 0 && policies.every((value) => value.retryable),
              }
              return { _tag: 'failure', prompt, usage, message, ...policy }
            }
            return Option.match(finish, {
              onNone: failure,
              onSome: (self): Disposition =>
                self.reason === 'stop' || self.reason === 'length' || self.reason === 'tool-calls'
                  ? {
                      _tag: self.reason === 'tool-calls' && calls.length > 0 ? 'tools' : 'answer',
                      prompt,
                      usage,
                      calls,
                    }
                  : failure(),
            })
          }),
      })
    })
    const prepareTool = Effect.fnUntraced(function* (
      agent: Registry.Resolved,
      input: Hook.ToolInput,
    ) {
      const found = Arr.findFirst(agent.tools, (tool) => tool.tool.name === input.name)
      const registration = yield* Effect.fromOption(found).pipe(
        Effect.mapError(
          () =>
            new ToolError({
              reason: new ToolUnavailable({
                name: input.name,
                message: `Tool ${input.name} is not available`,
              }),
            }),
        ),
      )
      let args = input.args
      if (registration.metadata.repair !== undefined)
        args = yield* Effect.suspend(
          () => registration.metadata.repair?.(args) ?? Effect.succeed(args),
        ).pipe(
          Effect.mapError(
            (cause) =>
              new ToolError({
                reason: new ToolInvalidParameters({
                  name: input.name,
                  message: Serialization.errorText(cause),
                  cause: cause,
                }),
              }),
          ),
        )
      args = yield* registration
        .decode(args)
        .pipe(Effect.provideService(ToolCall, noToolCall(input.id)))
      for (const handler of Registry.handlers(agent, 'tool')) {
        const callback = handler.beforeTool
        if (callback === undefined) continue
        const decision = yield* Effect.suspend(() =>
          callback.call(handler, { ...input, args }),
        ).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterrupts(cause)
              ? Effect.failCause(
                  Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)),
                )
              : Effect.fail(
                  new ToolError({
                    reason: new ToolBlocked({
                      name: input.name,
                      message: Serialization.errorText(Cause.squash(cause)),
                      cause: cause,
                    }),
                  }),
                ),
          ),
        )
        if (decision !== undefined && Hook.ToolDecision.$is('Block')(decision))
          return yield* new ToolError({
            reason: new ToolBlocked({ name: input.name, message: decision.block }),
          })
        if (decision !== undefined) args = decision.args
      }
      const encoded = yield* registration
        .encodeArgs(args)
        .pipe(Effect.provideService(ToolCall, noToolCall(input.id)))
      return yield* Tool.makeIntent(registration, { id: input.id, decoded: args, encoded })
    })
    const tool = Effect.fnUntraced(function* (
      intent: Tool.Intent,
      snapshot: Registry.Resolved,
      options: ToolOptions = {},
    ) {
      const invocation = yield* Invocation
      const agent =
        options.recovering === true
          ? yield* resolve(snapshot.state, options.settings ?? snapshot.settings)
          : snapshot
      const found = Arr.findFirst(agent.tools, (value) => value.tool.name === intent.name)
      if (
        options.recovering === true &&
        (intent.replay !== 'safe' ||
          Option.getOrElse(
            Option.map(found, (registration) => registration.metadata.replay),
            () => 'unsafe',
          ) !== 'safe')
      )
        return {
          outcome: 'interrupted' as const,
          result: {
            ...Tool.interruption(intent),
            ...options.previous,
            isError: true,
            details: { reason: 'interrupted', previous: options.previous?.details ?? null },
          },
        }
      const registration = Option.getOrElse(found, constUndefined)
      if (registration === undefined)
        return { outcome: 'unavailable' as const, result: Tool.unavailable(intent.name) }
      if (options.recovering === true)
        yield* invocation.progress({ clear: true, output: '', details: null, diagnostics: [] })
      const limits = Tool.outputLimits(registration.metadata)
      const outputFailure = (cause: import('./OutputError.ts').OutputError): ToolError =>
        new ToolError({
          reason: new ToolInvalidResult({ name: intent.name, message: cause.message, cause }),
        })
      const buffer = yield* Output.makeWindow(limits)
      const preview = yield* Ref.make<ToolResult | undefined>(undefined)
      const details = yield* Ref.make<Schema.Json | undefined>(undefined)
      const diagnostics = yield* Ref.make<ReadonlyArray<Diagnostic>>([])
      let written: {
        output: string
        details: Schema.Json | undefined
        hasDetails: boolean
        diagnostics: number
      } = { output: '', details: undefined, hasDetails: false, diagnostics: 0 }
      const write: Effect.Effect<number, ToolError> = Effect.gen(function* () {
        const retained = yield* buffer.snapshot.pipe(Effect.mapError(outputFailure))
        const currentDetails = yield* Ref.get(details)
        const currentDiagnostics = yield* Ref.get(diagnostics)
        const encodedDetails = JSON.stringify(currentDetails ?? null)
        const change = Output.delta(written.output, retained.text)
        const detailsChanged =
          !written.hasDetails || !Json.equals(currentDetails ?? null, written.details ?? null)
        const added = currentDiagnostics.slice(written.diagnostics)
        if (retained.text === written.output && !detailsChanged && added.length === 0) return 0
        const outputChanged = retained.text !== written.output
        const bytes = new TextEncoder().encode(
          (outputChanged ? change.text : '') +
            (detailsChanged ? encodedDetails : '') +
            JSON.stringify(added),
        ).length
        yield* invocation.progress({
          ...(outputChanged ? { output: retained.text } : {}),
          droppedBytes: retained.droppedBytes,
          droppedLines: retained.droppedLines,
          ...(detailsChanged && currentDetails !== undefined ? { details: currentDetails } : {}),
          ...(added.length === 0 ? {} : { diagnostics: added }),
        })
        written = {
          output: retained.text,
          details: currentDetails,
          hasDetails: true,
          diagnostics: currentDiagnostics.length,
        }
        return bytes
      })
      const progress = yield* Progress.make(write, {
        minIntervalMs: (options.settings ?? agent.settings).progress.outputIntervalMs,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ToolError({
              reason: new ToolInvalidResult({ name: intent.name, message: cause.message, cause }),
            }),
        ),
      )
      const ended = yield* Ref.make(false)
      const live = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E | ToolError> =>
        Effect.suspend<A, E | ToolError, never>(() =>
          Ref.getUnsafe(ended)
            ? Effect.fail(
                new ToolError({
                  reason: new ToolExecution({
                    name: intent.name,
                    message: `Tool call ${intent.id} has settled`,
                  }),
                }),
              )
            : effect,
        )
      const api = ToolCall.of({
        id: intent.id,
        preliminary: (value) =>
          live(
            Schema.encodeEffect(ToolResultSchema)(value).pipe(
              Effect.flatMap(Schema.decodeEffect(ToolResultSchema)),
              Effect.mapError(
                (cause) =>
                  new ToolError({
                    reason: new ToolInvalidResult({
                      name: intent.name,
                      message: cause.message,
                      cause: cause,
                    }),
                  }),
              ),
              Effect.flatMap(
                Effect.fnUntraced(function* (checked) {
                  yield* Ref.set(preview, checked)
                  yield* buffer.reset
                  yield* buffer
                    .push(
                      Arr.filterMap(checked.content ?? [], (part) =>
                        part.type === 'text' ? Result.succeed(part.text) : Result.failVoid,
                      ).join(''),
                    )
                    .pipe(
                      Effect.mapError(
                        (cause) =>
                          new ToolError({
                            reason: new ToolExecution({
                              name: intent.name,
                              message: cause.message,
                              cause: cause,
                            }),
                          }),
                      ),
                    )
                  if (checked.details !== undefined) yield* Ref.set(details, checked.details)
                  if (checked.diagnostics !== undefined)
                    yield* Ref.set(diagnostics, checked.diagnostics)
                  yield* progress.mark
                }),
              ),
            ),
          ),
        ...(limits.retain === 'tail' && registration.metadata.outputWindow !== false
          ? {
              outputWindow: {
                maxBytes: limits.maxBytes,
                maxLines: limits.maxLines,
                minIntervalMs: (options.settings ?? agent.settings).progress.outputIntervalMs,
                bytesPerSecond: Progress.bytesPerSecond,
              },
            }
          : {}),
        output: (chunk, skipped) =>
          live(
            buffer.push(chunk, skipped).pipe(
              Effect.mapError(
                (error) =>
                  new ToolError({
                    reason: new ToolExecution({
                      name: intent.name,
                      message: error.message,
                      cause: error,
                    }),
                  }),
              ),
              Effect.andThen(progress.mark),
            ),
          ),
        details: (value) =>
          live(
            Schema.decodeEffect(Schema.Json)(value).pipe(
              Effect.mapError(
                (cause) =>
                  new ToolError({
                    reason: new ToolInvalidResult({
                      name: intent.name,
                      message: cause.message,
                      cause: cause,
                    }),
                  }),
              ),
              Effect.flatMap((checked) => Ref.set(details, structuredClone(checked))),
              Effect.andThen(progress.markAndWait),
            ),
          ),
        diagnostic: (value) =>
          live(
            Ref.update(diagnostics, (old) => [...old, structuredClone(value)]).pipe(
              Effect.andThen(progress.mark),
            ),
          ),
      })
      const outcome = yield* Effect.exit(
        registration.execute(intent.args, intent.id).pipe(Effect.provideService(ToolCall, api)),
      )
      yield* Ref.set(ended, true)
      const retention = yield* Effect.exit(
        Effect.gen(function* () {
          yield* buffer.end.pipe(Effect.mapError(outputFailure))
          const buffered = yield* buffer.snapshot.pipe(Effect.mapError(outputFailure))
          const recordedDetails = yield* Ref.get(details)
          const recordedDiagnostics = yield* Ref.get(diagnostics)
          const preliminaryResult = yield* Ref.get(preview)
          const partial: ToolResult = {
            ...preliminaryResult,
            content: [
              ...(preliminaryResult?.content?.filter((part) => part.type !== 'text') ?? []),
              ...(buffered.text === '' ? [] : [AiPrompt.textPart({ text: buffered.text })]),
            ],
            ...(recordedDetails === undefined ? {} : { details: recordedDetails }),
            diagnostics: recordedDiagnostics,
          }
          return { buffered, partial, recordedDetails, recordedDiagnostics }
        }),
      )
      const pending = yield* progress.stop
      const final = yield* Effect.exit(
        Effect.gen(function* () {
          const { buffered, partial, recordedDetails, recordedDiagnostics } = yield* retention
          const projected = yield* Exit.match(outcome, {
            onFailure: (cause) => Tool.settleFailure(cause, partial),
            onSuccess: (native) => Tool.project(registration, native),
          })
          const finalDetails = projected.details === undefined ? recordedDetails : projected.details
          const result: ToolResult = {
            ...projected,
            content: projected.content ?? partial.content ?? [],
            ...(finalDetails === undefined ? {} : { details: finalDetails }),
            diagnostics: [...recordedDiagnostics, ...(projected.diagnostics ?? [])],
          }
          const hooked = yield* Hook.afterTool(
            Registry.handlers(agent, 'tool'),
            { id: intent.id, name: intent.name, args: intent.args },
            result,
          )
          const retainedDiagnostic =
            projected.content === undefined &&
            hooked.content === result.content &&
            buffered.droppedBytes > 0
              ? [
                  {
                    kind: 'truncated',
                    detail: {
                      droppedBytes: buffered.droppedBytes,
                      droppedLines: buffered.droppedLines,
                    },
                  },
                ]
              : []
          const bounded = Tool.boundResult(
            { ...hooked, diagnostics: [...(hooked.diagnostics ?? []), ...retainedDiagnostic] },
            limits,
          )
          const execution = {
            outcome: outcome._tag === 'Failure' ? ('failed' as const) : ('completed' as const),
            result: bounded,
          }
          if (options.commit !== undefined) yield* options.commit(execution)
          return execution
        }),
      )
      yield* Progress.settle(pending, Exit.map(final, constUndefined))
      return yield* final
    }, Effect.scoped)
    const prepareCompaction = Effect.fnUntraced(function* (
      input: CompactInput,
    ): Effect.fn.Return<CompactionPreparation, ModelError | Schema.SchemaError, Invocation> {
      const ref = yield* requireModel(input.state)
      const descriptor = yield* catalog.resolve(ref)
      const cut = Compaction.selectCut(
        input.view,
        input.settings.compaction.keepRecentTokens,
        descriptor.estimate,
      )
      const first = cut.pipe(Option.flatMap((index) => Arr.get(input.view.entries, index)))
      const selected = Option.product(cut, first)
      return yield* Option.match(selected, {
        onNone: () => Effect.succeed(CompactionPreparation.none()),
        onSome: ([cutIndex, firstKept]) =>
          Effect.gen(function* () {
            const agent = yield* resolve(input.state, input.settings)
            const decision = yield* Hook.beforeCompact(Registry.handlers(agent, 'compaction'), {
              reason: input.reason,
              view: input.view,
              firstKept: firstKept.id,
              instructions: input.instructions,
            })
            return yield* Option.match(decision, {
              onSome: (self) =>
                Effect.succeed(
                  Hook.CompactDecision.$match(self, {
                    Decline: () => CompactionPreparation.none(),
                    Summary: (value) =>
                      CompactionPreparation.summary({
                        firstKept: firstKept.id,
                        summary: value.summary,
                      }),
                  }),
                ),
              onNone: () =>
                Effect.gen(function* () {
                  const options = yield* Schema.decodeUnknownEffect(Model.RequestOptions)({
                    thinking: input.state.thinking ?? 'off',
                    options: Record.filter(input.settings.stream, (_, key) => key !== 'deferred'),
                    cache: 'none',
                    maxTokens: Math.min(
                      Math.floor(0.8 * input.settings.compaction.reserveTokens),
                      descriptor.maxOutputTokens > 0 ? descriptor.maxOutputTokens : Infinity,
                    ),
                    ...(input.sessionId === undefined ? {} : { sessionId: input.sessionId }),
                  })
                  const tail = yield* Schema.decodeEffect(EntryId)(
                    Math.max(...input.view.entries.map((entry) => entry.id)),
                  )
                  return CompactionPreparation.request({
                    request: {
                      request: {
                        model: ref,
                        prompt: Compaction.prompt(
                          Compaction.summarizedMessages(input.view, cutIndex),
                          input.instructions,
                        ),
                        tools: [],
                        options,
                        tail,
                      },
                      firstKept: firstKept.id,
                      tail,
                      attempt: 1,
                    },
                  })
                }),
            })
          }),
      })
    })
    const compact = Effect.fnUntraced(function* (pinned: SummaryRequest) {
      const descriptor = yield* catalog.resolve(pinned.request.model)
      const context = yield* descriptor.configure(pinned.request.options)
      const response = yield* descriptor.model
        .generateText({
          prompt: descriptor.normalizePrompt?.(pinned.request.prompt) ?? pinned.request.prompt,
          disableToolCallResolution: true,
        })
        .pipe(Effect.provideContext(context))
      if (
        response.finishReason !== 'stop' ||
        response.toolCalls.length > 0 ||
        response.text.trim() === ''
      )
        return yield* new ModelError({
          reason: new ModelInvalidResponse({
            usage:
              descriptor.usage?.(
                response.usage,
                Option.getOrElse(
                  Option.map(
                    Arr.findFirst(response.content, (part) => part.type === 'finish'),
                    (part) => part.metadata,
                  ),
                  () => ({}),
                ),
              ) ?? Usage.fromResponse(response.usage),
            message:
              response.finishReason === 'length'
                ? 'Summarization hit the token limit; the summary is incomplete'
                : 'Summarization did not produce a clean nonempty text response',
          }),
        })
      const finish = Arr.findFirst(response.content, (part) => part.type === 'finish')
      return {
        summary: Arr.filterMap(response.content, (part) =>
          part.type === 'text' ? Result.succeed(part.text) : Result.failVoid,
        )
          .join('\n')
          .trim(),
        usage:
          descriptor.usage?.(
            response.usage,
            Option.getOrElse(
              Option.map(finish, (part) => part.metadata),
              () => ({}),
            ),
          ) ?? Usage.fromResponse(response.usage),
      }
    })
    return Executor.of({
      resolve,
      prepare,
      generate,
      fetchDeferred,
      cancelDeferred,
      classifyResponse,
      classifyFailure: Effect.fnUntraced(function* (request, error) {
        const descriptor = yield* catalog.resolve(request.model)
        return descriptor.classify?.(error) ?? Model.classify(error, request.model.provider)
      }),
      prepareTool,
      tool,
      prepareCompaction,
      compact,
    })
  }),
)
function noToolCall(id: string): ToolCall['Service'] {
  return ToolCall.of({
    id,
    output: () => Effect.void,
    details: () => Effect.void,
    diagnostic: () => Effect.void,
  })
}

/**
 * Checks whether an unknown value satisfies the Request contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isRequest: (u: unknown) => u is Request = Schema.is(Request)

/**
 * Checks whether an unknown value satisfies the SummaryRequest contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isSummaryRequest: (u: unknown) => u is SummaryRequest = Schema.is(SummaryRequest)

/**
 * Checks whether an unknown value satisfies the Summary contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isSummary: (u: unknown) => u is Summary = Schema.is(Summary)

/**
 * Checks whether an unknown value satisfies the Disposition contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isDisposition: (u: unknown) => u is Disposition = Schema.is(Disposition)

/**
 * Type contracts owned by `Executor`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace Executor {
  /**
   * Executor preparation type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface Preparation {
    readonly request: Request
    readonly agent: Registry.Resolved
    readonly plan: ReturnType<typeof Prompt.plan>
  }
  /**
   * Executor prepare input type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface PrepareInput {
    readonly state: Agent.State
    readonly settings: Agent.Settings
    readonly view: ConversationContext.View
    readonly sessionId?: string | undefined
  }
  /**
   * Executor tool options type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface ToolOptions {
    readonly recovering?: boolean | undefined
    readonly previous?: ToolResult | undefined
    readonly settings?: Agent.Settings | undefined
    /** Persist the terminal result before detached progress acknowledgements settle. */
    readonly commit?: ((execution: Tool.Execution) => Effect.Effect<void>) | undefined
  }
  /**
   * Executor compact input type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface CompactInput extends PrepareInput {
    readonly reason: Hook.CompactInput['reason']
    readonly instructions?: string | undefined
  }
}
