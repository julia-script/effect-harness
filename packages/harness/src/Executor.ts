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
import * as AiResponse from 'effect/ai/Response'
import * as AiTool from 'effect/ai/Tool'
import type * as Toolkit from 'effect/ai/Toolkit'
import * as Agent from './Agent.ts'
import * as Compaction from './Compaction.ts'
import * as ConversationContext from './Context.ts'
import {
  ModelError,
  ToolError,
  ModelInvalidResponse,
  ModelNoModel,
  ModelUnsupported,
  ToolBlocked,
  ToolExecution,
  ToolInvalidParameters,
  ToolInvalidResult,
  ToolUnavailable,
} from './Error.ts'
import * as Hook from './Hook.ts'
import { Invocation, ToolCall, Result, type ToolResult, type Diagnostic } from './Invocation.ts'
import * as Model from './Model.ts'
import * as Output from './Output.ts'
import * as Prompt from './Prompt.ts'
import * as Registry from './Registry.ts'
import * as Tool from './Tool.ts'
import * as Usage from './Usage.ts'
import * as Progress from './Progress.ts'
import * as Exit from 'effect/Exit'

export const Request = Schema.Struct({
  model: Agent.ModelRef,
  prompt: AiPrompt.Prompt,
  options: Model.RequestOptions,
  tools: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      description: Schema.optionalKey(Schema.String),
      parameters: Schema.Record(Schema.String, Schema.Json),
      provider: Schema.optionalKey(
        Schema.Struct({
          id: Schema.String.check(Schema.isPattern(/^[^.]+\..+$/)),
          name: Schema.String,
          args: Schema.Json,
        }),
      ),
    }),
  ),
  tail: Schema.optionalKey(Schema.Int),
})
export type Request = typeof Request.Type
export interface Preparation {
  readonly request: Request
  readonly agent: Registry.Resolved
  readonly plan: ReturnType<typeof Prompt.plan>
}
export const SummaryRequest = Schema.Struct({
  request: Request,
  firstKept: Schema.Int,
  tail: Schema.Int,
  attempt: Schema.Int,
})
export type SummaryRequest = typeof SummaryRequest.Type
export const Summary = Schema.Struct({ summary: Schema.String, usage: Usage.Usage })
export type Summary = typeof Summary.Type
export type CompactionPreparation =
  | { readonly type: 'none' }
  | { readonly type: 'summary'; readonly firstKept: number; readonly summary: string }
  | { readonly type: 'request'; readonly request: SummaryRequest }
export type Part = AiResponse.StreamPart<Record<string, AiTool.Any>, 'encoded'>
export interface PrepareInput {
  readonly state: Agent.State
  readonly settings: Agent.Settings
  readonly view: ConversationContext.View
  readonly sessionId?: string | undefined
}
export interface ToolOptions {
  readonly recovering?: boolean | undefined
  readonly previous?: ToolResult | undefined
  readonly settings?: Agent.Settings | undefined
  /** Persist the terminal result before detached progress acknowledgements settle. */
  readonly commit?: ((execution: Tool.Execution) => Effect.Effect<void>) | undefined
}
export interface CompactInput extends PrepareInput {
  readonly reason: Hook.CompactInput['reason']
  readonly instructions?: string | undefined
}
const Call = Schema.Struct({
  ...AiResponse.ToolCallPart('', Schema.Unknown).fields,
  name: Schema.String,
})
export const Disposition = Schema.Union([
  Schema.Struct({ type: Schema.Literal('deferred'), decision: Model.DeferredDecision }),
  Schema.Struct({
    type: Schema.Literals(['answer', 'tools']),
    prompt: AiPrompt.Prompt,
    usage: Usage.Usage,
    calls: Schema.Array(Call),
  }),
  Schema.Struct({
    type: Schema.Literal('failure'),
    prompt: AiPrompt.Prompt,
    usage: Usage.Usage,
    message: Schema.String,
    retryable: Schema.Boolean,
    overflow: Schema.Boolean,
  }),
])
export type Disposition = typeof Disposition.Type

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
      parts: ReadonlyArray<AiResponse.AnyPart>,
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
    ) => Effect.Effect<Tool.Execution, never, Invocation>
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
    tools: Object.fromEntries(
      tools.map((declaration) => [declaration.name, nativeDeclaration(declaration)]),
    ),
    handle: () => Effect.die('Tool resolution must be disabled at the model request boundary'),
  }
}
export const layer: Layer.Layer<Executor, never, Registry.Registry | Model.Catalog> = Layer.effect(
  Executor,
  Effect.gen(function* () {
    const registry = yield* Registry.Registry
    const catalog = yield* Model.Catalog
    const resolve = Effect.fnUntraced(function* (state: Agent.State, settings: Agent.Settings) {
      return yield* Registry.resolve(yield* registry.snapshot, state, settings)
    })
    const prepare = Effect.fnUntraced(function* (input: PrepareInput) {
      const ref = yield* requireModel(input.state)
      yield* catalog.resolve(ref)
      const agent = yield* resolve(input.state, input.settings)
      const patches = ConversationContext.systemPatches(input.view)
      const sections = yield* Registry.render(agent, input.view, Prompt.replaySections(patches))
      const plan = Prompt.plan(input.view, sections, agent.tools.map(Tool.declaration))
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
        tools: yield* Schema.decodeUnknownEffect(Request.fields.tools)(
          agent.tools.map(Tool.declaration),
        ),
        ...(input.view.entries.at(-1) === undefined
          ? {}
          : { tail: Math.max(...input.view.entries.map((entry) => entry.id)) }),
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
      parts: ReadonlyArray<AiResponse.AnyPart>,
    ): Effect.fn.Return<Disposition, ModelError, Invocation> {
      const descriptor = yield* catalog.resolve(request.model)
      const deferred = descriptor.deferred?.inspect(parts)
      if (deferred !== undefined) return { type: 'deferred', decision: deferred }
      yield* Hook.afterResponse(Registry.handlers(agent, 'generation'), parts)
      const finish = parts.findLast((part) => part.type === 'finish')
      const usage =
        finish === undefined
          ? Usage.zero()
          : (descriptor.usage?.(finish.usage, finish.metadata) ?? Usage.fromResponse(finish.usage))
      const prompt = AiPrompt.fromResponseParts(parts)
      const calls = parts.filter(
        (part): part is AiResponse.ToolCallPart<string, unknown> =>
          part.type === 'tool-call' && !part.providerExecuted,
      )
      if (
        finish?.reason === 'stop' ||
        finish?.reason === 'length' ||
        finish?.reason === 'tool-calls'
      )
        return {
          type: finish.reason === 'tool-calls' && calls.length > 0 ? 'tools' : 'answer',
          prompt,
          usage,
          calls,
        }
      const errors = parts.flatMap((part) => (part.type === 'error' ? [part.error] : []))
      const message =
        errors.map(Model.errorText).join('\n') ||
        `Model finished with ${finish?.reason ?? 'no finish part'}`
      const policies = errors.map(
        (error) => descriptor.classify?.(error) ?? Model.classify(error, request.model.provider),
      )
      const policy = {
        overflow: policies.some((value) => value.overflow),
        retryable: policies.length > 0 && policies.every((value) => value.retryable),
      }
      return { type: 'failure', prompt, usage, message, ...policy }
    })
    const prepareTool = Effect.fnUntraced(function* (
      agent: Registry.Resolved,
      input: Hook.ToolInput,
    ) {
      const registration = agent.tools.find((tool) => tool.tool.name === input.name)
      if (registration === undefined)
        return yield* new ToolError({
          reason: new ToolUnavailable({
            name: input.name,
            message: `Tool ${input.name} is not available`,
          }),
        })
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
        if (decision !== undefined && 'block' in decision)
          return yield* new ToolError({
            reason: new ToolBlocked({ name: input.name, message: decision.block }),
          })
        if (decision !== undefined) args = decision.args
      }
      const encoded = yield* registration
        .encodeArgs(args)
        .pipe(Effect.provideService(ToolCall, noToolCall(input.id)))
      return yield* Tool.makeIntent(registration, input.id, args, encoded)
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
      const registration = agent.tools.find((value) => value.tool.name === intent.name)
      if (
        options.recovering === true &&
        (intent.replay !== 'safe' || registration?.metadata.replay !== 'safe')
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
      if (registration === undefined)
        return { outcome: 'unavailable' as const, result: Tool.unavailable(intent.name) }
      if (options.recovering === true)
        yield* invocation.progress({ clear: true, output: '', details: null, diagnostics: [] })
      const limits = Tool.outputLimits(registration.metadata)
      let buffer = Output.make(limits)
      const preview = yield* Ref.make<ToolResult | undefined>(undefined)
      const details = yield* Ref.make<Schema.Json | undefined>(undefined)
      const diagnostics = yield* Ref.make<ReadonlyArray<Diagnostic>>([])
      let written = { output: '', details: '', diagnostics: 0 }
      const write = Effect.gen(function* () {
        const retained = Output.snapshot(buffer)
        const currentDetails = yield* Ref.get(details)
        const currentDiagnostics = yield* Ref.get(diagnostics)
        const encodedDetails = JSON.stringify(currentDetails ?? null)
        const change = Output.delta(written.output, retained.text)
        const detailsChanged = encodedDetails !== written.details
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
          details: encodedDetails,
          diagnostics: currentDiagnostics.length,
        }
        return bytes
      })
      const progress = yield* Progress.make(
        write,
        (options.settings ?? agent.settings).progress.outputIntervalMs,
      )
      let ended = false
      const live = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E | ToolError> =>
        Effect.suspend<A, E | ToolError, never>(() =>
          ended
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
            Schema.encodeEffect(Result)(value).pipe(
              Effect.flatMap(Schema.decodeEffect(Result)),
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
              Effect.flatMap((checked) =>
                Effect.gen(function* () {
                  yield* Ref.set(preview, checked)
                  buffer = Output.make(limits)
                  yield* Output.push(
                    buffer,
                    checked.content
                      ?.flatMap((part) => (part.type === 'text' ? [part.text] : []))
                      .join('') ?? '',
                  ).pipe(
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
            Output.push(buffer, chunk, skipped).pipe(
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
      ended = true
      Output.end(buffer)
      const buffered = Output.snapshot(buffer)
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
      const pending = yield* progress.stop
      const final = yield* Effect.exit(
        Effect.gen(function* () {
          const projected =
            outcome._tag === 'Failure'
              ? yield* Tool.settleFailure(outcome.cause, partial)
              : (registration.metadata.project ?? Tool.defaultProject)(
                  outcome.value.result,
                  outcome.value.encoded,
                  outcome.value.isFailure,
                )
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
      yield* Progress.settle(
        pending,
        Exit.map(final, () => undefined),
      )
      return yield* final
    })
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
      const first = cut === undefined ? undefined : input.view.entries[cut]
      if (cut === undefined || first === undefined) return { type: 'none' }
      const agent = yield* resolve(input.state, input.settings)
      const decision = yield* Hook.beforeCompact(Registry.handlers(agent, 'compaction'), {
        reason: input.reason,
        view: input.view,
        firstKept: first.id,
        instructions: input.instructions,
      })
      if (decision !== undefined && 'decline' in decision) return { type: 'none' }
      if (decision !== undefined)
        return { type: 'summary', firstKept: first.id, summary: decision.summary }
      const options = yield* Schema.decodeUnknownEffect(Model.RequestOptions)({
        thinking: input.state.thinking ?? 'off',
        options: Object.fromEntries(
          Object.entries(input.settings.stream).filter(([key]) => key !== 'deferred'),
        ),
        cache: 'none',
        maxTokens: Math.min(
          Math.floor(0.8 * input.settings.compaction.reserveTokens),
          descriptor.maxOutputTokens > 0 ? descriptor.maxOutputTokens : Infinity,
        ),
        ...(input.sessionId === undefined ? {} : { sessionId: input.sessionId }),
      })
      const tail = Math.max(...input.view.entries.map((entry) => entry.id))
      return {
        type: 'request',
        request: {
          request: {
            model: ref,
            prompt: Compaction.prompt(
              Compaction.summarizedMessages(input.view, cut),
              input.instructions,
            ),
            tools: [],
            options,
            tail,
          },
          firstKept: first.id,
          tail,
          attempt: 1,
        },
      }
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
                response.content.find((part) => part.type === 'finish')?.metadata ?? {},
              ) ?? Usage.fromResponse(response.usage),
            message:
              response.finishReason === 'length'
                ? 'Summarization hit the token limit; the summary is incomplete'
                : 'Summarization did not produce a clean nonempty text response',
          }),
        })
      const finish = response.content.find((part) => part.type === 'finish')
      return {
        summary: response.content
          .flatMap((part) => (part.type === 'text' ? [part.text] : []))
          .join('\n')
          .trim(),
        usage:
          descriptor.usage?.(response.usage, finish?.metadata ?? {}) ??
          Usage.fromResponse(response.usage),
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
      tool: (...args) => Effect.scoped(tool(...args)),
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
