/**
 * Native model and tool execution with admitted progress settlement.
 */
import * as Result from 'effect/Result'
import * as Record from 'effect/Record'
import { constant, constUndefined } from 'effect/Function'
import * as Data from 'effect/Data'
import * as Array from 'effect/Array'
import * as Option from 'effect/Option'
import * as SchemaField from './SchemaField.ts'
import * as Serialization from './Serialization.ts'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as SynchronizedRef from 'effect/SynchronizedRef'
import * as Deferred from 'effect/Deferred'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as AiError from 'effect/ai/AiError'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as Tool from 'effect/ai/Tool'
import type * as Toolkit from 'effect/ai/Toolkit'
import * as Agent from './Agent.ts'
import * as Compaction from './Compaction.ts'
import * as Transcript from './Transcript.ts'
import {
  ModelError,
  ModelInvalidResponseError,
  ModelNoModelError,
  ModelUnsupportedError,
} from './ModelError.ts'
import {
  ToolError,
  ToolBlockedError,
  ToolExecutionError,
  ToolInvalidParametersError,
  ToolInvalidResultError,
  ToolUnavailableError,
} from './ToolError.ts'
import * as Hook from './Hook.ts'
import {
  Invocation,
  ToolCall,
  Result as ToolResultSchema,
  type Result as InvocationResult,
  type Diagnostic,
} from './Invocation.ts'
import * as Model from './Model.ts'
import * as Output from './Output.ts'
import * as PromptPreparation from './PromptPreparation.ts'
import * as Registry from './Registry.ts'
import * as ToolRegistration from './ToolRegistration.ts'
import * as Usage from './Usage.ts'
import * as Progress from './Progress.ts'
import * as Exit from 'effect/Exit'
import * as Json from './Json.ts'
import { EntryId } from './Identity.ts'

/**
 * Schema for pinned model reference, native prompt, options and offered tool declarations.
 *
 * @category schemas
 */
export const Request = Schema.Struct({
  model: Agent.ModelRef,
  prompt: Prompt.Prompt,
  options: Model.RequestOptions,
  tools: Schema.Array(SystemPatch.ToolDeclaration),
  tail: SchemaField.optional(EntryId),
})
/**
 * Pinned model reference, native prompt, options and offered tool declarations.
 *
 * @category models
 */
export type Request = typeof Request.Type
/**
 * Schema for pinned compaction request with the history cut and retry attempt.
 *
 * @category schemas
 */
export const SummaryRequest = Schema.Struct({
  request: Request,
  firstKept: EntryId,
  tail: EntryId,
  attempt: Schema.Int,
})
/**
 * Pinned compaction request with the history cut and retry attempt.
 *
 * @category models
 */
export type SummaryRequest = typeof SummaryRequest.Type
/**
 * Schema for compaction summary text and usage reported by the model.
 *
 * @category schemas
 */
export const Summary = Schema.Struct({ summary: Schema.String, usage: Usage.Usage })
/**
 * Compaction summary text and usage reported by the model.
 *
 * @category models
 */
export type Summary = typeof Summary.Type
/**
 * Decision to skip compaction, use a supplied summary or request one.
 *
 * @category models
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
 */
export const CompactionPreparation = Data.taggedEnum<CompactionPreparation>()
/**
 * Encoded native AI response part emitted by a model stream.
 *
 * @category models
 */
export type Part = Response.StreamPart<Record<string, Tool.Any>, 'encoded'>
const Call = Schema.Struct({
  ...Response.ToolCallPart('', Schema.Unknown).fields,
  name: Schema.String,
})
const DeferredPayload = Schema.Struct({ decision: Model.DeferredDecision })
const PromptUsagePayload = Schema.Struct({ prompt: Prompt.Prompt, usage: Usage.Usage })
const AnswerPayload = Schema.Struct({ ...PromptUsagePayload.fields, calls: Schema.Array(Call) })
const FailurePayload = Schema.Struct({
  ...PromptUsagePayload.fields,
  message: Schema.String,
  error: Schema.optionalKey(AiError.AiError),
  retryable: Schema.Boolean,
  overflow: Schema.Boolean,
})
/**
 * Tagged execution dispositions produced by model requests.
 *
 * @category schemas
 */
export const Disposition = Schema.Union([
  Schema.TaggedStruct('deferred', DeferredPayload.fields),
  Schema.TaggedStruct('answer', AnswerPayload.fields),
  Schema.TaggedStruct('tools', AnswerPayload.fields),
  Schema.TaggedStruct('failure', FailurePayload.fields),
])
/**
 * Tool-driven decision to continue, terminate, reset or extend available tools.
 *
 * @category models
 */
export type Disposition = typeof Disposition.Type

/**
 * Service preparing model requests and executing registered tools.
 *
 * **When to use**
 *
 * Use when you need model/tool execution without durable domain storage.
 *
 * **Details**
 *
 * Consumes Registry and Model.Catalog. Prepared requests pin model reference, prompt and
 * options; native LanguageModels handle streaming. Durable executor Layers add saved
 * receipts and ownership.
 *
 * **Gotchas**
 *
 * Undeclared tool-call settlement requires the workspace’s Effect AI patch. External actions
 * still need an explicit replay policy at a durable boundary.
 *
 * @category services
 */
export class Executor extends Context.Service<
  Executor,
  {
    readonly resolve: (
      state: Agent.State,
      settings: Agent.Settings,
    ) => Effect.Effect<Registry.Resolved, never, Invocation>
    readonly prepare: (
      input: Executor.PrepareInput,
    ) => Effect.Effect<Executor.Preparation, ModelError | Schema.SchemaError, Invocation>
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
      input: Hook.Handlers.ToolInput,
    ) => Effect.Effect<ToolRegistration.Intent, ToolError, Invocation>
    readonly tool: (
      intent: ToolRegistration.Intent,
      agent: Registry.Resolved,
      options?: Executor.ToolOptions,
    ) => Effect.Effect<ToolRegistration.Execution, ToolError, Invocation>
    readonly prepareCompaction: (
      input: Executor.CompactInput,
    ) => Effect.Effect<CompactionPreparation, ModelError | Schema.SchemaError, Invocation>
    readonly compact: (
      request: SummaryRequest,
    ) => Effect.Effect<Summary, ModelError | AiError.AiError>
  }
>()('effect-harness/Executor') {}
const requireModel = (state: Agent.State): Effect.Effect<Agent.ModelRef, ModelError> =>
  state.model === undefined
    ? Effect.fail(
        new ModelError({ reason: new ModelNoModelError({ message: 'No model is configured' }) }),
      )
    : Effect.succeed(state.model)
/** Definitions-only toolkit: native AI validates/declaratively offers tools but never executes them in this boundary. */
function nativeDeclaration(declaration: Request['tools'][number]) {
  if (declaration.provider === undefined)
    return Tool.dynamic(declaration.name, {
      parameters: declaration.parameters,
      ...(declaration.description === undefined ? {} : { description: declaration.description }),
    })
  const provider = declaration.provider
  // Request's schema validates the provider namespace separator before this conditional string type assertion.
  return Tool.providerDefined({
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
 * Provides request preparation, streaming and tool execution from Registry and
 * Model.Catalog.
 *
 * **Details**
 *
 * Host dependencies are captured when the Layer is built. Request-local Invocation and
 * ToolCall services remain supplied by the caller.
 *
 * @category combinators
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
    const prepare = Effect.fnUntraced(function* (input: Executor.PrepareInput) {
      const ref = yield* requireModel(input.state)
      yield* catalog.resolve(ref)
      const agent = yield* resolve(input.state, input.settings)
      const patches = Transcript.systemPatches(input.view)
      const sections = yield* Registry.render(
        agent,
        input.view,
        PromptPreparation.replaySections(patches),
      )
      const declarations = yield* Effect.forEach(agent.tools, ToolRegistration.declaration)
      const plan = PromptPreparation.plan(input.view, sections, declarations)
      const options = yield* Schema.decodeUnknownEffect(Model.RequestOptions)({
        thinking: input.state.thinking ?? 'off',
        options: input.settings.stream,
        ...(input.sessionId === undefined ? {} : { sessionId: input.sessionId }),
      })
      const request: Request = {
        model: ref,
        prompt: PromptPreparation.toPrompt(input.view.messages, sections, {
          managedSystemMessages: Transcript.managedMessages(input.view),
        }),
        options,
        tools: declarations,
        ...(Option.isNone(Array.last(input.view.entries))
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
              reason: new ModelUnsupportedError({
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
          reason: new ModelUnsupportedError({
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
        onNone: Effect.fnUntraced(function* () {
          yield* Hook.afterResponse(Registry.handlers(agent, 'generation'), parts)
          const finish = Array.findLast(parts, (part) => part.type === 'finish')
          const usage = Option.match(finish, {
            onNone: Usage.make,
            onSome: (self) =>
              descriptor.usage?.(self.usage, self.metadata) ?? Usage.fromResponse(self.usage),
          })
          const prompt = Prompt.fromResponseParts(parts)
          // effect-nit-allow P1-stdlib-collection-replacements: classifyResponse accepts caller-owned native response arrays. Native filtering skips missing parts and retains inherited numeric accessor reads; Effect Array.filter would invoke the part guard on holes.
          const calls = parts.filter(
            (part): part is Response.ToolCallPart<string, unknown> =>
              part.type === 'tool-call' && !part.providerExecuted,
          )
          const failure = (): Disposition => {
            const errors = Array.filterMap(parts, (part) =>
              part.type === 'error' ? Result.succeed(part.error) : Result.failVoid,
            )
            const message =
              errors.map(Model.errorText).join('\n') ||
              `Model finished with ${Option.getOrElse(
                Option.map(finish, (part) => part.reason),
                constant('no finish part'),
              )}`
            const policies = errors.map(
              (error) =>
                descriptor.classify?.(error) ?? Model.classify(error, request.model.provider),
            )
            const policy = {
              overflow: policies.some((value) => value.overflow),
              retryable: policies.length > 0 && policies.every((value) => value.retryable),
            }
            return {
              _tag: 'failure',
              prompt,
              usage,
              message,
              error: Model.providerError(errors[0] ?? message, request.model.provider),
              ...policy,
            }
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
      input: Hook.Handlers.ToolInput,
    ) {
      const found = Array.findFirst(agent.tools, (tool) => tool.tool.name === input.name)
      const registration = yield* Effect.fromOption(found).pipe(
        Effect.mapError(
          () =>
            new ToolError({
              reason: new ToolUnavailableError({
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
                reason: new ToolInvalidParametersError({
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
                  // effect-nit-allow P1-stdlib-collection-replacements: native Cause.fromReasons retains its caller array, which may be sparse. Native filtering skips missing reasons while retaining interruption order; Effect Array.filter would call isInterruptReason(undefined).
                  Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)),
                )
              : Effect.fail(
                  new ToolError({
                    reason: new ToolBlockedError({
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
            reason: new ToolBlockedError({ name: input.name, message: decision.block }),
          })
        if (decision !== undefined) args = decision.args
      }
      const encoded = yield* registration
        .encodeArgs(args)
        .pipe(Effect.provideService(ToolCall, noToolCall(input.id)))
      return yield* ToolRegistration.makeIntent(registration, {
        id: input.id,
        decoded: args,
        encoded,
      })
    })
    const tool = Effect.fnUntraced(function* (
      intent: ToolRegistration.Intent,
      snapshot: Registry.Resolved,
      options: Executor.ToolOptions = {},
    ) {
      const invocation = yield* Invocation
      const agent =
        options.recovering === true
          ? yield* resolve(snapshot.state, options.settings ?? snapshot.settings)
          : snapshot
      const found = Array.findFirst(agent.tools, (value) => value.tool.name === intent.name)
      if (
        options.recovering === true &&
        (intent.replay !== 'safe' ||
          Option.getOrElse(
            Option.map(found, (registration) => registration.metadata.replay),
            constant('unsafe'),
          ) !== 'safe')
      )
        return {
          outcome: 'interrupted' as const,
          result: {
            ...ToolRegistration.interruption(intent),
            ...options.previous,
            isError: true,
            details: { reason: 'interrupted', previous: options.previous?.details ?? null },
          },
        }
      const registration = Option.getOrElse(found, constUndefined)
      if (registration === undefined)
        return {
          outcome: 'unavailable' as const,
          result: ToolRegistration.unavailable(intent.name),
        }
      if (options.recovering === true)
        yield* invocation.progress({ clear: true, output: '', details: null, diagnostics: [] })
      const limits = ToolRegistration.outputLimits(registration.metadata)
      const outputFailure = (cause: import('./OutputError.ts').OutputError): ToolError =>
        new ToolError({
          reason: new ToolInvalidResultError({ name: intent.name, message: cause.message, cause }),
        })
      interface Written {
        readonly output: string
        readonly details: Schema.Json | undefined
        readonly hasDetails: boolean
        readonly diagnostics: number
      }
      interface ToolProgressState {
        readonly buffer: Output.Window
        readonly preview: InvocationResult | undefined
        readonly details: Schema.Json | undefined
        readonly diagnostics: ReadonlyArray<Diagnostic>
        readonly ended: boolean
        readonly written: Written
        readonly waiters: ReadonlyArray<Deferred.Deferred<void, ToolError>>
      }
      const state = yield* SynchronizedRef.make<ToolProgressState>({
        buffer: yield* Output.makeWindow(limits),
        preview: undefined,
        details: undefined,
        diagnostics: [],
        ended: false,
        written: { output: '', details: undefined, hasDetails: false, diagnostics: 0 },
        waiters: [],
      })
      const admit = <A, E>(
        command: (current: ToolProgressState) => Effect.Effect<readonly [A, ToolProgressState], E>,
      ): Effect.Effect<A, E | ToolError> =>
        SynchronizedRef.modifyEffect(
          state,
          (current): Effect.Effect<readonly [A, ToolProgressState], E | ToolError> =>
            current.ended
              ? Effect.fail(
                  new ToolError({
                    reason: new ToolExecutionError({
                      name: intent.name,
                      message: `Tool call ${intent.id} has settled`,
                    }),
                  }),
                )
              : command(current),
        )
      const write: Effect.Effect<number, ToolError> = Effect.gen(function* () {
        const captured = yield* SynchronizedRef.modifyEffect(
          state,
          Effect.fnUntraced(function* (current) {
            const retained = yield* Effect.exit(
              current.buffer.snapshot.pipe(Effect.mapError(outputFailure)),
            )
            return [
              {
                retained,
                currentDetails: current.details,
                currentDiagnostics: current.diagnostics,
                written: current.written,
                waiters: current.waiters,
              },
              { ...current, waiters: [] },
            ] as const
          }),
        )
        const published = yield* Effect.exit(
          Effect.gen(function* () {
            const retained = yield* captured.retained
            const { currentDetails, currentDiagnostics, written } = captured
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
              ...(detailsChanged && currentDetails !== undefined
                ? { details: currentDetails }
                : {}),
              ...(added.length === 0 ? {} : { diagnostics: added }),
            })
            yield* SynchronizedRef.update(state, (current) => ({
              ...current,
              written: {
                output: retained.text,
                details: currentDetails,
                hasDetails: true,
                diagnostics: currentDiagnostics.length,
              },
            }))
            return bytes
          }),
        )
        yield* Progress.settle(captured.waiters, Exit.map(published, constUndefined))
        return yield* published
      })
      const progress = yield* Progress.make(write, {
        minInterval: (options.settings ?? agent.settings).progress.outputInterval,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ToolError({
              reason: new ToolInvalidResultError({
                name: intent.name,
                message: cause.message,
                cause,
              }),
            }),
        ),
      )
      const api = ToolCall.of({
        id: intent.id,
        preliminary: (value) =>
          admit(
            Effect.fnUntraced(function* (current) {
              const checked = yield* Schema.encodeEffect(ToolResultSchema)(value).pipe(
                Effect.flatMap(Schema.decodeEffect(ToolResultSchema)),
                Effect.mapError(
                  (cause) =>
                    new ToolError({
                      reason: new ToolInvalidResultError({
                        name: intent.name,
                        message: cause.message,
                        cause: cause,
                      }),
                    }),
                ),
              )
              // Stage replacement output before admitting any part of the preliminary result.
              const replacement = yield* Output.makeWindow(limits)
              yield* replacement
                .push(
                  Array.filterMap(checked.content ?? [], (part) =>
                    part.type === 'text' ? Result.succeed(part.text) : Result.failVoid,
                  ).join(''),
                )
                .pipe(
                  Effect.mapError(
                    (cause) =>
                      new ToolError({
                        reason: new ToolExecutionError({
                          name: intent.name,
                          message: cause.message,
                          cause: cause,
                        }),
                      }),
                  ),
                )
              return [
                undefined,
                {
                  ...current,
                  buffer: replacement,
                  preview: checked,
                  details: checked.details === undefined ? current.details : checked.details,
                  diagnostics:
                    checked.diagnostics === undefined ? current.diagnostics : checked.diagnostics,
                },
              ] as const
            }),
          ).pipe(Effect.andThen(progress.mark)),
        ...(limits.retain === 'tail' && registration.metadata.outputWindow !== false
          ? {
              outputWindow: {
                maxBytes: limits.maxBytes,
                maxLines: limits.maxLines,
                minInterval: (options.settings ?? agent.settings).progress.outputInterval,
                bytesPerSecond: Progress.bytesPerSecond,
              },
            }
          : {}),
        output: (chunk, skipped) =>
          admit((current) =>
            current.buffer.push(chunk, skipped).pipe(
              Effect.mapError(
                (error) =>
                  new ToolError({
                    reason: new ToolExecutionError({
                      name: intent.name,
                      message: error.message,
                      cause: error,
                    }),
                  }),
              ),
              Effect.map(() => [undefined, current] as const),
            ),
          ).pipe(Effect.andThen(progress.mark)),
        details: (value) =>
          admit(
            Effect.fnUntraced(function* (current) {
              const checked = yield* Schema.decodeEffect(Schema.Json)(value).pipe(
                Effect.mapError(
                  (cause) =>
                    new ToolError({
                      reason: new ToolInvalidResultError({
                        name: intent.name,
                        message: cause.message,
                        cause: cause,
                      }),
                    }),
                ),
              )
              const owned = yield* Effect.sync(() => structuredClone(checked))
              const waiter = yield* Deferred.make<void, ToolError>()
              return [
                waiter,
                {
                  ...current,
                  details: owned,
                  waiters: [...current.waiters, waiter],
                },
              ] as const
            }),
          ).pipe(
            Effect.flatMap((waiter) => progress.mark.pipe(Effect.andThen(Deferred.await(waiter)))),
          ),
        diagnostic: (value) =>
          admit((current) =>
            Effect.sync(
              () =>
                [
                  undefined,
                  { ...current, diagnostics: [...current.diagnostics, structuredClone(value)] },
                ] as const,
            ),
          ).pipe(Effect.andThen(progress.mark)),
      })
      const outcome = yield* Effect.exit(
        registration.execute(intent.args, intent.id).pipe(Effect.provideService(ToolCall, api)),
      )
      yield* SynchronizedRef.update(state, (current) => ({ ...current, ended: true }))
      const retention = yield* Effect.exit(
        SynchronizedRef.modifyEffect(
          state,
          Effect.fnUntraced(function* (current) {
            yield* current.buffer.end
            const buffered = yield* current.buffer.snapshot
            const recordedDetails = current.details
            const recordedDiagnostics = current.diagnostics
            // preview is encoded and decoded through Invocation.Result before storage; its content array is dense.
            const preliminaryResult = current.preview
            const partial: InvocationResult = {
              ...preliminaryResult,
              content: [
                ...Array.filter(preliminaryResult?.content ?? [], (part) => part.type !== 'text'),
                ...(buffered.text === '' ? [] : [Prompt.textPart({ text: buffered.text })]),
              ],
              ...(recordedDetails === undefined ? {} : { details: recordedDetails }),
              diagnostics: recordedDiagnostics,
            }
            return [{ buffered, partial, recordedDetails, recordedDiagnostics }, current] as const
          }, Effect.mapError(outputFailure)),
        ),
      )
      const pending = yield* progress.stop
      const unpublished = yield* SynchronizedRef.modify(
        state,
        (current) => [current.waiters, { ...current, waiters: [] }] as const,
      )
      const final = yield* Effect.exit(
        Effect.gen(function* () {
          const { buffered, partial, recordedDetails, recordedDiagnostics } = yield* retention
          const projected = yield* Exit.match(outcome, {
            onFailure: (cause) => ToolRegistration.settleFailure(cause, partial),
            onSuccess: (native) => ToolRegistration.project(registration, native),
          })
          const finalDetails = projected.details === undefined ? recordedDetails : projected.details
          const result: InvocationResult = {
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
          const bounded = ToolRegistration.boundResult(
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
      const receipt = Exit.map(final, constUndefined)
      yield* Progress.settle([...pending, ...unpublished], receipt)
      return yield* final
    }, Effect.scoped)
    const prepareCompaction = Effect.fnUntraced(function* (
      input: Executor.CompactInput,
    ): Effect.fn.Return<CompactionPreparation, ModelError | Schema.SchemaError, Invocation> {
      const ref = yield* requireModel(input.state)
      const descriptor = yield* catalog.resolve(ref)
      const cut = Compaction.selectCut(
        input.view,
        input.settings.compaction.keepRecentTokens,
        descriptor.estimate,
      )
      const first = cut.pipe(Option.flatMap((index) => Array.get(input.view.entries, index)))
      const selected = Option.product(cut, first)
      return yield* Option.match(selected, {
        onNone: () => Effect.succeed(CompactionPreparation.none()),
        onSome: Effect.fnUntraced(function* ([cutIndex, firstKept]) {
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
            onNone: Effect.fnUntraced(function* () {
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
          reason: new ModelInvalidResponseError({
            usage:
              descriptor.usage?.(
                response.usage,
                Option.getOrElse(
                  Option.map(
                    Array.findFirst(response.content, (part) => part.type === 'finish'),
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
      const finish = Array.findFirst(response.content, (part) => part.type === 'finish')
      return {
        summary: Array.filterMap(response.content, (part) =>
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
 * Checks whether a value satisfies the decoded `Request` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isRequest: (u: unknown) => u is Request = Schema.is(Request)

/**
 * Checks whether a value satisfies the decoded `SummaryRequest` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isSummaryRequest: (u: unknown) => u is SummaryRequest = Schema.is(SummaryRequest)

/**
 * Checks whether a value satisfies the decoded `Summary` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isSummary: (u: unknown) => u is Summary = Schema.is(Summary)

/**
 * Checks whether a value satisfies the decoded `Disposition` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isDisposition: (u: unknown) => u is Disposition = Schema.is(Disposition)

/**
 * Type-level contracts for `Executor`.
 */
export declare namespace Executor {
  /**
   * Prepared request and resolved registry/model inputs.
   *
   * @category models
   */
  interface Preparation {
    readonly request: Request
    readonly agent: Registry.Resolved
    readonly plan: ReturnType<typeof PromptPreparation.plan>
  }
  /**
   * Conversation context, agent overrides and policy used to prepare a request.
   *
   * @category models
   */
  interface PrepareInput {
    readonly state: Agent.State
    readonly settings: Agent.Settings
    readonly view: Transcript.View
    readonly sessionId?: string | undefined
  }
  /**
   * Invocation policy and callbacks supplied to a bound tool execution.
   *
   * @category models
   */
  interface ToolOptions {
    readonly recovering?: boolean | undefined
    readonly previous?: InvocationResult | undefined
    readonly settings?: Agent.Settings | undefined
    /** Persist the terminal result before detached progress acknowledgements settle. */
    readonly commit?: ((execution: ToolRegistration.Execution) => Effect.Effect<void>) | undefined
  }
  /**
   * Context and compaction policy used to choose and prepare a summary.
   *
   * @category models
   */
  interface CompactInput extends PrepareInput {
    readonly reason: Hook.Handlers.CompactInput['reason']
    readonly instructions?: string | undefined
  }
}

import * as SystemPatch from './SystemPatch.ts'
