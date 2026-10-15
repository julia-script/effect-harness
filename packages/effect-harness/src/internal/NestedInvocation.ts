/** Parent-owned identities and result members; the existing scheduler owns every child. */
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Agent from '../Agent.js'
import * as Document from '../Document.js'
import * as Invocation from '../ToolInvocation.js'
import * as Inventory from '../ToolInventory.js'
import * as Tool from '../Tool.js'
import * as Prepared from '../PreparedRequest.js'
import * as ProgressPolicy from '../ProgressPolicy.js'
import * as Retention from '../OutputRetention.js'
import * as Record from '../Record.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as StorageFormat from '../StorageFormat.js'
import * as Task from '../Task.js'
import type * as TaskExecution from '../TaskExecution.js'
import * as ToolProgress from '../ToolProgress.js'
import * as Transaction from '../Transaction.js'
import * as Usage from '../Usage.js'
import { ExecutionError } from '../ExecutionError.js'
import * as Round from './ToolRound.js'
import * as Value from './DocumentValue.js'
import * as Preparation from './RequestPreparation.js'
import * as ResultRecord from './ResultRecord.js'

export const ContractSchema = Schema.Struct({
  tool: Prepared.ToolSchema,
  resultChannels: Schema.Boolean,
  structuredOutput: Schema.Boolean,
})
export const InputSchema = Schema.Struct({
  parentTaskId: Record.TaskId,
  parentCallId: Schema.optionalKey(Schema.NonEmptyString),
  key: Schema.NonEmptyString,
  logicalCallId: Usage.NestedCallId,
  call: Tool.CallSchema,
  caller: Schema.tag('tools'),
  contract: ContractSchema,
  agent: Agent.StateSchema,
  replay: Tool.ReplaySchema,
  retention: Retention.Policy,
  progress: ProgressPolicy.Policy,
  progressEnabled: Schema.Boolean,
})
export type Input = typeof InputSchema.Type
/** Terminal children retain only identity; the caller owns invocation arguments and bindings. */
export const ReferenceSchema = Schema.Struct({
  parentTaskId: Record.TaskId,
  key: Schema.NonEmptyString,
  logicalCallId: Usage.NestedCallId,
  call: Schema.Struct({ id: Schema.NonEmptyString, name: Schema.NonEmptyString }),
})
export type Reference = typeof ReferenceSchema.Type
export const inputs = Document.family({
  kind: 'harness.tool.nested.input',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ input: Schema.optionalKey(Schema.toCodecJson(InputSchema)) }),
  initial: () => ({}),
})
const inputTarget = (ref: Reference): Document.Target => ({
  scope: { _tag: 'task', taskId: ref.parentTaskId },
  key: ref.key,
})
export const readInput = Effect.fnUntraced(function* (tx: Transaction.Transaction, ref: Reference) {
  const saved = yield* Transaction.snapshot(tx, inputs, inputTarget(ref))
  if (Option.isNone(saved) || saved.value.value.input === undefined)
    return yield* error('missingResult', 'The caller-owned invocation input is absent')
  const input = saved.value.value.input
  if (
    input.parentTaskId !== ref.parentTaskId ||
    input.key !== ref.key ||
    input.logicalCallId !== ref.logicalCallId ||
    input.call.id !== ref.call.id ||
    input.call.name !== ref.call.name
  )
    return yield* error('conflict', 'The invocation input belongs to another call')
  return input
})
export const CompletionSchema = Schema.Struct({ taskId: Record.TaskId, callId: Usage.NestedCallId })
export type Definition = Task.Any & {
  readonly input: typeof ReferenceSchema
  readonly checkpoint: typeof Round.CheckpointSchema
  readonly success: typeof CompletionSchema
  readonly failure: typeof Schema.Json
}
export const calls = Document.define({
  kind: 'harness.tool.nested.calls',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({
    calls: Schema.Record(
      Schema.String,
      Schema.Struct({
        handle: Invocation.HandleSchema,
      }),
    ),
  }),
  initial: () => ({ calls: {} }),
})
export const results = Document.family({
  kind: 'harness.tool.nested.result',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({
    receipt: Schema.optionalKey(Schema.toCodecJson(ResultRecord.receipt)),
  }),
  initial: () => ({}),
})
export const target = (parentTaskId: Record.TaskId, child?: Record.TaskId): Document.Target => ({
  scope: { _tag: 'task', taskId: parentTaskId },
  ...(child === undefined ? {} : { key: String(child) }),
})
export const error = (reason: Invocation.InvocationError['reason'], message: string) =>
  new Invocation.InvocationError({ reason, message })
const keyCheck = (key: string) =>
  key === '' || key.includes('/') || key === '__proto__' || /^[1-9][0-9]*$/.test(key)
const sameInput = Schema.toEquivalence(InputSchema)
const sameHandle = Schema.toEquivalence(Invocation.HandleSchema)
export const signature = (item: RegistryScope.BoundTool) =>
  Preparation.declaration(item).pipe(
    Effect.map((tool) => ({
      tool,
      resultChannels: Tool.hasResultChannels(item.declaration),
      structuredOutput: Tool.hasStructuredOutput(item.declaration),
    })),
    Effect.mapError((cause) => error('incompatibleContract', cause.message)),
  )

/** Recheck the exact runtime's matching capability; inspection never activates a format. */
export const protection = (inspect: Effect.Effect<StorageFormat.Format, StorageFormat.Failure>) =>
  Effect.gen(function* () {
    const marker = yield* inspect
    yield* StorageFormat.validate(marker)
    if (marker.minimumReaderVersion < 12)
      return yield* new StorageFormat.StorageFormatError({
        reason: 'unsupported',
        message: 'Nested execution requires container and minimum reader protection 11/11',
      })
  })

export const assert = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
  taskId: Record.TaskId,
  input: Input,
) {
  const index = yield* Transaction.snapshot(tx, calls, target(input.parentTaskId))
  const saved =
    Option.isSome(index) && Object.hasOwn(index.value.value.calls, input.key)
      ? index.value.value.calls[input.key]
      : undefined
  const child = yield* Transaction.task(tx, taskId)
  if (
    saved === undefined ||
    saved.handle.taskId !== taskId ||
    Option.isNone(child) ||
    child.value.owner !== input.parentTaskId ||
    child.value.conversationId !== conversationId
  )
    return yield* error('conflict', 'The child does not own this parent invocation slot')
  if (
    !sameInput(yield* readInput(tx, yield* Task.decode(ReferenceSchema, child.value.input)), input)
  )
    return yield* error('conflict', 'The child invocation input changed')
  return saved.handle
})

interface Epoch {
  active: boolean
  admissions: number
  drained: Deferred.Deferred<void>
  readonly children: Set<Record.TaskId>
}
const closers = new WeakMap<
  Invocation.Access,
  (cancel: boolean) => Effect.Effect<void, Invocation.Failure>
>()
const stagers = new WeakMap<
  Invocation.Access,
  (tx: Transaction.Transaction) => Effect.Effect<void, Invocation.Failure>
>()
const borrowers = new WeakMap<
  Invocation.Access,
  (gate: Effect.Effect<void, ExecutionError>) => Invocation.Access
>()
const admissionGates = new WeakMap<
  TaskExecution.Access,
  (tx: Transaction.Transaction) => Effect.Effect<void, TaskExecution.Failure>
>()
/** Native scheduler only: first registration wins and cannot replace a reservation guard. */
export const registerGate = (
  access: TaskExecution.Access,
  gate: (tx: Transaction.Transaction) => Effect.Effect<void, TaskExecution.Failure>,
): boolean => {
  if (admissionGates.has(access)) return false
  admissionGates.set(access, gate)
  return true
}
class SelectedOwner extends Context.Service<
  SelectedOwner,
  { readonly id: string; readonly version: number }
>()('effect-harness/internal/NestedInvocation/SelectedOwner') {}
/** Fence admissions and mark all children in the parent's genuine completion batch. */
export const stageClose = (access: Invocation.Access, tx: Transaction.Transaction) =>
  stagers.get(access)?.(tx) ?? Effect.void
/** Stop admissions and optionally mark all owned children before joining any of them. */
export const close = (access: Invocation.Access, cancel = false) =>
  closers.get(access)?.(cancel) ?? Effect.void

export const make = Effect.fnUntraced(function* (
  execution: TaskExecution.Access,
  lease: RegistryScope.Lease,
  mode: 'run' | 'abort',
  definition: Definition,
  requireFormat: Effect.Effect<StorageFormat.Format, StorageFormat.Failure>,
  inspectFormat: Effect.Effect<StorageFormat.Format, StorageFormat.Failure>,
) {
  const nativeGate = admissionGates.get(execution)
  if (nativeGate === undefined)
    return yield* new ExecutionError({
      reason: 'unsupported',
      operation: 'tool.admit',
      message: 'Native invocation requires its actual scheduler transaction guard',
    })
  const agentAtInvocation = yield* Value.copy(Agent.StateSchema, yield* execution.agent)
  const root: Epoch = {
    active: true,
    admissions: 0,
    drained: Deferred.makeUnsafe<void>(),
    children: new Set(),
  }
  let sequence = 0
  let marked = false
  let joined = false
  const snapshotAgent = Value.copy(Agent.StateSchema, agentAtInvocation)
  const ownRequest = (request: Invocation.Request) => Value.copy(Invocation.RequestSchema, request)
  const key = (options?: Invocation.Options) => {
    const value = options?.key ?? String(++sequence)
    return options?.key !== undefined && keyCheck(value)
      ? Effect.fail(
          error('invalidKey', 'Explicit nested keys must be nonempty safe nonordinal segments'),
        )
      : Effect.succeed(value)
  }
  const deny = (slot: string, name: string, message: string): Invocation.WireReceipt => ({
    parentTaskId: execution.taskId,
    key: slot,
    callId: Usage.NestedCallId.make(`nested:${execution.taskId}:${slot}`),
    name,
    attempt: Usage.AttemptId.make(0),
    status: 'denied',
    result: {
      valueKind: 'structured',
      isError: true,
      diagnostics: [{ severity: 'error', message }],
    },
  })
  const selection = (
    agent: Agent.State,
    name: string,
    binding?: Invocation.Options['binding'],
  ): Inventory.Options => ({
    agent: {
      ...agent,
      ...(binding === undefined
        ? {}
        : {
            toolkits: {
              inherit: agent.toolkits?.inherit ?? true,
              registrations: agent.toolkits?.registrations ?? [],
              ...agent.toolkits,
              bindings: {
                ...agent.toolkits?.bindings,
                [name]: { binding: binding.id, version: binding.version },
              },
            },
          }),
    },
  })
  const markChildren = (tx: Transaction.Transaction, owned?: ReadonlySet<Record.TaskId>) =>
    Effect.gen(function* () {
      const index = yield* Transaction.snapshot(tx, calls, target(execution.taskId))
      const handles = Option.isSome(index)
        ? Object.values(index.value.value.calls).map((slot) => slot.handle)
        : []
      const selected =
        owned === undefined ? handles : handles.filter((handle) => owned.has(handle.taskId))
      for (const handle of selected) {
        const record = yield* Transaction.task(tx, handle.taskId)
        if (Option.isNone(record) || record.value.owner !== execution.taskId)
          return yield* error('conflict', 'Nested cleanup found a child with another owner')
        if (record.value.state.status !== 'terminal')
          yield* Transaction.requestTaskAbort(tx, handle.taskId, 'parent settled')
      }
      return selected
    })
  const makeAccess = (
    epoch: Epoch,
    gate: Effect.Effect<void, ExecutionError>,
  ): Invocation.Access => {
    const check = gate.pipe(
      Effect.andThen(
        Effect.suspend(() =>
          root.active && epoch.active
            ? Effect.void
            : Effect.fail(error('revoked', 'The invocation has ended')),
        ),
      ),
    )
    const admitOwned = Effect.fnUntraced(function* (
      tx: Transaction.Transaction,
      request: Invocation.Request,
      reservedKey?: string,
    ) {
      yield* nativeGate(tx)
      yield* check
      if (mode === 'abort')
        return yield* new ExecutionError({
          reason: 'revoked',
          operation: 'tool.admit',
          message: 'Abort cleanup cannot admit new children',
        })
      const owned = yield* ownRequest(request)
      const slot = reservedKey ?? (yield* key(owned.options))
      const agent = yield* snapshotAgent
      const pinned = yield* Effect.serviceOption(SelectedOwner)
      if (
        Option.isSome(pinned) &&
        owned.options?.binding !== undefined &&
        (pinned.value.id !== owned.options.binding.id ||
          pinned.value.version !== owned.options.binding.version)
      )
        return yield* error(
          'incompatibleContract',
          'Typed invocation cannot switch its leased schema owner',
        )
      const options = selection(
        agent,
        owned.name,
        Option.isSome(pinned) ? pinned.value : owned.options?.binding,
      )
      const selected = Inventory.inspect(lease.snapshot, owned.name, 'tools', options)
      if (selected._tag === 'denied')
        return { _tag: 'denied' as const, receipt: deny(slot, owned.name, selected.denial.message) }
      const item = yield* lease.resolveTool(owned.name, {
        binding: selected.registration,
        version: selected.version,
      })
      const input: Input = {
        parentTaskId: execution.taskId,
        key: slot,
        logicalCallId: Usage.NestedCallId.make(`nested:${execution.taskId}:${slot}`),
        call: {
          id: `nested:${execution.taskId}:${slot}`,
          name: owned.name,
          arguments: owned.arguments,
        },
        caller: 'tools',
        contract: yield* signature(item),
        agent: selection(agent, owned.name, {
          id: selected.registration,
          version: selected.version,
        }).agent!,
        replay: Tool.policy(item.declaration).replay,
        retention: Tool.outputRetention(item.declaration),
        progress: Tool.progress(item.declaration),
        progressEnabled: owned.options?.progress !== false,
      }
      const index = yield* Transaction.snapshot(tx, calls, target(execution.taskId))
      const old = Option.isSome(index) ? index.value.value.calls : {}
      const existing = Object.hasOwn(old, slot) ? old[slot] : undefined
      if (existing !== undefined) {
        if (!sameInput(yield* readInput(tx, input), input))
          return yield* error(
            'conflict',
            'This key already names another invocation contract or arguments',
          )
        epoch.children.add(existing.handle.taskId)
        yield* check
        return { _tag: 'admitted' as const, handle: existing.handle }
      }
      yield* requireFormat
      yield* protection(inspectFormat)
      yield* check
      yield* Transaction.ensureDocument(tx, inputs, inputTarget(input))
      yield* Transaction.setDocument(tx, inputs, inputTarget(input), { input })
      const child = yield* execution.createChild(tx, definition, {
        parentTaskId: input.parentTaskId,
        key: input.key,
        logicalCallId: input.logicalCallId,
        call: { id: input.call.id, name: input.call.name },
      })
      const handle: Invocation.Handle = {
        parentTaskId: execution.taskId,
        conversationId: execution.conversationId,
        key: slot,
        taskId: child.id,
        callId: input.logicalCallId,
        name: owned.name,
      }
      yield* Transaction.ensureDocument(tx, calls, target(execution.taskId))
      yield* Transaction.setDocument(tx, calls, target(execution.taskId), {
        calls: { ...old, [slot]: { handle } },
      })
      yield* Transaction.ensureDocument(tx, results, target(execution.taskId, child.id))
      if (input.progressEnabled)
        yield* Transaction.ensureDocument(tx, ToolProgress.document, {
          scope: { _tag: 'conversation', conversationId: execution.conversationId },
          key: Round.progressKey(child.id),
        })
      epoch.children.add(child.id)
      yield* check
      return { _tag: 'admitted' as const, handle }
    })
    // Abort cleanup can read old results after its admission epoch is sealed.
    const readable = gate.pipe(
      Effect.andThen(
        Effect.suspend(() =>
          epoch.active
            ? Effect.void
            : Effect.fail(error('revoked', 'The borrowed invocation has ended')),
        ),
      ),
    )
    const validateHandle = Effect.fnUntraced(function* (handle: Invocation.Handle) {
      yield* readable
      if (
        handle.parentTaskId !== execution.taskId ||
        handle.conversationId !== execution.conversationId
      )
        return yield* error('conflict', 'The handle belongs to another parent')
      const index = yield* execution.snapshot(calls, target(execution.taskId))
      const saved =
        Option.isSome(index) && Object.hasOwn(index.value.value.calls, handle.key)
          ? index.value.value.calls[handle.key]
          : undefined
      if (saved === undefined || !sameHandle(saved.handle, handle))
        return yield* error('conflict', 'The handle is absent from its parent index')
    })
    const result = Effect.fnUntraced(function* (handle: Invocation.Handle) {
      yield* validateHandle(handle)
      yield* protection(inspectFormat)
      const saved = yield* execution.snapshot(results, target(execution.taskId, handle.taskId))
      yield* readable
      return Option.isSome(saved)
        ? Option.fromUndefinedOr(saved.value.value.receipt)
        : Option.none()
    })
    const wait = Effect.fnUntraced(function* (handle: Invocation.Handle) {
      yield* validateHandle(handle)
      const outcome = yield* execution.wait({ id: handle.taskId, definition })
      const saved = yield* result(handle)
      if (Option.isSome(saved)) return saved.value
      if (outcome.status === 'completed')
        return yield* error('missingResult', 'A completed child has no parent result receipt')
      const status = outcome.status === 'failed' ? 'faulted' : outcome.status
      return {
        parentTaskId: handle.parentTaskId,
        key: handle.key,
        taskId: handle.taskId,
        callId: handle.callId,
        name: handle.name,
        attempt: Usage.AttemptId.make(0),
        status,
        result: {
          valueKind: 'structured' as const,
          isError: true,
          diagnostics: [
            {
              severity: 'error' as const,
              message: `Tool ended ${outcome.status} without a stored result`,
            },
          ],
        },
      } satisfies Invocation.WireReceipt
    })
    const access: Invocation.Access = {
      check,
      admit: (tx, request) => admitOwned(tx, request),
      result,
      wait,
      withTool: (declaration, use, third) =>
        Effect.gen(function* () {
          yield* check
          const ownedOptions =
            third?.options === undefined
              ? undefined
              : yield* Value.copy(Invocation.OptionsSchema, third.options)
          const agent = yield* snapshotAgent
          const selected = Inventory.inspect(
            lease.snapshot,
            declaration.name,
            'tools',
            selection(agent, declaration.name, ownedOptions?.binding),
          )
          if (selected._tag === 'denied') {
            const receipt = deny(
              yield* key(ownedOptions),
              declaration.name,
              selected.denial.message,
            )
            if (third?.onDenied === undefined)
              return yield* error('denied', selected.denial.message)
            const value = yield* Effect.suspend(() => third.onDenied!(receipt))
            yield* check
            return value
          }
          const item = yield* lease.resolveTool(declaration.name, {
            binding: selected.registration,
            version: selected.version,
          })
          if (
            item.declaration.parametersSchema !== declaration.parametersSchema ||
            item.declaration.successSchema !== declaration.successSchema ||
            item.declaration.failureSchema !== declaration.failureSchema ||
            Tool.hasResultChannels(item.declaration) !== Tool.hasResultChannels(declaration) ||
            Tool.hasStructuredOutput(item.declaration) !== Tool.hasStructuredOutput(declaration)
          )
            return yield* error(
              'incompatibleContract',
              'Typed invocation requires the selected outer schema tokens',
            )
          return yield* lease.withTool(
            declaration.name,
            () =>
              check.pipe(
                Effect.andThen(Effect.suspend(use)),
                Effect.tap(() => check),
                Effect.provideService(SelectedOwner, {
                  id: item.owner.id,
                  version: item.owner.version,
                }),
              ),
            { binding: item.owner.id, version: item.owner.version },
          )
        }),
      invokeTool: (name, args, options) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            yield* check
            const slot = yield* key(options)
            const request = yield* ownRequest({
              name,
              arguments: args,
              ...(options === undefined ? {} : { options }),
            })
            const epochs = epoch === root ? [root] : [root, epoch]
            for (const current of epochs)
              if (current.admissions++ === 0) current.drained = Deferred.makeUnsafe<void>()
            const admission = yield* execution
              .commit((tx) => admitOwned(tx, request, slot))
              .pipe(
                Effect.ensuring(
                  Effect.gen(function* () {
                    for (const current of epochs)
                      if (--current.admissions === 0)
                        yield* Deferred.succeed(current.drained, undefined)
                  }),
                ),
              )
            return admission._tag === 'denied'
              ? admission.receipt
              : yield* restore(wait(admission.handle))
          }),
        ),
    }
    borrowers.set(access, (gate) =>
      makeAccess(
        { active: true, admissions: 0, drained: Deferred.makeUnsafe<void>(), children: new Set() },
        gate,
      ),
    )
    closers.set(access, (cancel) =>
      Effect.gen(function* () {
        epoch.active = false
        if (epoch.admissions !== 0) yield* Deferred.await(epoch.drained)
        if (!cancel || (epoch === root && joined)) return
        const handles =
          epoch === root && marked
            ? (yield* execution.snapshot(calls, target(execution.taskId))).pipe(
                Option.map((index) => Object.values(index.value.calls).map((slot) => slot.handle)),
                Option.getOrElse(() => []),
              )
            : yield* execution.commit((tx) =>
                markChildren(tx, epoch === root ? undefined : epoch.children),
              )
        if (epoch === root) marked = true
        yield* Effect.forEach(
          handles,
          (handle) => execution.wait({ id: handle.taskId, definition }),
          { concurrency: 'unbounded', discard: true },
        )
        if (epoch === root) joined = true
      }),
    )
    return access
  }
  const access = makeAccess(root, Effect.void)
  stagers.set(access, (tx) =>
    Effect.gen(function* () {
      root.active = false
      yield* markChildren(tx)
      marked = true
    }),
  )
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      root.active = false
    }),
  )
  return access
})

/** Each handler owns its own epoch and child drain; sibling borrows remain valid. */
export const borrow = (
  access: Invocation.Access,
  gate: Effect.Effect<void, ExecutionError>,
): Invocation.Access => {
  const makeBorrow = borrowers.get(access)
  if (makeBorrow !== undefined) return makeBorrow(gate)
  return {
    check: gate.pipe(Effect.andThen(access.check)),
    admit: (tx, request) =>
      gate.pipe(
        Effect.andThen(access.admit(tx, request)),
        Effect.tap(() => gate),
      ),
    result: (handle) =>
      gate.pipe(
        Effect.andThen(access.result(handle)),
        Effect.tap(() => gate),
      ),
    wait: (handle) =>
      gate.pipe(
        Effect.andThen(access.wait(handle)),
        Effect.tap(() => gate),
      ),
    withTool: (declaration, use, third) =>
      gate.pipe(
        Effect.andThen(access.withTool(declaration, use, third)),
        Effect.tap(() => gate),
      ),
    invokeTool: (name, args, options) =>
      gate.pipe(
        Effect.andThen(access.invokeTool(name, args, options)),
        Effect.tap(() => gate),
      ),
  }
}
