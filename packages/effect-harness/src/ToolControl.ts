/** Validated model-call controls and a deterministic, complete tool-round barrier. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'

export const ControlSchema = Schema.Struct({
  terminate: Schema.optionalKey(Schema.Literal(true)),
  handoff: Schema.optionalKey(Schema.String),
  addTools: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
})
export type Control = typeof ControlSchema.Type

export class InvalidControl extends Schema.TaggedError<InvalidControl>()('InvalidToolControl', {
  reason: Schema.Literals(['invalidShape', 'unavailableTool', 'invalidRound']),
  message: Schema.String,
}) {}

const validateControl = (
  value: unknown,
  availableTools: ReadonlyArray<string>,
): Result.Result<Control, InvalidControl> => {
  const decoded = Schema.decodeUnknownResult(ControlSchema)(value, { onExcessProperty: 'error' })
  if (Result.isFailure(decoded))
    return Result.fail(
      new InvalidControl({
        reason: 'invalidShape',
        message: 'Tool controls do not match the declared schema',
      }),
    )
  const unavailable = decoded.success.addTools?.find((name) => !availableTools.includes(name))
  if (unavailable !== undefined)
    return Result.fail(
      new InvalidControl({
        reason: 'unavailableTool',
        message: `Tool ${unavailable} is unavailable for model selection`,
      }),
    )
  return Result.succeed(decoded.success)
}

/** Validate again after hooks. Only an explicit inventory can admit added tool names. */
export const validate = Effect.fn('ToolControl.validate')(
  (value: unknown, availableTools: ReadonlyArray<string>) =>
    Effect.fromResult(validateControl(value, availableTools)),
)

export const ReceiptSchema = Schema.Struct({
  callId: Schema.NonEmptyString,
  caller: Schema.Literals(['model', 'tools']),
  status: Schema.Literals(['succeeded', 'failed', 'cancelled', 'denied']),
  control: Schema.optionalKey(ControlSchema),
})
export type Receipt = typeof ReceiptSchema.Type
export const BarrierSchema = Schema.Struct({
  terminate: Schema.Boolean,
  addTools: Schema.Array(Schema.NonEmptyString),
  handoff: Schema.optionalKey(Schema.String),
})
export type Barrier = typeof BarrierSchema.Type

/**
 * Reduce only a fully settled round. Original call order owns union order and last handoff;
 * receipt completion order is irrelevant. Denied, failed, cancelled or nested slots veto
 * termination. An empty round never terminates. This does not activate run-loop controls.
 */
export const reduceRound = (
  callOrder: ReadonlyArray<string>,
  receipts: ReadonlyArray<Receipt>,
  availableTools: ReadonlyArray<string>,
): Result.Result<Barrier, InvalidControl> => {
  const decoded = Schema.decodeResult(Schema.Array(ReceiptSchema))(receipts, {
    onExcessProperty: 'error',
  })
  if (Result.isFailure(decoded))
    return Result.fail(
      new InvalidControl({
        reason: 'invalidRound',
        message: 'Final receipts must match the round receipt schema',
      }),
    )
  const byId = new Map(decoded.success.map((receipt) => [receipt.callId, receipt]))
  if (
    new Set(callOrder).size !== callOrder.length ||
    byId.size !== receipts.length ||
    byId.size !== callOrder.length ||
    callOrder.some((id) => !byId.has(id))
  ) {
    return Result.fail(
      new InvalidControl({
        reason: 'invalidRound',
        message: 'Every original call requires exactly one final receipt',
      }),
    )
  }
  let terminate = callOrder.length > 0
  let handoff: string | undefined
  const addTools = new Set<string>()
  for (const id of callOrder) {
    const receipt = byId.get(id)!
    const checked =
      receipt.control === undefined
        ? Result.succeed<Control>({})
        : validateControl(receipt.control, availableTools)
    if (Result.isFailure(checked)) return Result.fail(checked.failure)
    const control = checked.success
    const eligible = receipt.caller === 'model' && receipt.status === 'succeeded'
    terminate = terminate && eligible && control?.terminate === true
    if (!eligible) continue
    for (const name of control?.addTools ?? []) addTools.add(name)
    if (control?.handoff !== undefined) handoff = control.handoff
  }
  return Result.succeed({
    terminate,
    addTools: [...addTools],
    ...(handoff === undefined ? {} : { handoff }),
  })
}

/** Effect adapter for the pure complete-round reducer; it performs no runtime activation. */
export const reduce = Effect.fn('ToolControl.reduce')(
  (
    callOrder: ReadonlyArray<string>,
    receipts: ReadonlyArray<Receipt>,
    availableTools: ReadonlyArray<string>,
  ) => Effect.fromResult(reduceRound(callOrder, receipts, availableTools)),
)
