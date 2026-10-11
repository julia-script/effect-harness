import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Record from '../../src/Record.js'
import * as Registry from '../../src/Registry.js'
import * as Tool from '../../src/Tool.js'
import * as Toolkit from '../../src/Toolkit.js'
import { ToolExecution } from '../../src/ToolExecution.js'

// No session operations are part of these isolated registry calls.
export const execution = Layer.succeed(ToolExecution, {
  conversationId: Schema.decodeSync(Record.ConversationId)(1),
  taskId: Schema.decodeSync(Record.TaskId)(1),
  callId: 'registry-test',
  snapshot: () => Effect.succeedNone,
  watch: () => Stream.empty,
  commit: () => Effect.die('unexpected session commit'),
  output: () => Effect.void,
  details: () => Effect.void,
  diagnostic: () => Effect.void,
})

export class Label extends Context.Service<Label, string>()('test/registry/Label') {}
export const declaration = Tool.make('same', {
  success: Schema.String,
  failure: Schema.String,
}).addDependency(Label)
export const toolkit = Toolkit.make(declaration)
export const handlers = toolkit.toLayer({ same: () => Label })
export const registration = (id: string, label: string, version = 1, enabled = true) =>
  Registry.provide(
    Registry.toolkit({ id, version, enabled }, toolkit),
    handlers.pipe(Layer.provideMerge(Layer.succeed(Label, label))),
  )
export const call = { id: 'call', name: 'same', arguments: {} } as const
