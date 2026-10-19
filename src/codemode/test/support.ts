/**
 * Test support: a Harness with code mode and test tools over the `Faux` model, which a test can
 * close and reopen over the same storage, as a restart.
 *
 * - `echo` returns `echo <text>` and counts its runs by text;
 * - `hold` (replay-safe) waits while `world.block` is set, signalling `reached`, then returns
 *   `held <id>`;
 * - `slow` returns `slow <id>` once `world.release(id)` was called;
 * - `value` (structured) returns `{ doubled }`, failing with `{ reason }` for a negative `id`.
 */
import { BunServices } from '@effect/platform-bun'
import type { CodemodeRun, CodemodeSandboxBase } from '@earendil-works/pi-codemode'
import { CodemodeSandbox } from '@earendil-works/pi-codemode'
import { Deferred, Effect, Exit, Layer, Schema, Scope } from 'effect'
import type { Faux } from '../../model/index.ts'
import type { Hook } from '../../task/index.ts'
import type { Service as StorageService } from '../../storage/Storage.ts'
import type { ConversationId } from '../../storage/schema/Ids.ts'
import { Tool, Toolkit, ToolResult, ToolTask } from '../../tool/index.ts'
import * as Extension from '../../harness/Extension.ts'
import type * as Harness from '../../harness/Harness.ts'
import { entriesOf, open } from '../../harness/test/generationSupport.ts'
import { Codemode, CodemodeHandlers } from '../index.ts'

const Params = Schema.Struct({
  text: Schema.optionalKey(Schema.String),
  id: Schema.optionalKey(Schema.Finite),
})
const declare = <const Name extends string>(name: Name, replay: 'safe' | 'unsafe' = 'unsafe') =>
  Tool.make(name, {
    description: `The ${name} tool`,
    parameters: Params,
    success: ToolResult.Output,
    failure: ToolResult.Output,
    replay,
  })
const Value = Tool.make('value', {
  description: 'Double an id',
  parameters: Params,
  success: Schema.Struct({ doubled: Schema.Finite }),
  failure: Schema.Struct({ reason: Schema.String }),
})
const Picture = Tool.make('picture', {
  description: 'A picture',
  parameters: Schema.Struct({ caption: Schema.Boolean }),
  success: ToolResult.Output,
})
const Later = Tool.make('later', {
  description: 'A deferred tool',
  parameters: Schema.Struct({}),
  success: ToolResult.Output,
  deferred: true,
})
const tools = Toolkit.make(
  declare('echo'),
  declare('hold', 'safe'),
  declare('slow'),
  declare('fails'),
  declare('mcp__github__list_issues'),
  Value,
  Picture,
  Later,
)

export const PNG =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=='

/** What the test tools share across restarts. */
export interface World {
  readonly echo: Map<string, number>
  block: boolean
  reached: Deferred.Deferred<void>
  holdRuns: number
  readonly gates: Map<number, Deferred.Deferred<void>>
  readonly finished: Array<number>
  sandbox: CodemodeSandboxBase
  starts: number
  onStart?: (run: CodemodeRun) => void
}

const gate = (world: World, id: number) =>
  Effect.gen(function* () {
    const existing = world.gates.get(id)
    if (existing !== undefined) return existing
    const made = yield* Deferred.make<void>()
    world.gates.set(id, made)
    return made
  })

export const release = (world: World, id: number) =>
  Effect.flatMap(gate(world, id), (each) => Deferred.done(each, Exit.void))

const handlers = (world: World) =>
  tools.toLayer({
    echo: ({ text = '' }) =>
      Effect.sync(() => {
        world.echo.set(text, (world.echo.get(text) ?? 0) + 1)
        return ToolResult.text(`echo ${text}`)
      }),
    hold: ({ id }) =>
      Effect.gen(function* () {
        world.holdRuns += 1
        if (world.block) {
          yield* Deferred.done(world.reached, Exit.void)
          return yield* Effect.never
        }
        return ToolResult.text(`held ${id ?? ''}`)
      }),
    slow: ({ id = 0 }) =>
      Effect.gen(function* () {
        yield* Deferred.await(yield* gate(world, id))
        world.finished.push(id)
        return ToolResult.text(`slow ${id}`)
      }),
    fails: () => Effect.fail(ToolResult.text('permission denied')),
    mcp__github__list_issues: () => Effect.succeed(ToolResult.text('')),
    value: ({ id = 0 }) =>
      id < 0 ? Effect.fail({ reason: 'negative' }) : Effect.succeed({ doubled: id * 2 }),
    picture: ({ caption }) =>
      Effect.succeed(
        ToolResult.output([
          ...(caption ? [{ type: 'text' as const, text: 'a cat' }] : []),
          { type: 'image', data: PNG, mimeType: 'image/png' },
        ]),
      ),
    later: () => Effect.succeed(ToolResult.text('later ran')),
  })

/** A sandbox that counts the VMs it starts, in the test's scope. */
export const world = Effect.gen(function* () {
  const sandbox = yield* Effect.acquireRelease(
    Effect.sync(() => new CodemodeSandbox({ memoryLimitBytes: 64 * 1024 * 1024 })),
    (made) => Effect.promise(() => made.close()),
  )
  const made: World = {
    echo: new Map(),
    block: true,
    reached: yield* Deferred.make<void>(),
    holdRuns: 0,
    gates: new Map(),
    finished: [],
    sandbox,
    starts: 0,
  }
  const start = sandbox.start.bind(sandbox)
  sandbox.start = (code, options) => {
    made.starts += 1
    const run = start(code, options)
    made.onStart?.(run)
    return run
  }
  return made
})

export interface Options extends CodemodeHandlers.Options, Codemode.Options {
  /** Hooks of an extension of their own. */
  readonly hooks?: ReadonlyArray<Hook.Registration>
}

export interface Opened {
  readonly harness: Harness.Service
  readonly faux: Faux.Faux
  readonly close: Effect.Effect<void>
}

/** Open a Harness with code mode and the test tools, closed by `close`. */
export const openChat = (storage: StorageService, current: World, options: Options = {}) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const opened = yield* Scope.provide(
      Effect.gen(function* () {
        const made = yield* open(storage, { agent: () => ({ tools: [] }) })
        const testTools = yield* Layer.build(handlers(current))
        yield* Effect.orDie(
          made.extensions
            .install(Extension.make('tools', { toolkit: tools }))
            .pipe(Effect.provideContext(testTools)),
        )
        const codemode = yield* Layer.build(
          CodemodeHandlers.layer(options).pipe(
            Layer.provide(Layer.succeed(CodemodeHandlers.Sandbox, current.sandbox)),
            Layer.provide(BunServices.layer),
          ),
        )
        yield* Effect.orDie(
          made.extensions
            .install(Codemode.extension(options))
            .pipe(Effect.provideContext(codemode)),
        )
        if (options.hooks !== undefined)
          yield* Effect.orDie(
            made.extensions.install(Extension.make('hooks', { hooks: options.hooks })),
          )
        return made
      }),
      scope,
    )
    const result: Opened = {
      harness: opened.harness,
      faux: opened.faux,
      close: Scope.close(scope, Exit.void),
    }
    return result
  })

/** The code mode call's result: its text without the header, and whether it is an error. */
export const resultOf = (harness: Harness.Service, conversationId: ConversationId) =>
  Effect.map(entriesOf(harness, conversationId), (entries) => {
    const entry = entries.find((each) => each.kind === 'pi.tool-result')
    const message = entry?.model?.[0] ?? {}
    const content: unknown = message['content']
    const blocks = Array.isArray(content) ? content : []
    const text = blocks
      .map((block: { readonly text?: string }) => block.text ?? '')
      .join('')
      .replace(/^Script (completed|failed)\nWall time [\d.]+ seconds\nOutput:\n/u, '')
    return { text, isError: message['isError'] === true, details: message['details'] }
  })

export const isNested = Schema.is(ToolTask.NestedInput)

/** The tool tasks, by kind of call. */
export const toolTasks = (harness: Harness.Service) =>
  Effect.map(
    Effect.orDie(harness.session.commit((tx) => tx.scanTasks({ kind: ToolTask.name }, 100))),
    (page) => ({
      model: page.items.filter((task) => !isNested(task.input)),
      nested: page.items.filter((task) => isNested(task.input)),
    }),
  )

/** The code mode call's task. */
export const scriptTask = (harness: Harness.Service) =>
  Effect.flatMap(toolTasks(harness), ({ model }) =>
    Effect.orDie(Effect.fromNullishOr(model[0]?.id)),
  )
