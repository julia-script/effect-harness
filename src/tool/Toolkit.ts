/**
 * Toolkits: a set of declared tools, and their implementations. `make` groups declarations, which
 * the client and the loop can both import; `toLayer` implements them, in the loop only, with one
 * handler per tool.
 *
 * A handler gets the decoded arguments and a context with the call's ID and the tool's decoded
 * config, and returns the tool's success or fails with its declared failure. Anything else it
 * does, a defect or an interruption, is not a result: the tool task reports a defect as a broken
 * tool. Handlers need no services of their own: build them in an Effect that reads the services
 * they use, as `toLayer` accepts.
 */
import { Context, Effect, Layer } from 'effect'
import type { Scope } from 'effect'
import { implement } from './internal/runner.ts'
import type * as runner from './internal/runner.ts'
import type * as Tool from './Tool.ts'

/** What running a call came to, with the tool's values encoded. */
export type Outcome = runner.Outcome

/**
 * A tool with its handler behind a signature of encoded values, as the tool task runs it by name.
 * Extensions wrap runners to decorate a tool another extension implements.
 */
export type Runner = runner.Runner

/** What a handler gets besides the arguments. */
export interface HandlerContext<Config> {
  /** The call's ID in its assistant message. */
  readonly callId: string
  readonly config: Config
}

/** The implementation of `T`. */
export type Handler<T extends Tool.Any> = (
  params: Tool.Parameters<T>,
  context: HandlerContext<Tool.Config<T>>,
) => Effect.Effect<Tool.Success<T>, Tool.Failure<T>>

/** One handler per tool, by name. */
export type Handlers<Tools extends Record<string, Tool.Any>> = {
  readonly [Name in keyof Tools]: Handler<Tools[Name]>
}

/** The service a toolkit layer provides for the tool `Name`. */
export interface Implementation<Name extends string> {
  readonly name: Name
  readonly implementation: unique symbol
}

/** What implementing the tools `Names` needs: their implementations, or nothing for no tools. */
export type Requires<Name extends string> = [Name] extends [never] ? never : Implementation<Name>

/** The tools of a list, by name. */
export type ByName<Tools extends ReadonlyArray<Tool.Any>> = {
  readonly [T in Tools[number] as T['name']]: T
}

/** The names of a toolkit's tools. */
export type Names<Tools extends Record<string, Tool.Any>> = Tools[keyof Tools]['name']

export interface Toolkit<Tools extends Record<string, Tool.Any>> {
  readonly tools: Tools
  /** Type `handlers` against the toolkit. */
  readonly of: (handlers: Handlers<Tools>) => Handlers<Tools>
  /** Implement every tool, with handlers or an Effect that builds them. */
  readonly toLayer: <E = never, R = never>(
    build: Handlers<Tools> | Effect.Effect<Handlers<Tools>, E, R>,
  ) => Layer.Layer<Implementation<Names<Tools>>, E, Exclude<R, Scope.Scope>>
}

/** The key of tool `name`'s implementation. */
export const implementation = (name: string) =>
  Context.Service<Implementation<string>, Runner>(`effect-harness/tool/Toolkit/${name}`)

/** The implementations `handlers` give the tools of `tools`. */
const implementations = <Tools extends Record<string, Tool.Any>>(
  tools: Tools,
  handlers: Handlers<Tools>,
): Context.Context<Implementation<string>> => {
  const runners = new Map<string, Runner>()
  for (const name in tools) {
    if (Object.hasOwn(tools, name)) {
      const runner = implement(tools[name], handlers[name])
      runners.set(implementation(runner.tool.name).key, runner)
    }
  }
  return Context.makeUnsafe(runners)
}

/** A toolkit of `tools`. */
export function make<const Tools extends ReadonlyArray<Tool.Any>>(
  ...tools: Tools
): Toolkit<ByName<Tools>>
export function make(...tools: ReadonlyArray<Tool.Any>): Toolkit<Record<string, Tool.Any>> {
  const byName: Record<string, Tool.Any> = {}
  for (const tool of tools) byName[tool.name] = tool
  const toolkit: Toolkit<Record<string, Tool.Any>> = {
    tools: byName,
    of: (handlers) => handlers,
    toLayer: (build) =>
      Layer.effectContext(
        Effect.map(Effect.isEffect(build) ? build : Effect.succeed(build), (handlers) =>
          implementations(byName, handlers),
        ),
      ),
  }
  return toolkit
}
