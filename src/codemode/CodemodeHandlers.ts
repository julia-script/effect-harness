/**
 * The implementation of code mode, where the loop runs: the handler over a `Sandbox`, and layers
 * of the sandboxes `@earendil-works/pi-codemode` offers. The handler writes the result's files,
 * the full output and images, with `FileSystem` and `Path`.
 */
import {
  CodemodeSandbox,
  InlineCodemodeSandbox,
  loadQuickJSWasm,
} from '@earendil-works/pi-codemode'
import type {
  CodemodeSandboxBase,
  CodemodeSandboxOptions,
  InlineCodemodeSandboxOptions,
} from '@earendil-works/pi-codemode'
import { Context, Effect, Layer } from 'effect'
import { toolkit } from './Codemode.ts'
import type { Namespace } from './Codemode.ts'
import { execute } from './internal/execute.ts'
import { files } from './internal/files.ts'
import type { Global as GlobalType } from './internal/launch.ts'

/** Runs the scripts. */
export class Sandbox extends Context.Service<Sandbox, CodemodeSandboxBase>()(
  'effect-harness/codemode/CodemodeHandlers/Sandbox',
) {}

const scoped = (make: () => CodemodeSandboxBase) =>
  Layer.effect(
    Sandbox,
    Effect.acquireRelease(Effect.sync(make), (sandbox) => Effect.promise(() => sandbox.close())),
  )

/** Each script's VM in a worker thread, on Node or Bun; closed with the layer. */
export const layerSandbox = (options: CodemodeSandboxOptions = {}) =>
  scoped(() => new CodemodeSandbox(options))

/**
 * Each script's VM on the calling thread, for runtimes without worker threads; a script that
 * computes blocks the thread until it awaits a call. Default wasm: the installed `quickjs-wasi`.
 */
export const layerInlineSandbox = (options: Partial<InlineCodemodeSandboxOptions> = {}) =>
  scoped(() => new InlineCodemodeSandbox({ ...options, wasm: options.wasm ?? loadQuickJSWasm() }))

/** A function scripts call as a top-level global; its outcome is journaled. */
export type Global = GlobalType

export interface Options {
  /** The same namespaces as the extension's, for `describeNamespace()` and `searchTools()`. */
  readonly namespaces?: ReadonlyArray<Namespace>
  /** Host functions scripts call as globals. */
  readonly globals?: ReadonlyArray<Global>
}

/** The implementation of `Codemode`, over `Sandbox`, `FileSystem` and `Path`. */
export const layer = (options: Options = {}) =>
  toolkit.toLayer(
    Effect.gen(function* () {
      const deps = {
        sandbox: yield* Sandbox,
        namespaces: options.namespaces ?? [],
        globals: options.globals ?? [],
        files: yield* files,
      }
      return toolkit.of({ codemode: execute(deps) })
    }),
  )
