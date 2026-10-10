/** Local durable execution. Applications access it through the separate Harness client. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Agent from './Agent.js'
import type * as ConversationInitializer from './ConversationInitializer.js'
import type * as Extension from './Extension.js'
import { HarnessBackend, type HarnessBackendService } from './HarnessBackend.js'
import type { HarnessError } from './HarnessError.js'
import type * as Hook from './Hook.js'
import type { HookExecution } from './HookExecution.js'
import type * as Model from './Model.js'
import type * as Session from './Session.js'
import type { Storage } from './Storage.js'
import type * as Tool from './Tool.js'
import type { ToolExecution } from './ToolExecution.js'
import type * as Toolkit from './Toolkit.js'
import * as HarnessRuntimeImpl from './internal/HarnessRuntime.js'

export interface HarnessRuntimeService {
  readonly session: Session.Session
  readonly backend: HarnessBackendService
}
export class HarnessRuntime extends Context.Service<HarnessRuntime, HarnessRuntimeService>()(
  'effect-harness/HarnessRuntime',
) {}
export interface Options<
  Tools extends Record<string, Tool.Any> = {},
  Hooks extends ReadonlyArray<Hook.Any> = readonly [],
  Extensions extends ReadonlyArray<Extension.Any> = readonly [],
  Models extends ReadonlyArray<Model.Any> = readonly [],
  Initializers extends ReadonlyArray<ConversationInitializer.Any> = readonly [],
> {
  /** Transactional callbacks after built-in agent initialization, including raw Session creation. */
  readonly initializers?: Initializers
  readonly tools?: Toolkit.Toolkit<Tools>
  readonly hooks?: Hooks
  readonly extensions?: Extensions
  readonly models?: Models
  readonly agent?: Agent.State
  readonly maxTurns?: number
}
export type Requirements<
  T extends Record<string, Tool.Any>,
  H extends ReadonlyArray<Hook.Any>,
  X extends ReadonlyArray<Extension.Any>,
  M extends ReadonlyArray<Model.Any>,
  I extends ReadonlyArray<ConversationInitializer.Any> = readonly [],
> =
  | Storage
  | ConversationInitializer.Requirements<I[number]>
  | Toolkit.HandlersFor<T>
  | Toolkit.ContextServices<T>
  | Exclude<
      | Hook.Requirements<H[number]>
      | Extension.Requirements<X[number]>
      | Model.Requirements<M[number]>,
      ToolExecution | HookExecution | Scope.Scope
    >
  | ([M[number]] extends [never] ? LanguageModel.LanguageModel : never)

export const make = <
  const T extends Record<string, Tool.Any> = {},
  const H extends ReadonlyArray<Hook.Any> = readonly [],
  const X extends ReadonlyArray<Extension.Any> = readonly [],
  const M extends ReadonlyArray<Model.Any> = readonly [],
  const I extends ReadonlyArray<ConversationInitializer.Any> = readonly [],
>(
  options: Options<T, H, X, M, I> = {},
): Effect.Effect<HarnessRuntimeService, HarnessError, Scope.Scope | Requirements<T, H, X, M, I>> =>
  HarnessRuntimeImpl.make(options)

export const layer = <
  const T extends Record<string, Tool.Any> = {},
  const H extends ReadonlyArray<Hook.Any> = readonly [],
  const X extends ReadonlyArray<Extension.Any> = readonly [],
  const M extends ReadonlyArray<Model.Any> = readonly [],
  const I extends ReadonlyArray<ConversationInitializer.Any> = readonly [],
>(
  options: Options<T, H, X, M, I> = {},
): Layer.Layer<HarnessRuntime | HarnessBackend, HarnessError, Requirements<T, H, X, M, I>> =>
  Layer.effectContext(
    make(options).pipe(
      Effect.map((runtime) =>
        Context.make(HarnessRuntime, runtime).pipe(Context.add(HarnessBackend, runtime.backend)),
      ),
    ),
  )
