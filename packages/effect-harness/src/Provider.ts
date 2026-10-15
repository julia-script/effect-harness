/** Provider definitions describe protocols once; Providers owns user-configured instances. */
import type * as Effect from 'effect/Effect'
import type * as Schema from 'effect/Schema'
import type * as Model from './Model.js'
import type * as ModelInfo from './ModelInfo.js'
import type * as ModelCatalog from './ModelCatalog.js'
import type * as Credential from './provider-auth/Credential.js'
import type { AuthError } from './provider-auth/AuthError.js'

/** Persistence codecs are deterministic and require no ambient services. */
export type Codec = Schema.ConstraintCodec<unknown, unknown, never, never>
export interface Context<C, S> {
  readonly id: string
  readonly revision: string
  readonly config: C
  readonly credential?: Credential.Binding
  /** Resolves current secrets on demand; values never enter public instance records. */
  readonly secrets: Effect.Effect<S | undefined, AuthError>
  /** Rejects stale instances after update, removal, or credential replacement. */
  readonly check: Effect.Effect<void, AuthError>
}
export interface Resolved {
  readonly info: ModelInfo.ModelInfo
  readonly options: ModelInfo.Options
}
export interface Any {
  readonly version: number
  readonly config: Codec
  readonly secrets: Codec
  readonly models: {
    readonly list: (
      context: never,
      input: ModelCatalog.ListOptions,
    ) => Effect.Effect<ModelCatalog.Page, unknown, unknown>
    readonly get: (
      context: never,
      modelId: string,
    ) => Effect.Effect<ModelInfo.ModelInfo, unknown, unknown>
  }
  readonly createModel: (
    context: never,
    resolved: Resolved,
  ) => Effect.Effect<Model.Any, unknown, unknown>
}
export interface Definition<
  C extends Codec,
  S extends Codec,
  LE,
  LR,
  GE,
  GR,
  M extends Model.Any,
  CE,
  CR,
> extends Any {
  readonly config: C
  readonly secrets: S
  readonly models: {
    readonly list: (
      context: Context<C['Type'], S['Type']>,
      input: ModelCatalog.ListOptions,
    ) => Effect.Effect<ModelCatalog.Page, LE, LR>
    readonly get: (
      context: Context<C['Type'], S['Type']>,
      modelId: string,
    ) => Effect.Effect<ModelInfo.ModelInfo, GE, GR>
  }
  readonly createModel: (
    context: Context<C['Type'], S['Type']>,
    resolved: Resolved,
  ) => Effect.Effect<M, CE, CR>
}
export const define = <
  C extends Codec,
  S extends Codec,
  LE,
  LR,
  GE,
  GR,
  M extends Model.Any,
  CE,
  CR,
>(
  input: Definition<C, S, LE, LR, GE, GR, M, CE, CR>,
): Definition<C, S, LE, LR, GE, GR, M, CE, CR> => ({ ...input, models: { ...input.models } })
export const definitions = <const D extends Readonly<Record<string, Any>>>(input: D): D => input
export type ListError<P extends Any> = Effect.Error<ReturnType<P['models']['list']>>
export type ListServices<P extends Any> = Effect.Services<ReturnType<P['models']['list']>>
export type GetError<P extends Any> = Effect.Error<ReturnType<P['models']['get']>>
export type GetServices<P extends Any> = Effect.Services<ReturnType<P['models']['get']>>
export type ModelOf<P extends Any> = Effect.Success<ReturnType<P['createModel']>>
export type CreateError<P extends Any> = Effect.Error<ReturnType<P['createModel']>>
export type CreateServices<P extends Any> = Effect.Services<ReturnType<P['createModel']>>
