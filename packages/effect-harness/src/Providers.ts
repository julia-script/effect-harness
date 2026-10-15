/** Dynamic provider instances with schema-checked public configuration and referenced secrets. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Provider from './Provider.js'
import * as ModelCatalog from './ModelCatalog.js'
import * as ModelInfo from './ModelInfo.js'
import * as Instance from './ProviderInstance.js'
import { ProviderStore } from './ProviderStore.js'
import { ProviderError } from './ProviderError.js'
import { ModelCatalogError } from './ModelCatalogError.js'
import * as Credentials from './provider-auth/CredentialStore.js'
import type * as Credential from './provider-auth/Credential.js'
import { AuthError } from './provider-auth/AuthError.js'
import * as ProviderModel from './internal/ProviderModel.js'

type Definitions = Readonly<Record<string, Provider.Any>>
type Key<D> = keyof D & string
export type InstanceOf<D extends Definitions, K extends Key<D> = Key<D>> = {
  [P in K]: Instance.ProviderInstance<P, D[P]['config']['Type']>
}[K]
export type Add<D extends Definitions> = {
  [P in Key<D>]: {
    readonly provider: P
    readonly label: string
    readonly config: D[P]['config']['Type']
    readonly secrets?: D[P]['secrets']['Type']
  }
}[Key<D>]
export interface Update<P extends Provider.Any> {
  readonly label?: string
  /** Complete replacement, validated and encoded by the definition. */
  readonly config?: P['config']['Type']
  /** Omission preserves credentials; null explicitly removes them. */
  readonly secrets?: P['secrets']['Type'] | null
  readonly expectedRevision?: string
}
type Failure = ProviderError | AuthError
export interface Service<D extends Definitions> {
  readonly add: <const A extends Add<D>>(
    input: A,
  ) => Effect.Effect<InstanceOf<D, A['provider']>, Failure>
  readonly get: (id: string) => Effect.Effect<InstanceOf<D>, Failure>
  readonly list: Effect.Effect<ReadonlyArray<InstanceOf<D>>, Failure>
  readonly update: <K extends Key<D>>(
    id: Instance.Id<K>,
    input: Update<D[NoInfer<K>]>,
  ) => Effect.Effect<InstanceOf<D, K>, Failure>
  readonly remove: (id: string) => Effect.Effect<void, Failure>
  readonly models: {
    readonly list: <K extends Key<D>>(
      input: ModelCatalog.ListOptions & { readonly instanceId: Instance.Id<K> },
    ) => Effect.Effect<
      ModelCatalog.Page,
      Failure | ModelCatalogError | Provider.ListError<D[K]>,
      Exclude<Provider.ListServices<D[K]>, Credentials.CredentialStore>
    >
    readonly get: <K extends Key<D>>(input: {
      readonly instanceId: Instance.Id<K>
      readonly modelId: string
    }) => Effect.Effect<
      ModelInfo.ModelInfo,
      Failure | ModelCatalogError | Provider.GetError<D[K]>,
      Exclude<Provider.GetServices<D[K]>, Credentials.CredentialStore>
    >
    readonly resolve: <K extends Key<D>>(input: {
      readonly instanceId: Instance.Id<K>
      readonly modelId: string
      readonly options?: ModelInfo.Options
    }) => Effect.Effect<
      Provider.ModelOf<D[K]>,
      Failure | ModelCatalogError | Provider.GetError<D[K]> | Provider.CreateError<D[K]>,
      Exclude<
        Provider.GetServices<D[K]> | Provider.CreateServices<D[K]>,
        Credentials.CredentialStore
      >
    >
  }
}

/** The heterogeneous definition map is erased only inside this dispatch boundary. */
type RuntimeDefinition<D extends Definitions> = Provider.Definition<
  Provider.Codec,
  Provider.Codec,
  Provider.ListError<D[Key<D>]>,
  Provider.ListServices<D[Key<D>]>,
  Provider.GetError<D[Key<D>]>,
  Provider.GetServices<D[Key<D>]>,
  import('./Model.js').Any,
  Provider.CreateError<D[Key<D>]>,
  Provider.CreateServices<D[Key<D>]>
>

export const make = Effect.fnUntraced(function* <const D extends Definitions>(options: {
  readonly definitions: D
}) {
  const store = yield* ProviderStore
  const credentials = yield* Credentials.CredentialStore
  const definitions = new Map<string, RuntimeDefinition<D>>(
    Object.entries(options.definitions).map(([id, definition]) => [
      id,
      definition as RuntimeDefinition<D>,
    ]),
  )
  for (const [id, definition] of definitions) {
    if (id.length === 0 || !Number.isSafeInteger(definition.version) || definition.version <= 0)
      return yield* new ProviderError({ reason: 'InvalidConfiguration' })
  }
  const definition = (provider: string) => {
    const found = definitions.get(provider)
    return found === undefined
      ? Effect.fail(new ProviderError({ reason: 'UnknownProvider' }))
      : Effect.succeed(found)
  }
  const encode = (schema: Provider.Codec, value: unknown) =>
    Schema.encodeEffect(Schema.toCodecJson(schema), { onExcessProperty: 'error' })(value).pipe(
      Effect.mapError(() => new ProviderError({ reason: 'InvalidConfiguration' })),
    )
  const decode = Effect.fnUntraced(function* (stored: Instance.Stored) {
    const current = yield* Schema.decodeEffect(Instance.Stored, {
      onExcessProperty: 'error',
    })(stored).pipe(Effect.mapError(() => new ProviderError({ reason: 'InvalidConfiguration' })))
    const selected = yield* definition(current.provider)
    if (selected.version !== current.version)
      return yield* new ProviderError({ reason: 'UnsupportedVersion', instanceId: current.id })
    const config = yield* Schema.decodeEffect(Schema.toCodecJson(selected.config), {
      onExcessProperty: 'error',
    })(current.config).pipe(
      Effect.mapError(
        () => new ProviderError({ reason: 'InvalidConfiguration', instanceId: current.id }),
      ),
    )
    return { ...current, config }
  })
  const read = Effect.fnUntraced(function* (id: string) {
    const stored = yield* store.read(id)
    if (stored === undefined)
      return yield* new ProviderError({ reason: 'NotFound', instanceId: id })
    if (stored.id !== id)
      return yield* new ProviderError({ reason: 'InvalidConfiguration', instanceId: id })
    return yield* decode(stored)
  })
  const removeCredential = (binding: Credential.Binding, id: string) =>
    Credentials.remove(binding).pipe(
      Effect.provideService(Credentials.CredentialStore, credentials),
      Effect.mapError(
        () => new ProviderError({ reason: 'CredentialCleanupFailed', instanceId: id }),
      ),
    )
  const mutate = (
    id: string,
    change: (
      current: Instance.Stored | undefined,
    ) => Effect.Effect<Instance.Stored | undefined, Failure>,
    secrets?: { readonly schema: Provider.Codec; readonly value: unknown } | null,
  ) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        let created: Credential.Binding | undefined
        let retired: Credential.Binding | undefined
        const result = yield* restore(
          store.modify(id, (current) =>
            Effect.gen(function* () {
              let next = yield* change(current)
              if (secrets !== undefined && next !== undefined) {
                const { credential: old, ...publicInfo } = next
                retired = old
                if (secrets === null) next = publicInfo
                else {
                  created = yield* Credentials.create(secrets.schema, secrets.value).pipe(
                    Effect.provideService(Credentials.CredentialStore, credentials),
                  )
                  next = { ...publicInfo, credential: created }
                }
              }
              if (next === undefined) retired = current?.credential
              return [next, next] as const
            }),
          ),
        ).pipe(
          // An uncertain publication may already reference the new credential. Retain both.
          Effect.catchIf(
            (error: Failure) => created !== undefined && error.reason !== 'StoreCommitUncertain',
            (error) =>
              Effect.andThen(
                created === undefined ? Effect.void : removeCredential(created, id),
                Effect.fail(error),
              ),
          ),
          Effect.onInterrupt(() =>
            Effect.gen(function* () {
              // The store finishes an acquired commit even if its caller is interrupted.
              // Reconcile the committed reference before reclaiming either credential.
              const published = yield* store.read(id)
              if (
                created !== undefined &&
                published?.credential?.credentialId !== created.credentialId
              )
                yield* removeCredential(created, id)
              if (
                retired !== undefined &&
                published?.credential?.credentialId !== retired.credentialId
              )
                yield* removeCredential(retired, id)
            }),
          ),
        )
        if (retired !== undefined) yield* removeCredential(retired, id)
        return result
      }),
    )
  const add = Effect.fnUntraced(function* (input: Add<D>) {
    const selected = yield* definition(input.provider)
    const config = yield* encode(selected.config, input.config)
    const record = yield* Schema.decodeEffect(Instance.Stored)({
      id: crypto.randomUUID(),
      provider: input.provider,
      version: selected.version,
      revision: crypto.randomUUID(),
      label: input.label,
      config,
    }).pipe(Effect.mapError(() => new ProviderError({ reason: 'InvalidConfiguration' })))
    yield* mutate(
      record.id,
      (current) =>
        current === undefined
          ? Effect.succeed(record)
          : Effect.fail(new ProviderError({ reason: 'Conflict', instanceId: record.id })),
      input.secrets === undefined ? undefined : { schema: selected.secrets, value: input.secrets },
    )
    return yield* read(record.id)
  })
  const update = Effect.fnUntraced(function* (id: string, input: Update<Provider.Any>) {
    // Resolve the codec before locking; the definition of an instance is immutable.
    const before = yield* read(id)
    const selected = yield* definition(before.provider)
    const config =
      input.config === undefined ? undefined : yield* encode(selected.config, input.config)
    let secrets: { readonly schema: Provider.Codec; readonly value: unknown } | null | undefined
    if (input.secrets === null) secrets = null
    else if (input.secrets !== undefined)
      secrets = { schema: selected.secrets, value: input.secrets }
    yield* mutate(
      id,
      (current) =>
        Effect.gen(function* () {
          if (current === undefined)
            return yield* new ProviderError({ reason: 'NotFound', instanceId: id })
          yield* decode(current)
          if (
            current.provider !== before.provider ||
            (input.expectedRevision !== undefined && input.expectedRevision !== current.revision)
          )
            return yield* new ProviderError({ reason: 'Conflict', instanceId: id })
          return yield* Schema.decodeEffect(Instance.Stored)({
            ...current,
            ...(input.label === undefined ? {} : { label: input.label }),
            ...(config === undefined ? {} : { config }),
            revision:
              config === undefined && input.secrets === undefined
                ? current.revision
                : crypto.randomUUID(),
          }).pipe(
            Effect.mapError(
              () => new ProviderError({ reason: 'InvalidConfiguration', instanceId: id }),
            ),
          )
        }),
      secrets,
    )
    return yield* read(id)
  })
  const bound = Effect.fnUntraced(function* (id: string) {
    const instance = yield* read(id)
    const selected = yield* definition(instance.provider)
    const check = Effect.gen(function* () {
      const current = yield* store
        .read(id)
        .pipe(Effect.mapError(() => new AuthError({ reason: 'StoreFailure' })))
      if (
        current === undefined ||
        current.revision !== instance.revision ||
        current.provider !== instance.provider ||
        current.version !== instance.version ||
        current.credential?.credentialId !== instance.credential?.credentialId ||
        current.credential?.identity !== instance.credential?.identity
      )
        return yield* new AuthError({ reason: 'AccountMismatch' })
      if (instance.credential !== undefined) {
        const stored = yield* credentials.read(instance.credential.credentialId)
        if (stored === undefined) return yield* new AuthError({ reason: 'MissingCredential' })
        if (
          stored.binding.identity !== instance.credential.identity ||
          stored.binding.credentialId !== instance.credential.credentialId
        )
          return yield* new AuthError({ reason: 'AccountMismatch' })
      }
    })
    const secrets = Effect.andThen(
      check,
      instance.credential === undefined
        ? Effect.void
        : Credentials.read(instance.credential, selected.secrets).pipe(
            Effect.provideService(Credentials.CredentialStore, credentials),
          ),
    )
    const context: Provider.Context<unknown, unknown> = {
      id,
      revision: instance.revision,
      config: instance.config,
      secrets,
      check,
      ...(instance.credential === undefined ? {} : { credential: instance.credential }),
    }
    return { selected, context, instance }
  })
  const getModel = Effect.fnUntraced(function* (
    binding: Effect.Success<ReturnType<typeof bound>>,
    modelId: string,
  ) {
    const info = yield* ModelInfo.decode(
      yield* binding.selected.models
        .get(binding.context, modelId)
        .pipe(Effect.provideService(Credentials.CredentialStore, credentials)),
    )
    if (info.id !== modelId)
      return yield* new ModelCatalogError({
        reason: 'invalidMetadata',
        modelId,
        message: 'Discovery returned a different model',
      })
    return info
  })
  const service = {
    add,
    get: read,
    list: Effect.flatMap(store.list, (items) => Effect.forEach(items, decode)),
    update,
    remove: (id: string) =>
      Effect.asVoid(
        mutate(id, (current) =>
          current === undefined
            ? Effect.fail(new ProviderError({ reason: 'NotFound', instanceId: id }))
            : // undefined is the store's delete value, not an ignored return value.
              // oxlint-disable-next-line effecttsgo/effect-succeed-with-void
              Effect.succeed(undefined),
        ),
      ),
    models: {
      list: Effect.fnUntraced(function* (
        input: ModelCatalog.ListOptions & { readonly instanceId: string },
      ) {
        const binding = yield* bound(input.instanceId)
        const { instanceId: _, ...query } = input
        const options = yield* Schema.decodeEffect(ModelCatalog.ListOptionsSchema, {
          onExcessProperty: 'error',
        })(query).pipe(
          Effect.mapError(
            () =>
              new ModelCatalogError({
                reason: 'unsupportedOptions',
                message: 'Invalid model list options',
              }),
          ),
        )
        return yield* Schema.decodeEffect(ModelCatalog.PageSchema)(
          yield* binding.selected.models
            .list(binding.context, options)
            .pipe(Effect.provideService(Credentials.CredentialStore, credentials)),
        ).pipe(
          Effect.mapError(
            () =>
              new ModelCatalogError({ reason: 'invalidMetadata', message: 'Invalid model page' }),
          ),
        )
      }),
      get: Effect.fnUntraced(function* (input: {
        readonly instanceId: string
        readonly modelId: string
      }) {
        return yield* getModel(yield* bound(input.instanceId), input.modelId)
      }),
      resolve: Effect.fnUntraced(function* (input: {
        readonly instanceId: string
        readonly modelId: string
        readonly options?: ModelInfo.Options
      }) {
        const binding = yield* bound(input.instanceId)
        const info = yield* getModel(binding, input.modelId)
        const options = yield* ModelInfo.validateOptions(info, input.options ?? {})
        const model = yield* binding.selected
          .createModel(binding.context, { info, options })
          .pipe(Effect.provideService(Credentials.CredentialStore, credentials))
        if (model.definition.ref.modelId !== info.id)
          return yield* new ModelCatalogError({
            reason: 'invalidMetadata',
            message: 'Provider constructed a different model',
            modelId: info.id,
          })
        return ProviderModel.bind(model, binding.context)
      }),
    },
  }
  // Every operation dispatches through the matching definition and its validated codecs.
  return service as unknown as Service<D>
})
