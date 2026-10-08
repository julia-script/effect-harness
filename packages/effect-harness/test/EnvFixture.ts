import * as MutationLocks from 'effect-harness/MutationLocks'

import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Scope from 'effect/Scope'

import * as FileSystem from 'effect/FileSystem'

import type * as PlatformError from 'effect/PlatformError'

import * as Layer from 'effect/Layer'

import * as NodeServices from '@effect/platform-node/NodeServices'

import * as NodeEnv from 'effect-harness/NodeEnv'

import { type Env } from 'effect-harness/Env'

import { Invocation } from 'effect-harness/Invocation'

class NativeDirectory extends Context.Service<NativeDirectory, { readonly cwd: string }>()(
  'effect-harness/test/EnvFixture/NativeDirectory',
) {
  static layer = Layer.effect(NativeDirectory)(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      return NativeDirectory.of({
        cwd: yield* fs.makeTempDirectoryScoped({ prefix: 'harness-tools-test-' }),
      })
    }),
  )
}

export const withEnv = <A, E, R>(
  program: Effect.Effect<
    A,
    E,
    R | Env | FileSystem.FileSystem | Invocation | MutationLocks.MutationLocks
  >,
): Effect.Effect<
  A,
  E | PlatformError.PlatformError,
  | Exclude<Exclude<R, Env | Invocation | MutationLocks.MutationLocks>, NodeServices.NodeServices>
  | Scope.Scope
> =>
  Effect.gen(function* () {
    const owner = yield* Scope.Scope
    const services = yield* Layer.buildWithScope(NativeDirectory.layer, owner)
    const { cwd } = Context.get(services, NativeDirectory)
    return yield* program.pipe(
      Effect.provide(
        Layer.mergeAll(
          MutationLocks.layer,
          NodeEnv.layer({
            cwd,
            shell: '/bin/sh',
            env: {
              BASH_ENV: '',
              PATH: '/usr/bin:/bin',
              SSH_CLIENT: '',
              SSH2_CLIENT: '',
              SSH_CONNECTION: '',
              SSH_TTY: '',
            },
          }),
          Layer.succeed(
            Invocation,
            Invocation.of({
              cwd,
              report: () => Effect.void,
              progress: () => Effect.void,
            }),
          ),
        ),
      ),
    )
  }).pipe(Effect.provide(NodeServices.layer))
