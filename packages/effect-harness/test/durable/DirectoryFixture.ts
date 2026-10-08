/** Per-test native directory ownership, attached to the runner's existing resource Scope. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'

export class Directory extends Context.Service<Directory, { readonly path: string }>()(
  'effect-harness/test/durable/DirectoryFixture/Directory',
) {
  static layer(options?: Parameters<FileSystem.FileSystem['makeTempDirectoryScoped']>[0]) {
    return Layer.effect(Directory)(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        return Directory.of({ path: yield* fs.makeTempDirectoryScoped(options) })
      }),
    )
  }
}
export const make = (options?: Parameters<FileSystem.FileSystem['makeTempDirectoryScoped']>[0]) =>
  Effect.gen(function* () {
    const owner = yield* Scope.Scope
    const services = yield* Layer.buildWithScope(Directory.layer(options), owner)
    return Context.get(services, Directory).path
  })
