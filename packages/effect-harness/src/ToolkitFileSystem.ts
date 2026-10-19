/**
 * Toolkit-local adapters for the native FileSystem and Path services.
 *
 * These Layers bind operations to supplied backends. Canonical registry invocation
 * selection must later retain each toolkit's context separately; these adapters
 * do not repair the runtime's current merging of handler contexts.
 */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
import * as Sink from 'effect/Sink'
import * as Stream from 'effect/Stream'

/** Immutable configuration in the supplied backend's path namespace. */
export interface Options {
  /** An absolute path; construction never reads or changes the host cwd. */
  readonly root: string
  /**
   * Defaults to unrestricted absolute paths. within-root rejects lexical escapes
   * only: symlinks, mount points and child processes require backend enforcement.
   * This policy is not a security sandbox.
   */
  readonly pathPolicy?: 'unrestricted' | 'within-root' | undefined
}

/**
 * Bind native FileSystem and Path services to an absolute root.
 *
 * Supply one coherent backend Layer with Layer.provide. Acquisition errors and
 * unsupplied inputs remain in the composed Layer type. All native operations,
 * stream/sink failures and resource finalizers delegate to that backend, without
 * host fallback. Temporary paths default to the bound root. Link source paths
 * are root-relative, and symbolic links store the resolved absolute target.
 * Path.resolve and Path.toFileUrl resolve relative inputs against the root;
 * lexical restriction is checked by filesystem operations, not pure Path methods.
 */
export const layer = (
  options: Options,
): Layer.Layer<
  FileSystem.FileSystem | Path.Path,
  PlatformError.PlatformError,
  FileSystem.FileSystem | Path.Path
> => {
  const { root, pathPolicy = 'unrestricted' } = options
  return Layer.effectContext(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      if (!path.isAbsolute(root)) {
        return yield* PlatformError.badArgument({
          module: 'ToolkitFileSystem',
          method: 'layer',
          description: 'root must be absolute',
        })
      }
      const boundRoot = path.resolve(root)
      const boundPath: Path.Path = {
        ...path,
        resolve: (...segments) => path.resolve(boundRoot, ...segments),
        toFileUrl: (value) => path.toFileUrl(path.resolve(boundRoot, value)),
      }
      const resolve = (value: string): Effect.Effect<string, PlatformError.PlatformError> =>
        Effect.suspend(() => {
          const resolved = boundPath.resolve(value)
          const relative = path.relative(boundRoot, resolved)
          if (
            pathPolicy === 'within-root' &&
            (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative))
          ) {
            return Effect.fail(
              PlatformError.systemError({
                _tag: 'PermissionDenied',
                module: 'ToolkitFileSystem',
                method: 'resolve',
                pathOrDescriptor: value,
                description: 'path is outside the bound root',
              }),
            )
          }
          return Effect.succeed(resolved)
        })
      const pair = (a: string, b: string) => Effect.all([resolve(a), resolve(b)])
      const bound: FileSystem.FileSystem = {
        ...fs,
        access: (p, o) => Effect.flatMap(resolve(p), (p) => fs.access(p, o)),
        copy: (a, b, o) => Effect.flatMap(pair(a, b), ([a, b]) => fs.copy(a, b, o)),
        copyFile: (a, b) => Effect.flatMap(pair(a, b), ([a, b]) => fs.copyFile(a, b)),
        chmod: (p, mode) => Effect.flatMap(resolve(p), (p) => fs.chmod(p, mode)),
        chown: (p, uid, gid) => Effect.flatMap(resolve(p), (p) => fs.chown(p, uid, gid)),
        glob: (pattern, o) =>
          Effect.flatMap(resolve(o?.root ?? '.'), (root) =>
            Effect.flatMap(resolve(path.resolve(root, pattern)), () =>
              fs.glob(pattern, { ...o, root }),
            ),
          ),
        exists: (p) => Effect.flatMap(resolve(p), (p) => fs.exists(p)),
        link: (a, b) => Effect.flatMap(pair(a, b), ([a, b]) => fs.link(a, b)),
        makeDirectory: (p, o) => Effect.flatMap(resolve(p), (p) => fs.makeDirectory(p, o)),
        makeTempDirectory: (o) =>
          Effect.flatMap(resolve(o?.directory ?? '.'), (directory) =>
            fs.makeTempDirectory({ ...o, directory }),
          ),
        makeTempDirectoryScoped: (o) =>
          Effect.flatMap(resolve(o?.directory ?? '.'), (directory) =>
            fs.makeTempDirectoryScoped({ ...o, directory }),
          ),
        makeTempFile: (o) =>
          Effect.flatMap(resolve(o?.directory ?? '.'), (directory) =>
            fs.makeTempFile({ ...o, directory }),
          ),
        makeTempFileScoped: (o) =>
          Effect.flatMap(resolve(o?.directory ?? '.'), (directory) =>
            fs.makeTempFileScoped({ ...o, directory }),
          ),
        open: (p, o) => Effect.flatMap(resolve(p), (p) => fs.open(p, o)),
        readDirectory: (p, o) => Effect.flatMap(resolve(p), (p) => fs.readDirectory(p, o)),
        readFile: (p) => Effect.flatMap(resolve(p), (p) => fs.readFile(p)),
        readFileString: (p, encoding) =>
          Effect.flatMap(resolve(p), (p) => fs.readFileString(p, encoding)),
        readLink: (p) => Effect.flatMap(resolve(p), (p) => fs.readLink(p)),
        realPath: (p) => Effect.flatMap(resolve(p), (p) => fs.realPath(p)),
        remove: (p, o) => Effect.flatMap(resolve(p), (p) => fs.remove(p, o)),
        rename: (a, b) => Effect.flatMap(pair(a, b), ([a, b]) => fs.rename(a, b)),
        sink: (p, o) => Sink.unwrap(Effect.map(resolve(p), (p) => fs.sink(p, o))),
        stat: (p) => Effect.flatMap(resolve(p), (p) => fs.stat(p)),
        stream: (p, o) => Stream.unwrap(Effect.map(resolve(p), (p) => fs.stream(p, o))),
        symlink: (a, b) => Effect.flatMap(pair(a, b), ([a, b]) => fs.symlink(a, b)),
        truncate: (p, size) => Effect.flatMap(resolve(p), (p) => fs.truncate(p, size)),
        utimes: (p, a, m) => Effect.flatMap(resolve(p), (p) => fs.utimes(p, a, m)),
        watch: (p, o) => Stream.unwrap(Effect.map(resolve(p), (p) => fs.watch(p, o))),
        writeFile: (p, data, o) => Effect.flatMap(resolve(p), (p) => fs.writeFile(p, data, o)),
        writeFileString: (p, data, o) =>
          Effect.flatMap(resolve(p), (p) => fs.writeFileString(p, data, o)),
      }
      return Context.make(FileSystem.FileSystem, bound).pipe(Context.add(Path.Path, boundPath))
    }),
  )
}
