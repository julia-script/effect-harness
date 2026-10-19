import * as NodePath from '@effect/platform-node/NodePath'
import { opendir } from 'node:fs/promises'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as DirectoryReader from './DirectoryReader.js'
import type * as ToolkitFileSystem from './ToolkitFileSystem.js'
import { native, nativeCode, nativeMessage } from './internal/DirectoryWatchBinding.js'

const capabilities: DirectoryReader.Capabilities = {
  identity: 'opened-directory',
  allocation: 'native-buffer',
  bufferEntries: 32,
  membership: 'enumeration-time',
}

/** Explicit Node adapter. Bun's path-based Dir implementation is deliberately refused. */
export const adapter: Layer.Layer<DirectoryReader.Adapter> = Layer.succeed(
  DirectoryReader.Adapter,
  {
    capabilities,
    open: Effect.fn('DirectoryReaderNode.open')(function* (path: string) {
      const fail = (operation: string) => (cause: unknown) => {
        const code = nativeCode(cause)
        let reason: DirectoryReader.DirectoryReaderError['reason'] = 'backend'
        if (code === 'ENOENT') reason = 'notFound'
        else if (code === 'ENOTDIR') reason = 'notDirectory'
        else if (code === 'EPERM' || code === 'EACCES') reason = 'permission'
        return new DirectoryReader.DirectoryReaderError({
          reason,
          operation,
          path,
          message: nativeMessage(cause),
          cause,
        })
      }
      if (typeof Bun !== 'undefined')
        return yield* new DirectoryReader.DirectoryReaderError({
          reason: 'unsupported',
          operation: 'open',
          path,
          message:
            'Use DirectoryReaderBun: Bun node:fs Dir does not open a native directory handle',
        })
      let closed = false
      const directory = yield* Effect.acquireRelease(
        native(() => opendir(path, { bufferSize: 32 }), fail('open')),
        (dir) =>
          Effect.orDie(
            Effect.suspend(() => {
              if (closed) return Effect.void
              closed = true
              return native(() => dir.close(), fail('close'))
            }),
          ),
      )
      const close = Effect.suspend(() => {
        if (closed) return Effect.void
        closed = true
        return native(() => directory.close(), fail('close'))
      })
      return {
        capabilities,
        close,
        read: native(() => directory.read(), fail('read')).pipe(
          Effect.map((entry) => {
            if (entry === null) return Option.none()
            let kind: DirectoryReader.Entry['kind'] = 'other'
            if (entry.isFile()) kind = 'file'
            else if (entry.isDirectory()) kind = 'directory'
            else if (entry.isSymbolicLink()) kind = 'symlink'
            return Option.some({ name: entry.name, kind })
          }),
        ),
      }
    }),
  },
)

export const layer = (
  options: ToolkitFileSystem.Options,
): Layer.Layer<DirectoryReader.DirectoryReader, DirectoryReader.DirectoryReaderError> =>
  DirectoryReader.layer(options).pipe(Layer.provide([adapter, NodePath.layer]))
