import { dlopen, ptr, toArrayBuffer } from 'bun:ffi'
import * as BunPath from '@effect/platform-bun/BunPath'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as DirectoryReader from './DirectoryReader.js'
import type * as ToolkitFileSystem from './ToolkitFileSystem.js'
import { nativeMessage } from './internal/DirectoryWatchBinding.js'

/**
 * Actual libc DIR handles: Bun1.4.2's node:fs Dir loads a whole path-based array.
 * The ABI below is limited to Darwin64 and Linux64 little-endian x64/arm64.
 * Other ABIs/Windows fail explicitly. libc buffering is opaque, not a byte bound.
 * FFI is acquired in each reader's Scope, never eagerly or through core imports.
 */
export const adapter: Layer.Layer<DirectoryReader.Adapter> = Layer.succeed(
  DirectoryReader.Adapter,
  {
    capabilities: {
      identity: 'opened-directory',
      allocation: 'native-buffer',
      membership: 'enumeration-time',
    },
    open: Effect.fn('DirectoryReaderBun.open')(function* (path: string) {
      const fail = (
        operation: string,
        path: string,
        message: string,
        reason: DirectoryReader.DirectoryReaderError['reason'] = 'backend',
      ) => new DirectoryReader.DirectoryReaderError({ reason, operation, path, message })
      const darwin = process.platform === 'darwin'
      if (
        (!darwin && process.platform !== 'linux') ||
        (process.arch !== 'arm64' && process.arch !== 'x64')
      )
        return yield* fail(
          'load',
          '',
          'Native Bun directory handles require Darwin/Linux64 x64/arm64',
          'unsupported',
        )
      const library = yield* Effect.acquireRelease(
        Effect.try({
          try: () =>
            dlopen(darwin ? '/usr/lib/libSystem.B.dylib' : 'libc.so.6', {
              opendir: { args: ['ptr'], returns: 'ptr' },
              ...(darwin && process.arch === 'x64'
                ? ({ readdir$INODE64: { args: ['ptr'], returns: 'ptr' } } as const)
                : ({ readdir: { args: ['ptr'], returns: 'ptr' } } as const)),
              closedir: { args: ['ptr'], returns: 'int' },
              ...(darwin
                ? ({ __error: { args: [], returns: 'ptr' } } as const)
                : ({ __errno_location: { args: [], returns: 'ptr' } } as const)),
            }),
          catch: (cause) => fail('load', '', nativeMessage(cause), 'unsupported'),
        }),
        (lib) => Effect.sync(() => lib.close()),
      )
      const symbols = library.symbols
      const errno = () => {
        const address = '__error' in symbols ? symbols.__error() : symbols.__errno_location()
        if (!address) return undefined
        return new Int32Array(toArrayBuffer(address, 0, 4))
      }
      const error = (operation: string) => {
        const code = errno()?.[0]
        let reason: DirectoryReader.DirectoryReaderError['reason'] = 'backend'
        if (code === 2) reason = 'notFound'
        else if (code === 20) reason = 'notDirectory'
        else if (code === 1 || code === 13) reason = 'permission'
        return fail(operation, path, `libc ${operation} failed (errno ${code})`, reason)
      }
      let closed = false
      const bytes = new TextEncoder().encode(`${path}\0`)
      const handle = yield* Effect.acquireRelease(
        Effect.suspend(() => {
          const handle = symbols.opendir(ptr(bytes))
          return handle ? Effect.succeed(handle) : Effect.fail(error('open'))
        }),
        (handle) =>
          Effect.orDie(
            Effect.suspend(() => {
              if (closed) return Effect.void
              closed = true
              return symbols.closedir(handle) === 0 ? Effect.void : Effect.fail(error('close'))
            }),
          ),
      )
      const close = Effect.suspend(() => {
        if (closed) return Effect.void
        closed = true
        return symbols.closedir(handle) === 0 ? Effect.void : Effect.fail(error('close'))
      })
      const read: DirectoryReader.Handle['read'] = Effect.suspend(() => {
        if (closed) return Effect.fail(fail('read', path, 'directory handle is closed', 'closed'))
        const errnoValue = errno()
        if (errnoValue) errnoValue[0] = 0
        const address =
          'readdir' in symbols ? symbols.readdir(handle) : symbols['readdir$INODE64'](handle)
        if (!address)
          return (errnoValue?.[0] ?? 0) === 0 ? Effect.succeedNone : Effect.fail(error('read'))
        const headerSize = darwin ? 21 : 19
        const header = new DataView(toArrayBuffer(address, 0, headerSize))
        const recordSize = header.getUint16(16, true)
        if (recordSize <= headerSize || recordSize > 65535)
          return Effect.fail(fail('read', path, 'Unexpected native dirent ABI', 'unsupported'))
        const nameBytes = new Uint8Array(
          toArrayBuffer(address, headerSize, recordSize - headerSize),
        )
        const nul = nameBytes.indexOf(0)
        if (nul < 0)
          return Effect.fail(
            fail('read', path, 'Native dirent name is not terminated', 'unsupported'),
          )
        const name = new TextDecoder().decode(nameBytes.subarray(0, nul))
        if (name === '.' || name === '..') return read
        const type = header.getUint8(darwin ? 20 : 18)
        let kind: DirectoryReader.Entry['kind'] = 'other'
        if (type === 8) kind = 'file'
        else if (type === 4) kind = 'directory'
        else if (type === 10) kind = 'symlink'
        else if (type === 0) kind = 'unknown'
        return Effect.succeedSome({ name, kind })
      })
      return {
        capabilities: {
          identity: 'opened-directory',
          allocation: 'native-buffer',
          membership: 'enumeration-time',
        } as const,
        read,
        close,
      }
    }),
  },
)

export const layer = (
  options: ToolkitFileSystem.Options,
): Layer.Layer<DirectoryReader.DirectoryReader, DirectoryReader.DirectoryReaderError> =>
  DirectoryReader.layer(options).pipe(Layer.provide([adapter, BunPath.layer]))
