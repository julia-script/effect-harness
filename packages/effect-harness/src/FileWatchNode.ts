import * as NodePath from '@effect/platform-node/NodePath'
import * as Layer from 'effect/Layer'
import * as FileWatch from './FileWatch.js'
import type * as ToolkitFileSystem from './ToolkitFileSystem.js'
import { subscribe } from './internal/NativeFileWatch.js'

/** Explicit native Node subscription adapter, without automatic polling fallback. */
export const adapter: Layer.Layer<FileWatch.Adapter> = Layer.succeed(FileWatch.Adapter, {
  subscribe,
})

export const layer = (
  options: ToolkitFileSystem.Options,
): Layer.Layer<FileWatch.FileWatch, FileWatch.FileWatchError> =>
  FileWatch.layer(options).pipe(Layer.provide([adapter, NodePath.layer]))
