import * as BunPath from '@effect/platform-bun/BunPath'
import * as Layer from 'effect/Layer'
import * as FileWatch from './FileWatch.js'
import type * as ToolkitFileSystem from './ToolkitFileSystem.js'
import { subscribe } from './internal/NativeFileWatch.js'

/** Explicit Bun node:fs native-watch compatibility adapter; recursion failure stays typed. */
export const adapter: Layer.Layer<FileWatch.Adapter> = Layer.succeed(FileWatch.Adapter, {
  subscribe,
})

export const layer = (
  options: ToolkitFileSystem.Options,
): Layer.Layer<FileWatch.FileWatch, FileWatch.FileWatchError> =>
  FileWatch.layer(options).pipe(Layer.provide([adapter, BunPath.layer]))
