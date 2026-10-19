/**
 * The implementations of `CodingTools`, over the `FileSystem`, `Path` and `ChildProcessSpawner` in
 * context. With Bun, `BunServices.layer` provides all three on the local machine.
 */
import { Effect } from 'effect'
import { toolkit } from './CodingTools.ts'
import { bash } from './internal/bash.ts'
import { edit, write } from './internal/mutate.ts'
import { read } from './internal/read.ts'
import * as Workspace from './internal/workspace.ts'

/** Handlers of `read`, `write`, `edit` and `bash`; mutations of one file run one at a time. */
export const layer = toolkit.toLayer(
  Effect.map(Workspace.make, (workspace) => ({
    read: (params, { config }) => read(workspace, params, config),
    write: (params, { config }) => write(workspace, params, config),
    edit: (params, { config }) => edit(workspace, params, config),
    bash: (params, { config }) => bash(workspace, params, config),
  })),
)
