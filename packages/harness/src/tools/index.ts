import type * as AiTool from 'effect/ai/Tool'
import type { MutationLocks } from '../MutationLocks.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Toolkit from 'effect/ai/Toolkit'
import type { Env } from '../Env.ts'
import type * as Extension from '../Extension.ts'
import * as Tool from '../Tool.ts'
import * as Read from './Read.ts'
import * as Write from './Write.ts'
import * as Edit from './Edit.ts'
import * as Bash from './Bash.ts'
export { Read, Write, Edit, Bash }
export const toolkit = Toolkit.make(Read.tool, Write.tool, Edit.tool, Bash.tool)
export const handlers = (
  options: Bash.Options = {},
): Layer.Layer<AiTool.HandlersFor<Toolkit.Tools<typeof toolkit>>> =>
  toolkit.toLayer({
    read: Read.handler,
    write: Write.handler,
    edit: Edit.handler,
    bash: Bash.handler(options),
  })
export const make = (
  options: Bash.Options = {},
): Effect.Effect<Extension.Extension, never, Env | MutationLocks> =>
  Tool.bind(toolkit).pipe(
    Effect.provide(handlers(options)),
    Effect.map((tools) => CodingTools.of({ name: 'coding-tools', tools })),
  )
export class CodingTools extends Context.Service<CodingTools, Extension.Extension>()(
  '@effect-harness/harness/tools/CodingTools',
) {}
export const layer = (
  options: Bash.Options = {},
): Layer.Layer<CodingTools, never, Env | MutationLocks> => Layer.effect(CodingTools, make(options))
export const powerShellToolkit = Toolkit.make(Bash.powershell)
export const makePowerShell = (
  options: Bash.PowerShellOptions = {},
): Effect.Effect<Extension.Extension, never, Env> =>
  Tool.bind(powerShellToolkit).pipe(
    Effect.provide(powerShellToolkit.toLayer({ powershell: Bash.powerShellHandler(options) })),
    Effect.map((tools) => ({ name: 'powershell', tools })),
  )
