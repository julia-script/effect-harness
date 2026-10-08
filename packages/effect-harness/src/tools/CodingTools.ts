/**
 * Native coding toolkits, handler layers and executable extensions.
 */
import type * as Tool from 'effect/ai/Tool'
import type { MutationLocks } from '../MutationLocks.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Toolkit from 'effect/ai/Toolkit'
import type { Env } from '../Env.ts'
import type * as Extension from '../Extension.ts'
import * as ToolRegistration from '../ToolRegistration.ts'
import * as Read from './Read.ts'
import * as Write from './Write.ts'
import * as Edit from './Edit.ts'
import * as Bash from './Bash.ts'
/**
 * Native toolkit containing read, write, edit and Bash declarations.
 *
 * @category constants
 */
export const toolkit = Toolkit.make(Read.tool, Write.tool, Edit.tool, Bash.tool)
/**
 * Provides native Toolkit handlers for the portable coding tools.
 *
 * **Details**
 *
 * Tool.bind captures Env and the shared MutationLocks service when constructing harness
 * registrations from these handlers.
 *
 * @category layers
 */
export const layerHandlers = (
  options: Bash.handler.Options = {},
): Layer.Layer<Tool.HandlersFor<Toolkit.Tools<typeof toolkit>>> =>
  toolkit.toLayer({
    read: Read.handler,
    write: Write.handler,
    edit: Edit.handler,
    bash: Bash.handler(options),
  })
/**
 * Binds portable read, write, edit and bash tools as a named extension.
 *
 * **Details**
 *
 * Consumes Env and the shared MutationLocks service. Native Toolkit handlers and their
 * codecs are captured for later invocation.
 *
 * **Gotchas**
 *
 * Built-in tools use unsafe replay. File and shell effects may have happened before a
 * durable receipt is saved.
 *
 * @category constructors
 */
export const make = (
  options: Bash.handler.Options = {},
): Effect.Effect<Extension.Extension, never, Env | MutationLocks> =>
  ToolRegistration.bind(toolkit).pipe(
    Effect.provide(layerHandlers(options)),
    Effect.map((tools) => CodingTools.of({ name: 'coding-tools', tools })),
  )
/**
 * Service holding the bound portable coding-tools extension.
 *
 * @category services
 */
export class CodingTools extends Context.Service<CodingTools, Extension.Extension>()(
  'effect-harness/tools/CodingTools',
) {}
/**
 * Provides a CodingTools extension using native handlers and the supplied environment.
 *
 * @category layers
 */
export const layer = (
  options: Bash.handler.Options = {},
): Layer.Layer<CodingTools, never, Env | MutationLocks> => Layer.effect(CodingTools, make(options))
/**
 * Native Toolkit containing the PowerShell tool declaration.
 *
 * **Gotchas**
 *
 * This Toolkit contains only the PowerShell tool; read, write and edit are in toolkit.
 *
 * @category constants
 */
export const powerShellToolkit = Toolkit.make(Bash.powershell)
/**
 * Binds PowerShell coding handlers and returns the executable coding extension.
 *
 * @category constructors
 */
export const makePowerShell = (
  options: Bash.powerShellHandler.Options = {},
): Effect.Effect<Extension.Extension, never, Env> =>
  ToolRegistration.bind(powerShellToolkit).pipe(
    Effect.provide(powerShellToolkit.toLayer({ powershell: Bash.powerShellHandler(options) })),
    Effect.map((tools) => ({ name: 'powershell', tools })),
  )
