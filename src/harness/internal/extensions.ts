/**
 * Installing extensions: their tools' implementations, the checks of a registry, and keeping the
 * task `Registry` in step with the installed extensions' task definitions.
 */
import { Effect } from 'effect'
import type { Service as RegistryService } from '../../task/Registry.ts'
import type * as Toolkit from '../../tool/Toolkit.ts'
import * as ToolTask from '../../tool/ToolTask.ts'
import type { Extension } from '../Extension.ts'
import type { Installed, Snapshot } from '../Extensions.ts'
import { InvalidExtension } from '../HarnessError.ts'
import { COMPACTION_KIND } from './compaction/status.ts'
import { RUN_KIND } from './run.ts'

/** The reserved section key of the agent's instructions. */
export const INSTRUCTIONS_KEY = 'instructions'

const SECTION_KEY = /^[a-z][a-z0-9_-]*$/u

/** The built-in tasks, which no extension replaces. */
const BUILT_IN = new Set([RUN_KIND, ToolTask.name, COMPACTION_KIND])

/** `extension` with its tools' implementations from the context. */
export const implementations = <Names extends string>(extension: Extension<Names>) =>
  Effect.map(Effect.context<Toolkit.Requires<Names>>(), (context): Installed => {
    const byName = ToolTask.install(extension.tools, context, extension.config)
    const tools = extension.tools.flatMap((tool) => {
      const each = byName.get(tool.name)
      return each === undefined ? [] : [each]
    })
    return { extension, tools }
  })

const invalid = (extension: string, message: string) =>
  new InvalidExtension({ message: `Extension ${extension}: ${message}`, extension })

/** Why the section keys of one extension are invalid: each must be valid, unreserved, unique. */
const sectionProblem = (extension: Extension): string | undefined => {
  const keys = new Set<string>()
  for (const { key } of extension.sections) {
    if (!SECTION_KEY.test(key)) return `section key ${JSON.stringify(key)} is invalid`
    if (key === INSTRUCTIONS_KEY) return `section key ${key} is reserved`
    if (keys.has(key)) return `two sections have key ${key}`
    keys.add(key)
  }
  return undefined
}

/** The extension of `next` with a task name the built-in tasks or an earlier extension have. */
const taskProblem = (next: Snapshot): readonly [string, string] | undefined => {
  const tasks = new Set(BUILT_IN)
  for (const { extension } of next) {
    for (const task of extension.tasks) {
      if (tasks.has(task.name)) return [extension.name, `task ${task.name} is already installed`]
      tasks.add(task.name)
    }
  }
  return undefined
}

/**
 * Check the registry `next` would be, after installing `name`: its sections, and task names
 * unique across the built-in tasks and every extension.
 */
export const validate = (next: Snapshot, name: string): Effect.Effect<void, InvalidExtension> => {
  const installed = next.find((each) => each.extension.name === name)
  const sections = installed === undefined ? undefined : sectionProblem(installed.extension)
  if (sections !== undefined) return Effect.fail(invalid(name, sections))
  const tasks = taskProblem(next)
  return tasks === undefined ? Effect.void : Effect.fail(invalid(...tasks))
}

/**
 * Bring the task `Registry` in step: definitions of new or replaced extensions are installed,
 * and those no installed extension has any more are removed.
 */
export const publish = Effect.fnUntraced(function* (
  registry: RegistryService,
  current: Snapshot,
  next: Snapshot,
) {
  const kept = new Set(next.flatMap(({ extension }) => extension.tasks.map((task) => task.name)))
  for (const { extension } of current)
    for (const task of extension.tasks)
      if (!kept.has(task.name)) yield* registry.uninstall(task.name)
  for (const installed of next) {
    if (current.some((each) => each.extension === installed.extension)) continue
    for (const task of installed.extension.tasks) yield* registry.install(task)
  }
})
