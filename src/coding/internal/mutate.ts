/**
 * The `write` and `edit` handlers, ported from pi's `tools/write.ts` and `tools/edit.ts`. Both
 * run in the queue of mutations of their file.
 */
import { Effect, Result } from 'effect'
import type { Config, Replacement } from '../CodingTools.ts'
import type { Output } from '../../tool/ToolResult.ts'
import { text } from '../../tool/ToolResult.ts'
import { displayDiff, unifiedPatch } from './diff.ts'
import {
  applyEdits,
  detectLineEnding,
  normalizeToLF,
  restoreLineEndings,
  stripBom,
} from './replace.ts'
import type { Workspace } from './workspace.ts'
import { resolve, withMutation } from './workspace.ts'

export const write = (
  workspace: Workspace,
  params: { readonly path: string; readonly content: string },
  config: Config,
): Effect.Effect<Output, Output> => {
  const { fs, path } = workspace
  const absolute = resolve(workspace, config, params.path)
  const writing = Effect.andThen(
    fs.makeDirectory(path.dirname(absolute), { recursive: true }),
    fs.writeFileString(absolute, params.content),
  )
  return withMutation(
    workspace,
    absolute,
    Effect.mapBoth(writing, {
      onFailure: (error) => text(`Could not write ${params.path}: ${error.message}`),
      onSuccess: () => text(`Successfully wrote to ${params.path}`),
    }),
  )
}

/** A decoder that keeps a byte order mark, so the edit can write it back. */
const keepingBom = new TextDecoder('utf-8', { ignoreBOM: true })

const accessError = (path: string, error: { readonly message: string }) =>
  text(`Could not edit file: ${path}. ${error.message}`)

/** The edited text of `content`, keeping its byte order mark and line endings. */
const edited = (content: string, edits: ReadonlyArray<Replacement>, path: string) => {
  const { bom, text: body } = stripBom(content)
  const ending = detectLineEnding(body)
  const before = normalizeToLF(body)
  return Result.map(applyEdits(before, edits, path), (after) => ({
    before,
    after,
    final: bom + restoreLineEndings(after, ending),
  }))
}

export const edit = (
  workspace: Workspace,
  params: { readonly path: string; readonly edits: ReadonlyArray<Replacement> },
  config: Config,
): Effect.Effect<Output, Output> => {
  const { fs } = workspace
  const { path, edits } = params
  const absolute = resolve(workspace, config, path)
  const editing = Effect.gen(function* () {
    if (edits.length === 0)
      return yield* Effect.fail(
        text('Edit tool input is invalid. edits must contain at least one replacement.'),
      )
    const info = yield* Effect.mapError(fs.stat(absolute), (error) => accessError(path, error))
    if (info.type !== 'File' && info.type !== 'SymbolicLink')
      return yield* Effect.fail(text(`Could not edit file: ${path}. Path is not a file.`))
    const bytes = yield* Effect.mapError(fs.readFile(absolute), (error) => accessError(path, error))
    const content = keepingBom.decode(bytes)
    const change = edited(content, edits, path)
    if (Result.isFailure(change)) return yield* Effect.fail(text(change.failure))
    const { before, after, final } = change.success
    yield* Effect.mapError(fs.writeFileString(absolute, final), (error) => accessError(path, error))
    const { diff, firstChangedLine } = displayDiff(before, after)
    const details = {
      diff,
      patch: unifiedPatch(path, before, after),
      ...(firstChangedLine === undefined ? {} : { firstChangedLine }),
    }
    return text(`Successfully replaced ${edits.length} block(s) in ${path}.`, { details })
  })
  return withMutation(workspace, absolute, editing)
}
