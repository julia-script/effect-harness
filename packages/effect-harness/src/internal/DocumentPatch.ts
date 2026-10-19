/** Economical structural operations over owned, schema-validated encoded JSON. */
import type * as Schema from 'effect/Schema'
import type * as Record from '../Record.js'
import * as Json from '../Json.js'

type Path = readonly [string | number, ...Array<string | number>]

/** Arrays with inserted/removed spans use one splice, preserving equal prefix/suffix. */
export const diff = (
  before: Schema.JsonObject,
  after: Schema.JsonObject,
): ReadonlyArray<Record.Op> => {
  const operations: Array<Record.Op> = []
  const visit = (previous: Schema.Json, value: Schema.Json, path: Path): void => {
    if (Json.equals(previous, value)) return
    if (Array.isArray(previous) && Array.isArray(value)) {
      if (previous.length !== value.length) {
        let start = 0
        let end = 0
        while (
          start < previous.length &&
          start < value.length &&
          previous[start] !== undefined &&
          value[start] !== undefined &&
          Json.equals(previous[start], value[start])
        )
          start++
        while (
          end < previous.length - start &&
          end < value.length - start &&
          equalAt(previous, previous.length - end - 1, value, value.length - end - 1)
        )
          end++
        operations.push([
          'splice',
          path,
          start,
          previous.length - start - end,
          value.slice(start, value.length - end),
        ])
      } else {
        for (const [index, next] of value.entries()) {
          const prior = previous[index]
          if (prior !== undefined) visit(prior, next, [...path, index])
        }
      }
      return
    }
    if (isObject(previous) && isObject(value)) {
      object(previous, value, path)
      return
    }
    operations.push(['set', path, value])
  }
  const object = (
    previous: Schema.JsonObject,
    value: Schema.JsonObject,
    path: ReadonlyArray<string | number>,
  ): void => {
    for (const key of Object.keys(previous))
      if (!Object.hasOwn(value, key)) operations.push(['delete', extend(path, key)])
    for (const [key, next] of Object.entries(value)) {
      const prior = previous[key]
      if (Object.hasOwn(previous, key) && prior !== undefined) visit(prior, next, extend(path, key))
      else operations.push(['set', extend(path, key), next])
    }
  }
  object(before, after, [])
  return operations
}

const isObject = (value: Schema.Json): value is Schema.JsonObject =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

/** Includes publication operations: a checkpoint can retain the logical splice. */
export const requiresFormat3 = (writes: ReadonlyArray<Record.StorageWrite>): boolean =>
  writes.some((write) => {
    if (write._tag !== 'document.change' && write._tag !== 'document.create') return false
    if (write.content._tag === 'delta' && write.content.ops.some((op) => op[0] === 'splice'))
      return true
    return (
      write._tag === 'document.change' &&
      write.publicationOps?.some((op) => op[0] === 'splice') === true
    )
  })

const equalAt = (
  left: ReadonlyArray<Schema.Json>,
  l: number,
  right: ReadonlyArray<Schema.Json>,
  r: number,
): boolean => {
  const a = left[l]
  const b = right[r]
  return a !== undefined && b !== undefined && Json.equals(a, b)
}

const extend = (path: ReadonlyArray<string | number>, key: string): Path => {
  const [first, ...rest] = path
  return first === undefined ? [key] : [first, ...rest, key]
}
