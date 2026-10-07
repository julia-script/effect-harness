import * as Effect from 'effect/Effect'
import { Env, type FileError } from '../Env.ts'
import { Invocation } from '../Invocation.ts'
const unicodeSpaces = /[\u00a0\u2000-\u200a\u202f\u205f\u3000]/g
export const normalize = (path: string): string => {
  const value = path.replace(unicodeSpaces, ' ')
  return value.startsWith('@') ? value.slice(1) : value
}
export const resolve = Effect.fnUntraced(function* (
  path: string,
): Effect.fn.Return<string, FileError, Env | Invocation> {
  return yield* (yield* Env).absolutePath(normalize(path), (yield* Invocation).cwd)
})
export const resolveRead = Effect.fnUntraced(function* (
  path: string,
): Effect.fn.Return<string, FileError, Env | Invocation> {
  const env = yield* Env
  const absolute = yield* resolve(path)
  const variants = [
    absolute,
    absolute.replace(/ (AM|PM)\./gi, '\u202f$1.'),
    absolute.normalize('NFD'),
    absolute.replace(/'/g, '\u2019'),
    absolute.normalize('NFD').replace(/'/g, '\u2019'),
  ]
  for (const value of new Set(variants)) if (yield* env.exists(value)) return value
  return absolute
})
