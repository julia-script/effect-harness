import * as Arr from 'effect/Array'
import * as Effect from 'effect/Effect'
import { Env, type FileError } from '../../Env.ts'
import { Invocation } from '../../Invocation.ts'
const unicodeSpaces = /[\u00a0\u2000-\u200a\u202f\u205f\u3000]/g
export const normalize = (self: string): string => {
  const value = self.replace(unicodeSpaces, ' ')
  return value.startsWith('@') ? value.slice(1) : value
}
export const resolve = (self: string): Effect.Effect<string, FileError, Env | Invocation> =>
  Effect.flatMap(Env, (env) =>
    Effect.flatMap(Invocation, (invocation) => env.absolutePath(normalize(self), invocation.cwd)),
  )
export const resolveRead = Effect.fnUntraced(function* (
  self: string,
): Effect.fn.Return<string, FileError, Env | Invocation> {
  const env = yield* Env
  const absolute = yield* resolve(self)
  const variants = [
    absolute,
    absolute.replace(/ (AM|PM)\./gi, '\u202f$1.'),
    absolute.normalize('NFD'),
    absolute.replace(/'/g, '\u2019'),
    absolute.normalize('NFD').replace(/'/g, '\u2019'),
  ]
  for (const value of Arr.dedupe(variants)) if (yield* env.exists(value)) return value
  return absolute
})
