/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The keychain store over a fake keychain: what it writes where, and how it fails. The shared
 * store cases run it in `credentials.test.ts`.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Redacted } from 'effect'
import { runTest } from '../../task/test/support.ts'
import { CredentialsError } from '../CredentialsError.ts'
import * as keychain from '../internal/keychain.ts'
import { makeFakeKeychain } from './fakeKeychain.ts'

const entry = (service: string, name: string) => JSON.stringify([service, name])

describe('keychain store', () => {
  test('keeps one entry per provider and an index without secrets', () =>
    runTest(() =>
      Effect.gen(function* () {
        const entries = new Map<string, string>()
        const store = yield* keychain.make('app', makeFakeKeychain(entries))
        yield* store.set('openai', { _tag: 'ApiKey', key: Redacted.make('sk-1') })
        expect(JSON.parse(entries.get(entry('app', 'openai')) ?? '')).toEqual({
          _tag: 'ApiKey',
          key: 'sk-1',
        })
        const index = entries.get(entry('app:index', 'providers')) ?? ''
        expect(JSON.parse(index)).toEqual([{ _tag: 'Info', provider: 'openai', type: 'ApiKey' }])
        expect(index).not.toContain('sk-1')

        const reopened = yield* keychain.make('app', makeFakeKeychain(entries))
        const stored = yield* reopened.get('openai')
        expect(stored?._tag === 'ApiKey' && Redacted.value(stored.key)).toBe('sk-1')
        yield* reopened.remove('openai')
        expect([...entries.keys()]).toEqual([entry('app:index', 'providers')])
        expect(yield* reopened.list).toEqual([])
      }),
    ))

  test('keeps services apart', () =>
    runTest(() =>
      Effect.gen(function* () {
        const entries = new Map<string, string>()
        const work = yield* keychain.make('work', makeFakeKeychain(entries))
        const home = yield* keychain.make('home', makeFakeKeychain(entries))
        yield* work.set('openai', { _tag: 'ApiKey', key: Redacted.make('w') })
        expect(yield* home.get('openai')).toBeUndefined()
        expect(yield* home.list).toEqual([])
      }),
    ))

  test('reports a malformed entry or a failing keychain as CredentialsError', () =>
    runTest(() =>
      Effect.gen(function* () {
        const entries = new Map([[entry('app', 'openai'), '{"type":"nope"}']])
        const store = yield* keychain.make('app', makeFakeKeychain(entries))
        expect(yield* Effect.flip(store.get('openai'))).toBeInstanceOf(CredentialsError)

        const locked = yield* keychain.make('app', {
          ...makeFakeKeychain(),
          set: () => Effect.fail(new CredentialsError({ message: 'locked' })),
        })
        const failure = yield* Effect.flip(
          locked.set('openai', { _tag: 'ApiKey', key: Redacted.make('x') }),
        )
        expect(failure.message).toBe('locked')
      }),
    ))
})
