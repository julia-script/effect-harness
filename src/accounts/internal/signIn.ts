/** Stream-owned OAuth attempts: reserve a name, authorize, then commit staged credentials. */
import {
  Cause,
  Deferred,
  Effect,
  Equal,
  Queue,
  Redacted,
  Semaphore,
  Stream,
  SubscriptionRef,
} from 'effect'
import type { Credential } from '../../credentials/Credential.ts'
import { CredentialStore } from '../../credentials/CredentialStore.ts'
import * as AccountError from '../AccountError.ts'
import type { Saved, SignIn, SignInEvent } from '../Account.ts'
import type { Account, Any, Base } from '../Adapter.ts'
import * as entries from './entry.ts'
import type { Entry } from './entry.ts'
import * as registry from './registry.ts'
import type { State } from './registry.ts'
import * as stagedCredentials from './stagedCredentials.ts'

export interface Attempt {
  readonly id: string
  readonly name: string
  readonly browser: boolean
  readonly cancelled: Deferred.Deferred<void>
  readonly completion: Deferred.Deferred<Redacted.Redacted>
  readonly staged: stagedCredentials.Staged
  phase: 'active' | 'cancelled' | 'committed'
  submitted?: Redacted.Redacted
}

const invalid = () =>
  entries.fail(new AccountError.InvalidSignIn(), 'This sign-in does not accept that operation')
const cancelled = () =>
  entries.fail(new AccountError.SignInCancelled(), 'The sign-in was cancelled')
const changed = () =>
  entries.fail(
    new AccountError.SignInChanged(),
    'The account or its credential changed during sign-in',
  )
const failed = () => entries.fail(new AccountError.SignInFailed(), 'Signing in failed')
const storeFailed = () =>
  entries.fail(new AccountError.StoreFailed(), 'The sign-in could not be stored')

/** Must be called under the registry lock, including removal's fence. */
export const cancelAttempt = (attempt: Attempt) =>
  Effect.gen(function* () {
    attempt.phase = 'cancelled'
    attempt.staged.close()
    yield* Deferred.succeed(attempt.cancelled, undefined)
  })

const reserve = (state: State, input: SignIn) =>
  Semaphore.withPermit(
    state.line,
    Effect.gen(function* () {
      if (state.attempts.has(input.name)) {
        return yield* entries.fail(
          new AccountError.SignInActive(),
          'This account already has an active sign-in',
        )
      }
      const existing = yield* registry.lookup(state, input.name)
      if (existing?.problem !== undefined) return yield* existing.problem
      const id = existing?.saved.adapter ?? input.adapter
      if (id === undefined) return yield* entries.notFound(input.name)
      const found = yield* entries.adapterOf(state.group, state.implementations, id)
      const login = found.implementation.login
      if (
        login === undefined ||
        (found.adapter.auth !== 'deviceCode' && found.adapter.auth !== 'browserOAuth')
      ) {
        return yield* entries.fail(
          new AccountError.Unsupported(),
          'This account does not support OAuth sign-in',
        )
      }
      const account: Account = existing?.account ?? {
        adapter: id,
        name: input.name,
        label: input.label ?? found.adapter.label,
        settings: yield* entries.decode(found.adapter, input.settings ?? {}),
      }
      const saved: Saved = existing?.saved ?? {
        adapter: account.adapter,
        name: account.name,
        label: account.label,
        settings: yield* entries.encode(found.adapter, account.settings),
      }
      const before = yield* state.credentials.get(input.name).pipe(Effect.mapError(storeFailed))
      const attempt: Attempt = {
        id: globalThis.crypto.randomUUID(),
        name: input.name,
        browser: found.adapter.auth === 'browserOAuth',
        cancelled: yield* Deferred.make<void>(),
        completion: yield* Deferred.make<Redacted.Redacted>(),
        staged: stagedCredentials.make(input.name),
        phase: 'active',
      }
      state.attempts.set(input.name, attempt)
      return { attempt, existing, before, account, saved, found, login }
    }),
  )

interface Reserved {
  readonly attempt: Attempt
  readonly existing: Entry | undefined
  readonly before: Credential | undefined
  readonly account: Account
  readonly saved: Saved
  readonly found: { readonly adapter: Any; readonly implementation: Base<Any> }
  readonly login: NonNullable<Base<Any>['login']>
}

/** Restore only the credential we committed, never overwrite a replacement. */
const rollback = (
  state: State,
  attempt: Attempt,
  before: Credential | undefined,
  next: Credential,
) =>
  Effect.gen(function* () {
    if (before !== undefined) {
      yield* state.credentials
        .update(attempt.name, next, before)
        .pipe(Effect.catchTag('CredentialChanged', () => Effect.void))
      return
    }
    const current = yield* state.credentials.get(attempt.name)
    if (Equal.equals(current, next)) {
      yield* state.credentials.remove(attempt.name)
    }
  }).pipe(Effect.mapError(storeFailed))

/** Tokens precede account metadata; the registry lock fences removal and target replacement. */
const commit = (state: State, reserved: Reserved) =>
  Semaphore.withPermit(
    state.line,
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const { attempt, existing, before, account, saved, found } = reserved
        if (attempt.phase !== 'active') return yield* cancelled()
        if (
          state.attempts.get(attempt.name) !== attempt ||
          (yield* registry.lookup(state, attempt.name)) !== existing
        ) {
          return yield* changed()
        }
        const next = attempt.staged.current()
        if (next?.type !== 'oauth') return yield* failed()
        const current = yield* state.credentials
          .get(attempt.name)
          .pipe(Effect.mapError(storeFailed))
        if (!Equal.equals(current, before)) return yield* changed()
        const entry =
          existing ?? entries.ready(found.adapter, found.implementation, account, saved, false)
        const all = yield* registry.current(state)
        const updated = existing === undefined ? [...all, entry] : all
        yield* (
          before === undefined
            ? state.credentials.set(attempt.name, next)
            : state.credentials.update(attempt.name, before, next)
        ).pipe(
          Effect.mapError((error) =>
            error._tag === 'CredentialChanged' ? changed() : storeFailed(),
          ),
        )
        if (existing === undefined) {
          yield* restore(
            state.store.set(
              state.key,
              updated.filter(({ fixed }) => !fixed).map(({ saved: record }) => record),
            ),
          ).pipe(
            Effect.mapError(storeFailed),
            Effect.catchCause((cause) =>
              rollback(state, attempt, before, next).pipe(Effect.andThen(Effect.failCause(cause))),
            ),
          )
        }
        attempt.phase = 'committed'
        yield* registry.accept(state, attempt.name)
        yield* SubscriptionRef.set(state.live, updated)
        const names = new Set([attempt.name])
        return entries.status(entry, names, new Map(), next)
      }),
    ),
  )

const release = (state: State, { attempt }: Reserved) =>
  Semaphore.withPermit(
    state.line,
    Effect.sync(() => {
      if (state.attempts.get(attempt.name) === attempt) state.attempts.delete(attempt.name)
      attempt.staged.close()
      if (attempt.submitted !== undefined) Redacted.wipeUnsafe(attempt.submitted)
      if (attempt.phase === 'active') attempt.phase = 'cancelled'
    }),
  )

const run = Effect.fnUntraced(function* (
  state: State,
  input: SignIn,
  reserved: Reserved,
  queue: Queue.Queue<SignInEvent, AccountError.AccountError | Cause.Done>,
) {
  const { attempt, login, account } = reserved
  yield* Queue.offer(queue, { type: 'started', id: attempt.id, name: attempt.name })
  const loginEffect = Effect.suspend(() =>
    login(account, {
      onCode: (code) =>
        Effect.asVoid(Queue.offer(queue, { type: 'code', id: attempt.id, ...code })),
      onAuthorize: (url) =>
        Effect.asVoid(Queue.offer(queue, { type: 'authorization', id: attempt.id, url })),
      completion: Effect.map(Deferred.await(attempt.completion), Redacted.value),
      ...(input.redirectUri === undefined ? {} : { redirectUri: input.redirectUri }),
    }),
  ).pipe(
    Effect.provideService(CredentialStore, attempt.staged.service),
    Effect.provideContext(state.context),
    Effect.catchCause((cause) =>
      Cause.hasInterrupts(cause) ? Effect.interrupt : Effect.fail(failed()),
    ),
  )
  yield* Effect.raceFirst(
    loginEffect,
    Effect.andThen(Deferred.await(attempt.cancelled), Effect.fail(cancelled())),
  ).pipe(Effect.mapError((error) => (attempt.phase === 'cancelled' ? cancelled() : error)))
  const status = yield* commit(state, reserved)
  yield* Queue.offer(queue, { type: 'done', id: attempt.id, account: status })
})

export const stream =
  (state: State) =>
  (input: SignIn): Stream.Stream<SignInEvent, AccountError.AccountError> =>
    Stream.callback<SignInEvent, AccountError.AccountError>((queue) =>
      Effect.acquireUseRelease(
        reserve(state, input),
        (reserved) => run(state, input, reserved, queue),
        (reserved) => release(state, reserved),
      ).pipe(
        Effect.andThen(Queue.end(queue)),
        Effect.catch((error) => Queue.fail(queue, error)),
      ),
    )

export const complete = (state: State) => (id: string, completion: Redacted.Redacted) =>
  Semaphore.withPermit(
    state.line,
    Effect.gen(function* () {
      const attempt = [...state.attempts.values()].find((candidate) => candidate.id === id)
      if (
        attempt === undefined ||
        attempt.phase !== 'active' ||
        !attempt.browser ||
        attempt.submitted !== undefined
      ) {
        return yield* invalid()
      }
      attempt.submitted = Redacted.make(Redacted.value(completion))
      yield* Deferred.succeed(attempt.completion, attempt.submitted)
    }),
  )

export const cancel = (state: State) => (id: string) =>
  Semaphore.withPermit(
    state.line,
    Effect.gen(function* () {
      const attempt = [...state.attempts.values()].find((candidate) => candidate.id === id)
      if (attempt === undefined || attempt.phase !== 'active') return yield* invalid()
      yield* cancelAttempt(attempt)
    }),
  )
