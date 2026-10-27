import { Effect } from 'effect'
import { StorageError } from '../../StorageError.ts'

/**
 * The slice of the IndexedDB API the backend uses, typed structurally so the package needs no DOM
 * library: a browser's `indexedDB` and `fake-indexeddb` both satisfy it. Handlers take `never` so
 * the DOM's `(this: IDBRequest, event: Event) => any` handlers are assignable to them.
 */

type Handler = ((event: never) => void) | null

export type Key = number | string | Array<number | string>

export interface IdbError {
  readonly name: string
  readonly message: string
}

export interface IdbRequest<T> {
  readonly result: T
  readonly error: IdbError | null
  onsuccess: Handler
  onerror: Handler
}

export interface IdbOpenRequest extends IdbRequest<IdbDatabase> {
  onupgradeneeded: Handler
  onblocked: Handler
}

export interface IdbObjectStore {
  readonly put: (value: unknown, key: Key) => IdbRequest<unknown>
  readonly delete: (key: Key) => IdbRequest<unknown>
  readonly getAll: () => IdbRequest<ReadonlyArray<unknown>>
}

export interface IdbTransaction {
  readonly error: IdbError | null
  readonly objectStore: (name: string) => IdbObjectStore
  readonly abort: () => void
  oncomplete: Handler
  onerror: Handler
  onabort: Handler
}

export type Durability = 'default' | 'strict' | 'relaxed'

export interface IdbDatabase {
  readonly objectStoreNames: { readonly contains: (name: string) => boolean }
  readonly createObjectStore: (name: string) => unknown
  readonly transaction: (
    stores: Array<string>,
    mode: 'readonly' | 'readwrite',
    options?: { readonly durability?: Durability },
  ) => IdbTransaction
  readonly close: () => void
  onversionchange: Handler
  onclose: Handler
}

export interface IdbFactory {
  readonly open: (name: string, version: number) => IdbOpenRequest
}

const failure = (message: string, cause?: unknown) =>
  new StorageError(cause === undefined ? { message } : { message, cause })

/** `globalThis.indexedDB`, when the runtime has one. */
export const globalFactory = (): IdbFactory | undefined =>
  (globalThis as { readonly indexedDB?: IdbFactory }).indexedDB

/**
 * Open `name` at `version`, running `upgrade` when the database is new or older. An interrupted or
 * failed open closes the connection once it arrives.
 */
export const open = (
  factory: IdbFactory,
  name: string,
  version: number,
  upgrade: (db: IdbDatabase) => void,
): Effect.Effect<IdbDatabase, StorageError> =>
  Effect.callback<IdbDatabase, StorageError>((resume) => {
    let abandoned = false
    let request: IdbOpenRequest
    try {
      request = factory.open(name, version)
    } catch (cause) {
      resume(Effect.fail(failure(`IndexedDB database ${name} cannot be opened`, cause)))
      return
    }
    request.onupgradeneeded = () => {
      try {
        upgrade(request.result)
      } catch (cause) {
        abandoned = true
        resume(Effect.fail(failure(`IndexedDB database ${name} cannot be upgraded`, cause)))
      }
    }
    request.onsuccess = () => {
      if (abandoned) request.result.close()
      else resume(Effect.succeed(request.result))
    }
    request.onerror = () => {
      resume(Effect.fail(failure(`IndexedDB database ${name} cannot be opened`, request.error)))
    }
    return Effect.sync(() => {
      abandoned = true
    })
  })

/**
 * Run `body` in one transaction over `stores` and succeed with what it returns once the
 * transaction completes, so request results it returns are ready to read. A throwing body or a
 * failed request aborts the whole transaction.
 */
export const transact = <A>(
  db: IdbDatabase,
  stores: ReadonlyArray<string>,
  mode: 'readonly' | 'readwrite',
  durability: Durability,
  body: (transaction: IdbTransaction) => A,
): Effect.Effect<A, StorageError> =>
  Effect.callback<A, StorageError>((resume) => {
    let transaction: IdbTransaction
    let result: A
    try {
      transaction = db.transaction([...stores], mode, { durability })
    } catch (cause) {
      resume(Effect.fail(failure('IndexedDB transaction cannot start', cause)))
      return
    }
    transaction.oncomplete = () => resume(Effect.succeed(result))
    transaction.onabort = () =>
      resume(Effect.fail(failure('IndexedDB transaction was aborted', transaction.error)))
    try {
      result = body(transaction)
    } catch (cause) {
      transaction.onabort = () =>
        resume(Effect.fail(failure('IndexedDB transaction was rejected', cause)))
      try {
        transaction.abort()
      } catch {
        resume(Effect.fail(failure('IndexedDB transaction was rejected', cause)))
      }
    }
  })
