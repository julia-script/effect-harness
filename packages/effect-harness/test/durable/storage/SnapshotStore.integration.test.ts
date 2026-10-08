import * as DirectoryFixture from '../DirectoryFixture.ts'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as NodeServices from '@effect/platform-node/NodeServices'

import * as Path from 'effect/Path'

import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'

import * as Layer from 'effect/Layer'

import { persistence } from './TestStore.ts'

import { cases } from './SnapshotStoreCases.ts'

const Database = persistence.pipe(Layer.provide(SqliteClient.layer({ filename: ':memory:' })))
describe('SnapshotStore', () => {
  cases('SQLite', Database)

  // effect-nit-allow P8-it-live-or-withLive-for-real-time: independent native SQLite clients must serialize physical commits on the same database; their coordinator/COMMIT callbacks are outside TestClock.
  it.live('serializes snapshot commits from independent SQLite clients', () =>
    Effect.gen(function* () {
      const path = yield* Path.Path
      const directory = yield* DirectoryFixture.make()
      const filename = path.join(directory, 'state.sqlite')
      const firstContext = yield* Layer.build(
        persistence.pipe(Layer.provide(SqliteClient.layer({ filename }))),
      )
      const secondContext = yield* Layer.build(
        persistence.pipe(Layer.provide(SqliteClient.layer({ filename }))),
      )
      const first = yield* SnapshotStore.make().pipe(Effect.provideContext(firstContext))
      const second = yield* SnapshotStore.make().pipe(Effect.provideContext(secondContext))
      yield* Effect.forEach(
        Array.from({ length: 12 }, (_, i) => i),
        (i) => (i % 2 === 0 ? first : second).commit([], { key: `client-${i}` }),
        { concurrency: 'unbounded', discard: true },
      )
      assert.strictEqual((yield* first.committed).receipts.length, 12)
      assert.strictEqual((yield* second.committed).nextSeq, 13)
    }).pipe(Effect.provide(NodeServices.layer)),
  )
})
