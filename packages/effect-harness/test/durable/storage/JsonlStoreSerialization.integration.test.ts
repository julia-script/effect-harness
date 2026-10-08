import * as DirectoryFixture from '../DirectoryFixture.ts'

import * as NodeServices from '@effect/platform-node/NodeServices'

import * as FileSystem from 'effect/FileSystem'

import * as Path from 'effect/Path'

import * as JsonlStore from 'effect-harness/durable/storage/JsonlStore'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Record from 'effect-harness/durable/Record'

describe('JsonlStoreSerialization', () => {
  it.effect('preserves exact JSONL snapshot bytes and schema-owned values on reopen', () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const directory = yield* DirectoryFixture.make()
      const store = yield* JsonlStore.make({ directory })
      yield* store.commit(
        [
          {
            _tag: 'conversation' as const,
            value: { id: Record.ROOT_CONVERSATION_ID },
          },
        ],
        {
          key: 'jsonl-bytes',
          fingerprint: 'unicode \ud800',
        },
      )
      const state = yield* store.read
      const frames = (yield* store.journal(0)).frames
      const expectedState: Record.State = {
        format: 1,
        nextId: 2,
        nextSeq: 2,
        conversations: [{ id: Record.ConversationId.make(1) }],
        entries: [],
        tasks: [],
        submissions: [],
        documents: [],
        receipts: [
          { key: 'jsonl-bytes', fingerprint: 'unicode \ud800', result: 1, seq: Record.Seq.make(1) },
        ],
      }
      const expectedFrames: ReadonlyArray<Record.Frame> = [
        {
          seq: Record.Seq.make(1),
          writes: [{ _tag: 'conversation', value: { id: Record.ConversationId.make(1) } }],
          documents: [],
        },
      ]
      assert.deepStrictEqual(state, expectedState)
      assert.deepStrictEqual(frames, expectedFrames)
      assert.strictEqual(
        yield* fs.readFileString(path.join(directory, 'commits.jsonl')),
        `${JSON.stringify({ state: expectedState, frames: expectedFrames })}\n`,
      )
      const reopened = yield* JsonlStore.make({ directory })
      assert.deepStrictEqual(yield* reopened.read, expectedState)
      assert.deepStrictEqual((yield* reopened.journal(0)).frames, expectedFrames)
    }).pipe(Effect.provide(NodeServices.layer)),
  )
})
