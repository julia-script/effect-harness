import { expect, test } from 'bun:test'
import { Effect, Exit } from 'effect'
import { runConformance } from './conformance/suite.ts'
import { runWithLayer } from './support.ts'
import { Storage } from '../index.ts'
import { Ids } from '../schema/index.ts'

runConformance('memory', runWithLayer(Storage.layerMemory))

test('fails every operation after its scope closes', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const storage = yield* Effect.scoped(Storage.makeMemory)
      const exit = yield* Effect.exit(storage.conversation(Ids.ROOT_CONVERSATION_ID))
      expect(Exit.isFailure(exit)).toBe(true)
      expect((yield* Effect.flip(storage.commit([]))).message).toBe('MemoryStorage is closed')
      expect((yield* Effect.flip(storage.mintId(Ids.EntryId))).message).toBe(
        'MemoryStorage is closed',
      )
    }),
  ))
