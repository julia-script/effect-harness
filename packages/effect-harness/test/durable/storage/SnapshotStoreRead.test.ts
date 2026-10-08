import { describe } from '@effect/vitest'

import * as Layer from 'effect/Layer'

import * as KeyValueStore from 'effect/persistence/KeyValueStore'

import * as EventJournal from 'effect/eventlog/EventJournal'

import { cases } from './SnapshotStoreReadCases.ts'

const Memory = Layer.mergeAll(KeyValueStore.layerMemory, EventJournal.layerMemory)
describe('SnapshotStoreRead', () => {
  cases('memory', Memory)
})
