import { describe } from '@effect/vitest'

import * as KeyValueStore from 'effect/persistence/KeyValueStore'

import * as EventJournal from 'effect/eventlog/EventJournal'

import * as Layer from 'effect/Layer'

import { cases } from './SnapshotStoreCases.ts'

const Memory = Layer.mergeAll(KeyValueStore.layerMemory, EventJournal.layerMemory)
describe('SnapshotStore', () => {
  cases('memory', Memory)
})
