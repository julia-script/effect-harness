import { runConformance } from '../conformance/suite.ts'
import { runIndexedDb, runReopening } from './support.ts'

runConformance('indexeddb', runIndexedDb)
runConformance('indexeddb across reopen', runReopening)
