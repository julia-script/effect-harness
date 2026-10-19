import { runConformance } from '../conformance/suite.ts'
import { runJsonl, runReopening } from './support.ts'

runConformance('jsonl', runJsonl)
runConformance('jsonl across reopen', runReopening)
