import { afterAll } from 'bun:test'
import { runConformance } from '../conformance/suite.ts'
import { pglite, runReopening, runSql, sqlite } from './support.ts'

const postgres = pglite()
afterAll(() => postgres.dispose())

runConformance('sql (sqlite)', runSql(sqlite))
runConformance('sql (sqlite) across reopen', runReopening(sqlite))
runConformance('sql (pglite)', runSql(postgres))
runConformance('sql (pglite) across reopen', runReopening(postgres))
