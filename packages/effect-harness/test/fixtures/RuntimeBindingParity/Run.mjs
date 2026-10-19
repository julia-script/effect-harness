import { Effect } from 'effect'
import { run, services, structural } from './Consumer.ts'
const [backend, directory, phase] = process.argv.slice(2)
const result = await Effect.runPromise(
  (phase === 'structural' ? structural(backend) : run(backend, directory, phase)).pipe(
    Effect.provide(services),
  ),
)
console.log(JSON.stringify(result))
