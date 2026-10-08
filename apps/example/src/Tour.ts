/** Run one self-contained example, or the complete offline tour. */
import { BunRuntime, BunServices } from '@effect/platform-bun'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Quickstart from './tour/Quickstart.ts'
import * as Recovery from './tour/Recovery.ts'
import * as Forks from './tour/Forks.ts'
import * as Extensions from './tour/Extensions.ts'
import * as Subagent from './tour/Subagent.ts'
import * as Checkout from './tour/Checkout.ts'
import * as Reminder from './tour/Reminder.ts'
import * as Context from './tour/Context.ts'
import * as Documents from './tour/Documents.ts'
import * as Multiplayer from './tour/Multiplayer.ts'

const Choice = Schema.Literals([
  'all',
  '--help',
  'quickstart',
  'recovery',
  'forks',
  'extensions',
  'subagent',
  'checkout',
  'reminder',
  'context',
  'documents',
  'multiplayer',
])
const examples = [
  { name: 'quickstart', run: Quickstart.run },
  { name: 'recovery', run: Recovery.run },
  { name: 'forks', run: Forks.run },
  { name: 'extensions', run: Extensions.run },
  { name: 'subagent', run: Subagent.run },
  { name: 'checkout', run: Checkout.run },
  { name: 'reminder', run: Reminder.run },
  { name: 'context', run: Context.run },
  { name: 'documents', run: Documents.run },
  { name: 'multiplayer', run: Multiplayer.run },
]

const program = Effect.gen(function* () {
  const selected = yield* Schema.decodeUnknownEffect(Choice)(process.argv[2] ?? 'all')
  if (selected === '--help') {
    yield* Console.log(
      'bun run --cwd apps/example tour -- [all|quickstart|recovery|forks|extensions|subagent|checkout|reminder|context|documents|multiplayer]',
    )
    return
  }
  for (const example of examples) {
    if (selected !== 'all' && selected !== example.name) continue
    const result = yield* example.run
    yield* Console.log(`${example.name}: ${JSON.stringify(result)}`)
  }
})

if (import.meta.main) BunRuntime.runMain(program.pipe(Effect.provide(BunServices.layer)))
