import * as NodeServices from '@effect/platform-node/NodeServices'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Stream from 'effect/Stream'
import * as Cli from '../src/Cli.ts'

// A fake executable exercises the real native process finalizer without login or inference.
const fake = `#!/usr/bin/env node
const fs = require('node:fs');
if (process.argv.includes('auth')) {
  console.log(JSON.stringify({loggedIn:true,authMethod:'claude.ai',apiProvider:'firstParty'}));
} else {
  fs.writeFileSync('pid', String(process.pid));
  console.log(JSON.stringify({type:'system',subtype:'init',tools:[]}));
  setInterval(() => {}, 1000);
}
`

describe('real native process ownership', () => {
  it.live('closing a consumer stream terminates a hanging child process', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'effect-harness-fake-cli-' })
        const executable = `${directory}/fake.cjs`
        yield* fs.writeFileString(executable, fake)
        yield* fs.chmod(executable, 0o700)
        yield* Cli.Cli.use((cli) =>
          cli
            .run({
              model: 'model',
              system: '',
              content: [{ type: 'text', text: 'fake' }],
              cwd: directory,
            })
            .pipe(Stream.take(1), Stream.runDrain),
        ).pipe(Effect.provide(Cli.layer({ executable, policyTrust: 'trusted-installed-cli' })))
        const pid = Number(yield* fs.readFileString(`${directory}/pid`))
        assert.isTrue(Number.isSafeInteger(pid))
        const running = yield* Effect.sync(() => {
          try {
            process.kill(pid, 0)
            return true
          } catch {
            return false
          }
        })
        assert.isFalse(running)
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )
})
