import * as NodeAssert from 'node:assert/strict'
import * as NodeServices from '@effect/platform-node/NodeServices'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Stream from 'effect/Stream'
import * as Cli from 'effect-harness/provider-claude-code/Cli'
import * as DirectoryFixture from '../durable/DirectoryFixture.ts'

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

describe('Cli', () => {
  describe('real native process ownership', () => {
    // effect-nit-allow P8-it-live-or-withLive-for-real-time: the native process-group finalizer uses host Date.now/setTimeout; TestClock cannot terminate the hanging child.
    it.live('closing a consumer stream terminates a hanging child process', () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* DirectoryFixture.make({ prefix: 'effect-harness-fake-cli-' })
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
        yield* Effect.sync(() =>
          NodeAssert.throws(
            () => process.kill(pid, 0),
            (error: unknown) => error instanceof Error && 'code' in error && error.code === 'ESRCH',
          ),
        )
      }).pipe(Effect.provide(NodeServices.layer)),
    )
  })
})
