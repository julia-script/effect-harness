/** Exercise the packed npm artifact, never the source checkout or its dependency tree. */
import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  realpathSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { isAbsolute, join, relative, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

const repository = realpathSync(fileURLToPath(new URL('..', import.meta.url)))
const temporary = realpathSync(mkdtempSync(join(tmpdir(), 'effect-harness-package-')))
const consumer = join(temporary, 'consumer')
// Node's ambient loader overrides must not make an otherwise broken package appear usable.
const environment = { ...process.env }
delete environment['NODE_PATH']
delete environment['NODE_OPTIONS']

const run = (command: string, args: string[], cwd: string, capture = false): string => {
  const result = spawnSync(command, args, {
    cwd,
    env: environment,
    encoding: 'utf8',
    stdio: capture ? ['ignore', 'pipe', 'inherit'] : 'inherit',
    timeout: 300_000,
    maxBuffer: 16 * 1024 * 1024,
  })
  if (result.error) throw result.error
  assert.equal(
    result.status,
    0,
    `${command} ${args.join(' ')} failed (${result.signal ?? result.status})`,
  )
  return result.stdout ?? ''
}

const isWithin = (parent: string, child: string): boolean => {
  const path = relative(parent, child)
  return path === '' || (!isAbsolute(path) && path !== '..' && !path.startsWith(`..${sep}`))
}

const exportTargets = (value: unknown): string[] => {
  if (typeof value === 'string') return [value]
  if (value === null) return []
  assert.equal(typeof value, 'object', 'Invalid package exports')
  return Object.values(value as Record<string, unknown>).flatMap(exportTargets)
}

const runtimeConsumer = `import assert from 'node:assert/strict'
import { realpathSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Context, Effect, Layer, Schema } from 'effect'
import * as Root from 'effect-harness'
import { HarnessClient, HarnessTransport } from 'effect-harness/client'
import { Extension, HarnessServer } from 'effect-harness/harness'
import { Faux, Model } from 'effect-harness/model'
import { Storage } from 'effect-harness/storage'
import { Tool, Toolkit } from 'effect-harness/tool'

assert.ok(Number(process.versions.node.split('.')[0]) >= 24, 'Consumer requires Node 24 or later')
const installed = realpathSync(join(dirname(fileURLToPath(import.meta.url)), '..', 'node_modules'))
for (const name of ['effect', 'effect-harness']) {
  const resolved = realpathSync(fileURLToPath(import.meta.resolve(name)))
  assert.ok(resolved.startsWith(installed + '/'), name + ' escaped the isolated install')
}
assert.equal(Root.Client.HarnessClient, HarnessClient)
assert.equal(Root.Harness.HarnessServer, HarnessServer)
assert.equal(Root.Model.Faux, Faux)
assert.equal(Root.Storage.Storage, Storage)
assert.equal(Root.Tool.Tool, Tool)
for (const namespace of [Root.Session, Root.Task, Root.Delta, Root.Credentials, Root.Accounts]) {
  assert.equal(typeof namespace, 'object')
}

const Weather = Tool.make('weather', {
  description: 'Read the weather in a city.',
  parameters: Schema.Struct({ city: Schema.String }),
  success: Schema.Struct({ celsius: Schema.Finite }),
})
const toolkit = Toolkit.make(Weather)
const weather = Extension.make('weather', { toolkit })
const invocations: string[] = []
const handlers = toolkit.toLayer({
  weather: ({ city }) => Effect.sync(() => {
    invocations.push(city)
    return { celsius: 21 }
  }),
})

const program = Effect.gen(function* () {
  const faux = yield* Faux.make()
  yield* faux.setResponses([
    Faux.message(Faux.toolCall('weather', { city: 'Lisbon' }), { stopReason: 'toolUse' }),
    Faux.message('21 degrees.'),
  ])
  const server = HarnessServer.layer({
    extensions: [weather],
    settings: { model: faux.ref() },
  }).pipe(Layer.provide([handlers, Model.layer([faux.provider]), Storage.layerMemory]))
  const app = Layer.merge(server, HarnessClient.layer).pipe(
    Layer.provide(HarnessTransport.layerMemory),
  )
  const context = yield* Layer.build(app)
  const client = Context.get(context, HarnessClient.HarnessClient)
  const conversation = yield* client.root()
  const submission = yield* conversation.submit({ type: 'input', content: 'Weather in Lisbon?' })
  const result = yield* submission.wait
  assert.equal(result.status, 'done')
  assert.equal(result.id, submission.id)
  assert.deepEqual(invocations, ['Lisbon'])
  const { items } = yield* conversation.entries({ order: 'ascending' }, 50)
  assert.deepEqual(items.map((entry) => entry.kind), [
    '$user', '$system', '$assistant', '$tool-result', '$assistant',
  ])
  assert.deepEqual(items.at(-1)?.model?.[0]?.['content'], [{ type: 'text', text: '21 degrees.' }])
  assert.equal((yield* faux.state).callCount, 2)
})

await Effect.runPromise(program.pipe(Effect.scoped, Effect.timeout('30 seconds')))
console.log('Packed package: Node ' + process.versions.node + ' ESM, core exports, tool round, and transcript passed.')
`

try {
  assert.ok(!isWithin(repository, temporary), 'Consumer must be outside the source checkout')
  mkdirSync(consumer)
  run(
    'node',
    [
      '--eval',
      "require('node:assert/strict').ok(Number(process.versions.node.split('.')[0]) >= 24, 'Node 24 or later is required')",
    ],
    consumer,
  )

  // npm pack runs the package's prepack build. Its output stays inside our cleanup boundary.
  run('npm', ['pack', '--ignore-scripts=false', '--pack-destination', temporary], repository)
  const archives = readdirSync(temporary).filter((name) => name.endsWith('.tgz'))
  assert.equal(archives.length, 1, 'npm pack must produce exactly one archive')
  const archive = join(temporary, archives[0]!)
  const packedFiles = new Set(
    run('tar', ['-tzf', archive], consumer, true)
      .trim()
      .split('\n')
      .map((name) => {
        assert.ok(name.startsWith('package/'), `Unexpected tarball entry: ${name}`)
        const path = name.slice('package/'.length)
        assert.ok(
          !/(^|\/)(?:test|tests|__tests__)(\/|$)|\.(?:test|spec)\.[^/]+$/.test(path),
          `Tests leaked into package: ${path}`,
        )
        return path
      }),
  )

  writeFileSync(
    join(consumer, 'package.json'),
    JSON.stringify(
      {
        name: 'effect-harness-package-consumer',
        private: true,
        type: 'module',
        dependencies: {
          'effect-harness': `file:${archive}`,
          effect: '4.0.5',
        },
        devDependencies: {
          typescript: '7.0.2',
          '@types/node': '^24.0.0',
        },
      },
      null,
      2,
    ),
  )
  // Deliberately use normal npm peer resolution and lifecycle scripts, as a consumer does.
  run('npm', ['install', '--ignore-scripts=false', '--no-audit', '--no-fund'], consumer)
  const packageDirectory = join(consumer, 'node_modules', 'effect-harness')
  assert.ok(
    isWithin(consumer, realpathSync(packageDirectory)),
    'Installed package is linked outside consumer',
  )
  const manifest = JSON.parse(readFileSync(join(packageDirectory, 'package.json'), 'utf8')) as {
    exports: Record<string, unknown>
    peerDependenciesMeta?: Record<string, { optional?: boolean }>
  }
  assert.ok(Object.hasOwn(manifest.exports, '.'), 'Package must declare its root export')
  for (const target of exportTargets(manifest.exports)) {
    assert.ok(
      target.startsWith('./') && !target.includes('*'),
      `Expected a concrete export target: ${target}`,
    )
    const installedTarget = resolve(packageDirectory, target)
    assert.ok(isWithin(packageDirectory, installedTarget), `Export escaped package: ${target}`)
    assert.ok(packedFiles.has(target.slice(2)), `Export missing from tarball: ${target}`)
    assert.ok(statSync(installedTarget).isFile(), `Export missing from installation: ${target}`)
  }
  for (const [name, metadata] of Object.entries(manifest.peerDependenciesMeta ?? {})) {
    if (metadata.optional) {
      assert.ok(
        !existsSync(join(consumer, 'node_modules', name)),
        `Optional integration unexpectedly installed: ${name}`,
      )
    }
  }

  writeFileSync(join(consumer, 'consumer.ts'), runtimeConsumer)
  writeFileSync(
    join(consumer, 'tsconfig.json'),
    JSON.stringify(
      {
        compilerOptions: {
          target: 'ES2022',
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          strict: true,
          skipLibCheck: false,
          types: ['node'],
          outDir: 'output',
        },
        files: ['consumer.ts'],
      },
      null,
      2,
    ),
  )
  // The same consumer is compiled against shipped declarations, then run as plain Node ESM.
  run(join(consumer, 'node_modules', '.bin', 'tsc'), ['--project', 'tsconfig.json'], consumer)
  run('node', [join(consumer, 'output', 'consumer.js')], consumer)
} finally {
  rmSync(temporary, { recursive: true, force: true })
}
