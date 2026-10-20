/**
 * A loop in its own process, as a server deployment runs it: the harness over WebSockets at
 * `/harness` on `PORT`, its store in the SQLite file `DB`, the coding tools in `WORKSPACE`, and
 * the OpenAI-compatible model at `MODEL_URL`. Tests kill it to see what a crash leaves.
 *
 *   PORT=3000 DB=./agent.db WORKSPACE=./work MODEL_URL=http://127.0.0.1:4000 bun server.ts
 */
import { BunHttpServer, BunRuntime, BunServices } from '@effect/platform-bun'
import { SqliteClient } from '@effect/sql-sqlite-bun'
import { Config, Duration, Effect, Layer, Redacted, Schema } from 'effect'
import { FetchHttpClient, HttpRouter } from 'effect/http'
import { CodingHandlers, CodingTools } from '../../../coding/index.ts'
import { Extension, HarnessServer } from '../../../harness/index.ts'
import { Model } from '../../../model/index.ts'
import { OpenAi } from '../../../openai/index.ts'
import { Storage } from '../../../storage/index.ts'
import { Tool, Toolkit } from '../../../tool/index.ts'
import { HarnessTransport } from '../../index.ts'

const Wait = Tool.make('wait', {
  description: 'Wait some seconds.',
  parameters: Schema.Struct({ seconds: Schema.Finite }),
  success: Schema.String,
})

const waiting = Toolkit.make(Wait)

const program = Effect.gen(function* () {
  const port = yield* Config.Int('PORT')
  const db = yield* Config.String('DB')
  const workspace = yield* Config.String('WORKSPACE')
  const modelUrl = yield* Config.String('MODEL_URL')
  const cwd = { cwd: workspace }
  const coding = Extension.make('coding-tools', {
    toolkit: CodingTools.toolkit,
    config: { read: cwd, write: cwd, edit: cwd, bash: cwd },
  })
  const loop = HarnessServer.layer({
    extensions: [coding, Extension.make('waiting', { toolkit: waiting })],
    settings: {
      model: { provider: 'local', modelId: 'test-model' },
      retry: { baseDelayMs: 200, maxDelayMs: 1_000 },
      compaction: {
        modelOverrides: {
          'local/test-model': {
            reserveTokens: 120_000,
            keepRecentTokens: 2_000,
            backgroundTokens: 0,
          },
        },
      },
    },
  }).pipe(
    Layer.provide([
      HarnessTransport.layerWebSocketServer('/harness'),
      CodingHandlers.layer,
      waiting.toLayer({
        wait: ({ seconds }) =>
          Effect.as(Effect.sleep(Duration.seconds(seconds)), `Waited ${seconds} seconds`),
      }),
      Model.layer([
        OpenAi.provider({
          name: 'local',
          apiUrl: `${modelUrl}/v1`,
          apiKey: Redacted.make('sk-test'),
          models: [{ id: 'test-model' }],
        }),
      ]),
      Storage.layerSql().pipe(Layer.provide(SqliteClient.layer({ filename: db }))),
    ]),
    Layer.provide([FetchHttpClient.layer, BunServices.layer]),
  )
  return yield* HttpRouter.serve(loop, { disableLogger: true }).pipe(
    Layer.provide(BunHttpServer.layer({ port, hostname: '127.0.0.1' })),
    Layer.launch,
  )
})

BunRuntime.runMain(program)
