/** Run a background-response adapter against a local scripted transport, without credentials. */
import * as Clock from 'effect/Clock'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Toolkit from 'effect/ai/Toolkit'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Identity from 'effect-harness/Identity'
import * as Usage from 'effect-harness/Usage'
import * as OpenAiClient from 'effect-harness/provider-openai/OpenAiClient'
import * as DeferredResponse from 'effect-harness/provider-openai/DeferredResponse'

const program = Effect.gen(function* () {
  let calls = 0
  const transport = HttpClient.make((request) => {
    calls++
    const complete = calls > 1
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        new Response(
          JSON.stringify({
            id: 'scripted-response',
            model: 'gpt-4.1',
            created_at: 0,
            status: complete ? 'completed' : 'queued',
            output: complete
              ? [
                  {
                    type: 'message',
                    id: 'message',
                    role: 'assistant',
                    status: 'completed',
                    content: [
                      {
                        type: 'output_text',
                        text: 'The deferred response completed.',
                        annotations: [],
                      },
                    ],
                  },
                ]
              : [],
          }),
          { headers: { 'content-type': 'application/json' } },
        ),
      ),
    )
  })
  const client = yield* OpenAiClient.make({ apiUrl: 'https://scripted.invalid/v1' }).pipe(
    Effect.provideService(HttpClient.HttpClient, transport),
  )
  const adapter = yield* DeferredResponse.make({ model: 'gpt-4.1', pollIntervalMs: 1 }).pipe(
    Effect.provideService(OpenAiClient.OpenAiClient, client),
  )
  // A durable runtime must save immutable request/configuration identity BEFORE start,
  // then save the encoded handle and deadline before polling. This example has no storage.
  const prepared = {
    requestId: Identity.RequestId.make('task:1/request:1/config:1'),
    attempt: Usage.AttemptId.make(0),
    prompt: Prompt.make('Explain background responses.'),
    toolkit: Toolkit.empty,
  }
  const start = yield* adapter.start(prepared)
  if (start._tag === 'Complete') return yield* Console.log(start.response.text)
  const now = yield* Clock.currentTimeMillis
  yield* Effect.sleep(Math.max(0, start.nextPollAt - now))
  const result = yield* adapter.poll(start.handle, start.nextPollAt, prepared)
  yield* Console.log(
    result._tag === 'Complete' ? result.response.text : `Pending until ${result.nextPollAt}`,
  )
  // User cancellation requires a persisted abort mark and late-completion fencing BEFORE
  // bounded adapter.cancel(handle). Closing this local scope alone must not remote-cancel.
})

await Effect.runPromise(program)
