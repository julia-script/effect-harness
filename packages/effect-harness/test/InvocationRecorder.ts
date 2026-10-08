import * as Effect from 'effect/Effect'

import * as Ref from 'effect/Ref'

import { ToolCall, type Diagnostic } from 'effect-harness/Invocation'

export const recording = Effect.gen(function* () {
  const output = yield* Ref.make('')
  const diagnostics = yield* Ref.make<ReadonlyArray<Diagnostic>>([])
  const api = ToolCall.of({
    id: 'test',
    output: (text) =>
      Ref.update(
        output,
        (value) => value + (typeof text === 'string' ? text : new TextDecoder().decode(text)),
      ),
    details: () => Effect.void,
    diagnostic: (diagnostic) => Ref.update(diagnostics, (values) => [...values, diagnostic]),
  })
  return { output, diagnostics, api }
})
