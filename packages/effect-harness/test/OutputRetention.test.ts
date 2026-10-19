import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import * as Retention from '../src/OutputRetention.ts'
import * as Buffer from '../src/internal/OutputBuffer.ts'

const encoder = new TextEncoder()
const countLines = (s: string) =>
  (s.match(/\n/g)?.length ?? 0) + (s.length > 0 && !s.endsWith('\n') ? 1 : 0)
// Whole-line oracle expressed in code points and line tokens, independent of the byte ring.
const oracle = (text: string, policy: Retention.Policy): string => {
  if (policy.maxBytes === 0 || policy.maxLines === 0) return ''
  const lines = text.match(/[^\n]*\n|[^\n]+$/g) ?? []
  if (policy.retain === 'tail') lines.reverse()
  let kept = ''
  for (const line of lines.slice(0, policy.maxLines)) {
    const joined = policy.retain === 'tail' ? line + kept : kept + line
    if (encoder.encode(joined).length <= policy.maxBytes) kept = joined
    else {
      if (kept === '') {
        const chars = Array.from(line)
        if (policy.retain === 'tail') chars.reverse()
        for (const c of chars) {
          if (encoder.encode(kept + c).length > policy.maxBytes) break
          kept = policy.retain === 'tail' ? c + kept : kept + c
        }
      }
      break
    }
  }
  return kept
}
describe('bounded incremental output', () => {
  it('matches whole-line head/tail oracle for every byte split and budget', () => {
    const texts = [
      '',
      '\n',
      '😀漢é',
      'α\nβ\n終\n',
      '\ufeffabc\n\n😀xyz\nlast',
      'long first line without newline\nsmall\n',
    ]
    for (const text of texts)
      for (const retain of ['head', 'tail'] as const) {
        const bytes = encoder.encode(text)
        for (let maxBytes = 0; maxBytes <= bytes.length + 1; maxBytes++)
          for (let maxLines = 0; maxLines < 5; maxLines++) {
            const policy = { retain, maxBytes, maxLines }
            for (let split = 0; split <= bytes.length; split++) {
              const buffer = Buffer.make(policy)
              Buffer.append(buffer, bytes.subarray(0, split))
              Buffer.snapshot(buffer)
              Buffer.append(buffer, bytes.subarray(split))
              Buffer.end(buffer)
              const actual = Buffer.snapshot(buffer)
              assert.strictEqual(
                actual.text,
                oracle(text, policy),
                JSON.stringify({ text, policy, split }),
              )
              assert.strictEqual(actual.totalBytes, bytes.length)
              assert.strictEqual(actual.totalLines, countLines(text))
              assert.strictEqual(actual.retainedBytes, encoder.encode(actual.text).length)
              assert.strictEqual(actual.retainedLines, countLines(actual.text))
              assert.strictEqual(actual.skippedBytes, bytes.length - actual.retainedBytes)
              assert.strictEqual(actual.skippedLines, countLines(text) - actual.retainedLines)
            }
          }
      }
  })
  it('huge first lines and tiny chunks never grow storage beyond the declared byte ring', () => {
    for (const retain of ['head', 'tail', 'full'] as const) {
      const buffer = Buffer.make({ retain, maxBytes: 31, maxLines: 2 })
      const block = encoder.encode('漢'.repeat(100000))
      Buffer.append(buffer, block)
      for (let i = 0; i < 20000; i++) Buffer.append(buffer, new Uint8Array([120]))
      Buffer.end(buffer)
      assert.strictEqual(buffer.storage.length, 35)
      assert.isAtMost(buffer.length, 35)
      assert.isAtMost(Buffer.snapshot(buffer).retainedBytes, 31)
      assert.strictEqual(Buffer.snapshot(buffer).totalBytes, block.length + 20000)
    }
  })
  it.effect('counts skipped tail bytes/newlines and rejects insufficient or head gaps', () =>
    Effect.gen(function* () {
      const buffer = yield* Retention.make({ retain: 'tail', maxBytes: 4, maxLines: 1 })
      yield* Retention.append(buffer, 'old')
      yield* Retention.append(buffer, '終\n', { bytes: 6, newlines: 2, endsWithNewline: true })
      assert.deepStrictEqual(yield* Retention.finish(buffer), {
        text: '終\n',
        totalBytes: 13,
        totalLines: 3,
        retainedBytes: 4,
        retainedLines: 1,
        skippedBytes: 9,
        skippedLines: 2,
        truncated: true,
      })
      const small = yield* Retention.append(buffer, '', {
        bytes: 1,
        newlines: 0,
        endsWithNewline: false,
      }).pipe(Effect.result)
      assert.isTrue(Result.isFailure(small))
      const head = yield* Retention.make({ retain: 'head', maxBytes: 4, maxLines: 1 })
      assert.isTrue(
        Result.isFailure(
          yield* Retention.append(head, '', { bytes: 5, newlines: 0, endsWithNewline: false }).pipe(
            Effect.result,
          ),
        ),
      )
    }),
  )
  it.effect('full refuses loss; invalid policies and final incomplete UTF8 are explicit', () =>
    Effect.gen(function* () {
      for (const maxBytes of [-1, 0.5, Infinity, NaN])
        assert.isTrue(
          Result.isFailure(
            yield* Retention.make({ retain: 'head', maxBytes, maxLines: 1 }).pipe(Effect.result),
          ),
        )
      const full = yield* Retention.make({ retain: 'full', maxBytes: 5, maxLines: 2 })
      yield* Retention.append(full, 'abc😀')
      const result = yield* Retention.finish(full).pipe(Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure.reason, 'overflow')
      const buffer = yield* Retention.make({ retain: 'tail', maxBytes: 20, maxLines: 2 })
      yield* Retention.append(buffer, new Uint8Array([0xf0, 0x9f]))
      assert.strictEqual(Retention.snapshot(buffer).text, '')
      assert.strictEqual((yield* Retention.finish(buffer)).text, '�')
      assert.strictEqual((yield* Retention.finish(buffer)).totalBytes, 3)
    }),
  )
})
