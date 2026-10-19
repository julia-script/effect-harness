import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as Cause from 'effect/Cause'
import * as Option from 'effect/Option'
import { readFileSync } from 'node:fs'
import * as Photon from '@cf-wasm/photon/node'
import sharp from 'sharp'
import type * as Images from '../src/ImagePreparation.js'
import * as NodeImages from '../src/images-node/index.js'
import * as WasmImages from '../src/images-wasm/index.js'
import { fixture } from './fixtures/images-native/Conformance.js'

const request: Images.EncodingRequest = {
  mimeType: 'image/png',
  dimensions: { width: 2, height: 1 },
  orientation: 1,
  frame: 'first',
  transparency: 'preserve',
}

for (const stage of ['decode', 'encode'] as const) {
  for (const outcome of ['success', 'failure', 'interruption'] as const) {
    it.effect(`explicit owning Scope.close waits for sharp ${stage} (${outcome})`, () =>
      Effect.gen(function* () {
        const entered = Promise.withResolvers<void>()
        const gate = Promise.withResolvers<void>()
        const events: Array<string> = []
        const destroyed = new Set<number>()
        let created = 0
        const targetHandle = stage === 'decode' ? 1 : 2
        const codec: typeof sharp = new Proxy(sharp, {
          apply(target, _receiver, args: Parameters<typeof sharp>) {
            const own = ++created
            // Cause a real libvips raw-loader rejection when testing encoder failure.
            const handle =
              outcome === 'failure' && stage === 'encode' && own === 2
                ? target(new Uint8Array(1), args[1])
                : target(...args)
            const destroy = handle.destroy.bind(handle)
            handle.destroy = (error) => {
              assert.isFalse(destroyed.has(own))
              destroyed.add(own)
              events.push(`destroy-${own}`)
              return destroy(error)
            }
            handle.toBuffer = new Proxy(handle.toBuffer.bind(handle), {
              apply(buffer, receiver, args: [{ resolveWithObject: true }]) {
                const pending = buffer.call(receiver, ...args)
                if (own !== targetHandle) return pending
                entered.resolve()
                events.push('terminal-start')
                return pending.then(
                  async (value) => {
                    await gate.promise
                    events.push('terminal-settled')
                    return value
                  },
                  async (error: unknown) => {
                    await gate.promise
                    events.push('terminal-settled')
                    throw error
                  },
                )
              },
            })
            return handle
          },
        })
        const scope = yield* Scope.make()
        const adapter = NodeImages.makeAdapter(codec, WasmImages.makeAdapter(Photon))
        let decoded: Images.DecodedImage | undefined
        const operation = Effect.gen(function* () {
          decoded = yield* adapter.decode(
            stage === 'decode' && outcome === 'failure'
              ? readFileSync(
                  new URL('./fixtures/images-strict/truncated-entropy-90.jpg', import.meta.url),
                )
              : fixture('opaque.png'),
            stage === 'decode' && outcome === 'failure' ? 'image/jpeg' : 'image/png',
            4096,
          )
          if (stage === 'encode') return yield* decoded.encode(request)
          return decoded
        })
        const fiber = yield* Scope.provide(operation, scope).pipe(
          Effect.forkChild({ startImmediately: true }),
        )
        yield* Effect.promise(() => entered.promise)
        if (outcome === 'interruption') fiber.interruptUnsafe()
        const closing = yield* Scope.close(scope, Exit.void).pipe(
          Effect.forkChild({ startImmediately: true }),
        )
        // Give the other fiber an actual event-loop turn to begin its finalizer.
        yield* Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)))
        assert.strictEqual(destroyed.size, 0)
        assert.isUndefined(closing.pollUnsafe())
        if (stage === 'encode') {
          const rejected = yield* decoded!.encode(request).pipe(Effect.flip)
          assert.strictEqual(rejected._tag, 'ImageEncodeFailed')
        }
        gate.resolve()
        const result = yield* Fiber.await(fiber)
        yield* Fiber.join(closing)
        assert.strictEqual(created, targetHandle)
        assert.strictEqual(destroyed.size, created)
        assert.isAbove(events.indexOf('destroy-1'), events.indexOf('terminal-settled'))
        if (outcome === 'failure') {
          assert.isTrue(Exit.isFailure(result))
          if (Exit.isFailure(result)) {
            const failure = Cause.findErrorOption(result.cause)
            assert.isTrue(Option.isSome(failure))
            if (Option.isSome(failure))
              assert.strictEqual(
                failure.value._tag,
                stage === 'decode' ? 'ImageDecodeFailed' : 'ImageEncodeFailed',
              )
          }
        } else if (outcome === 'success') assert.isTrue(Exit.isSuccess(result))
        else assert.isTrue(Exit.isFailure(result))
        if (decoded) {
          const rejected = yield* decoded.encode(request).pipe(Effect.flip)
          assert.strictEqual(rejected._tag, 'ImageEncodeFailed')
          assert.strictEqual(created, targetHandle)
        }
      }),
    )
  }
}
