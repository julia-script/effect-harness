import * as TestSchema from 'effect/testing/TestSchema'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as SystemPatch from 'effect-harness/SystemPatch'

import * as Executor from 'effect-harness/Executor'

describe('SystemPatchSchema', () => {
  it.effect('managed patch codecs preserve opaque JSON arguments and shared request schemas', () =>
    Effect.gen(function* () {
      assert.strictEqual(Executor.Request.fields.tools.value, SystemPatch.ToolDeclaration)
      const wire = {
        sections: { deleted: null, active: 'text' },
        toolsRemoved: ['old'],
        toolsAdded: [
          {
            name: 'search',
            parameters: { type: 'object', arbitrary: [1, null, true] },
            provider: {
              id: 'custom.search',
              name: 'remote',
              args: { opaque: ['native', { extra: 7 }] },
            },
          },
        ],
      }
      const codec = Schema.fromJsonString(Schema.toCodecJson(SystemPatch.SystemPatch))
      const patchAsserts = new TestSchema.Asserts(codec)
      yield* patchAsserts.decoding().succeedEffect(JSON.stringify(wire), wire)
      yield* patchAsserts.encoding().succeedEffect(wire, JSON.stringify(wire))
      const declarationAsserts = new TestSchema.Asserts(SystemPatch.ToolDeclaration)
      yield* declarationAsserts.decoding().failEffect(
        {
          name: 'bad',
          parameters: [],
          provider: { id: 'missingNamespace', name: 'bad', args: {} },
        },
        'Expected object\n  at ["parameters"]',
      )
      yield* declarationAsserts.decoding().failEffect(
        {
          name: 'bad',
          parameters: {},
          provider: { id: 'custom.bad', name: 'bad', args: () => 'opaque function' },
        },
        'Expected JSON value\n  at ["provider"]["args"]',
      )
    }),
  )
})
