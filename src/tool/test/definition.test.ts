import { describe, expect, test } from 'bun:test'
import { Schema } from 'effect'
import { Tool } from '../index.ts'

describe('Tool.definition', () => {
  test('offers a tool without parameters an empty object', () => {
    const tool = Tool.make('who', { description: 'List agents.', parameters: Schema.Struct({}) })
    expect(Tool.definition(tool).parameters).toEqual({
      type: 'object',
      properties: {},
      additionalProperties: false,
    })
  })

  test('keeps the schema of a tool with parameters', () => {
    const tool = Tool.make('read', {
      description: 'Read a file.',
      parameters: Schema.Struct({ path: Schema.String }),
    })
    expect(Tool.definition(tool).parameters).toMatchObject({
      type: 'object',
      properties: { path: { type: 'string' } },
    })
  })
})
