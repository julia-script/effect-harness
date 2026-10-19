/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The tool's description and the `codemode` section, ported from pi's
 * `codemode-describe.test.ts`.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Schema } from 'effect'
import { entriesOf, ownerless, runTest } from '../../harness/test/generationSupport.ts'
import { Faux } from '../../model/index.ts'
import { Tool } from '../../tool/index.ts'
import { Codemode } from '../index.ts'
import { section } from '../internal/describe.ts'
import { openChat, world } from './support.ts'

const tool = (name: string, description: string) =>
  Tool.make(name, {
    description,
    parameters: Schema.Struct({ path: Schema.String.annotate({ description: 'File path' }) }),
    success: Schema.String,
  })

const GITHUB: Codemode.Namespace = {
  name: 'github',
  description: 'GitHub MCP server',
  prefix: 'mcp__github__',
}

describe('code mode description and section', () => {
  test('keeps the tool description the same everywhere', () => {
    const text = Codemode.Codemode.description
    expect(text).toContain('`await tools.<name>({ ...args })`')
    expect(text).toContain('searchTools(query')
    expect(text).toContain('The `codemode` section of the system prompt lists the tools')
  })

  test('lists callable tools as declarations, grouped by namespace', () => {
    const shown =
      section(
        [tool('read', 'Read a file.'), tool('mcp__github__list_issues', 'List issues.')],
        [GITHUB],
        3000,
      ) ?? ''
    expect(shown).toContain('### `read`\nRead a file.')
    expect(shown).toContain('read(args: {')
    expect(shown).toContain('## github\nGitHub MCP server')
    expect(shown.indexOf('### `read`')).toBeLessThan(shown.indexOf('## github'))
    expect(shown.indexOf('## github')).toBeLessThan(shown.indexOf('### `mcp__github__list_issues`'))
    expect(shown).toContain('Promise<string>')
  })

  test('lists what fits the budget, every namespace before any is complete', () => {
    const github = Array.from({ length: 30 }, (_, index) =>
      tool(`mcp__github__tool${index}`, 'x'.repeat(200)),
    )
    const shown = section([tool('read', 'Read a file.'), ...github], [GITHUB], 400) ?? ''
    expect(shown).toContain('### `read`')
    expect(shown).toContain('## github (some tools not listed)')
    expect(shown).not.toContain('tool29')
    expect(section(github, [GITHUB], 10)).toContain('## github (tools not listed)')
    expect(section([], [GITHUB], 3000)).toBeUndefined()
  })

  test('uses the longest matching namespace prefix', () => {
    const shown =
      section([tool('mcp__github__x', 'x')], [{ name: 'git', prefix: 'mcp__git' }, GITHUB], 3000) ??
      ''
    expect(shown).toContain('## github')
    expect(shown).not.toContain('## git\n')
  })

  test("renders the section from the conversation's callable tools into the system prompt", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, faux } = yield* openChat(storage, yield* world)
        yield* faux.setResponses([Faux.message('hi')])
        const conversation = yield* ownerless(harness)
        yield* Effect.flatMap(
          Effect.orDie(conversation.submit({ type: 'input', content: 'go' })),
          (submission) => submission.wait,
        )
        const system = (yield* entriesOf(harness, conversation.id)).find(
          (entry) => entry.kind === 'pi.system',
        )
        const sections: unknown = system?.model?.[0]?.['sections']
        const shown = JSON.stringify(sections)
        expect(shown).toContain('### `echo`')
        // Deferred tools are found with searchTools(), not listed.
        expect(shown).not.toContain('### `later`')
        // The code mode tool itself is not callable from scripts.
        expect(shown).not.toContain('### `codemode`')
      }),
    ))
})
