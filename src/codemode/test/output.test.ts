/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * What a script's result shows the model, and finding tools from a script, ported from pi's
 * `codemode.test.ts`.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, FileSystem } from 'effect'
import { BunServices } from '@effect/platform-bun'
import { entriesOf, runTest } from '../../harness/test/generationSupport.ts'
import type { Details } from '../Codemode.ts'
import { runScript } from './scripts.ts'
import { PNG, world } from './support.ts'
import type { Options } from './support.ts'

/** Run `code` once; the whole text of its result, its details and its content. */
const run = (code: string, options: Options = {}) =>
  runTest((storage) =>
    Effect.gen(function* () {
      const current = yield* world
      current.block = false
      const { harness, conversation, result } = yield* runScript(storage, current, code, options)
      const entries = yield* entriesOf(harness, conversation.id)
      const message = entries.find((entry) => entry.kind === 'pi.tool-result')?.model?.[0] ?? {}
      const content: unknown = message['content']
      const blocks: ReadonlyArray<{ readonly type: string; readonly text?: string }> =
        Array.isArray(content) ? content : []
      return {
        text: blocks.map((block) => block.text ?? `<${block.type}>`).join(''),
        blocks,
        isError: result.isError,
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- the tool's details
        details: result.details as Details | undefined,
      }
    }),
  )

const readFile = (path: string) =>
  Effect.runPromise(
    Effect.flatMap(FileSystem.FileSystem, (fs) => fs.readFile(path)).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide
      Effect.provide(BunServices.layer),
    ),
  )

describe('code mode output', () => {
  test('lays out text items, console lines, and the returned value under a header', async () => {
    const { text } = await run(
      'text("one"); console.log("log", 1); text({ two: 2 }); return "three";',
    )
    expect(text).toMatch(/^Script completed\nWall time [\d.]+ seconds\nOutput:\n/u)
    expect(text).toContain(
      '==> text 1/3 <==\none\n==> text 2/3 <==\n{"two":2}\n==> text 3/3 <==\nthree',
    )
    expect(text).toContain('<console_output>\nlog 1\n</console_output>')
  })

  test('cuts text past the token budget to its start and end, with the full text in a file', async () => {
    const { text, details } = await run(
      '// @options: {"max_output_tokens": 20}\ntext("BEGIN" + "x".repeat(1000) + "END");',
    )
    expect(text).toContain('Warning: truncated output')
    expect(text).toContain('BEGIN')
    expect(text).toContain('END')
    const path = details?.fullOutputPath ?? ''
    expect(text).toContain(`[Full output: ${path}`)
    expect(new TextDecoder().decode(await readFile(path))).toBe(`BEGIN${'x'.repeat(1000)}END`)
  })

  test("keeps a result inside the tool's output limits whatever max_output_tokens says", async () => {
    const long = await run(
      '// @options: {"max_output_tokens": 2000000}\nreturn Array.from({ length: 200000 }, () => "a").join("\\n");',
    )
    expect(long.text).toContain('Warning: truncated output')
    expect(long.text.split('\n').length).toBeLessThan(100_000)
    expect(long.details?.fullOutputPath).toBeDefined()
  })

  test('saves images to files and names them next to the image', async () => {
    const { text, blocks } = await run(`image("data:image/png;base64,${PNG}");`)
    const saved = /\[Image saved to (\S+) \(image\/png, \d+ bytes\)\]/u.exec(text)
    expect(saved).not.toBeNull()
    expect(Buffer.from(await readFile(saved?.[1] ?? '')).toString('base64')).toBe(PNG)
    expect(blocks.some((block) => block.type === 'image')).toBe(true)
  })

  test('reports a failed script with its error and the calls already made', async () => {
    const { text, isError } = await run('await tools.echo({ text: "a" }); throw new Error("boom");')
    expect(isError).toBe(true)
    expect(text).toMatch(/^Script failed\n/u)
    expect(text).toContain('Error: boom')
    expect(text).toContain('Tool calls made before the failure (they are not undone): echo (ok)')
  })

  test('answers an invalid options line with an error result', async () => {
    const { text, isError } = await run('// @options: {"timeout_ms": "soon"}\nreturn 1')
    expect(isError).toBe(true)
    expect(text).toContain('Invalid script')
  })
})

describe('finding tools from a script', () => {
  test('searchTools(), describeTool(), describeNamespace() and ALL_TOOLS', async () => {
    const { text } = await run(
      `const found = await searchTools("issues");
      const inNamespace = await searchTools("echo", { namespace: "github" });
      return {
        found: found.map((tool) => tool.name),
        inNamespace,
        described: (await describeTool("echo")).includes("echo(args:"),
        missing: (await describeTool("nope")) ?? null,
        namespace: await describeNamespace("github"),
        all: ALL_TOOLS.map((tool) => tool.name).includes("mcp__github__list_issues"),
      };`,
      {
        namespaces: [{ name: 'github', description: 'GitHub MCP server', prefix: 'mcp__github__' }],
      },
    )
    expect(
      JSON.parse(text.replace(/^Script completed\nWall time [\d.]+ seconds\nOutput:\n/u, '')),
    ).toEqual({
      found: ['mcp__github__list_issues'],
      inNamespace: [],
      described: true,
      missing: null,
      namespace: {
        name: 'github',
        description: 'GitHub MCP server',
        tools: ['mcp__github__list_issues'],
      },
      all: true,
    })
  })
})
