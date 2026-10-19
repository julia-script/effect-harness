/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The `$tool` task over scripted handlers: results, the ways a call is refused or breaks, and
 * recovery and abort. pi covers these through whole runs (`harness-tools.test.ts`,
 * `harness-tools-recovery.test.ts`); here the tests create the calls a tool round would.
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Layer, Schema } from 'effect'
import {
  conversation,
  eventually,
  open,
  openClosable,
  runTest,
  statusOf,
} from '../../task/test/support.ts'
import { Registry } from '../../task/index.ts'
import { Tool, ToolResult, ToolTask } from '../index.ts'
import { answer, call, Read, script, tasks, toolkit } from './support.ts'

const text = (message: unknown) =>
  Schema.decodeUnknownSync(
    Schema.Struct({ content: Schema.Array(Schema.Struct({ text: Schema.String })) }),
  )(message).content.map((block) => block.text)

describe('tool task', () => {
  test('runs a call and answers it with a result entry', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { session, scheduler } = yield* open(storage, [yield* tasks(yield* script)])
        const id = yield* call(session, yield* conversation(session), 'read', { path: 'a.txt' })
        const { state } = yield* scheduler.waitForTask(id)
        expect(state.outcome.status).toBe('completed')
        const { entry, message, diagnostics } = yield* answer(session, state.outcome)
        expect(entry).toMatchObject({ kind: ToolResult.kind, byTaskId: id })
        expect(message).toMatchObject({
          role: 'toolResult',
          toolCallId: 'call-1',
          toolName: 'read',
          isError: false,
          content: [{ type: 'text', text: 'contents of a.txt' }],
        })
        expect(diagnostics).toEqual([])
      }),
    ))

  test("passes the tool's config to its handler", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* script
        const configured = yield* open(storage, [
          yield* tasks(current, { config: { read: { root: '/srv/' } } }),
        ])
        const id = yield* call(
          configured.session,
          yield* conversation(configured.session),
          'read',
          {
            path: 'a.txt',
          },
        )
        const { state } = yield* configured.scheduler.waitForTask(id)
        const { message } = yield* answer(configured.session, state.outcome)
        expect(text(message)).toEqual(['contents of /srv/a.txt'])
      }),
    ))

  test('answers a call whose config does not decode with invalid_config', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- an invalid config is the point
        const config = { read: { root: 1 } } as unknown as ToolTask.Config<typeof toolkit.tools>
        const { session, scheduler } = yield* open(storage, [
          yield* tasks(yield* script, { config }),
        ])
        const id = yield* call(session, yield* conversation(session), 'read', { path: 'a.txt' })
        const { state } = yield* scheduler.waitForTask(id)
        expect(state.outcome.status).toBe('completed')
        const { diagnostics } = yield* answer(session, state.outcome)
        expect(diagnostics.map((diagnostic) => diagnostic.code)).toEqual(['invalid_config'])
      }),
    ))

  test('answers calls it cannot run without running them', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* script
        const { session, scheduler } = yield* open(storage, [yield* tasks(current)])
        const conversationId = yield* conversation(session)
        const cases = [
          { name: 'write', args: {}, code: 'tool_unavailable' },
          { name: 'read', args: { path: 1 }, code: 'invalid_arguments' },
          { name: 'read', args: {}, code: 'invalid_arguments' },
        ]
        for (const { name, args, code } of cases) {
          const id = yield* call(session, conversationId, name, args)
          const { state } = yield* scheduler.waitForTask(id)
          expect(state.outcome.status).toBe('completed')
          const { message, diagnostics } = yield* answer(session, state.outcome)
          expect(message).toMatchObject({ isError: true })
          expect(diagnostics.map((diagnostic) => diagnostic.code)).toEqual([code])
          expect(text(message).at(-1)).toStartWith('<harness>\n[error] ')
        }
        expect(current.runs).toEqual([])
      }),
    ))

  test("answers the tool's declared failure as an error result, and a defect as tool_error", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { session, scheduler } = yield* open(storage, [yield* tasks(yield* script)])
        const conversationId = yield* conversation(session)
        const failed = yield* scheduler.waitForTask(
          yield* call(session, conversationId, 'read', { path: 'missing' }),
        )
        expect(failed.state.outcome.status).toBe('completed')
        const declared = yield* answer(session, failed.state.outcome)
        expect(declared.message).toMatchObject({ isError: true })
        expect(text(declared.message)).toEqual(['{"reason":"missing not found"}'])
        const died = yield* scheduler.waitForTask(
          yield* call(session, conversationId, 'read', { path: 'boom' }),
        )
        expect(died.state.outcome).toMatchObject({
          status: 'failed',
          error: { message: 'Tool read failed' },
        })
        const defect = yield* answer(session, died.state.outcome)
        expect(defect.diagnostics).toMatchObject([{ code: 'tool_error' }])
        expect(defect.diagnostics[0]?.message).toContain('disk on fire')
      }),
    ))

  test('bounds the output to the tool limits and says what it dropped', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { session, scheduler } = yield* open(storage, [yield* tasks(yield* script)])
        const id = yield* call(session, yield* conversation(session), 'lines', { lines: 10 })
        const { state } = yield* scheduler.waitForTask(id)
        const { message, diagnostics } = yield* answer(session, state.outcome)
        expect(text(message)[0]).toBe('line 8\nline 9\nline 10')
        expect(diagnostics).toEqual([
          {
            severity: 'warn',
            code: 'truncated',
            message: 'Output truncated to its end: 7 lines, 49 bytes dropped',
          },
        ])
      }),
    ))

  test('answers an aborted call', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* script
        const { session, scheduler } = yield* open(storage, [yield* tasks(current)])
        const id = yield* call(session, yield* conversation(session), 'slow', {})
        yield* Deferred.await(current.started)
        yield* scheduler.abort(id)
        const { state } = yield* scheduler.waitForTask(id)
        expect(state.outcome.status).toBe('aborted')
        const { diagnostics } = yield* answer(session, state.outcome)
        expect(diagnostics).toMatchObject([{ code: 'aborted', message: 'Tool slow was aborted' }])
      }),
    ))
})

describe('tool recovery', () => {
  test('answers a call that is not replay-safe, interrupted after intent, without rerunning it', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const first = yield* script
        const before = yield* openClosable(storage, [yield* tasks(first)])
        const id = yield* call(before.session, yield* conversation(before.session), 'slow', {})
        yield* Deferred.await(first.started)
        yield* before.close
        const second = yield* script
        const { session, scheduler } = yield* open(storage, [yield* tasks(second)])
        const { state } = yield* scheduler.waitForTask(id)
        expect(state.outcome).toMatchObject({ status: 'failed' })
        const { diagnostics } = yield* answer(session, state.outcome)
        expect(diagnostics).toMatchObject([
          { code: 'interrupted', message: 'Tool slow was interrupted and may have partially run' },
        ])
        expect(second.runs).toEqual([])
      }),
    ))

  test('reruns a replay-safe call interrupted after intent', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const first = yield* script
        const before = yield* openClosable(storage, [yield* tasks(first)])
        const id = yield* call(before.session, yield* conversation(before.session), 'slow_safe', {})
        yield* Deferred.await(first.started)
        yield* before.close
        const second = yield* script
        yield* Deferred.done(second.release, Exit.void)
        const { session, scheduler } = yield* open(storage, [yield* tasks(second)])
        const { state } = yield* scheduler.waitForTask(id)
        expect(state.outcome.status).toBe('completed')
        expect(text((yield* answer(session, state.outcome)).message)).toEqual(['slow_safe done'])
        expect(second.runs).toEqual(['slow_safe'])
        yield* eventually(Effect.map(statusOf(session, id), (status) => status === 'terminal'))
      }),
    ))
})

describe('ToolTask.layer', () => {
  test('installs the tool task over the handlers a toolkit layer provides', () =>
    runTest(() =>
      Effect.gen(function* () {
        const handlers = toolkit.toLayer({
          read: ({ path }) => Effect.succeed(path),
          lines: () => Effect.succeed(''),
          slow: () => Effect.never,
          slow_safe: () => Effect.never,
        })
        const registry = yield* Registry.make()
        yield* Layer.build(
          ToolTask.layer(toolkit).pipe(
            Layer.provide(handlers),
            Layer.provide(Layer.succeed(Registry.Registry, registry)),
          ),
        )
        expect([...(yield* registry.snapshot).keys()]).toEqual([ToolTask.name])
      }),
    ))
})

describe('Tool', () => {
  test('offers a declaration to the model as a tool definition with JSON Schema parameters', () => {
    expect(Tool.definition(Read)).toMatchObject({
      name: 'read',
      description: 'Read a file',
      parameters: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] },
    })
    expect(Read.replay).toBe('unsafe')
    expect(Read.outputLimits).toEqual(Tool.defaultOutputLimits)
  })
})
