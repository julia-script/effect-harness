/** Named tools, positional sections, wrappers and checkpointed approval decisions. */
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Conversation from 'effect-harness/Conversation'
import type * as Extension from 'effect-harness/Extension'
import * as Hook from 'effect-harness/Hook'
import * as HookError from 'effect-harness/HookError'
import * as Invocation from 'effect-harness/Invocation'
import * as Submission from 'effect-harness/Submission'
import { TaskRuntime } from 'effect-harness/TaskRuntime'
import * as ToolError from 'effect-harness/ToolError'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as Memory from 'effect-harness/storage/Memory'
import * as Runtime from './Runtime.ts'

const SearchIssues = Tool.make('search_issues', {
  description: 'Search the example issue tracker without changing it.',
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.String,
  failure: ToolError.ToolError,
}).addDependency(Invocation.ToolCall)
const Deploy = Tool.make('deploy', {
  description: 'Simulate a deployment; this example makes no external changes.',
  parameters: Schema.Struct({ version: Schema.String }),
  success: Schema.String,
  failure: ToolError.ToolError,
})
const toolkit = Toolkit.make(SearchIssues, Deploy)

export const Timing = Schema.Struct({ tool: Schema.String, milliseconds: Schema.Finite })
export type Timing = typeof Timing.Type

export const Result = Schema.Struct({
  selectedTools: Schema.Array(Schema.String),
  replay: Schema.Struct({
    search_issues: Schema.Literal('safe'),
    deploy: Schema.Literal('unsafe'),
  }),
  committedSections: Schema.Array(Schema.String),
  calls: Schema.Array(Schema.String),
  timings: Schema.Array(Timing),
  approvalRequests: Schema.Int,
  firstWriterWins: Schema.Boolean,
})

/** The selected wrapper decorates whichever named registration wins resolution. */
export const timing = (measurements: Ref.Ref<ReadonlyArray<Timing>>): Extension.Extension => ({
  name: 'timing',
  toolWraps: [
    {
      name: SearchIssues.name,
      wrap: (registration) =>
        Effect.succeed({
          ...registration,
          execute: Effect.fn('tour.timedSearch')(function* (args: unknown, id: string) {
            const started = yield* Clock.currentTimeMillis
            return yield* registration
              .execute(args, id)
              .pipe(
                Effect.ensuring(
                  Clock.currentTimeMillis.pipe(
                    Effect.flatMap((ended) =>
                      Ref.update(measurements, (values) => [
                        ...values,
                        { tool: registration.tool.name, milliseconds: ended - started },
                      ]),
                    ),
                  ),
                ),
              )
          }),
        }),
    },
  ],
})

export const operations = Effect.fn('tour.operations')(function* (
  version: string,
  calls: Ref.Ref<ReadonlyArray<string>>,
) {
  const tools = yield* ToolRegistration.bind(toolkit, {
    search_issues: { replay: 'safe' },
    deploy: { replay: 'unsafe' },
  }).pipe(
    Effect.provide(
      toolkit.toLayer({
        search_issues: Effect.fn('tour.searchIssues')(function* ({ query }) {
          yield* (yield* Invocation.ToolCall).output(`Searching for ${query}\n`)
          yield* Ref.update(calls, (values) => [...values, `search:${version}`])
          return `${version}: issue matching ${query}`
        }),
        deploy: ({ version: release }) =>
          Ref.update(calls, (values) => [...values, `deploy:${version}`]).pipe(
            Effect.as(`Simulated deployment ${release} using ${version}`),
          ),
      }),
    ),
  )
  return {
    name: 'ops',
    tools,
    sections: [
      { key: 'project_context', render: () => Effect.succeed(`Project context ${version}`) },
    ],
  } satisfies Extension.Extension
})

/** Hooks may replay. The first committed answer wins over replacement code's candidate. */
export const approval = Effect.fn('tour.approval')(function* (options: {
  readonly candidate: boolean
  readonly asked: Ref.Ref<number>
  readonly firstWriterWins: Ref.Ref<boolean>
  readonly afterMemo?: Effect.Effect<void>
}) {
  const handlers = yield* Hook.bind(
    {
      beforeTool: Effect.fn('tour.approveDeployment')(
        function* (call: Hook.Handlers.ToolInput) {
          if (call.name !== Deploy.name) return undefined
          const runtime = yield* TaskRuntime
          let approved = yield* runtime.memo('approval:deploy')
          if (approved === undefined) {
            yield* Ref.update(options.asked, (count) => count + 1)
            approved = yield* runtime.memo('approval:deploy', options.candidate)
          }
          const competing = yield* runtime.memo('approval:deploy', false)
          yield* Ref.set(options.firstWriterWins, competing === approved)
          yield* options.afterMemo ?? Effect.void
          return approved === true
            ? undefined
            : Hook.ToolDecision.Block({ block: 'Deployment denied.' })
        },
        Effect.mapError(
          (cause) =>
            new HookError.HookError({
              reason: new HookError.HookFailureError({ message: cause.message, cause }),
            }),
        ),
      ),
    },
    [TaskRuntime],
  )
  return {
    name: 'approval',
    hooks: [{ operation: 'tool', handlers }],
  } satisfies Extension.Extension
})

const provider: Runtime.Provider = {
  generateText: () => Effect.succeed([{ type: 'text', text: 'Summary' }, Runtime.finish('stop')]),
  streamText: ({ prompt }) => {
    const command = Runtime.lastUserText(prompt)
    const id = `operation:${command}`
    const completed = prompt.content.some(
      (message) =>
        message.role === 'tool' &&
        message.content.some((part) => part.type === 'tool-result' && part.id === id),
    )
    if (completed) return Runtime.answer(`Completed ${command}`)
    const deploy = command.startsWith('deploy:')
    return Stream.fromIterable([
      {
        type: 'tool-call' as const,
        id,
        name: deploy ? Deploy.name : SearchIssues.name,
        params: deploy ? { version: command } : { query: command },
        providerExecuted: false,
      },
      Runtime.finish('tool-calls'),
    ])
  },
}

export const run = Effect.scoped(
  Effect.gen(function* () {
    const calls = yield* Ref.make<ReadonlyArray<string>>([])
    const measurements = yield* Ref.make<ReadonlyArray<Timing>>([])
    const initial = yield* operations('v1', calls)
    const { harness, registry } = yield* Runtime.open({
      provider,
      extensions: [initial, timing(measurements)],
      agent: { extensions: ['ops', 'timing'], tools: { remove: [Deploy.name] } },
    })
    const conversation = yield* harness.root
    yield* Runtime.ask(conversation, 'search:first')
    const before = yield* Conversation.snapshot(conversation)
    const system = yield* Effect.forEach(
      before.entries.filter((entry) => entry.kind === 'harness.system' && entry.data !== undefined),
      (entry) => Schema.decodeUnknownEffect(Schema.toCodecJson(Conversation.Data))(entry.data),
    )
    const offered = system.flatMap(
      (entry) => entry.harness.system?.toolsAdded?.map((tool) => tool.name) ?? [],
    )
    yield* Runtime.check(
      offered.includes(SearchIssues.name) && !offered.includes(Deploy.name),
      'Read-only selection offered deploy',
    )
    yield* registry.install([yield* operations('v2', calls)])
    yield* Runtime.ask(conversation, 'search:second')
    const after = yield* Conversation.snapshot(conversation)
    const history = yield* Effect.forEach(
      after.entries.filter((entry) => entry.kind === 'harness.system' && entry.data !== undefined),
      (entry) => Schema.decodeUnknownEffect(Schema.toCodecJson(Conversation.Data))(entry.data),
    )
    const sections = history.flatMap((entry) =>
      Object.values(entry.harness.system?.sections ?? {}).filter((value) => value !== null),
    )
    yield* Runtime.check(
      sections.some((text) => text.includes('v1')) && sections.some((text) => text.includes('v2')),
      'Section changes were not committed',
    )
    yield* Runtime.check(
      (yield* Ref.get(calls)).join(',') === 'search:v1,search:v2',
      'Hot replacement missed the next call',
    )

    const store = yield* Memory.make
    const entered = yield* Deferred.make<void>()
    const asked = yield* Ref.make(0)
    const firstWriterWins = yield* Ref.make(false)
    const firstApproval = yield* approval({
      candidate: true,
      asked,
      firstWriterWins,
      afterMemo: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    })
    const first = yield* Runtime.open({
      store,
      provider,
      extensions: [yield* operations('before-restart', calls), firstApproval],
    })
    const root = yield* first.harness.root
    const admitted = yield* Conversation.submit(root, 'deploy:approved')
    yield* Deferred.await(entered)
    const pending = yield* Conversation.snapshot(root)
    yield* Runtime.check(
      pending.tasks.some(
        (task) => task.kind === 'harness.tool' && task.memos?.['approval:deploy'] === true,
      ),
      'Approval memo was not persisted',
    )
    yield* first.harness.close
    const replacement = yield* approval({ candidate: false, asked, firstWriterWins })
    const second = yield* Runtime.open({
      store,
      provider,
      extensions: [yield* operations('after-restart', calls), replacement],
    })
    yield* second.harness.resume
    const settled = yield* Submission.wait({ id: admitted.id, harness: second.harness })
    yield* Runtime.check(settled.status === 'done', 'Recovered approval failed to execute')
    yield* Runtime.check((yield* Ref.get(asked)) === 1, 'Approval was requested twice')
    yield* Runtime.check(
      (yield* Ref.get(firstWriterWins)) && (yield* Ref.get(calls)).includes('deploy:after-restart'),
      'Saved approval did not survive code replacement',
    )
    return yield* Schema.decodeEffect(Result)({
      selectedTools: [SearchIssues.name],
      replay: { search_issues: 'safe', deploy: 'unsafe' },
      committedSections: sections,
      calls: yield* Ref.get(calls),
      timings: yield* Ref.get(measurements),
      approvalRequests: yield* Ref.get(asked),
      firstWriterWins: yield* Ref.get(firstWriterWins),
    })
  }),
)
