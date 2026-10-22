/**
 * One live demo: a harness loop running in this tab, its transcript, its raw agent events, and the
 * controls to talk to it or crash it. `make` builds the Submodel for one agent; the loop is a
 * Managed Resource that lives while the demo is on screen.
 */
import { Array, Effect, Match, Number, Option, Schema, Stream, String } from 'effect'
import type { Scope } from 'effect'
import { AgentEvent } from 'effect-harness/protocol'
import { Command, ManagedResource, Submodel, Subscription, type Update } from 'foldkit'
import type { Html, HtmlBuilder } from 'foldkit/html'
import { defineMessageUnion } from 'foldkit/message'
import { defineTaggedUnion } from 'foldkit/schema'
import { modifyFields } from 'foldkit/struct'
import type { Lab } from './lab.ts'
import * as Transcript from './transcript.ts'

// MODEL

export const Loop = defineTaggedUnion({
  Off: {},
  /** `epoch` counts the loop's lives: the one starting, running, or last run. */
  Starting: { epoch: Schema.Finite },
  Running: { epoch: Schema.Finite },
  Crashed: { epoch: Schema.Finite },
  Failed: { epoch: Schema.Finite, reason: Schema.String },
})
export type Loop = typeof Loop.Type

export const Model = Schema.Struct({
  loop: Loop,
  transcript: Transcript.Transcript,
  log: Schema.Array(Transcript.LogLine),
  input: Schema.String,
  crashes: Schema.Finite,
})
export type Model = typeof Model.Type

// MESSAGE

export const Message = defineMessageUnion({
  AcquiredLab: {},
  ReleasedLab: {},
  FailedAcquireLab: { reason: Schema.String },
  ClickedStart: {},
  ClickedCrash: {},
  CompletedStartLoop: {},
  FailedStartLoop: { reason: Schema.String },
  CompletedCrashLoop: {},
  UpdatedInput: { value: Schema.String },
  SubmittedInput: {},
  ClickedPrompt: { content: Schema.String },
  CompletedSubmit: {},
  FailedSubmit: { reason: Schema.String },
  ReceivedEvents: { events: Schema.Array(AgentEvent.AgentEvent) },
  LostEvents: { reason: Schema.String },
})
export type Message = typeof Message.Type

// INIT

export const init = (): Model => ({
  loop: Loop.Off(),
  transcript: Transcript.empty,
  log: [],
  input: '',
  crashes: 0,
})

// CONFIG

/** How people talk to the demo's agent: free text with suggestions, or one button. */
export const Composer = defineTaggedUnion({
  Chat: { suggestions: Schema.Array(Schema.String) },
  Action: { label: Schema.String, content: Schema.String },
})
export type Composer = typeof Composer.Type

export interface Config {
  /** Unique per demo: it names the resource and the Commands. */
  readonly id: string
  readonly acquire: Effect.Effect<Lab, never, Scope.Scope>
  readonly composer: Composer
  readonly crashable: boolean
}

const epochOf = (loop: Loop): number =>
  Loop.match(loop, {
    Off: () => 0,
    Starting: ({ epoch }) => epoch,
    Running: ({ epoch }) => epoch,
    Crashed: ({ epoch }) => epoch,
    Failed: ({ epoch }) => epoch,
  })

const isRunning = (loop: Loop): boolean => loop._tag === 'Running'

export const make = (config: Config) => {
  const LabResource = ManagedResource.tag<Lab>()(`${config.id}/Lab`)
  type LabService = ManagedResource.ServiceOf<typeof LabResource>

  const failure = (error: { readonly message: string }) => error.message

  // COMMAND

  const StartLoop = Command.define(`${config.id}/StartLoop`, {
    messages: [Message.CompletedStartLoop, Message.FailedStartLoop],
    execute: LabResource.get.pipe(
      Effect.flatMap((lab) => lab.start),
      Effect.as(Message.CompletedStartLoop()),
      Effect.catch((error) => Effect.succeed(Message.FailedStartLoop({ reason: failure(error) }))),
    ),
  })

  const CrashLoop = Command.define(`${config.id}/CrashLoop`, {
    messages: [Message.CompletedCrashLoop],
    execute: LabResource.get.pipe(
      Effect.flatMap((lab) => lab.crash),
      Effect.catchTag('ResourceNotAvailable', () => Effect.void),
      Effect.as(Message.CompletedCrashLoop()),
    ),
  })

  const Submit = Command.define(`${config.id}/Submit`, {
    args: { content: Schema.String },
    messages: [Message.CompletedSubmit, Message.FailedSubmit],
    execute: ({ content }) =>
      LabResource.get.pipe(
        Effect.flatMap((lab) => lab.submit(content)),
        Effect.as(Message.CompletedSubmit()),
        Effect.catch((error) => Effect.succeed(Message.FailedSubmit({ reason: failure(error) }))),
      ),
  })

  // UPDATE

  type UpdateReturn = Update.Return<Model, Message, LabService>

  const submit = (model: Model, content: string): UpdateReturn =>
    isRunning(model.loop) && String.isNonEmpty(String.trim(content))
      ? {
          model: modifyFields(model, { input: () => '' }),
          commands: [Submit({ content: String.trim(content) })],
        }
      : { model }

  const update = (model: Model, message: Message) =>
    Message.match<UpdateReturn>(message, {
      AcquiredLab: () => ({
        model: modifyFields(model, {
          loop: () => Loop.Starting({ epoch: 1 }),
          transcript: () => Transcript.empty,
          log: () => Transcript.note([], 'lab', 'storage ready, starting the loop'),
          crashes: () => 0,
        }),
        commands: [StartLoop()],
      }),

      ReleasedLab: () => ({ model: modifyFields(model, { loop: () => Loop.Off() }) }),

      FailedAcquireLab: ({ reason }) => ({
        model: modifyFields(model, { loop: () => Loop.Failed({ epoch: 0, reason }) }),
      }),

      ClickedStart: () =>
        Match.value(model.loop).pipe(
          Match.withReturnType<UpdateReturn>(),
          Match.tag('Crashed', 'Failed', ({ epoch }) => ({
            model: modifyFields(model, {
              loop: () => Loop.Starting({ epoch: Number.increment(epoch) }),
              log: (log) => Transcript.note(log, 'restart', 'reopening the same storage'),
            }),
            commands: [StartLoop()],
          })),
          Match.orElse(() => ({ model })),
        ),

      ClickedCrash: () =>
        Match.value(model.loop).pipe(
          Match.withReturnType<UpdateReturn>(),
          Match.tag('Running', ({ epoch }) => ({
            model: modifyFields(model, {
              loop: () => Loop.Crashed({ epoch }),
              transcript: Transcript.settle,
              log: (log) => Transcript.note(log, 'crash', 'the loop is gone mid-flight'),
              crashes: Number.increment,
            }),
            commands: [CrashLoop()],
          })),
          Match.orElse(() => ({ model })),
        ),

      CompletedStartLoop: () => ({
        model: modifyFields(model, {
          loop: () => Loop.Running({ epoch: epochOf(model.loop) }),
        }),
      }),

      FailedStartLoop: ({ reason }) => ({
        model: modifyFields(model, {
          loop: () => Loop.Failed({ epoch: epochOf(model.loop), reason }),
        }),
      }),

      CompletedCrashLoop: () => ({ model }),

      UpdatedInput: ({ value }) => ({ model: modifyFields(model, { input: () => value }) }),

      SubmittedInput: () => submit(model, model.input),

      ClickedPrompt: ({ content }) => submit(model, content),

      CompletedSubmit: () => ({ model }),

      FailedSubmit: ({ reason }) => ({
        model: modifyFields(model, { log: (log) => Transcript.note(log, 'error', reason) }),
      }),

      ReceivedEvents: ({ events }) =>
        isRunning(model.loop)
          ? {
              model: modifyFields(model, {
                transcript: (transcript) => Array.reduce(events, transcript, Transcript.apply),
                log: (log) => Transcript.log(log, events),
              }),
            }
          : { model },

      LostEvents: ({ reason }) => ({
        model: modifyFields(model, { log: (log) => Transcript.note(log, 'error', reason) }),
      }),
    })

  // MANAGED RESOURCE

  const managedResources = ManagedResource.make<Model, Message>()((entry) => ({
    lab: entry(Schema.Option(Schema.Null), {
      resource: LabResource,
      modelToMaybeRequirements: () => Option.some(null),
      acquire: () => config.acquire,
      release: () => Effect.void,
      onAcquired: () => Message.AcquiredLab(),
      onReleased: () => Message.ReleasedLab(),
      onAcquireError: () => Message.FailedAcquireLab({ reason: 'The lab could not start.' }),
    }),
  }))

  // SUBSCRIPTION

  const subscriptions = Subscription.make<Model, Message, LabService>()((entry) => ({
    events: entry(
      { epoch: Schema.Finite, isRunning: Schema.Boolean },
      {
        modelToDependencies: (model) => ({
          epoch: epochOf(model.loop),
          isRunning: isRunning(model.loop),
        }),
        dependenciesToStream: ({ isRunning }) =>
          Stream.when(
            Stream.unwrap(
              LabResource.get.pipe(
                Effect.map((lab) =>
                  lab.events.pipe(
                    Stream.map((events) => Message.ReceivedEvents({ events })),
                    Stream.catch((error) =>
                      Stream.make(Message.LostEvents({ reason: failure(error) })),
                    ),
                  ),
                ),
                Effect.catchTag('ResourceNotAvailable', () => Effect.succeed(Stream.empty)),
              ),
            ),
            Effect.sync(() => isRunning),
          ),
      },
    ),
  }))

  // VIEW

  const view = Submodel.defineView<Model, Message>((model, h) => demoView(config, model, h))

  return { resource: LabResource, update, managedResources, subscriptions, view }
}

// VIEW

const statusView = (loop: Loop, h: HtmlBuilder<Message>): Html => {
  const status = Loop.match(loop, {
    Off: () => ({ dot: 'bg-zinc-500', text: 'Waiting for the page' }),
    Starting: () => ({ dot: 'bg-zinc-500 animate-pulse', text: 'Opening storage…' }),
    Running: ({ epoch }) => ({
      dot: 'bg-zinc-100',
      text: epoch === 1 ? 'Loop running' : `Loop running · life #${epoch}`,
    }),
    Crashed: () => ({ dot: 'ring-1 ring-zinc-500', text: 'Loop crashed · storage intact' }),
    Failed: ({ reason }) => ({ dot: 'ring-1 ring-zinc-500', text: `Failed: ${reason}` }),
  })
  return h.div(
    [h.Class('flex items-center gap-2 text-sm text-zinc-300')],
    [h.span([h.Class(`size-2.5 rounded-full ${status.dot}`)]), h.span([], [status.text])],
  )
}

const controlsView = (config: Config, model: Model, h: HtmlBuilder<Message>): Html =>
  config.crashable
    ? Loop.match(model.loop, {
        Off: () => h.empty,
        Starting: () => h.empty,
        Running: () =>
          h.button(
            [
              h.Type('button'),
              h.OnClick(Message.ClickedCrash()),
              h.Class(
                'rounded-lg bg-white/5 px-3 py-1.5 text-sm font-semibold text-zinc-300 ring-1 ring-white/15 transition hover:bg-white/10',
              ),
            ],
            ['Crash the loop'],
          ),
        Crashed: () => restartButton(h),
        Failed: () => restartButton(h),
      })
    : h.empty

const restartButton = (h: HtmlBuilder<Message>): Html =>
  h.button(
    [
      h.Type('button'),
      h.OnClick(Message.ClickedStart()),
      h.Class(
        'rounded-lg bg-white/5 px-3 py-1.5 text-sm font-semibold text-zinc-300 ring-1 ring-white/15 transition hover:bg-white/10',
      ),
    ],
    ['Restart the loop'],
  )

const blockView = (block: Transcript.Block, h: HtmlBuilder<Message>): Html =>
  Transcript.Block.match(block, {
    Text: ({ text }) => h.p([h.Class('whitespace-pre-wrap leading-relaxed')], [text]),
    Thinking: ({ text }) => h.p([h.Class('text-sm italic leading-relaxed text-zinc-400')], [text]),
    ToolCall: ({ name, args }) =>
      h.p(
        [
          h.Class(
            'w-fit rounded-md bg-white/5 px-2 py-1 font-mono text-xs text-zinc-300 ring-1 ring-white/15',
          ),
        ],
        [`${name}(${args})`],
      ),
  })

const assistantView = (
  blocks: ReadonlyArray<Transcript.Block>,
  isDraft: boolean,
  h: HtmlBuilder<Message>,
): Html =>
  h.div(
    [h.Class('flex flex-col gap-2 self-start max-w-[90%]')],
    [
      h.span(
        [h.Class('text-xs font-semibold uppercase tracking-wider text-zinc-300')],
        [isDraft ? 'assistant · streaming' : 'assistant'],
      ),
      ...Array.map(blocks, (block) => blockView(block, h)),
      isDraft ? h.span([h.Class('h-4 w-2 animate-pulse bg-zinc-300')]) : h.empty,
    ],
  )

const bubbleView = (bubble: Transcript.Bubble, h: HtmlBuilder<Message>): Html =>
  Transcript.Bubble.match(bubble, {
    User: ({ id, text }) =>
      h.keyed('div')(
        `${id}`,
        [
          h.Class(
            'self-end max-w-[85%] rounded-xl rounded-br-sm bg-white/5 px-4 py-2 text-zinc-200 ring-1 ring-white/15',
          ),
        ],
        [text],
      ),
    Assistant: ({ id, blocks }) =>
      h.keyed('div')(`${id}`, [h.Class('flex')], [assistantView(blocks, false, h)]),
    ToolResult: ({ id, toolName, text, isError }) =>
      h.keyed('div')(
        `${id}`,
        [
          h.Class(
            `self-start max-w-[90%] rounded-lg px-3 py-2 font-mono text-xs ring-1 ${
              isError
                ? 'bg-white/5 text-zinc-200 ring-white/15'
                : 'bg-white/5 text-zinc-200 ring-white/15'
            }`,
          ),
        ],
        [`${toolName}${isError ? ' failed' : ''}: ${text}`],
      ),
    System: ({ id, tools }) =>
      h.keyed('div')(
        `${id}`,
        [h.Class('self-center rounded-full bg-zinc-800 px-3 py-1 text-xs text-zinc-400')],
        [
          Array.match(tools, {
            onEmpty: () => 'system prompt',
            onNonEmpty: (names) => `system prompt · tools: ${Array.join(names, ', ')}`,
          }),
        ],
      ),
    Note: ({ id, kind }) =>
      h.keyed('div')(`${id}`, [h.Class('self-center font-mono text-xs text-zinc-500')], [kind]),
  })

const runningToolsView = (
  tools: ReadonlyArray<Transcript.RunningTool>,
  h: HtmlBuilder<Message>,
): Html =>
  h.div(
    [h.Class('flex flex-wrap gap-2')],
    Array.map(tools, ({ callId, name }) =>
      h.keyed('span')(
        callId,
        [
          h.Class(
            'flex items-center gap-2 rounded-md bg-white/5 px-2 py-1 font-mono text-xs text-zinc-200 ring-1 ring-white/15',
          ),
        ],
        [h.span([h.Class('size-2 animate-ping rounded-full bg-zinc-300')]), `running ${name}…`],
      ),
    ),
  )

const transcriptView = (transcript: Transcript.Transcript, h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class('flex h-80 flex-col-reverse overflow-y-auto p-4 sm:h-96')],
    [
      h.div(
        [h.Class('flex flex-col gap-3')],
        [
          ...Array.map(
            Array.filter(transcript.bubbles, (bubble) => bubble._tag !== 'Note'),
            (bubble) => bubbleView(bubble, h),
          ),
          Option.match(transcript.maybeDraft, {
            onNone: () => h.empty,
            onSome: (blocks) => assistantView(blocks, true, h),
          }),
          runningToolsView(transcript.runningTools, h),
          Array.isReadonlyArrayEmpty(transcript.bubbles)
            ? h.p(
                [h.Class('m-auto py-16 text-center text-sm text-zinc-500')],
                ['Nothing here yet. Say something to the agent.'],
              )
            : h.empty,
        ],
      ),
    ],
  )

/** The demo's own lines stand out from the loop's events. */
const DEMO_LOG_TYPES: ReadonlyArray<string> = ['crash', 'error', 'restart', 'lab']

const logTypeClass = (type: string): string =>
  Array.contains(DEMO_LOG_TYPES, type) ? 'text-zinc-100' : 'text-zinc-400'

const logView = (log: ReadonlyArray<Transcript.LogLine>, h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class('flex h-80 flex-col-reverse overflow-y-auto p-3 font-mono text-[11px] sm:h-96')],
    [
      h.ol(
        [h.Class('flex flex-col gap-0.5')],
        Array.map(log, ({ seq, type, detail }) =>
          h.keyed('li')(
            `${seq}`,
            [h.Class('flex gap-2 leading-5')],
            [
              h.span([h.Class('w-7 shrink-0 text-right text-zinc-600')], [`${seq}`]),
              h.span([h.Class(`shrink-0 ${logTypeClass(type)}`)], [type]),
              h.span([h.Class('truncate text-zinc-500')], [detail]),
            ],
          ),
        ),
      ),
    ],
  )

const composerView = (config: Config, model: Model, h: HtmlBuilder<Message>): Html => {
  const isDisabled = !isRunning(model.loop)
  return Composer.match(config.composer, {
    Chat: ({ suggestions }) =>
      h.div(
        [h.Class('flex flex-col gap-3 border-t border-white/10 p-3')],
        [
          h.div(
            [h.Class('flex flex-wrap gap-2')],
            Array.map(suggestions, (suggestion) =>
              h.button(
                [
                  h.Type('button'),
                  h.Disabled(isDisabled),
                  h.OnClick(Message.ClickedPrompt({ content: suggestion })),
                  h.Class(
                    'rounded-full bg-white/5 px-3 py-1 text-xs text-zinc-300 ring-1 ring-white/10 transition hover:bg-white/10 disabled:opacity-40',
                  ),
                ],
                [suggestion],
              ),
            ),
          ),
          h.form(
            [h.OnSubmit(Message.SubmittedInput()), h.Class('flex gap-2')],
            [
              h.input([
                h.Value(model.input),
                h.OnInput((value) => Message.UpdatedInput({ value })),
                h.Placeholder('Ask the agent to shout, reverse or count something…'),
                h.AriaLabel('Message'),
                h.Disabled(isDisabled),
                h.Class(
                  'min-w-0 flex-1 rounded-lg bg-black/40 px-3 py-2 text-sm text-zinc-100 ring-1 ring-white/10 outline-none placeholder:text-zinc-500 focus:ring-white/15',
                ),
              ]),
              h.button(
                [
                  h.Type('submit'),
                  h.Disabled(isDisabled),
                  h.Class(
                    'rounded-lg bg-zinc-100 px-4 py-2 text-sm font-semibold text-ink-950 transition hover:bg-white disabled:opacity-40',
                  ),
                ],
                ['Send'],
              ),
            ],
          ),
        ],
      ),
    Action: ({ label, content }) =>
      h.div(
        [h.Class('flex items-center gap-3 border-t border-white/10 p-3')],
        [
          h.button(
            [
              h.Type('button'),
              h.Disabled(isDisabled),
              h.OnClick(Message.ClickedPrompt({ content })),
              h.Class(
                'rounded-lg bg-zinc-100 px-4 py-2 text-sm font-semibold text-ink-950 transition hover:bg-white disabled:opacity-40',
              ),
            ],
            [label],
          ),
          h.span(
            [h.Class('text-xs text-zinc-500')],
            [
              model.crashes === 0
                ? 'Then crash the loop while a step is running.'
                : `Crashed ${model.crashes} ${model.crashes === 1 ? 'time' : 'times'}. Nothing committed was lost.`,
            ],
          ),
        ],
      ),
  })
}

const demoView = (config: Config, model: Model, h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class('overflow-hidden rounded-xl bg-ink-900 ring-1 ring-white/10')],
    [
      h.div(
        [h.Class('flex items-center justify-between gap-3 border-b border-white/10 px-4 py-3')],
        [statusView(model.loop, h), controlsView(config, model, h)],
      ),
      h.div(
        [h.Class('grid md:grid-cols-[1fr_minmax(0,18rem)]')],
        [
          h.div(
            [h.Class('flex min-w-0 flex-col')],
            [transcriptView(model.transcript, h), composerView(config, model, h)],
          ),
          h.div(
            [h.Class('min-w-0 border-t border-white/10 bg-black/30 md:border-t-0 md:border-l')],
            [
              h.div(
                [
                  h.Class(
                    'border-b border-white/10 px-3 py-2 text-xs font-semibold uppercase tracking-wider text-zinc-500',
                  ),
                ],
                ['Agent events'],
              ),
              logView(model.log, h),
            ],
          ),
        ],
      ),
    ],
  )
