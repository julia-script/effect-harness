import { Array } from 'effect'
import type { Html, HtmlBuilder } from 'foldkit/html'
import wordsmithSource from '../demo/agents/wordsmith.ts?highlight'
import * as Demos from '../demos.ts'
import * as Diagram from '../diagram.ts'
import { Message } from '../message.ts'
import type { Model } from '../model.ts'
import { guideRouter, playgroundRouter } from '../route.ts'
import { container, copyCommand, eyebrow, sectionTitle } from './shared.ts'

const INSTALL = 'bun add effect-harness effect'

const features: ReadonlyArray<readonly [icon: string, title: string, text: string]> = [
  [
    '💾',
    'Durable by default',
    'Runs are durable tasks with checkpoints. Restart the process and unfinished generations, tool calls and compactions resume from storage.',
  ],
  [
    '🧩',
    'Schemas all the way down',
    'Tools are Effect Schemas and implementations are Layers. A missing handler is a type error, not a runtime surprise.',
  ],
  [
    '🔌',
    'Client and loop, split',
    'HarnessClient speaks typed RPC to HarnessServer: in one process, over a WebSocket, or inside a Cloudflare Durable Object.',
  ],
  [
    '🤖',
    'Real providers, real accounts',
    'OpenAI, Anthropic, Codex through a ChatGPT sign-in, Claude subscriptions, and any OpenAI-compatible server.',
  ],
  [
    '🧠',
    'A loop that manages itself',
    'Steering and follow-up queues, retries with backoff, compaction before the window fills, deferred tools and usage ledgers.',
  ],
  [
    '🛠️',
    'Batteries included',
    'read, write, edit and bash coding tools, a durable code mode over QuickJS, and a scripted Faux model for tests.',
  ],
]

const ARCHITECTURE = `flowchart LR
  subgraph app["Your app (anywhere)"]
    C["HarnessClient"]
  end
  subgraph loop["The loop (HarnessServer)"]
    H["Harness<br/>conversations · submissions · runs"]
    S["Scheduler<br/>durable tasks"]
    X["Extensions<br/>tools · sections · hooks"]
  end
  C <-- "RPC: memory · WebSocket · Durable Object" --> H
  H --> S
  S -- "$generation" --> M(["Model<br/>OpenAI · Anthropic · Codex · Faux"])
  S -- "$tool" --> T["Tool handlers<br/>(your Layers)"]
  X -. selected per conversation .-> S
  H --> ST[("Storage<br/>memory · JSONL · SQL")]`

const button = (href: string, label: string, isPrimary: boolean, h: HtmlBuilder<Message>): Html =>
  h.a(
    [
      h.Href(href),
      h.Class(
        isPrimary
          ? 'rounded-xl bg-linear-to-r from-sky-accent via-violet-accent to-pink-accent px-5 py-3 text-sm font-semibold text-ink-950 shadow-lg shadow-violet-accent/25 transition hover:brightness-110'
          : 'rounded-xl bg-white/5 px-5 py-3 text-sm font-semibold text-white ring-1 ring-white/10 transition hover:bg-white/10',
      ),
    ],
    [label],
  )

const hero = (model: Model, h: HtmlBuilder<Message>): Html =>
  h.section(
    [h.Class('relative overflow-hidden')],
    [
      h.div([
        h.Class(
          'bg-grid absolute inset-0 [mask-image:radial-gradient(ellipse_at_top,black,transparent_70%)]',
        ),
      ]),
      h.div([
        h.Class(
          'absolute -top-40 left-1/2 size-[42rem] -translate-x-1/2 rounded-full bg-violet-accent/20 blur-3xl',
        ),
      ]),
      h.div(
        [h.Class(`${container} relative flex flex-col items-center py-20 text-center sm:py-28`)],
        [
          h.p(
            [
              h.Class(
                'mb-6 rounded-full bg-white/5 px-4 py-1.5 text-xs font-medium text-zinc-300 ring-1 ring-white/10',
              ),
            ],
            ['Built on Effect 4 · runs on Bun, Node and Cloudflare Workers'],
          ),
          h.h1(
            [h.Class('max-w-4xl text-5xl font-extrabold tracking-tight text-white sm:text-7xl')],
            ['An agent loop you can ', h.span([h.Class('text-gradient')], ['kill at any moment.'])],
          ),
          h.p(
            [h.Class('mt-6 max-w-2xl text-lg leading-8 text-zinc-400')],
            [
              'Every conversation, tool call and model response is committed to storage as it happens. A crash, a deploy or an eviction picks up exactly where it stopped.',
            ],
          ),
          h.div(
            [h.Class('mt-10 flex flex-wrap justify-center gap-3')],
            [
              button(guideRouter({ slug: 'getting-started' }), 'Get started', true, h),
              button(playgroundRouter(), 'Open the playground', false, h),
            ],
          ),
          h.div(
            [h.Class('mt-8 flex w-full justify-center')],
            [copyCommand(INSTALL, model.copied, h)],
          ),
        ],
      ),
    ],
  )

const step = (index: number, title: string, text: string, h: HtmlBuilder<Message>): Html =>
  h.li(
    [h.Class('flex gap-4')],
    [
      h.span(
        [
          h.Class(
            'flex size-8 shrink-0 items-center justify-center rounded-full bg-violet-accent/15 font-mono text-sm font-semibold text-violet-200 ring-1 ring-violet-accent/40',
          ),
        ],
        [`${index}`],
      ),
      h.div(
        [],
        [
          h.p([h.Class('font-semibold text-white')], [title]),
          h.p([h.Class('mt-1 text-sm leading-6 text-zinc-400')], [text]),
        ],
      ),
    ],
  )

const crashDemo = (model: Model, h: HtmlBuilder<Message>): Html =>
  h.section(
    [h.Class(`${container} py-16`)],
    [
      h.div(
        [h.Class('grid items-start gap-10 lg:grid-cols-[minmax(0,22rem)_1fr]')],
        [
          h.div(
            [h.Class('lg:sticky lg:top-24')],
            [
              eyebrow('Live, in this tab', h),
              sectionTitle('Go on, crash it.', h),
              h.p(
                [h.Class('mt-4 leading-7 text-zinc-400')],
                [
                  'This is the real effect-harness loop, running in your browser over in-memory storage, with a scripted model. The pipeline has three slow steps.',
                ],
              ),
              h.ol(
                [h.Class('mt-8 flex flex-col gap-6')],
                [
                  step(
                    1,
                    'Ship it',
                    'The agent builds, tests and deploys, one tool call at a time.',
                    h,
                  ),
                  step(
                    2,
                    'Crash the loop mid-step',
                    'Its scope closes with work in flight, just like a killed process.',
                    h,
                  ),
                  step(
                    3,
                    'Restart it',
                    'A new loop opens the same storage. build and test are replay-safe, so they run again; deploy is not, so the model checks what landed before deploying twice.',
                    h,
                  ),
                ],
              ),
            ],
          ),
          h.submodel({
            slotId: 'release-demo',
            model: model.release,
            view: Demos.release.view,
            toParentMessage: (message) => Message.GotReleaseMessage({ message }),
          }),
        ],
      ),
    ],
  )

const featureGrid = (h: HtmlBuilder<Message>): Html =>
  h.section(
    [h.Class(`${container} py-16`)],
    [
      eyebrow('Why effect-harness', h),
      sectionTitle('Everything an agent needs to survive production.', h),
      h.div(
        [h.Class('mt-10 grid gap-4 sm:grid-cols-2 lg:grid-cols-3')],
        Array.map(features, ([icon, title, text]) =>
          h.div(
            [
              h.Class(
                'rounded-2xl bg-white/[0.03] p-6 ring-1 ring-white/10 transition hover:bg-white/[0.05] hover:ring-violet-accent/30',
              ),
            ],
            [
              h.p([h.Class('text-2xl')], [icon]),
              h.h3([h.Class('mt-4 font-semibold text-white')], [title]),
              h.p([h.Class('mt-2 text-sm leading-6 text-zinc-400')], [text]),
            ],
          ),
        ),
      ),
    ],
  )

const firstAgent = (h: HtmlBuilder<Message>): Html =>
  h.section(
    [h.Class(`${container} py-16`)],
    [
      h.div(
        [h.Class('grid gap-10 lg:grid-cols-[minmax(0,22rem)_1fr]')],
        [
          h.div(
            [],
            [
              eyebrow('Five moving parts', h),
              sectionTitle('Tools are Schemas. Handlers are Layers.', h),
              h.p(
                [h.Class('mt-4 leading-7 text-zinc-400')],
                [
                  'Declare a tool with Schemas, implement it as a Layer, bundle it in an extension, and hand the loop a model and a store. The client talks to the loop over typed RPC.',
                ],
              ),
              h.p(
                [h.Class('mt-4 leading-7 text-zinc-400')],
                [
                  'This file is the agent behind the playground’s chat demo, exactly as it runs there.',
                ],
              ),
              h.div(
                [h.Class('mt-8 flex flex-wrap gap-3')],
                [
                  button(playgroundRouter(), 'Talk to it', true, h),
                  button(guideRouter({ slug: 'tools' }), 'Read about tools', false, h),
                ],
              ),
            ],
          ),
          h.div(
            [
              h.Class(
                'highlighted overflow-hidden rounded-2xl bg-ink-900/60 font-mono ring-1 ring-white/10',
              ),
            ],
            [
              h.div(
                [
                  h.Class(
                    'flex items-center gap-2 border-b border-white/10 px-4 py-3 font-sans text-xs text-zinc-500',
                  ),
                ],
                [
                  h.span([h.Class('size-3 rounded-full bg-rose-400/70')]),
                  h.span([h.Class('size-3 rounded-full bg-amber-300/70')]),
                  h.span([h.Class('size-3 rounded-full bg-emerald-400/70')]),
                  h.span([h.Class('ml-2 font-mono')], ['site/src/demo/agents/wordsmith.ts']),
                ],
              ),
              h.div([h.Class('max-h-[34rem] overflow-auto'), h.InnerHTML(wordsmithSource.html)]),
            ],
          ),
        ],
      ),
    ],
  )

const architecture = (h: HtmlBuilder<Message>): Html =>
  h.section(
    [h.Class(`${container} py-16`)],
    [
      eyebrow('How it fits together', h),
      sectionTitle('Your app holds a client. The loop runs anywhere.', h),
      Diagram.view('diagram-home-architecture', ARCHITECTURE, h),
      h.div(
        [h.Class('mt-12 flex flex-col items-center gap-4 text-center')],
        [
          h.p([h.Class('text-xl font-semibold text-white')], ['Ready to build one?']),
          button(
            guideRouter({ slug: 'getting-started' }),
            'Read the guide, it takes ten minutes',
            true,
            h,
          ),
        ],
      ),
    ],
  )

export const view = (model: Model, h: HtmlBuilder<Message>): Html =>
  h.div([], [hero(model, h), crashDemo(model, h), featureGrid(h), firstAgent(h), architecture(h)])
