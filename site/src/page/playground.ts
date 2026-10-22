import type { Html, HtmlBuilder } from 'foldkit/html'
import releaseSource from '../demo/agents/release.ts?highlight'
import wordsmithSource from '../demo/agents/wordsmith.ts?highlight'
import * as Demos from '../demos.ts'
import { Message } from '../message.ts'
import type { Model } from '../model.ts'
import { guideRouter } from '../route.ts'
import { container, eyebrow, sectionTitle, sourcePanel } from './shared.ts'

const prose = (text: ReadonlyArray<string | Html>, h: HtmlBuilder<Message>): Html =>
  h.p([h.Class('mt-4 max-w-3xl leading-7 text-zinc-400')], text)

const link = (href: string, label: string, h: HtmlBuilder<Message>): Html =>
  h.a([h.Href(href), h.Class('font-medium text-zinc-100 hover:underline')], [label])

const legendItem = (type: string, text: string, h: HtmlBuilder<Message>) =>
  h.li(
    [h.Class('flex gap-3 text-sm leading-6')],
    [
      h.code([h.Class('w-44 shrink-0 font-mono text-xs leading-6 text-zinc-300')], [type]),
      h.span([h.Class('text-zinc-400')], [text]),
    ],
  )

export const view = (model: Model, h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class(`${container} py-16`)],
    [
      eyebrow('Playground', h),
      h.h1(
        [h.Class('text-4xl font-extrabold tracking-tight text-white sm:text-5xl')],
        ['The real loop, in your browser.'],
      ),
      prose(
        [
          'Both demos below run effect-harness itself in this tab: the same Session, Scheduler, generation and tool tasks a server runs, over in-memory storage and the in-memory transport. Only the model is scripted, so nothing leaves your machine.',
        ],
        h,
      ),

      h.section(
        [h.Class('mt-16')],
        [
          sectionTitle('Your first agent', h),
          prose(
            [
              'Three tools and a model that picks them by keyword. Ask for more than one and the model calls them in a single round. Watch the event log: each commit delivers a batch, and the streamed response arrives as ',
              h.code([h.Class('font-mono text-zinc-200')], ['message_update']),
              ' deltas before its ',
              h.code([h.Class('font-mono text-zinc-200')], ['message_end']),
              '.',
            ],
            h,
          ),
          h.div(
            [h.Class('mt-8')],
            [
              h.submodel({
                slotId: 'wordsmith-demo',
                model: model.wordsmith,
                view: Demos.wordsmith.view,
                toParentMessage: (message) => Message.GotWordsmithMessage({ message }),
              }),
            ],
          ),
          sourcePanel(
            'Show the code running this agent',
            'site/src/demo/agents/wordsmith.ts',
            wordsmithSource,
            h,
          ),
        ],
      ),

      h.section(
        [h.Class('mt-20')],
        [
          sectionTitle('Kill the loop', h),
          prose(
            [
              'Ship a release, then crash the loop while a step runs. Restarting opens the same storage: the scheduler resumes every unfinished task from its last checkpoint. Tools declared ',
              h.code([h.Class('font-mono text-zinc-200')], ["replay: 'safe'"]),
              ' run again; the unsafe deploy answers ',
              h.code([h.Class('font-mono text-zinc-200')], ['interrupted']),
              ' and the model checks before deploying twice. See ',
              link(guideRouter({ slug: 'tools' }) + '#replay-safety', 'replay safety', h),
              ' and ',
              link(guideRouter({ slug: 'persistence' }), 'persistence', h),
              '.',
            ],
            h,
          ),
          h.div(
            [h.Class('mt-8')],
            [
              h.submodel({
                slotId: 'release-demo',
                model: model.release,
                view: Demos.release.view,
                toParentMessage: (message) => Message.GotReleaseMessage({ message }),
              }),
            ],
          ),
          sourcePanel(
            'Show the code running this agent',
            'site/src/demo/agents/release.ts',
            releaseSource,
            h,
          ),
        ],
      ),

      h.section(
        [h.Class('mt-20 rounded-xl bg-white/[0.03] p-6 ring-1 ring-white/10 sm:p-8')],
        [
          h.h2([h.Class('text-xl font-bold text-white')], ['Reading the event log']),
          prose(
            [
              'The right-hand column is ',
              h.code([h.Class('font-mono text-zinc-200')], ['conversation.events']),
              ', the same stream a UI subscribes to. It starts with a snapshot and then delivers one batch per commit. ',
              link(
                guideRouter({ slug: 'conversations' }) + '#watching-live',
                'Every event type',
                h,
              ),
              ' is in the guide.',
            ],
            h,
          ),
          h.ul(
            [h.Class('mt-6 flex flex-col gap-2')],
            [
              legendItem('snapshot', 'Where a watch starts: entries, the run, tools in flight.', h),
              legendItem('message_*', 'A message committed, or the response streaming in.', h),
              legendItem('tool_execution_*', 'A durable $tool task started or finished.', h),
              legendItem('crash · restart', 'Written by the demo, not the loop.', h),
            ],
          ),
        ],
      ),
    ],
  )
