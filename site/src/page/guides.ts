import { Array } from 'effect'
import type { Html, HtmlBuilder } from 'foldkit/html'
import { guides } from '../guides.ts'
import type { Message } from '../message.ts'
import { guideRouter, playgroundRouter } from '../route.ts'
import { container, eyebrow } from './shared.ts'

export const view = (h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class(`${container} py-16`)],
    [
      eyebrow('Documentation', h),
      h.h1([h.Class('text-4xl font-extrabold tracking-tight text-white sm:text-5xl')], ['Guides']),
      h.p(
        [h.Class('mt-4 max-w-2xl leading-7 text-zinc-400')],
        [
          'New here? Start with Getting started, then read Concepts. The rest can be read in any order, or ',
          h.a(
            [h.Href(playgroundRouter()), h.Class('font-medium text-sky-accent hover:underline')],
            ['try the playground'],
          ),
          ' first.',
        ],
      ),
      h.div(
        [h.Class('mt-10 grid gap-4 sm:grid-cols-2 lg:grid-cols-3')],
        Array.map(guides, (guide) =>
          h.a(
            [
              h.Href(guideRouter({ slug: guide.slug })),
              h.Class(
                'group rounded-2xl bg-white/[0.03] p-6 ring-1 ring-white/10 transition hover:-translate-y-0.5 hover:bg-white/[0.05] hover:ring-violet-accent/40',
              ),
            ],
            [
              h.p([h.Class('text-2xl')], [guide.icon]),
              h.h2(
                [h.Class('mt-4 font-semibold text-white group-hover:text-gradient')],
                [guide.title],
              ),
              h.p([h.Class('mt-2 text-sm leading-6 text-zinc-400')], [guide.blurb]),
            ],
          ),
        ),
      ),
    ],
  )
