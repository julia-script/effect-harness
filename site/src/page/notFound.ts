import type { Html, HtmlBuilder } from 'foldkit/html'
import type { Message } from '../message.ts'
import { guidesRouter, homeRouter } from '../route.ts'
import { container } from './shared.ts'

export const view = (path: string, h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class(`${container} flex flex-col items-center py-32 text-center`)],
    [
      h.p([h.Class('font-mono text-sm text-violet-accent')], ['404']),
      h.h1([h.Class('mt-4 text-4xl font-extrabold text-white')], ['This page crashed for good.']),
      h.p(
        [h.Class('mt-4 text-zinc-400')],
        [`Nothing lives at ${path}. Unlike the loop, it will not resume.`],
      ),
      h.div(
        [h.Class('mt-8 flex gap-3')],
        [
          h.a(
            [
              h.Href(homeRouter()),
              h.Class(
                'rounded-xl bg-white/5 px-5 py-3 text-sm font-semibold text-white ring-1 ring-white/10 hover:bg-white/10',
              ),
            ],
            ['Home'],
          ),
          h.a(
            [
              h.Href(guidesRouter()),
              h.Class(
                'rounded-xl bg-white/5 px-5 py-3 text-sm font-semibold text-white ring-1 ring-white/10 hover:bg-white/10',
              ),
            ],
            ['Guides'],
          ),
        ],
      ),
    ],
  )
