import type { Html, HtmlBuilder } from 'foldkit/html'
import { REPOSITORY } from './markdown/views.ts'
import type { Message } from './message.ts'
import type { AppRoute } from './route.ts'
import { guidesRouter, homeRouter, playgroundRouter } from './route.ts'

const NPM = 'https://www.npmjs.com/package/effect-harness'

const navLink = (href: string, label: string, isActive: boolean, h: HtmlBuilder<Message>): Html =>
  h.a(
    [
      h.Href(href),
      h.Class(
        `rounded-lg px-3 py-1.5 text-sm font-medium transition ${
          isActive ? 'bg-white/10 text-white' : 'text-zinc-400 hover:text-white'
        }`,
      ),
    ],
    [label],
  )

export const logo = (h: HtmlBuilder<Message>): Html =>
  h.a(
    [
      h.Href(homeRouter()),
      h.Class('flex items-center gap-2.5 font-bold tracking-tight text-white'),
    ],
    [h.img([h.Src('/favicon.svg'), h.Alt(''), h.Class('size-7')]), h.span([], ['effect-harness'])],
  )

export const header = (route: AppRoute, h: HtmlBuilder<Message>): Html =>
  h.header(
    [h.Class('sticky top-0 z-30 border-b border-white/5 bg-ink-950/75 backdrop-blur-xl')],
    [
      h.div(
        [h.Class('mx-auto flex h-16 max-w-7xl items-center justify-between gap-4 px-4 sm:px-6')],
        [
          logo(h),
          h.nav(
            [h.Class('flex items-center gap-1'), h.AriaLabel('Main')],
            [
              navLink(guidesRouter(), 'Docs', route._tag === 'Guides' || route._tag === 'Guide', h),
              navLink(playgroundRouter(), 'Playground', route._tag === 'Playground', h),
              navLink(REPOSITORY, 'GitHub', false, h),
            ],
          ),
        ],
      ),
    ],
  )

export const footer = (h: HtmlBuilder<Message>): Html =>
  h.footer(
    [h.Class('border-t border-white/5')],
    [
      h.div(
        [
          h.Class(
            'mx-auto flex max-w-7xl flex-col items-start justify-between gap-4 px-4 py-10 text-sm text-zinc-500 sm:flex-row sm:items-center sm:px-6',
          ),
        ],
        [
          h.p(
            [],
            [
              'effect-harness ports ',
              h.a(
                [
                  h.Href('https://github.com/earendil-works/pi'),
                  h.Class('text-zinc-300 hover:text-white'),
                ],
                ['Pi'],
              ),
              ' to ',
              h.a(
                [h.Href('https://effect.website'), h.Class('text-zinc-300 hover:text-white')],
                ['Effect'],
              ),
              '. This site is built with ',
              h.a(
                [h.Href('https://foldkit.dev'), h.Class('text-zinc-300 hover:text-white')],
                ['Foldkit'],
              ),
              '.',
            ],
          ),
          h.div(
            [h.Class('flex gap-4')],
            [
              h.a([h.Href(REPOSITORY), h.Class('hover:text-white')], ['GitHub']),
              h.a([h.Href(NPM), h.Class('hover:text-white')], ['npm']),
              h.a(
                [h.Href(`${REPOSITORY}/blob/main/CHANGELOG.md`), h.Class('hover:text-white')],
                ['Changelog'],
              ),
            ],
          ),
        ],
      ),
    ],
  )
